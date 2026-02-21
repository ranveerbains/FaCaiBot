//! Authenticated User WebSocket — trade fills and order events.
//!
//! Implements the Polymarket User WS:
//! `wss://ws-subscriptions-clob.polymarket.com/ws/user`
//!
//! Skipped in simulation mode (no credentials required).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use crossbeam_channel::Sender;
use fastwebsockets::{Frame, OpCode};
use tracing::{debug, info, warn};

use crate::types::IngestorEvent;
use crate::types::market::{DataSource, TradeStatus};

use super::tls_helpers::tls_connect;
use super::{BACKOFF_INITIAL_MS, BACKOFF_MAX_MS, USER_WS_URL};

/// Connect to the authenticated User WS and stream trade / order events.
///
/// Skipped automatically in simulation mode (no credentials → no connection).
/// Runs forever with exponential-backoff reconnection.
pub(super) async fn run_user_ws(
    shutdown: Arc<AtomicBool>,
    sim_mode: bool,
    api_key: Option<String>,
    tx: Sender<IngestorEvent>,
) -> Result<()> {
    if sim_mode {
        info!("simulation mode — User WS skipped");
        // Park indefinitely so callers can `tokio::select!` on this without
        // immediate completion.
        std::future::pending::<()>().await;
        return Ok(());
    }

    let api_key = api_key
        .as_deref()
        .context("api_key required for User WS")?
        .to_string();

    let mut backoff_ms = BACKOFF_INITIAL_MS;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("User WS shutdown requested — exiting");
            return Ok(());
        }

        info!("connecting to Polymarket User WS");

        match user_ws_session(shutdown.clone(), &api_key, &tx).await {
            Ok(()) => {
                info!("Polymarket User WS closed cleanly — reconnecting");
            }
            Err(e) => {
                warn!(
                    error = %e,
                    backoff_ms,
                    "Polymarket User WS error — reconnecting"
                );
            }
        }

        let _ = tx.try_send(IngestorEvent::WsStatus {
            source: DataSource::PolymarketUser,
            connected: false,
        });

        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms * 2).min(BACKOFF_MAX_MS);
    }
}

/// Single User WS session: connect, authenticate, pump frames.
async fn user_ws_session(
    shutdown: Arc<AtomicBool>,
    api_key: &str,
    tx: &Sender<IngestorEvent>,
) -> Result<()> {
    let mut ws = tls_connect(USER_WS_URL).await?;

    let _ = tx.try_send(IngestorEvent::WsStatus {
        source: DataSource::PolymarketUser,
        connected: true,
    });
    info!("Polymarket User WS connected");

    // Authentication subscription message (Section 5.1 — User WS).
    // The Integration Agent will wire the full HMAC auth when the SDK
    // credential derivation (`create_or_derive_api_creds`) is available.
    // For now we use the api_key as a placeholder identity.
    let auth_msg = serde_json::json!({
        "type": "user",
        "apiKey": api_key,
    })
    .to_string();

    ws.write_frame(Frame::text(auth_msg.into_bytes().into()))
        .await
        .context("failed to send User WS auth subscription")?;
    debug!("sent User WS auth subscription");

    // Main frame loop.
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let frame = ws.read_frame().await.context("User WS read_frame error")?;

        match frame.opcode {
            OpCode::Text | OpCode::Binary => {
                let json = std::str::from_utf8(&frame.payload)
                    .context("User WS payload is not valid UTF-8")?;
                if let Err(e) = handle_user_message(json, tx) {
                    debug!(error = %e, "User WS message handling error (non-fatal)");
                }
            }
            OpCode::Ping => {
                ws.write_frame(Frame::pong(frame.payload))
                    .await
                    .context("failed to send pong")?;
            }
            OpCode::Close => {
                info!("Polymarket User WS server sent Close");
                return Ok(());
            }
            _ => {}
        }
    }

    Ok(())
}

// ─── User WS message parsing ──────────────────────────────────────────────────

/// Parse a raw JSON frame from the Polymarket User WS.
///
/// Events of interest:
/// - `"trade"` → `IngestorEvent::TradeStatusUpdate`
/// - `"order"` → log at info level (placements, cancellations)
pub(super) fn handle_user_message(json: &str, tx: &Sender<IngestorEvent>) -> Result<()> {
    let value: serde_json::Value =
        serde_json::from_str(json).context("User WS JSON parse error")?;

    let events = match &value {
        serde_json::Value::Array(arr) => arr.as_slice().to_vec(),
        _ => vec![value],
    };

    for event in &events {
        let event_type = event
            .get("event_type")
            .or_else(|| event.get("type"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        match event_type {
            "trade" => match parse_trade_event(event) {
                Ok(ev) => {
                    info!(
                        order_id = %match &ev {
                            IngestorEvent::TradeStatusUpdate { order_id, .. } => order_id.as_str(),
                            _ => "?",
                        },
                        "User WS: trade status update"
                    );
                    if tx.try_send(ev).is_err() {
                        warn!("channel full — TradeStatusUpdate dropped");
                    }
                }
                Err(e) => {
                    debug!(error = %e, "User WS trade event parse error");
                }
            },
            "order" => {
                let order_id = event
                    .get("id")
                    .or_else(|| event.get("order_id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                let status = event.get("status").and_then(|v| v.as_str()).unwrap_or("?");
                info!(order_id, status, "User WS: order event");
            }
            "" => {
                debug!("User WS: received frame with no event_type — likely ack");
            }
            other => {
                debug!(event_type = other, "User WS: unknown event type — ignoring");
            }
        }
    }

    Ok(())
}

/// Parse a `trade` event from the User WS into a `TradeStatusUpdate`.
///
/// Expected trade status strings (from CLOB): MATCHED, MINED, CONFIRMED,
/// RETRYING, FAILED.
pub(super) fn parse_trade_event(event: &serde_json::Value) -> Result<IngestorEvent> {
    let order_id = event
        .get("order_id")
        .or_else(|| event.get("id"))
        .and_then(|v| v.as_str())
        .context("trade event missing order_id")?
        .to_string();

    let status_str = event
        .get("status")
        .and_then(|v| v.as_str())
        .context("trade event missing status")?;

    let status = parse_trade_status(status_str)?;

    Ok(IngestorEvent::TradeStatusUpdate { order_id, status })
}

/// Map a CLOB status string to `TradeStatus`.
pub(super) fn parse_trade_status(s: &str) -> Result<TradeStatus> {
    match s.to_uppercase().as_str() {
        "MATCHED" => Ok(TradeStatus::Matched),
        "MINED" => Ok(TradeStatus::Mined),
        "CONFIRMED" => Ok(TradeStatus::Confirmed),
        "RETRYING" => Ok(TradeStatus::Retrying),
        "FAILED" => Ok(TradeStatus::Failed),
        other => Err(anyhow!("unknown trade status: '{}'", other)),
    }
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_trade_status_matched() {
        let status = parse_trade_status("MATCHED").expect("parse failed");
        assert_eq!(status, TradeStatus::Matched);
    }

    #[test]
    fn test_parse_trade_status_confirmed() {
        let status = parse_trade_status("CONFIRMED").expect("parse failed");
        assert_eq!(status, TradeStatus::Confirmed);
    }

    #[test]
    fn test_parse_trade_status_failed() {
        let status = parse_trade_status("FAILED").expect("parse failed");
        assert_eq!(status, TradeStatus::Failed);
    }

    #[test]
    fn test_parse_trade_status_retrying() {
        let status = parse_trade_status("RETRYING").expect("parse failed");
        assert_eq!(status, TradeStatus::Retrying);
    }

    #[test]
    fn test_parse_trade_status_case_insensitive() {
        let status = parse_trade_status("matched").expect("lowercase parse");
        assert_eq!(status, TradeStatus::Matched);
    }

    #[test]
    fn test_parse_trade_status_unknown_errors() {
        let result = parse_trade_status("PENDING_QUEUE");
        assert!(result.is_err(), "unknown status should return Err");
    }

    #[test]
    fn test_parse_trade_event() {
        let json = serde_json::json!({
            "event_type": "trade",
            "order_id": "ord_abc123",
            "status": "MATCHED"
        });

        match parse_trade_event(&json).expect("parse failed") {
            IngestorEvent::TradeStatusUpdate { order_id, status } => {
                assert_eq!(order_id, "ord_abc123");
                assert_eq!(status, TradeStatus::Matched);
            }
            other => panic!("wrong variant: {:?}", other),
        }
    }
}
