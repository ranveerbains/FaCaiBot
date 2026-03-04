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

use rust_decimal::Decimal;

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
    secret: Option<String>,
    passphrase: Option<String>,
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
    let secret = secret
        .as_deref()
        .context("secret required for User WS")?
        .to_string();
    let passphrase = passphrase
        .as_deref()
        .context("passphrase required for User WS")?
        .to_string();

    let mut backoff_ms = BACKOFF_INITIAL_MS;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("User WS shutdown requested — exiting");
            return Ok(());
        }

        info!("connecting to Polymarket User WS");

        match user_ws_session(shutdown.clone(), &api_key, &secret, &passphrase, &tx).await {
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

/// Keepalive interval — Polymarket requires text "PING" every 10 seconds.
const PING_INTERVAL: Duration = Duration::from_secs(10);

/// Single User WS session: connect, authenticate, pump frames.
async fn user_ws_session(
    shutdown: Arc<AtomicBool>,
    api_key: &str,
    secret: &str,
    passphrase: &str,
    tx: &Sender<IngestorEvent>,
) -> Result<()> {
    let mut ws = tls_connect(USER_WS_URL).await?;

    let _ = tx.try_send(IngestorEvent::WsStatus {
        source: DataSource::PolymarketUser,
        connected: true,
    });
    info!("Polymarket User WS connected");

    // Authentication subscription message.
    // Format matches SDK's `WithCredentials::as_authenticated()` (ws/traits.rs).
    let auth_msg = build_user_auth_msg(api_key, secret, passphrase);

    ws.write_frame(Frame::text(auth_msg.into_bytes().into()))
        .await
        .context("failed to send User WS auth subscription")?;
    debug!("sent User WS auth subscription");

    // Main frame loop with text-based PING keepalive.
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Wait for next frame OR send PING on timeout.
        let frame = match tokio::time::timeout(PING_INTERVAL, ws.read_frame()).await {
            Ok(result) => result.context("User WS read_frame error")?,
            Err(_timeout) => {
                // No data received within PING_INTERVAL — send text PING.
                ws.write_frame(Frame::text(fastwebsockets::Payload::Borrowed(b"PING")))
                    .await
                    .context("failed to send User WS PING")?;
                continue;
            }
        };

        match frame.opcode {
            OpCode::Text | OpCode::Binary => {
                let payload = std::str::from_utf8(&frame.payload)
                    .context("User WS payload is not valid UTF-8")?;
                // Filter text-based PONG responses.
                if payload == "PONG" {
                    continue;
                }
                if let Err(e) = handle_user_message(payload, tx) {
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

/// Build the authenticated User WS subscription message.
///
/// Format matches SDK's `WithCredentials::as_authenticated()` (ws/traits.rs:36-51).
pub(super) fn build_user_auth_msg(api_key: &str, secret: &str, passphrase: &str) -> String {
    serde_json::json!({
        "type": "user",
        "operation": "subscribe",
        "markets": [],
        "asset_ids": [],
        "initial_dump": true,
        "auth": {
            "apiKey": api_key,
            "secret": secret,
            "passphrase": passphrase
        }
    })
    .to_string()
}

// ─── User WS message parsing ──────────────────────────────────────────────────

/// Parse a raw JSON frame from the Polymarket User WS.
///
/// Events of interest:
/// - `"order"` → `IngestorEvent::TradeStatusUpdate` (hex order hash matches engine state)
/// - `"trade"` → debug-logged only (UUID trade IDs never match stored order hashes)
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
            "trade" => {
                // "trade" events carry UUID trade IDs (e.g. "7d3508f8-...")
                // which never match stored hex order hashes. Log only — do NOT
                // forward to the engine (would pollute the pending_fills buffer).
                let trade_id = event
                    .get("id")
                    .or_else(|| event.get("order_id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                let status_str = event.get("status").and_then(|v| v.as_str()).unwrap_or("?");
                debug!(trade_id, status = status_str, "User WS: trade event (ignored)");
            }
            "order" => {
                let order_id = event
                    .get("id")
                    .or_else(|| event.get("order_id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                let status_str = event.get("status").and_then(|v| v.as_str()).unwrap_or("?");
                info!(order_id, status = status_str, "User WS: order event");

                // Parse optional size fields for partial fill detection.
                let size_matched = event
                    .get("size_matched")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<Decimal>().ok());
                let original_size = event
                    .get("original_size")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<Decimal>().ok());

                // Forward actionable order status changes to the engine.
                // "order" events carry the correct hex order hash (matching
                // what the engine stores), unlike "trade" events which use
                // a UUID trade ID.
                if let Ok(status) = parse_trade_status(status_str) {
                    let ev = IngestorEvent::TradeStatusUpdate {
                        order_id: order_id.to_string(),
                        status,
                        size_matched,
                        original_size,
                    };
                    if tx.try_send(ev).is_err() {
                        warn!("channel full — order TradeStatusUpdate dropped");
                    }
                }
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

/// Map a CLOB status string to `TradeStatus`.
pub(super) fn parse_trade_status(s: &str) -> Result<TradeStatus> {
    match s.to_uppercase().as_str() {
        "MATCHED" => Ok(TradeStatus::Matched),
        "MINED" => Ok(TradeStatus::Mined),
        "CONFIRMED" => Ok(TradeStatus::Confirmed),
        "RETRYING" => Ok(TradeStatus::Retrying),
        "FAILED" => Ok(TradeStatus::Failed),
        "CANCELED" | "CANCELLED" => Ok(TradeStatus::Canceled),
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
    fn test_parse_trade_status_canceled() {
        let status = parse_trade_status("CANCELED").expect("parse failed");
        assert_eq!(status, TradeStatus::Canceled);
    }

    #[test]
    fn test_parse_trade_status_cancelled_british() {
        let status = parse_trade_status("CANCELLED").expect("parse failed");
        assert_eq!(status, TradeStatus::Canceled);
    }

    #[test]
    fn test_parse_trade_status_canceled_lowercase() {
        let status = parse_trade_status("canceled").expect("lowercase parse");
        assert_eq!(status, TradeStatus::Canceled);
    }

    #[test]
    fn test_parse_trade_status_unknown_errors() {
        let result = parse_trade_status("PENDING_QUEUE");
        assert!(result.is_err(), "unknown status should return Err");
    }

    #[test]
    fn test_build_user_auth_msg_format() {
        let msg = build_user_auth_msg("my-api-key", "my-secret", "my-passphrase");
        let parsed: serde_json::Value = serde_json::from_str(&msg).expect("valid JSON");

        assert_eq!(parsed["type"], "user");
        assert_eq!(parsed["operation"], "subscribe");
        assert_eq!(parsed["initial_dump"], true);
        assert!(parsed["markets"].as_array().unwrap().is_empty());
        assert!(parsed["asset_ids"].as_array().unwrap().is_empty());

        let auth = &parsed["auth"];
        assert_eq!(auth["apiKey"], "my-api-key");
        assert_eq!(auth["secret"], "my-secret");
        assert_eq!(auth["passphrase"], "my-passphrase");
    }

}
