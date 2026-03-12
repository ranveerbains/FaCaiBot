//! Market WebSocket connection — public order book and price streaming.
//!
//! Implements the Polymarket public Market WS:
//! `wss://ws-subscriptions-clob.polymarket.com/ws/market`
//!
//! Streams `book`, `price_change`, `best_bid_ask`, `tick_size_change`,
//! `market_resolved` events for active token IDs.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use crossbeam_channel::Sender;
use fastwebsockets::{Frame, OpCode};
use rust_decimal::Decimal;
use tracing::{debug, info, warn};

use crate::types::IngestorEvent;
use crate::types::market::{DataSource, OrderBook, PriceLevel};
use crate::types::order::Side;

use super::tls_helpers::tls_connect;
use super::{BACKOFF_INITIAL_MS, BACKOFF_MAX_MS, MARKET_WS_URL};

// ─── Shared helpers re-used from parent ──────────────────────────────────────

/// Run the public Market WS, reconnecting with exponential backoff.
///
/// Watches `token_rx` for new token lists pushed by `run_market_rotation`.
/// When tokens change, the current WS session is dropped and a new one
/// opens with the updated subscription.
pub(super) async fn run_market_ws(
    shutdown: Arc<AtomicBool>,
    mut token_rx: tokio::sync::watch::Receiver<Vec<String>>,
    tx: Sender<IngestorEvent>,
) -> Result<()> {
    let mut backoff_ms = BACKOFF_INITIAL_MS;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("market WS shutdown requested — exiting");
            return Ok(());
        }

        let token_ids = token_rx.borrow_and_update().clone();

        // If no tokens yet, wait for the first rotation before connecting.
        if token_ids.is_empty() {
            info!("Market WS: waiting for market rotation to discover tokens...");
            if token_rx.changed().await.is_err() {
                return Ok(()); // sender dropped
            }
            continue;
        }

        info!(
            tokens = ?token_ids,
            "connecting to Polymarket Market WS"
        );

        // Run the WS session until it errors OR the token list changes.
        tokio::select! {
            result = market_ws_session(shutdown.clone(), &token_ids, &tx) => {
                match result {
                    Ok(()) => {
                        info!("Polymarket Market WS closed cleanly — reconnecting");
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            backoff_ms,
                            "Polymarket Market WS error — reconnecting"
                        );
                    }
                }
                let _ = tx.try_send(IngestorEvent::WsStatus {
                    source: DataSource::PolymarketMarket,
                    connected: false,
                });
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(BACKOFF_MAX_MS);
            }
            _ = token_rx.changed() => {
                // Token list updated by rotation — reconnect with new tokens.
                info!("Market WS: token list updated — reconnecting with new subscription");
                backoff_ms = BACKOFF_INITIAL_MS; // reset backoff on rotation
            }
        }
    }
}

/// Keepalive interval — Polymarket requires text "PING" every 10 seconds.
const PING_INTERVAL: Duration = Duration::from_secs(10);

/// Single Market WS session: connect, subscribe, pump frames.
async fn market_ws_session(
    shutdown: Arc<AtomicBool>,
    token_ids: &[String],
    tx: &Sender<IngestorEvent>,
) -> Result<()> {
    let mut ws = tls_connect(MARKET_WS_URL).await?;

    // Emit connected status.
    let _ = tx.try_send(IngestorEvent::WsStatus {
        source: DataSource::PolymarketMarket,
        connected: true,
    });
    info!("Polymarket Market WS connected");

    // Send subscription message.
    let sub_msg = build_market_subscribe_msg(token_ids);
    debug!(msg = %sub_msg, "sending Market WS subscription");
    ws.write_frame(Frame::text(sub_msg.into_bytes().into()))
        .await
        .context("failed to send Market WS subscription")?;

    // Main frame loop with text-based PING keepalive.
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Wait for next frame OR send PING on timeout.
        let frame = match tokio::time::timeout(PING_INTERVAL, ws.read_frame()).await {
            Ok(result) => result.context("Market WS read_frame error")?,
            Err(_timeout) => {
                // No data received within PING_INTERVAL — send text PING.
                ws.write_frame(Frame::text(fastwebsockets::Payload::Borrowed(b"PING")))
                    .await
                    .context("failed to send Market WS PING")?;
                continue;
            }
        };

        match frame.opcode {
            OpCode::Text | OpCode::Binary => {
                let payload = std::str::from_utf8(&frame.payload)
                    .context("Market WS payload is not valid UTF-8")?;
                // Filter text-based PONG responses.
                if payload == "PONG" {
                    continue;
                }
                if let Err(e) = handle_market_message(payload, tx) {
                    debug!(error = %e, "Market WS message handling error (non-fatal)");
                }
            }
            OpCode::Ping => {
                ws.write_frame(Frame::pong(frame.payload))
                    .await
                    .context("failed to send pong")?;
            }
            OpCode::Close => {
                info!("Polymarket Market WS server sent Close");
                return Ok(());
            }
            _ => {}
        }
    }

    Ok(())
}

// ─── Market WS subscription builder ──────────────────────────────────────────

/// Build the JSON subscription message for the Market WS.
///
/// PRD Section 5.1:
/// ```json
/// {"type": "market", "assets_ids": ["YES", "NO"], "custom_feature_enabled": true}
/// ```
pub(super) fn build_market_subscribe_msg(token_ids: &[String]) -> String {
    serde_json::json!({
        "type": "market",
        "assets_ids": token_ids,
        "custom_feature_enabled": true
    })
    .to_string()
}

// ─── Market WS message parsing ────────────────────────────────────────────────

/// Parse a raw JSON frame from the Polymarket Market WS and emit IngestorEvents.
///
/// The Market WS delivers an **array** of event objects in each frame.
/// Each object has a `"type"` field identifying the event.
pub(super) fn handle_market_message(json: &str, tx: &Sender<IngestorEvent>) -> Result<()> {
    // The server sends an array of event objects or a single ping/ack.
    let value: serde_json::Value =
        serde_json::from_str(json).context("Market WS JSON parse error")?;

    match &value {
        // Array of events (normal case).
        serde_json::Value::Array(events) => {
            for event in events {
                if let Err(e) = dispatch_market_event(event, tx) {
                    debug!(error = %e, "market event dispatch error");
                }
            }
        }
        // Single object (e.g. connection acknowledgment or ping).
        serde_json::Value::Object(_) => {
            if let Err(e) = dispatch_market_event(&value, tx) {
                debug!(error = %e, "market event dispatch error (single object)");
            }
        }
        _ => {
            debug!(json = %json, "unexpected Market WS frame shape");
        }
    }

    Ok(())
}

/// Dispatch a single Market WS event object to the appropriate `IngestorEvent`.
fn dispatch_market_event(event: &serde_json::Value, tx: &Sender<IngestorEvent>) -> Result<()> {
    let event_type = event
        .get("event_type")
        .or_else(|| event.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    match event_type {
        "book" => {
            let book = parse_book_event(event)?;
            if tx.try_send(IngestorEvent::PolymarketBook(book)).is_err() {
                warn!("channel full — PolymarketBook dropped");
            }
        }
        "price_change" => {
            let events = parse_price_change_event(event)?;
            for ev in events {
                if tx.try_send(ev).is_err() {
                    warn!("channel full — PolymarketPriceChange dropped");
                }
            }
        }
        "best_bid_ask" => {
            let ev = parse_best_bid_ask_event(event)?;
            if tx.try_send(ev).is_err() {
                warn!("channel full — PolymarketBestBidAsk dropped");
            }
        }
        "tick_size_change" => {
            let ev = parse_tick_size_change_event(event)?;
            if tx.try_send(ev).is_err() {
                warn!("channel full — PolymarketTickSizeChange dropped");
            }
        }
        "market_resolved" => {
            let ev = parse_market_resolved_event(event)?;
            if tx.try_send(ev).is_err() {
                warn!("channel full — PolymarketMarketResolved dropped");
            }
        }
        "last_trade_price" => {
            // Informational only — log at debug, do not emit.
            let price = event.get("price").and_then(|v| v.as_str()).unwrap_or("?");
            let asset = event
                .get("asset_id")
                .and_then(|v| v.as_str())
                .unwrap_or("?");
            debug!(
                asset_id = asset,
                price = price,
                "last_trade_price (informational)"
            );
        }
        "" => {
            // Possibly a connection ack or heartbeat from the server.
            debug!("Market WS: received frame with no event_type — likely ack");
        }
        other => {
            debug!(
                event_type = other,
                "Market WS: unknown event type — ignoring"
            );
        }
    }

    Ok(())
}

// ─── Individual event parsers ─────────────────────────────────────────────────

/// Parse a `book` event into an `OrderBook`.
///
/// Expected shape (Polymarket CLOB Market WS):
/// ```json
/// {
///   "event_type": "book",
///   "asset_id": "...",
///   "bids": [{"price": "0.48", "size": "100"}, ...],
///   "asks": [{"price": "0.52", "size": "80"}, ...],
///   "timestamp": "1714000000"
/// }
/// ```
pub(super) fn parse_book_event(event: &serde_json::Value) -> Result<OrderBook> {
    let asset_id = event
        .get("asset_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let timestamp_ms = parse_timestamp_field(event, "timestamp");

    let bids = parse_price_levels(event.get("bids"))?;
    let asks = parse_price_levels(event.get("asks"))?;

    // Ensure correct sort order: bids descending, asks ascending.
    let mut bids = bids;
    let mut asks = asks;
    bids.sort_by(|a, b| b.price.cmp(&a.price)); // highest first
    asks.sort_by(|a, b| a.price.cmp(&b.price)); // lowest first

    Ok(OrderBook {
        asset_id,
        bids,
        asks,
        timestamp_ms,
    })
}

/// Parse a `price_change` event.
///
/// Polymarket sends an array of changes in a single frame under `"changes"`.
/// Each change: `{"asset_id": "...", "side": "BUY"|"SELL", "price": "...", "size": "...",
///               "best_bid": "...", "best_ask": "..."}`
pub(super) fn parse_price_change_event(event: &serde_json::Value) -> Result<Vec<IngestorEvent>> {
    let changes = event
        .get("changes")
        .and_then(|v| v.as_array())
        .map(|a| a.as_slice())
        .unwrap_or(&[]);

    let mut events = Vec::with_capacity(changes.len());

    for change in changes {
        let asset_id = change
            .get("asset_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let price = parse_decimal_field(change, "price")?;
        let size = parse_decimal_field(change, "size").unwrap_or(Decimal::ZERO);
        let best_bid = parse_decimal_field(change, "best_bid").unwrap_or(Decimal::ZERO);
        let best_ask = parse_decimal_field(change, "best_ask").unwrap_or(Decimal::ZERO);

        let side_str = change.get("side").and_then(|v| v.as_str()).unwrap_or("BUY");
        let side = if side_str.eq_ignore_ascii_case("SELL") {
            Side::Sell
        } else {
            Side::Buy
        };

        events.push(IngestorEvent::PolymarketPriceChange {
            asset_id,
            price,
            size,
            side,
            best_bid,
            best_ask,
        });
    }

    Ok(events)
}

/// Parse a `best_bid_ask` event.
///
/// Shape: `{"event_type": "best_bid_ask", "asset_id": "...", "bid": "0.48", "ask": "0.52"}`
pub(super) fn parse_best_bid_ask_event(event: &serde_json::Value) -> Result<IngestorEvent> {
    let asset_id = event
        .get("asset_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let best_bid = parse_decimal_field(event, "bid")
        .or_else(|_| parse_decimal_field(event, "best_bid"))
        .unwrap_or(Decimal::ZERO);
    let best_ask = parse_decimal_field(event, "ask")
        .or_else(|_| parse_decimal_field(event, "best_ask"))
        .unwrap_or(Decimal::ZERO);

    Ok(IngestorEvent::PolymarketBestBidAsk {
        asset_id,
        best_bid,
        best_ask,
    })
}

/// Parse a `tick_size_change` event.
///
/// Shape: `{"event_type": "tick_size_change", "asset_id": "...",
///           "old_tick_size": "0.01", "new_tick_size": "0.001"}`
pub(super) fn parse_tick_size_change_event(event: &serde_json::Value) -> Result<IngestorEvent> {
    use tracing::warn;

    let asset_id = event
        .get("asset_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let old_tick_size =
        parse_decimal_field(event, "old_tick_size").unwrap_or_else(|_| Decimal::new(1, 2)); // default 0.01
    let new_tick_size = parse_decimal_field(event, "new_tick_size")
        .or_else(|_| parse_decimal_field(event, "tick_size"))?;

    warn!(
        asset_id = %asset_id,
        old = %old_tick_size,
        new = %new_tick_size,
        "tick_size_change event received — updating cache"
    );

    Ok(IngestorEvent::PolymarketTickSizeChange {
        asset_id,
        old_tick_size,
        new_tick_size,
    })
}

/// Parse a `market_resolved` event.
///
/// Shape: `{"event_type": "market_resolved", "market": "0xcond...", "winner": "0xtoken..."}`
pub(super) fn parse_market_resolved_event(event: &serde_json::Value) -> Result<IngestorEvent> {
    use tracing::info;

    let market = event
        .get("market")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let winning_asset_id = event
        .get("winner")
        .or_else(|| event.get("winning_asset_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    info!(
        market = %market,
        winner = %winning_asset_id,
        "market_resolved event received"
    );

    Ok(IngestorEvent::PolymarketMarketResolved {
        market,
        winning_asset_id,
    })
}

// ─── Shared JSON helpers ──────────────────────────────────────────────────────

/// Parse `[[price_str, size_str], ...]` or `[{price: "...", size: "..."}, ...]`
/// into `Vec<PriceLevel>`.
pub(super) fn parse_price_levels(val: Option<&serde_json::Value>) -> Result<Vec<PriceLevel>> {
    use anyhow::anyhow;

    let arr = match val.and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return Ok(Vec::new()),
    };

    arr.iter()
        .map(|entry| {
            if let Some(obj) = entry.as_object() {
                // Object form: {"price": "0.48", "size": "100"}
                let price = obj
                    .get("price")
                    .and_then(|v| v.as_str())
                    .context("price level missing price")?
                    .parse::<Decimal>()
                    .context("price level price Decimal")?;
                let size = obj
                    .get("size")
                    .and_then(|v| v.as_str())
                    .unwrap_or("0")
                    .parse::<Decimal>()
                    .context("price level size Decimal")?;
                Ok(PriceLevel { price, size })
            } else if let Some(arr2) = entry.as_array() {
                // Array form: ["0.48", "100"]
                let price = arr2
                    .first()
                    .and_then(|v| v.as_str())
                    .context("price level array[0] price")?
                    .parse::<Decimal>()
                    .context("price level array price Decimal")?;
                let size = arr2
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("0")
                    .parse::<Decimal>()
                    .context("price level array size Decimal")?;
                Ok(PriceLevel { price, size })
            } else {
                Err(anyhow!("unexpected price level format: {:?}", entry))
            }
        })
        .collect()
}

/// Extract a `Decimal` from a named string field on a JSON object.
pub(super) fn parse_decimal_field(obj: &serde_json::Value, field: &str) -> Result<Decimal> {
    let s = obj
        .get(field)
        .and_then(|v| v.as_str())
        .with_context(|| format!("missing field '{field}'"))?;
    s.parse::<Decimal>()
        .with_context(|| format!("field '{field}' is not a valid Decimal: '{s}'"))
}

/// Extract a timestamp (epoch ms) from a named field.
/// Accepts string or number; interprets ≤ 10_digits as epoch seconds and converts.
pub(super) fn parse_timestamp_field(obj: &serde_json::Value, field: &str) -> u64 {
    use super::now_epoch_ms;

    let val = match obj.get(field) {
        Some(v) => v,
        None => return now_epoch_ms(),
    };

    let n = if let Some(s) = val.as_str() {
        s.parse::<u64>().ok()
    } else {
        val.as_u64()
    };

    match n {
        Some(t) if t < 10_000_000_000 => t * 1_000, // seconds → ms
        Some(t) => t,
        None => now_epoch_ms(),
    }
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "tests/market_ws_tests.rs"]
mod tests;
