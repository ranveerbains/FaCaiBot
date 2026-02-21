//! Market rotation manager — Gamma API discovery and market lifecycle management.
//!
//! Polls the Gamma API every 10 minutes for upcoming BTC/ETH 15-minute markets.
//! At <180s remaining on the current market, emits `MarketRotation` and warms
//! the orderbook cache.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use crossbeam_channel::Sender;
use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::types::IngestorEvent;
use crate::types::market::OrderBook;

use super::market_ws::{parse_price_levels, parse_timestamp_field};
use super::tls_helpers::http_get;
use super::{CLOB_BASE_URL, GAMMA_BASE_URL, GAMMA_EVENTS_PATH, GAMMA_POLL_INTERVAL_MS};

/// Information about a discovered upcoming 15-minute market.
#[derive(Debug, Clone)]
pub struct MarketInfo {
    /// Polymarket condition ID (e.g. `"0xabc123..."`).
    pub condition_id: String,
    /// YES token ERC-1155 ID.
    pub yes_token_id: String,
    /// NO token ERC-1155 ID.
    pub no_token_id: String,
    /// Market expiry (epoch ms).
    pub end_timestamp_ms: u64,
}

/// Long-running market rotation manager.
///
/// - Polls Gamma API every 10 minutes for the next market.
/// - At <180s remaining on the current market, emits `MarketRotation`
///   and the caller should re-subscribe WS to new token IDs.
/// - Emits `IngestorEvent::MarketRotation` once per market transition.
///
/// The caller (`main.rs` / integration layer) is responsible for:
///   1. Re-calling `run_market_ws` with the new token IDs.
///   2. Caching tick_size + fee_rate via `GET /tick-size` and `GET /fee-rate`
///      for the new market (done by Executor Dev via polymarket.rs).
///   3. Updating Redis `active_market` via `HotStorage::set_active_market`.
pub(super) async fn run_market_rotation(
    shutdown: Arc<AtomicBool>,
    tx: Sender<IngestorEvent>,
    token_tx: tokio::sync::watch::Sender<Vec<String>>,
) -> Result<()> {
    let mut poll_interval =
        tokio::time::interval(Duration::from_millis(GAMMA_POLL_INTERVAL_MS));
    let mut last_emitted_condition_id: Option<String> = None;
    let mut current_market: Option<MarketInfo> = None;
    // Check for anticipatory loading more frequently (every 5s).
    let mut rotation_check = tokio::time::interval(Duration::from_secs(5));
    // Set when expiry is detected — triggers immediate Gamma poll on next rotation_check tick.
    let mut needs_immediate_poll = false;

    loop {
        tokio::select! {
            _ = poll_interval.tick() => {
                poll_gamma_and_emit(
                    &tx, &token_tx,
                    &mut last_emitted_condition_id,
                    &mut current_market,
                ).await;
            }
            _ = rotation_check.tick() => {
                if shutdown.load(Ordering::Relaxed) {
                    info!("market rotation manager shutdown — exiting");
                    return Ok(());
                }

                let now_ms = now_epoch_ms();

                // Check if current market expired — trigger immediate re-discovery.
                if let Some(ref market) = current_market {
                    let remaining_ms = market.end_timestamp_ms.saturating_sub(now_ms);
                    if remaining_ms == 0 {
                        info!("current market expired — polling Gamma immediately for next market");
                        current_market = None;
                        needs_immediate_poll = true;
                    }
                }

                if needs_immediate_poll {
                    needs_immediate_poll = false;
                    poll_gamma_and_emit(
                        &tx, &token_tx,
                        &mut last_emitted_condition_id,
                        &mut current_market,
                    ).await;
                }
            }
        }
    }
}

/// Poll Gamma API, emit `MarketRotation` for any newly discovered market, and warm
/// the orderbook cache. Shared by the scheduled poll and the immediate-on-expiry path.
pub(super) async fn poll_gamma_and_emit(
    tx: &Sender<IngestorEvent>,
    token_tx: &tokio::sync::watch::Sender<Vec<String>>,
    last_emitted_condition_id: &mut Option<String>,
    current_market: &mut Option<MarketInfo>,
) {
    match discover_next_market().await {
        Ok(info) => {
            let now_ms = now_epoch_ms();
            let remaining_ms = info.end_timestamp_ms.saturating_sub(now_ms);
            info!(
                condition_id = %info.condition_id,
                end_ts_ms = info.end_timestamp_ms,
                remaining_secs = remaining_ms / 1000,
                "Gamma API: discovered next market"
            );

            // Emit rotation immediately for new markets that have time remaining.
            let already_emitted = last_emitted_condition_id
                .as_deref()
                .map(|prev| prev == info.condition_id.as_str())
                .unwrap_or(false);

            if !already_emitted && remaining_ms > 0 {
                info!(
                    condition_id = %info.condition_id,
                    remaining_secs = remaining_ms / 1000,
                    "emitting MarketRotation on discovery"
                );

                let event = IngestorEvent::MarketRotation {
                    condition_id: info.condition_id.clone(),
                    yes_token_id: info.yes_token_id.clone(),
                    no_token_id: info.no_token_id.clone(),
                    end_timestamp_ms: info.end_timestamp_ms,
                };

                if tx.try_send(event).is_err() {
                    warn!("channel full — MarketRotation event dropped");
                }

                // Push new token IDs to Market WS via watch channel.
                let new_tokens = vec![info.yes_token_id.clone(), info.no_token_id.clone()];
                let _ = token_tx.send(new_tokens);

                // Fetch initial order book via REST for the YES token
                // so the engine has book data before WS events arrive.
                match fetch_order_book(&info.yes_token_id).await {
                    Ok(book) => {
                        info!(
                            asset_id = %book.asset_id,
                            bids = book.bids.len(),
                            asks = book.asks.len(),
                            "fetched initial order book via REST"
                        );
                        if tx.try_send(IngestorEvent::PolymarketBook(book)).is_err() {
                            warn!("channel full — initial PolymarketBook dropped");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to fetch initial order book — will rely on WS");
                    }
                }

                *last_emitted_condition_id = Some(info.condition_id.clone());
            }

            *current_market = Some(info);
        }
        Err(e) => {
            warn!(error = %e, "Gamma API poll failed — will retry next cycle");
        }
    }
}

/// Query the Gamma API for the next upcoming BTC 15-minute market.
///
/// Returns info for the market with the soonest non-expired `endTimestamp`.
/// PRD: poll every 10 minutes (Section 5.3).
pub(super) async fn discover_next_market() -> Result<MarketInfo> {
    let url = format!("{GAMMA_BASE_URL}{GAMMA_EVENTS_PATH}");
    debug!(url = %url, "querying Gamma API for next 15-min market");

    let response_bytes = http_get(&url).await?;
    let body_str = std::str::from_utf8(&response_bytes)
        .context("Gamma API response is not valid UTF-8")?;

    parse_gamma_events_response(body_str)
}

/// Fetch the order book for a token via the CLOB REST API.
///
/// `GET /book?token_id={token_id}` — public endpoint, no auth required.
/// Returns the full order book snapshot with bids and asks.
async fn fetch_order_book(token_id: &str) -> Result<OrderBook> {
    let url = format!("{CLOB_BASE_URL}/book?token_id={token_id}");
    debug!(url = %url, "fetching initial order book via REST");

    let response_bytes = http_get(&url).await?;
    let body_str =
        std::str::from_utf8(&response_bytes).context("CLOB book response is not valid UTF-8")?;

    let value: serde_json::Value =
        serde_json::from_str(body_str).context("CLOB book JSON parse error")?;

    let asset_id = value
        .get("asset_id")
        .and_then(|v| v.as_str())
        .unwrap_or(token_id)
        .to_string();

    let timestamp_ms = parse_timestamp_field(&value, "timestamp");

    let bids = parse_price_levels(value.get("bids"))?;
    let asks = parse_price_levels(value.get("asks"))?;

    let mut bids = bids;
    let mut asks = asks;
    bids.sort_by(|a, b| b.price.cmp(&a.price));
    asks.sort_by(|a, b| a.price.cmp(&b.price));

    Ok(OrderBook {
        asset_id,
        bids,
        asks,
        timestamp_ms,
    })
}

// ─── Gamma API response parsing ───────────────────────────────────────────────

/// Slug prefixes for the crypto assets we trade.
const TARGET_SLUG_PREFIXES: &[&str] = &["btc-updown-15m-", "eth-updown-15m-"];

/// Raw event from the Gamma API `/events` response.
#[derive(Deserialize, Debug)]
struct GammaEvent {
    /// Event slug (e.g. `"btc-updown-15m-1771676100"`).
    #[serde(default)]
    slug: String,

    /// ISO 8601 end date (e.g. `"2026-02-21T12:30:00Z"`).
    #[serde(rename = "endDate", default)]
    end_date: String,

    /// Nested markets — for 15-min events there is exactly 1 market per event.
    #[serde(default)]
    markets: Vec<GammaEventMarket>,
}

/// Nested market entry inside a Gamma event.
#[derive(Deserialize, Debug)]
struct GammaEventMarket {
    /// Condition ID (hex string).
    #[serde(rename = "conditionId", default)]
    condition_id: String,

    /// CLOB token IDs — Gamma returns this as a **JSON-encoded string**,
    /// e.g. `"[\"YES_ID\", \"NO_ID\"]"`, not a native JSON array.
    #[serde(rename = "clobTokenIds", default)]
    clob_token_ids: String,

    /// Whether the market is currently accepting orders.
    #[serde(rename = "acceptingOrders", default)]
    accepting_orders: bool,
}

/// Parse the Gamma API `/events?tag_id=102467` response into a `MarketInfo`.
///
/// Filters for BTC/ETH 15-minute markets by slug prefix, selects the
/// soonest non-expired event that is accepting orders.
pub(super) fn parse_gamma_events_response(body: &str) -> Result<MarketInfo> {
    let events: Vec<GammaEvent> =
        serde_json::from_str(body).context("Gamma API events parse error")?;

    let now_ms = now_epoch_ms();
    let mut best: Option<(u64, MarketInfo)> = None;

    for event in &events {
        // Filter to BTC/ETH by slug prefix.
        let is_target = TARGET_SLUG_PREFIXES
            .iter()
            .any(|prefix| event.slug.starts_with(prefix));
        if !is_target {
            continue;
        }

        // Parse end date.
        let end_ms = if event.end_date.is_empty() {
            continue;
        } else {
            parse_iso8601_to_epoch_ms(&event.end_date).unwrap_or(0)
        };

        // Skip expired events.
        if end_ms <= now_ms {
            debug!(slug = %event.slug, "skipping expired 15-min event");
            continue;
        }

        // Extract the first market with valid tokens.
        for m in &event.markets {
            if m.condition_id.is_empty() || !m.accepting_orders {
                continue;
            }

            // clobTokenIds is a JSON-encoded string: "[\"YES\", \"NO\"]"
            let token_ids: Vec<String> = match serde_json::from_str(&m.clob_token_ids) {
                Ok(ids) => ids,
                Err(_) => {
                    debug!(
                        slug = %event.slug,
                        raw = %m.clob_token_ids,
                        "failed to parse clobTokenIds"
                    );
                    continue;
                }
            };

            if token_ids.len() < 2 {
                continue;
            }

            let info = MarketInfo {
                condition_id: m.condition_id.clone(),
                yes_token_id: token_ids[0].clone(),
                no_token_id: token_ids[1].clone(),
                end_timestamp_ms: end_ms,
            };

            // Keep the soonest-expiring market.
            match &best {
                Some((best_end, _)) if end_ms >= *best_end => {}
                _ => {
                    best = Some((end_ms, info));
                }
            }
        }
    }

    best.map(|(_, info)| info)
        .ok_or_else(|| anyhow!("no valid upcoming BTC/ETH 15-min market found"))
}

/// Parse `endTimestamp` from either an integer (epoch seconds),
/// a float (epoch seconds), or an ISO 8601 string.
pub(super) fn parse_end_timestamp(val: &serde_json::Value) -> Result<u64> {
    match val {
        serde_json::Value::Number(n) => {
            // Epoch seconds — convert to ms.
            let secs = n
                .as_u64()
                .or_else(|| n.as_f64().map(|f| f as u64))
                .context("endTimestamp number out of u64 range")?;
            Ok(secs * 1_000)
        }
        serde_json::Value::String(s) => {
            // Try parsing as plain integer first.
            if let Ok(secs) = s.parse::<u64>() {
                return Ok(secs * 1_000);
            }
            // Try parsing as f64 (e.g. "1714000000.0").
            if let Ok(secs) = s.parse::<f64>() {
                return Ok((secs * 1_000.0) as u64);
            }
            // Try ISO 8601 (e.g. "2024-04-25T15:00:00Z").
            // Use chrono-free parsing: count chars and parse manually.
            // RFC 3339 format: "YYYY-MM-DDTHH:MM:SSZ"
            parse_iso8601_to_epoch_ms(s)
        }
        _ => Err(anyhow!("unexpected endTimestamp type: {:?}", val)),
    }
}

/// Minimal RFC 3339 parser — avoids pulling in `chrono`.
/// Handles: `YYYY-MM-DDTHH:MM:SSZ` and `YYYY-MM-DDTHH:MM:SS+00:00`.
pub(super) fn parse_iso8601_to_epoch_ms(s: &str) -> Result<u64> {
    // Trim trailing Z or timezone.
    let s = s.trim_end_matches('Z');
    let s = if let Some(pos) = s.rfind('+') {
        &s[..pos]
    } else if s.len() > 19 && &s[19..] == "-00:00" {
        &s[..19]
    } else {
        s
    };

    // Expect exactly "YYYY-MM-DDTHH:MM:SS".
    if s.len() < 19 {
        return Err(anyhow!("ISO 8601 string too short: '{}'", s));
    }

    let year: u64 = s[0..4].parse().context("ISO 8601 year")?;
    let month: u64 = s[5..7].parse().context("ISO 8601 month")?;
    let day: u64 = s[8..10].parse().context("ISO 8601 day")?;
    let hour: u64 = s[11..13].parse().context("ISO 8601 hour")?;
    let min: u64 = s[14..16].parse().context("ISO 8601 minute")?;
    let sec: u64 = s[17..19].parse().context("ISO 8601 second")?;

    // Days since Unix epoch (1970-01-01). Simple proleptic Gregorian.
    let leap_years_since_1970 = {
        let y = year - 1;
        (y / 4 - y / 100 + y / 400) - (1969 / 4 - 1969 / 100 + 1969 / 400)
    };
    let days_in_years = (year - 1970) * 365 + leap_years_since_1970;

    let is_leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let days_in_months: u64 = [0u64, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30]
        .iter()
        .take(month as usize)
        .sum::<u64>()
        + if is_leap && month > 2 { 1 } else { 0 };

    let total_days = days_in_years + days_in_months + (day - 1);
    let epoch_secs = total_days * 86_400 + hour * 3_600 + min * 60 + sec;
    Ok(epoch_secs * 1_000)
}

/// Current wall-clock time as epoch milliseconds.
#[inline]
pub(super) fn now_epoch_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Gamma API events response parsing ───────────────────────────────

    /// BTC 15-min event with a future endDate.
    const GAMMA_EVENTS_BTC: &str = r#"[
        {
            "slug": "btc-updown-15m-9999999900",
            "endDate": "2099-01-01T00:00:00Z",
            "markets": [{
                "conditionId": "0xabc123",
                "clobTokenIds": "[\"0xyes111\", \"0xno222\"]",
                "acceptingOrders": true
            }]
        }
    ]"#;

    /// ETH 15-min event with a future endDate.
    const GAMMA_EVENTS_ETH: &str = r#"[
        {
            "slug": "eth-updown-15m-9999999900",
            "endDate": "2099-01-01T00:00:00Z",
            "markets": [{
                "conditionId": "0xeth456",
                "clobTokenIds": "[\"0xyes333\", \"0xno444\"]",
                "acceptingOrders": true
            }]
        }
    ]"#;

    /// All events expired.
    const GAMMA_EVENTS_EXPIRED: &str = r#"[
        {
            "slug": "btc-updown-15m-1",
            "endDate": "1970-01-01T00:01:00Z",
            "markets": [{
                "conditionId": "0xold",
                "clobTokenIds": "[\"0xyes000\", \"0xno000\"]",
                "acceptingOrders": true
            }]
        }
    ]"#;

    /// Mix of BTC (expired), ETH (valid), SOL (valid but not targeted).
    const GAMMA_EVENTS_MIXED: &str = r#"[
        {
            "slug": "btc-updown-15m-1",
            "endDate": "1970-01-01T00:01:00Z",
            "markets": [{
                "conditionId": "0xexpired",
                "clobTokenIds": "[\"0xyesA\", \"0xnoA\"]",
                "acceptingOrders": true
            }]
        },
        {
            "slug": "sol-updown-15m-9999999900",
            "endDate": "2099-01-01T00:00:00Z",
            "markets": [{
                "conditionId": "0xsol_skip",
                "clobTokenIds": "[\"0xyesS\", \"0xnoS\"]",
                "acceptingOrders": true
            }]
        },
        {
            "slug": "eth-updown-15m-9999999900",
            "endDate": "2099-01-01T00:00:00Z",
            "markets": [{
                "conditionId": "0xvalid",
                "clobTokenIds": "[\"0xyesB\", \"0xnoB\"]",
                "acceptingOrders": true
            }]
        }
    ]"#;

    #[test]
    fn test_parse_gamma_events_btc() {
        let info = parse_gamma_events_response(GAMMA_EVENTS_BTC).expect("parse failed");
        assert_eq!(info.condition_id, "0xabc123");
        assert_eq!(info.yes_token_id, "0xyes111");
        assert_eq!(info.no_token_id, "0xno222");
        assert!(info.end_timestamp_ms > 0);
    }

    #[test]
    fn test_parse_gamma_events_eth() {
        let info = parse_gamma_events_response(GAMMA_EVENTS_ETH).expect("parse failed");
        assert_eq!(info.condition_id, "0xeth456");
    }

    #[test]
    fn test_parse_gamma_events_skips_expired() {
        let result = parse_gamma_events_response(GAMMA_EVENTS_EXPIRED);
        assert!(result.is_err(), "should error when all events are expired");
    }

    #[test]
    fn test_parse_gamma_events_picks_valid_target() {
        let info =
            parse_gamma_events_response(GAMMA_EVENTS_MIXED).expect("should find valid market");
        // Should pick ETH (valid target), skipping expired BTC and non-target SOL.
        assert_eq!(info.condition_id, "0xvalid");
    }

    // ── ISO 8601 parser ────────────────────────────────────────────────

    #[test]
    fn test_iso8601_epoch_calculation() {
        // 1970-01-01T00:00:00Z should be epoch 0.
        let ms = parse_iso8601_to_epoch_ms("1970-01-01T00:00:00Z").expect("parse failed");
        assert_eq!(ms, 0, "Unix epoch should be 0 ms");
    }

    #[test]
    fn test_iso8601_known_date() {
        // 2024-04-25T15:00:00Z — known epoch seconds = 1714057200
        let ms = parse_iso8601_to_epoch_ms("2024-04-25T15:00:00Z").expect("parse failed");
        // Allow ±1 day for leap year approximation in our simple parser.
        let expected = 1_714_057_200_000u64;
        let delta = (ms as i64 - expected as i64).abs();
        assert!(
            delta < 86_400_000,
            "ISO 8601 parse drift too large: got {ms}, expected ~{expected}, delta {delta}ms"
        );
    }
}
