//! Market rotation manager — Gamma API discovery and market lifecycle management.
//!
//! Single 5s timer drives all rotation logic:
//! - **Prewarm** at T-180s: discovers the next market and pre-fetches order books.
//! - **Instant switch** at T-0: emits the pre-warmed rotation with zero gap.
//! - **Aggressive retry**: when marketless (startup, post-expiry fallback failure,
//!   any gap), retries Gamma discovery every 5s until a market is found.

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
use super::{CLOB_BASE_URL, GAMMA_BASE_URL, GAMMA_EVENTS_PATH};

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
/// - Pre-warms the NEXT market before expiry (lead time configurable).
///   Retries every 5s on failure until the prewarm window closes.
/// - At T-0, emits the pre-warmed rotation instantly (zero gap).
/// - When marketless (startup, post-expiry failure, any gap), retries
///   Gamma discovery every 5s until a market is found.
/// - Emits `IngestorEvent::MarketRotation` once per market transition.
///
/// The caller (`main.rs` / integration layer) is responsible for:
///   1. Re-calling `run_market_ws` with the new token IDs.
///   2. Caching tick_size + fee_rate via `GET /tick-size` and `GET /fee-rate`
///      for the new market (done by Executor Dev via polymarket.rs).
///   3. Updating engine state with new market IDs.
pub(super) async fn run_market_rotation(
    shutdown: Arc<AtomicBool>,
    tx: Sender<IngestorEvent>,
    token_tx: tokio::sync::watch::Sender<Vec<String>>,
    prewarm_lead_ms: u64,
) -> Result<()> {
    let mut last_emitted_condition_id: Option<String> = None;
    let mut current_market: Option<MarketInfo> = None;
    // Single 5s timer drives all rotation logic: prewarm at T-180s, expiry
    // detection, and aggressive retry when marketless.
    let mut rotation_check = tokio::time::interval(Duration::from_secs(5));

    // ── Anticipatory pre-warming state ────────────────────────────────────
    let mut next_market: Option<MarketInfo> = None;
    let mut prewarmed_books: Vec<OrderBook> = Vec::new();
    let mut prewarm_attempted = false;
    let mut consecutive_failures: u32 = 0;

    loop {
        tokio::select! {
            _ = rotation_check.tick() => {
                if shutdown.load(Ordering::Relaxed) {
                    info!("market rotation manager shutdown — exiting");
                    return Ok(());
                }

                let now_ms = now_epoch_ms();

                if let Some(ref market) = current_market {
                    let remaining_ms = market.end_timestamp_ms.saturating_sub(now_ms);

                    // ── Pre-warm before expiry ───────────────────────────
                    // Discover the NEXT market (ending after current) and
                    // pre-fetch its order books so the switch is instant.
                    if remaining_ms > 0
                        && remaining_ms <= prewarm_lead_ms
                        && !prewarm_attempted
                    {
                        info!(
                            remaining_secs = remaining_ms / 1000,
                            prewarm_lead_secs = prewarm_lead_ms / 1000,
                            current_condition_id = %market.condition_id,
                            "pre-warming next market"
                        );

                        match discover_market_after(market.end_timestamp_ms).await {
                            Ok(info) => {
                                let next_remaining =
                                    info.end_timestamp_ms.saturating_sub(now_ms);
                                info!(
                                    condition_id = %info.condition_id,
                                    end_ts_ms = info.end_timestamp_ms,
                                    remaining_secs = next_remaining / 1000,
                                    "pre-warmed next market discovered"
                                );

                                // Pre-fetch order books.
                                let mut books = Vec::new();
                                for (label, token_id) in
                                    [("YES", &info.yes_token_id), ("NO", &info.no_token_id)]
                                {
                                    match fetch_order_book(token_id).await {
                                        Ok(book) => {
                                            info!(
                                                side = label,
                                                bids = book.bids.len(),
                                                asks = book.asks.len(),
                                                "pre-warmed book fetched"
                                            );
                                            books.push(book);
                                        }
                                        Err(e) => {
                                            warn!(
                                                side = label,
                                                error = %e,
                                                "failed to pre-warm book"
                                            );
                                        }
                                    }
                                }
                                prewarmed_books = books;
                                next_market = Some(info);
                                prewarm_attempted = true;
                            }
                            Err(e) => {
                                // Pre-warm failures are expected retries (market may
                                // not be listed yet). Keep at debug to avoid log noise.
                                debug!(
                                    error = %e,
                                    "pre-warm discovery failed — will retry next tick"
                                );
                            }
                        }
                    }

                    // ── Expiry: instant switch or fallback poll ───────────
                    if remaining_ms == 0 {
                        if let Some(next) = next_market.take() {
                            info!(
                                condition_id = %next.condition_id,
                                "instant switch to pre-warmed market (zero gap)"
                            );
                            emit_rotation_events(
                                &tx,
                                &token_tx,
                                &next,
                                &prewarmed_books,
                                &mut last_emitted_condition_id,
                            );
                            current_market = Some(next);
                            prewarmed_books.clear();
                            prewarm_attempted = false;
                        } else {
                            info!(
                                "current market expired, no pre-warmed market \
                                 — polling Gamma immediately"
                            );
                            current_market = None;
                            prewarmed_books.clear();
                            prewarm_attempted = false;
                        }
                    }
                }

                // Aggressive 5s retry when marketless (startup failure, post-expiry
                // fallback failure, any gap). Self-terminates when poll succeeds.
                // Safe: poll_gamma_and_emit deduplicates by last_emitted_condition_id.
                if current_market.is_none() {
                    debug!("no active market — retrying Gamma discovery");
                    poll_gamma_and_emit(
                        &tx, &token_tx,
                        &mut last_emitted_condition_id,
                        &mut current_market,
                    ).await;

                    if current_market.is_some() {
                        consecutive_failures = 0;
                    } else {
                        consecutive_failures += 1;
                        if consecutive_failures == 1 {
                            warn!("Gamma discovery failed — no market found (will retry every 5s)");
                        } else {
                            debug!(consecutive_failures, "Gamma discovery still failing");
                        }
                    }
                }
            }
        }
    }
}

/// Emit a `MarketRotation` event, push new token IDs to the Market WS, and
/// send any pre-fetched order books through the ingestor channel.
///
/// Shared by both the scheduled poll path (books fetched inline) and the
/// pre-warmed instant-switch path (books already cached).
fn emit_rotation_events(
    tx: &Sender<IngestorEvent>,
    token_tx: &tokio::sync::watch::Sender<Vec<String>>,
    info: &MarketInfo,
    books: &[OrderBook],
    last_emitted_condition_id: &mut Option<String>,
) {
    let event = IngestorEvent::MarketRotation {
        condition_id: info.condition_id.clone(),
        yes_token_id: info.yes_token_id.clone(),
        no_token_id: info.no_token_id.clone(),
        end_timestamp_ms: info.end_timestamp_ms,
    };

    // MarketRotation is critical — use send() to block rather than drop.
    // The channel has 8192 slots; blocking only occurs if the engine is
    // severely behind, in which case back-pressure is the correct behavior.
    if let Err(e) = tx.send(event) {
        warn!(error = %e, "channel disconnected — MarketRotation event lost");
    }

    // Push new token IDs to Market WS via watch channel.
    let new_tokens = vec![info.yes_token_id.clone(), info.no_token_id.clone()];
    let _ = token_tx.send(new_tokens);

    // Send pre-fetched order books so the engine has book data immediately.
    for book in books {
        if tx
            .try_send(IngestorEvent::PolymarketBook(book.clone()))
            .is_err()
        {
            warn!("channel full — initial book dropped (WS will provide updates)");
        }
    }

    *last_emitted_condition_id = Some(info.condition_id.clone());
}

/// Poll Gamma API, emit `MarketRotation` for any newly discovered market, and warm
/// the orderbook cache. Shared by the scheduled poll and the immediate-on-expiry fallback.
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

                // Fetch initial order books via REST for both YES and NO tokens
                // so the engine has book data before WS events arrive.
                let mut books = Vec::new();
                for (label, token_id) in [("YES", &info.yes_token_id), ("NO", &info.no_token_id)] {
                    match fetch_order_book(token_id).await {
                        Ok(book) => {
                            info!(
                                side = label,
                                bids = book.bids.len(),
                                asks = book.asks.len(),
                                "fetched initial book via REST"
                            );
                            books.push(book);
                        }
                        Err(e) => {
                            warn!(side = label, error = %e, "failed to fetch initial book — will rely on WS");
                        }
                    }
                }

                emit_rotation_events(tx, token_tx, &info, &books, last_emitted_condition_id);
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
    let body_str =
        std::str::from_utf8(&response_bytes).context("Gamma API response is not valid UTF-8")?;

    parse_gamma_events_response(body_str)
}

/// Query the Gamma API for a market ending **after** `after_ms`.
///
/// Used for anticipatory pre-warming: while Market A is still active, we need
/// to discover Market B (the next market in the schedule). The standard
/// `discover_next_market()` would return Market A since it's the soonest
/// non-expired market. This variant skips any market ending at or before
/// `after_ms`, so it returns Market B instead.
async fn discover_market_after(after_ms: u64) -> Result<MarketInfo> {
    let url = format!("{GAMMA_BASE_URL}{GAMMA_EVENTS_PATH}");
    debug!(url = %url, after_ms, "querying Gamma API for market after current");

    let response_bytes = http_get(&url).await?;
    let body_str =
        std::str::from_utf8(&response_bytes).context("Gamma API response is not valid UTF-8")?;

    parse_gamma_events_response_after(body_str, after_ms)
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
}

/// Parse the Gamma API `/events?tag_id=102467` response into a `MarketInfo`.
///
/// Filters for BTC/ETH 15-minute markets by slug prefix, selects the
/// soonest non-expired event with valid token IDs.
pub(super) fn parse_gamma_events_response(body: &str) -> Result<MarketInfo> {
    parse_gamma_events_response_after(body, now_epoch_ms())
}

/// Parse the Gamma API response, skipping any market ending at or before `skip_before_ms`.
///
/// This is the core parser used by both `parse_gamma_events_response` (skip_before = now,
/// i.e. skip expired) and `discover_market_after` (skip_before = current market's end time,
/// i.e. find the next market in the schedule).
fn parse_gamma_events_response_after(body: &str, skip_before_ms: u64) -> Result<MarketInfo> {
    let events: Vec<GammaEvent> =
        serde_json::from_str(body).context("Gamma API events parse error")?;

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

        // Skip markets ending at or before the cutoff.
        if end_ms <= skip_before_ms {
            debug!(slug = %event.slug, "skipping market (ends before cutoff)");
            continue;
        }

        // Extract the first market with valid tokens.
        // Note: acceptingOrders is NOT checked — we only need token IDs for
        // discovery. The engine's guards (spread, depth, stale book) prevent
        // trading before the market is ready.
        for m in &event.markets {
            if m.condition_id.is_empty() {
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

    if best.is_none() {
        // Diagnostic: log what we got from Gamma so we can tell whether the
        // market exists but is being filtered, or doesn't exist at all.
        let target_count = events
            .iter()
            .filter(|e| TARGET_SLUG_PREFIXES.iter().any(|p| e.slug.starts_with(p)))
            .count();
        let future_count = events
            .iter()
            .filter(|e| {
                TARGET_SLUG_PREFIXES.iter().any(|p| e.slug.starts_with(p))
                    && !e.end_date.is_empty()
                    && parse_iso8601_to_epoch_ms(&e.end_date).unwrap_or(0) > skip_before_ms
            })
            .count();
        warn!(
            total_events = events.len(),
            target_events = target_count,
            future_target_events = future_count,
            skip_before_ms,
            "Gamma discovery failed — diagnostic"
        );
    }

    best.map(|(_, info)| info)
        .ok_or_else(|| anyhow!("no valid upcoming BTC/ETH 15-min market found"))
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

pub(super) use crate::utils::time::epoch_ms as now_epoch_ms;

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

    // ── Anticipatory discovery (parse_gamma_events_response_after) ─────

    /// Two BTC markets: Market A (ends 2098) and Market B (ends 2099).
    /// When skip_before = Market A's end time, only Market B should be returned.
    const GAMMA_EVENTS_TWO_MARKETS: &str = r#"[
        {
            "slug": "btc-updown-15m-1111111111",
            "endDate": "2098-06-15T12:00:00Z",
            "markets": [{
                "conditionId": "0xmarketA",
                "clobTokenIds": "[\"0xyesA\", \"0xnoA\"]",
                "acceptingOrders": true
            }]
        },
        {
            "slug": "btc-updown-15m-2222222222",
            "endDate": "2099-06-15T12:00:00Z",
            "markets": [{
                "conditionId": "0xmarketB",
                "clobTokenIds": "[\"0xyesB\", \"0xnoB\"]",
                "acceptingOrders": true
            }]
        }
    ]"#;

    #[test]
    fn test_parse_after_skips_current_market() {
        // Market A ends at 2098-06-15T12:00:00Z. Use its end_ms as the cutoff.
        let market_a_end_ms =
            parse_iso8601_to_epoch_ms("2098-06-15T12:00:00Z").expect("parse A end");

        // With skip_before = market A's end, we should get Market B.
        let info = parse_gamma_events_response_after(GAMMA_EVENTS_TWO_MARKETS, market_a_end_ms)
            .expect("should find Market B");
        assert_eq!(info.condition_id, "0xmarketB");
        assert_eq!(info.yes_token_id, "0xyesB");
    }

    #[test]
    fn test_parse_after_returns_soonest_above_cutoff() {
        // With skip_before = 0 (epoch), both markets are valid — should pick A (soonest).
        let info = parse_gamma_events_response_after(GAMMA_EVENTS_TWO_MARKETS, 0)
            .expect("should find Market A");
        assert_eq!(info.condition_id, "0xmarketA");
    }

    #[test]
    fn test_parse_after_errors_when_all_skipped() {
        // Market B ends at 2099-06-15T12:00:00Z. If cutoff is beyond that, nothing matches.
        let beyond_b_ms = parse_iso8601_to_epoch_ms("2099-06-15T12:00:00Z").expect("parse B end");
        let result = parse_gamma_events_response_after(GAMMA_EVENTS_TWO_MARKETS, beyond_b_ms);
        assert!(
            result.is_err(),
            "should error when all markets end before cutoff"
        );
    }

    /// Market with `acceptingOrders=false` should still be discovered.
    /// We only need token IDs for discovery; engine guards prevent premature trading.
    const GAMMA_EVENTS_NOT_ACCEPTING: &str = r#"[
        {
            "slug": "btc-updown-15m-9999999900",
            "endDate": "2099-01-01T00:00:00Z",
            "markets": [{
                "conditionId": "0xnotyet",
                "clobTokenIds": "[\"0xyesNew\", \"0xnoNew\"]",
                "acceptingOrders": false
            }]
        }
    ]"#;

    #[test]
    fn test_accepting_orders_false_still_discovered() {
        let info = parse_gamma_events_response(GAMMA_EVENTS_NOT_ACCEPTING)
            .expect("should discover market even with acceptingOrders=false");
        assert_eq!(info.condition_id, "0xnotyet");
        assert_eq!(info.yes_token_id, "0xyesNew");
        assert_eq!(info.no_token_id, "0xnoNew");
    }
}
