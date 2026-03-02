// Telegram reporter — formats and sends simulation alerts via HTTP POST to the
// Telegram Bot API (https://api.telegram.org/bot{token}/sendMessage).
//
// Transport: hyper 1.x + tokio-rustls (direct connection, no legacy Client).
// We establish a TLS connection with tokio-rustls, then use hyper's connection
// API to send a single HTTP/1.1 request per message.
//
// All sends are fire-and-forget: spawned as detached tokio tasks so they never
// block the simulation executor. Failures are logged via `tracing` and silently
// dropped.
//
// Message format: HTML parse_mode (bold via <b>, code via <code>).
// Number conventions: prices 3 dp, percentages 1 dp, USDC amounts 2 dp.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::header::{CONTENT_TYPE, HOST};
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use rust_decimal::Decimal;
use serde_json::json;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tracing::{debug, error, warn};

use crate::control::types::NotifyFlags;
use crate::types::market::OrderBook;
use crate::types::order::TradeSignal;
use crate::types::simulation::{MarketSummary, SessionSummary, SimTrade};

// ─── TelegramReporter ────────────────────────────────────────────────────────

/// Sends formatted messages to a Telegram bot via raw HTTPS POST to the
/// Telegram Bot API. All methods are fire-and-forget: they spawn a detached
/// tokio task and return immediately, never blocking the executor.
#[derive(Clone)]
pub struct TelegramReporter {
    inner: Arc<ReporterInner>,
}

struct ReporterInner {
    bot_token: String,
    chat_id: String,
    /// Shared TLS connector built once; cheap to clone.
    tls_connector: TlsConnector,
    /// Epoch ms of the last dispatched message. Rate limiting: 5s minimum interval.
    last_send_ms: AtomicU64,
    /// Optional notification flags (shared with command listener).
    notify_flags: Option<Arc<NotifyFlags>>,
}

impl TelegramReporter {
    /// Construct a new reporter.
    ///
    /// # Panics
    /// Panics if the TLS root certificate store cannot be built (should not
    /// happen with bundled webpki-roots).
    pub fn new(bot_token: String, chat_id: String) -> Self {
        let root_store = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        let tls_connector = TlsConnector::from(Arc::new(tls_config));

        Self {
            inner: Arc::new(ReporterInner {
                bot_token,
                chat_id,
                tls_connector,
                last_send_ms: AtomicU64::new(0),
                notify_flags: None,
            }),
        }
    }

    /// Builder: attach notification flags for command-driven gating.
    /// Must be called immediately after `new()` before cloning.
    pub fn with_notify_flags(self, flags: Arc<NotifyFlags>) -> Self {
        // Re-create inner with flags set. Safe because we just constructed it.
        match Arc::try_unwrap(self.inner) {
            Ok(old) => Self {
                inner: Arc::new(ReporterInner {
                    bot_token: old.bot_token,
                    chat_id: old.chat_id,
                    tls_connector: old.tls_connector,
                    last_send_ms: old.last_send_ms,
                    notify_flags: Some(flags),
                }),
            },
            Err(arc) => {
                // Fallback: should never happen if called right after new().
                // Clone the fields we need.
                Self {
                    inner: Arc::new(ReporterInner {
                        bot_token: arc.bot_token.clone(),
                        chat_id: arc.chat_id.clone(),
                        tls_connector: arc.tls_connector.clone(),
                        last_send_ms: AtomicU64::new(arc.last_send_ms.load(Ordering::Relaxed)),
                        notify_flags: Some(flags),
                    }),
                }
            }
        }
    }

    // ─── Public API ───────────────────────────────────────────────────────────

    /// Send a startup notice — "FaCaiBot simulation started".
    pub fn send_startup_message(&self) {
        let text = concat!(
            "<b>FaCaiBot simulation started</b>\n\n",
            "Listening for signals on live Binance + Polymarket feeds. ",
            "No real orders will be placed.",
        )
        .to_owned();
        self.fire_and_forget(text);
    }

    /// Send a live-mode startup notice — "FaCaiBot live started".
    pub fn send_live_startup_message(&self) {
        let text = concat!(
            "<b>FaCaiBot live started</b>\n\n",
            "Connected to Binance SBE + Polymarket CLOB. ",
            "Real orders will be placed.",
        )
        .to_owned();
        self.fire_and_forget(text);
    }

    /// Send a generic alert string (kill switch, UMA dispute, etc.).
    pub fn send_alert(&self, msg: &str) {
        let text = format!("<b>ALERT</b>\n\n{}", formatter::escape_html(msg));
        self.fire_and_forget(text);
    }

    /// Tier 1 — real-time opportunity alert (Leg 1 simulated fill).
    ///
    /// Sent immediately when a signal is detected and Leg 1 post-only fill
    /// is simulated against the current orderbook.
    /// Gated by `NotifyFlags::trades_enabled`.
    pub fn send_opportunity_alert(
        &self,
        signal: &TradeSignal,
        leg1_fill_price: Decimal,
        leg1_fill_size: Decimal,
        book: &OrderBook,
    ) {
        if let Some(ref flags) = self.inner.notify_flags {
            if !flags.trades_on() {
                return;
            }
        }
        let text =
            formatter::format_opportunity_alert(signal, leg1_fill_price, leg1_fill_size, book);
        self.fire_critical(text);
    }

    /// Tier 1 — trade completion alert (both legs filled or hedge failed).
    ///
    /// Sent after Leg 2 fill (or emergency taker) completes the paired trade.
    /// Gated by `NotifyFlags::trades_enabled`.
    pub fn send_trade_completed(&self, trade: &SimTrade) {
        if let Some(ref flags) = self.inner.notify_flags {
            if !flags.trades_on() {
                return;
            }
        }
        let text = formatter::format_trade_completed(trade);
        self.fire_critical(text);
    }

    /// Tier 2 — market summary (sent at each 15-min market expiry).
    /// Uses fire_critical() so summaries are never silently dropped by the rate limiter.
    /// Gated by `NotifyFlags::summary_enabled`.
    pub fn send_market_summary(&self, summary: &MarketSummary) {
        if let Some(ref flags) = self.inner.notify_flags {
            if !flags.summary_on() {
                return;
            }
        }
        let text = formatter::format_market_summary(summary);
        self.fire_critical(text);
    }

    /// Tier 3 — session summary (sent hourly and on graceful shutdown).
    pub fn send_session_summary(&self, summary: &SessionSummary) {
        let text = formatter::format_session_summary(summary);
        self.fire_and_forget(text);
    }

    // ─── Internal ─────────────────────────────────────────────────────────────

    /// Spawn a detached tokio task that POSTs `text` to the Telegram sendMessage
    /// endpoint. Never blocks the caller. Errors are logged and dropped.
    fn fire_and_forget(&self, text: String) {
        // Rate limit: minimum 5s between sends to avoid Telegram 429.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let last = self.inner.last_send_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last) < 5_000 {
            debug!("Telegram rate limited — dropping message");
            return;
        }
        self.inner.last_send_ms.store(now_ms, Ordering::Relaxed);

        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            for chunk in split_message(&text) {
                if let Err(e) = post_telegram_message(
                    &inner.tls_connector,
                    &inner.bot_token,
                    &inner.chat_id,
                    &chunk,
                )
                .await
                {
                    error!(error = %e, "Telegram send failed");
                }
            }
        });
    }

    /// Fire a message unconditionally (never dropped). Spaces critical messages
    /// at least 1.5s apart so they don't pile up on Telegram.
    fn fire_critical(&self, text: String) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let last = self.inner.last_send_ms.swap(now_ms, Ordering::Relaxed);
        let gap_ms = now_ms.saturating_sub(last);

        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            // Space critical messages at least 1.5s apart so they don't pile up on Telegram.
            if gap_ms < 1_500 {
                tokio::time::sleep(std::time::Duration::from_millis(1_500 - gap_ms)).await;
            }
            for chunk in split_message(&text) {
                if let Err(e) = post_telegram_message(
                    &inner.tls_connector,
                    &inner.bot_token,
                    &inner.chat_id,
                    &chunk,
                )
                .await
                {
                    error!(error = %e, "Telegram critical send failed");
                }
            }
        });
    }
}

// ─── Message Chunking ─────────────────────────────────────────────────────────

const TELEGRAM_MAX_LEN: usize = 4096;

/// Split a message into chunks that fit within Telegram's 4096-character limit.
/// Splits at newline boundaries where possible to avoid cutting mid-line.
fn split_message(text: &str) -> Vec<String> {
    if text.len() <= TELEGRAM_MAX_LEN {
        return vec![text.to_owned()];
    }

    let mut chunks: Vec<String> = Vec::new();
    let mut current = String::new();

    for line in text.split('\n') {
        // +1 for the '\n' we'd add between lines
        let needed = if current.is_empty() {
            line.len()
        } else {
            current.len() + 1 + line.len()
        };

        if needed > TELEGRAM_MAX_LEN {
            // Flush the current chunk and start a new one.
            if !current.is_empty() {
                chunks.push(current.clone());
                current.clear();
            }
            // If a single line is itself over the limit, hard-truncate it.
            if line.len() > TELEGRAM_MAX_LEN {
                chunks.push(line[..TELEGRAM_MAX_LEN].to_owned());
            } else {
                current.push_str(line);
            }
        } else {
            if !current.is_empty() {
                current.push('\n');
            }
            current.push_str(line);
        }
    }

    if !current.is_empty() {
        chunks.push(current);
    }

    chunks
}

// ─── HTTP Transport ───────────────────────────────────────────────────────────

pub(crate) const TELEGRAM_HOST: &str = "api.telegram.org";
const TELEGRAM_PORT: u16 = 443;

/// Build a TLS connector for Telegram API calls.
pub(crate) fn build_tls_connector() -> TlsConnector {
    let root_store = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    TlsConnector::from(Arc::new(tls_config))
}

/// POST a single JSON message to the Telegram Bot API.
///
/// Opens a fresh TLS connection per call. This is intentionally simple
/// (no connection pooling) — Telegram calls are infrequent (< 1/s) so
/// connection overhead is acceptable.
pub(crate) async fn post_telegram_message(
    tls_connector: &TlsConnector,
    bot_token: &str,
    chat_id: &str,
    text: &str,
) -> anyhow::Result<()> {
    // ── Build JSON body ───────────────────────────────────────────────────────
    let body_json = json!({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": true,
    });
    let body_str = serde_json::to_string(&body_json)?;
    let body_bytes = Bytes::from(body_str);
    let content_len = body_bytes.len();

    // ── Establish TLS connection ───────────────────────────────────────────────
    let tcp = TcpStream::connect((TELEGRAM_HOST, TELEGRAM_PORT)).await?;
    let server_name = rustls::pki_types::ServerName::try_from(TELEGRAM_HOST)
        .map_err(|e| anyhow::anyhow!("invalid server name: {e}"))?
        .to_owned();
    let tls_stream = tls_connector.connect(server_name, tcp).await?;
    let io = TokioIo::new(tls_stream);

    // ── HTTP/1.1 handshake ────────────────────────────────────────────────────
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;

    // Drive the connection in the background.
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            warn!(error = %e, "Telegram HTTP connection error");
        }
    });

    // ── Build and send request ────────────────────────────────────────────────
    let path = format!("/bot{}/sendMessage", bot_token);
    let req = Request::builder()
        .method(Method::POST)
        .uri(&path)
        .header(HOST, TELEGRAM_HOST)
        .header(CONTENT_TYPE, "application/json")
        .header("Content-Length", content_len.to_string())
        .body(Full::new(body_bytes))?;

    let resp = sender.send_request(req).await?;
    let status = resp.status();

    if !status.is_success() {
        let body = resp.into_body().collect().await?.to_bytes();
        let body_text = String::from_utf8_lossy(&body);
        warn!(
            status = %status,
            body = %body_text,
            "Telegram API returned non-2xx response"
        );
        // Non-fatal — reporting is best-effort.
    }

    Ok(())
}

// ─── Message Formatters ──────────────────────────────────────────────────────
//
// All message formatting logic lives in this private inner module.
// The `TelegramReporter` send_* methods delegate here to build HTML strings.
// The transport layer (`post_telegram_message`, `fire_and_forget`) is kept
// completely separate from formatting concerns.

mod formatter {
    use rust_decimal::Decimal;

    use crate::types::market::OrderBook;
    use crate::types::order::TradeSignal;
    use crate::types::simulation::{MarketSummary, SessionSummary, SimTrade};

    // ─── Public formatters (called by TelegramReporter) ──────────────────────

    /// Tier 1 — opportunity detected + Leg 1 simulated fill.
    pub(super) fn format_opportunity_alert(
        signal: &TradeSignal,
        leg1_fill_price: Decimal,
        leg1_fill_size: Decimal,
        book: &OrderBook,
    ) -> String {
        use crate::types::market::Direction;

        let direction_str = match signal.direction {
            Direction::Up => "UP",
            Direction::Down => "DOWN",
        };
        let spike_sign = if signal.direction == Direction::Up {
            "+"
        } else {
            "-"
        };
        let spike_mag_pct = signal.spike_info.magnitude * Decimal::ONE_HUNDRED;
        let tier_label = signal.profit_target_tier.label();
        let profit_target_pct = signal.profit_target_pct * Decimal::ONE_HUNDRED;
        let tick = signal.tick_size;

        // Leg 2 target and break-even, both floored to tick size for display.
        let leg2_target = floor_to_tick(
            Decimal::ONE - signal.profit_target_pct - leg1_fill_price,
            tick,
        );
        let break_even = floor_to_tick(Decimal::ONE - leg1_fill_price, tick);

        // Emergency taker fee per share at the leg2 target price (for reference only).
        let emergency_fee_per_share = compute_taker_fee_per_share(leg2_target);

        // Estimated gross profit in USDC.
        let est_profit_usdc = signal.profit_target_pct * leg1_fill_size;

        let bid_depth = book.total_bid_depth();
        let market_short = short_market_id(&signal.condition_id);

        let leg1_side = match signal.direction {
            Direction::Up => "YES",
            Direction::Down => "NO",
        };
        let leg2_side = match signal.direction {
            Direction::Up => "NO",
            Direction::Down => "YES",
        };

        let bid_context = if signal.bot_contested {
            "Top of book (outbid wall)"
        } else {
            "Top of book"
        };

        format!(
            "<b>--- OPPORTUNITY DETECTED ---</b>\n\n\
            Market: {market}\n\
            Spike: {dir} {sign}{mag:.4}% | {sus}ms sustained\n\
            Confidence: {conf:.2} ({tier}) | Target: {target:.2}%\n\
            Capital: ${alloc:.2}\n\
            \n\
            <b>Leg 1</b>\n\
            Buy {l1_side}  <code>${price:.2}</code> \u{00d7} {size:.2}sh = <code>${l1_total:.2}</code>\n\
            {bid_ctx} | Depth: ${depth:.0}\n\
            \n\
            <b>Leg 2 target</b>\n\
            Buy {l2_side}  <code>${leg2_tgt}</code> | Break-even: ${be}\n\
            Est. profit: {target:.2}% (~<code>${est_usdc:.2}</code>) | FOK fee: ${ef:.4}/sh\n\
            \n\
            Status: Watching for hedge...",
            market = market_short,
            dir = direction_str,
            sign = spike_sign,
            mag = spike_mag_pct,
            sus = signal.spike_info.sustained_ms,
            conf = signal.confidence,
            tier = tier_label,
            target = profit_target_pct,
            alloc = signal.alloc_amount,
            l1_side = leg1_side,
            price = leg1_fill_price,
            size = leg1_fill_size,
            l1_total = leg1_fill_price * leg1_fill_size,
            bid_ctx = bid_context,
            depth = bid_depth,
            l2_side = leg2_side,
            leg2_tgt = leg2_target,
            be = break_even,
            est_usdc = est_profit_usdc,
            ef = emergency_fee_per_share,
        )
    }

    /// Tier 1 — trade completion (both legs simulated).
    pub(super) fn format_trade_completed(trade: &SimTrade) -> String {
        use crate::types::market::Direction;

        let market_short = short_market_id(&trade.market_id);

        let (l1_label, l2_label) = match trade.direction {
            Direction::Up => ("YES", "NO"),
            Direction::Down => ("NO", "YES"),
        };

        let total_cost_usdc = trade.pair_cost * trade.leg1.size;

        let leg2_str = if let Some(ref leg2) = trade.leg2 {
            let erosion_note = if trade.erosion_steps > 0 {
                format!(" (eroded {}\u{00d7})", trade.erosion_steps)
            } else {
                String::new()
            };
            let taker_tag = if trade.adverse_movement_hedge && trade.emergency_maker {
                " [ADVERSE POST-ONLY]".to_owned()
            } else if trade.adverse_movement_hedge {
                " [ADVERSE FOK FALLBACK]".to_owned()
            } else if trade.favorable_taker && trade.emergency_maker {
                " [FAVORABLE POST-ONLY]".to_owned()
            } else if trade.favorable_taker {
                " [FAVORABLE FOK FALLBACK]".to_owned()
            } else if trade.emergency_maker {
                " [EMERGENCY POST-ONLY]".to_owned()
            } else if leg2.was_taker {
                " [FOK FALLBACK]".to_owned()
            } else {
                String::new()
            };
            format!(
                "Leg 2: Buy {side}  <code>${price:.2}</code> \u{00d7} {size:.2}sh{taker}{erosion}",
                side = l2_label,
                price = leg2.price,
                size = leg2.size,
                taker = taker_tag,
                erosion = erosion_note,
            )
        } else {
            "<i>Leg 2: not filled — awaiting resolution</i>".to_owned()
        };

        let profit_line = if trade.leg2_was_taker && !trade.emergency_maker {
            let sign_g = if trade.gross_profit >= Decimal::ZERO {
                "+"
            } else {
                ""
            };
            let sign_n = if trade.net_profit >= Decimal::ZERO {
                "+"
            } else {
                ""
            };
            format!(
                "Gross: {sg}<code>${gross:.2}</code> | Fee: -${fee:.4} | Net: {sn}<code>${net:.2}</code> ({pct:.2}%)",
                sg = sign_g,
                gross = trade.gross_profit,
                fee = trade.taker_fee,
                sn = sign_n,
                net = trade.net_profit,
                pct = trade.profit_pct,
            )
        } else {
            let sign = if trade.net_profit >= Decimal::ZERO {
                "+"
            } else {
                ""
            };
            format!(
                "Profit: {sign}<code>${net:.2}</code> ({pct:.2}%)",
                sign = sign,
                net = trade.net_profit,
                pct = trade.profit_pct,
            )
        };

        format!(
            "<b>--- TRADE COMPLETED ---</b>\n\n\
            Market: {market}\n\
            \n\
            Leg 1: Buy {l1_side}  <code>${l1_price:.2}</code> \u{00d7} {l1_size:.2}sh = <code>${l1_total:.2}</code>\n\
            {leg2}\n\
            \n\
            Pair: <code>${pair:.3}</code>/sh \u{00d7} {l1_size:.2}sh = <code>${total_cost:.2}</code>\n\
            {profit}",
            market = market_short,
            l1_side = l1_label,
            l1_price = trade.leg1.price,
            l1_size = trade.leg1.size,
            l1_total = trade.leg1.price * trade.leg1.size,
            leg2 = leg2_str,
            pair = trade.pair_cost,
            total_cost = total_cost_usdc,
            profit = profit_line,
        )
    }

    /// Tier 2 — per-market summary (sent at each 15-min market expiry).
    pub(super) fn format_market_summary(s: &MarketSummary) -> String {
        let resolution_line = if s.resolution == "pending" {
            format!(
                "Resolution: pending (UMA challenge period: ~{}h remaining)",
                s.uma_hours_remaining.unwrap_or(2),
            )
        } else {
            format!("Resolution: {}", s.resolution)
        };

        let fill_rate_pct = pct_ratio(s.leg1_fills, s.signals_detected);
        let hedge_rate_pct = pct_ratio(s.trades_hedged, s.leg1_fills);

        let taker_fee_label = if s.taker_fees_paid.is_zero() {
            "$0.00 (both legs maker)".to_owned()
        } else {
            format!("${:.4}", s.taker_fees_paid)
        };

        let net_label = if s.taker_fees_paid.is_zero() {
            format!("${:.4} (zero fees in normal flow)", s.net_market_pnl)
        } else {
            format!("${:.4}", s.net_market_pnl)
        };

        let alloc_pct = if s.allocation_cap.is_zero() {
            0.0_f64
        } else {
            dec_to_f64(s.allocation_used / s.allocation_cap * Decimal::ONE_HUNDRED)
        };

        let gross_sign = if s.gross_market_pnl >= Decimal::ZERO {
            "+"
        } else {
            ""
        };

        // Per-trade detail lines.
        let trade_lines: String = s
            .trades
            .iter()
            .enumerate()
            .map(|(i, t)| {
                let leg2_info = if let Some(ref l2) = t.leg2 {
                    let fee_tag = if l2.was_taker { "taker" } else { "maker" };
                    let net_sign = if t.net_profit >= Decimal::ZERO { "+" } else { "" };
                    // pair_cost is per-share; net_profit is USDC
                    format!(
                        "YES@{l1:.3} + NO@{l2:.3} = ${pair:.3}/sh × {sz:.2}sh → maker+{fee} → net {sign}${net:.2} USDC ({pct:.2}%)",
                        l1 = t.leg1.price,
                        l2 = l2.price,
                        pair = t.pair_cost,
                        sz = t.leg1.size,
                        fee = fee_tag,
                        sign = net_sign,
                        net = t.net_profit,
                        pct = t.profit_pct,
                    )
                } else {
                    format!(
                        "YES@{l1:.3} × {sz:.2}sh — unhedged → awaiting resolution",
                        l1 = t.leg1.price,
                        sz = t.leg1.size,
                    )
                };
                format!(
                    "Trade {n}: conf={conf:.2} target={tier} alloc=${alloc:.0} → {detail}",
                    n = i + 1,
                    conf = t.confidence,
                    tier = t.profit_target_tier.label(),
                    alloc = t.alloc_amount,
                    detail = leg2_info,
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        format!(
            "<b>--- MARKET SUMMARY ---</b>\n\n\
            Market: {market}\n\
            {resolution}\n\
            Period: {period}\n\
            \n\
            Signals detected: {sigs}\n\
            Leg 1 fills: {l1_fills} ({fill_rate:.0}% fill rate)\n\
            Hedged: {hedged}/{l1_fills} ({hedge_rate:.0}%)\n\
            Walls outbid: {walls}\n\
            Emergency taker fills: {emergency} (post-only: {emergency_maker})\n\
            Favorable taker fills: {favorable}\n\
            \n\
            {trade_lines}\n\
            \n\
            Allocation used: ${alloc:.0} / ${cap:.0} ({alloc_pct:.0}%)\n\
            Taker fees paid: {fee_label}\n\
            Gross market PnL: {gross_sign}${gross:.4}\n\
            Net market PnL: {net_label}\n\
            Capital locked in resolution: ${locked:.2}",
            market = short_market_id(&s.market_id),
            resolution = resolution_line,
            period = s.period_label,
            sigs = s.signals_detected,
            l1_fills = s.leg1_fills,
            fill_rate = fill_rate_pct,
            hedged = s.trades_hedged,
            hedge_rate = hedge_rate_pct,
            walls = s.walls_outbid,
            emergency = s.emergency_taker_fills,
            emergency_maker = s.emergency_maker_fills,
            favorable = s.favorable_taker_fills,
            trade_lines = trade_lines,
            alloc = s.allocation_used,
            cap = s.allocation_cap,
            alloc_pct = alloc_pct,
            gross_sign = gross_sign,
            gross = s.gross_market_pnl,
            fee_label = taker_fee_label,
            net_label = net_label,
            locked = s.capital_locked,
        )
    }

    /// Tier 3 — session summary (hourly or on shutdown).
    pub(super) fn format_session_summary(s: &SessionSummary) -> String {
        let uptime_h = s.uptime_secs / 3600;
        let uptime_m = (s.uptime_secs % 3600) / 60;

        let fill_rate_pct = pct_ratio(s.leg1_fills, s.signals_detected);
        let hedge_rate_pct = pct_ratio(s.trades_hedged, s.leg1_fills);

        let pnl_sign = if s.net_pnl >= Decimal::ZERO { "+" } else { "" };
        let gross_sign = if s.gross_pnl >= Decimal::ZERO {
            "+"
        } else {
            ""
        };
        let avg_sign = if s.avg_net_profit_pct >= Decimal::ZERO {
            "+"
        } else {
            ""
        };

        let best_label = if s.total_trades > 0 {
            let sign = if s.best_trade_pct >= Decimal::ZERO {
                "+"
            } else {
                ""
            };
            format!(
                "Best trade: {sign}{pct:.1}% (market {mkt}, conf={conf:.2}, both maker)",
                sign = sign,
                pct = s.best_trade_pct,
                mkt = short_market_id(&s.best_trade_market),
                conf = s.best_trade_conf,
            )
        } else {
            "Best trade: n/a".to_owned()
        };

        let worst_label = if s.total_trades > 0 {
            let sign = if s.worst_trade_pct >= Decimal::ZERO {
                "+"
            } else {
                ""
            };
            format!(
                "Worst trade: {sign}{pct:.1}% (market {mkt}, conf={conf:.2})",
                sign = sign,
                pct = s.worst_trade_pct,
                mkt = short_market_id(&s.worst_trade_market),
                conf = s.worst_trade_conf,
            )
        } else {
            "Worst trade: n/a".to_owned()
        };

        let rebate_label = if s.est_maker_rebates.is_zero() {
            "Est. maker rebates: $0.000".to_owned()
        } else {
            format!("Est. maker rebates: ${:.3}", s.est_maker_rebates)
        };

        // HTML-escape the `<0.5` in the low confidence label.
        format!(
            "<b>--- SESSION SUMMARY ({uptime_h}h {uptime_m:02}m) ---</b>\n\n\
            Uptime: {uptime_h}h {uptime_m:02}m\n\
            Markets observed: {mkts}\n\
            \n\
            <b>Fill rate:</b>\n\
              Signals detected: {sigs}\n\
              Leg 1 fills (post-only): {l1_fills} ({fill_rate:.0}% fill rate)\n\
              Hedged: {hedged}/{l1_fills} ({hedge_rate:.0}%)\n\
            \n\
            <b>Smart outbidding:</b>\n\
              Depth walls detected: {walls}\n\
              Walls outbid: {walls}\n\
            \n\
            <b>Emergency exits (Leg 2):</b>\n\
              Adverse movement FOK: {adverse}\n\
              Break-even breach FOK: {break_even}\n\
              Timer/expiry deadline FOK: {timer}\n\
              Emergency post-only (maker): {emergency_maker}\n\
              Favorable taker fills: {favorable}\n\
            \n\
            <b>Allocation:</b>\n\
              High confidence (≥0.8, target 2.5%): {high_n} trades, avg ${high_avg:.0}\n\
              Medium confidence (≥0.5, target 1.5%): {med_n} trades, avg ${med_avg:.0}\n\
              Low confidence (&lt;0.5, target 1.0%): {low_n} trades, avg ${low_avg:.0}\n\
              Avg confidence: {avg_conf:.2}\n\
            \n\
            Gross PnL: {gross_sign}${gross:.3}\n\
            Emergency taker fees: ${fees:.3} ({emergency} emergency fill(s))\n\
            {rebate_label}\n\
            Net PnL: {pnl_sign}${net:.3}\n\
            \n\
            Win rate: {win_rate:.1}% ({hedged}/{l1_fills})\n\
            Average net profit: {avg_sign}{avg:.1}% per trade\n\
            {best}\n\
            {worst}\n\
            \n\
            Unfilled signals: {unfilled}\n\
              - {unfill_po}x post-only bid not matched (normal — zero cost)\n\
              - {unfill_liq}x insufficient liquidity\n\
              - {unfill_sp}x spread too wide\n\
            \n\
            Capital locked in resolution: ${locked:.2}\n\
            Virtual balance: ${balance:.3} (started: ${start:.2})",
            uptime_h = uptime_h,
            uptime_m = uptime_m,
            mkts = s.markets_observed,
            sigs = s.signals_detected,
            l1_fills = s.leg1_fills,
            fill_rate = fill_rate_pct,
            hedged = s.trades_hedged,
            hedge_rate = hedge_rate_pct,
            walls = s.walls_outbid,
            adverse = s.adverse_movement_fok,
            break_even = s.break_even_fok,
            timer = s.timer_deadline_fok,
            emergency_maker = s.emergency_maker_fills,
            favorable = s.favorable_taker_fills,
            high_n = s.high_conf_trades,
            high_avg = s.high_conf_avg_alloc,
            med_n = s.med_conf_trades,
            med_avg = s.med_conf_avg_alloc,
            low_n = s.low_conf_trades,
            low_avg = s.low_conf_avg_alloc,
            avg_conf = s.avg_confidence,
            gross_sign = gross_sign,
            gross = s.gross_pnl,
            fees = s.emergency_taker_fees,
            emergency = s.emergency_taker_fills,
            rebate_label = rebate_label,
            pnl_sign = pnl_sign,
            net = s.net_pnl,
            win_rate = s.win_rate_pct,
            avg_sign = avg_sign,
            avg = s.avg_net_profit_pct,
            best = best_label,
            worst = worst_label,
            unfilled = s.unfilled_signals,
            unfill_po = s.unfilled_post_only,
            unfill_liq = s.unfilled_liquidity,
            unfill_sp = s.unfilled_spread_wide,
            locked = s.capital_locked,
            balance = s.virtual_balance,
            start = s.starting_balance,
        )
    }

    // ─── Formatting Helpers ───────────────────────────────────────────────────

    /// Floor `price` down to the nearest `tick_size` multiple.
    /// Used to ensure displayed Leg 2 target prices are valid tick-aligned values.
    fn floor_to_tick(price: Decimal, tick: Decimal) -> Decimal {
        if tick.is_zero() {
            return price;
        }
        (price / tick).floor() * tick
    }

    /// Compute the taker fee per share at a given price.
    /// `fee = 0.25 * (price * (1 - price))^2`
    fn compute_taker_fee_per_share(price: Decimal) -> Decimal {
        let one = Decimal::ONE;
        let factor = Decimal::new(25, 2); // 0.25
        let inner = price * (one - price);
        factor * inner * inner
    }

    /// Produce a shortened market identifier for display (last 5 chars of condition ID).
    fn short_market_id(market_id: &str) -> String {
        if market_id.len() > 5 {
            let suffix = &market_id[market_id.len() - 5..];
            format!("#{}", suffix)
        } else {
            format!("#{}", market_id)
        }
    }

    /// Percentage ratio: `(numerator / denominator) * 100` as f64.
    /// Returns 0.0 if denominator is 0.
    fn pct_ratio(numerator: u32, denominator: u32) -> f64 {
        if denominator == 0 {
            0.0
        } else {
            numerator as f64 / denominator as f64 * 100.0
        }
    }

    /// Convert a Decimal to f64 for use in format strings (non-critical display only).
    fn dec_to_f64(d: Decimal) -> f64 {
        use rust_decimal::prelude::ToPrimitive;
        d.to_f64().unwrap_or(0.0)
    }

    /// Escape HTML special characters for Telegram HTML parse_mode.
    pub(super) fn escape_html(s: &str) -> String {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    }
}
