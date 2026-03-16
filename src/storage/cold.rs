use anyhow::{Context, Result};
use questdb::ingress::{Buffer, Protocol, Sender as QuestSender, SenderBuilder, TimestampMicros};
use rust_decimal::Decimal;
use tracing::{info, warn};

use crate::types::BinanceTick;
use crate::types::order::ExitReason;

// ─── Flush Thresholds ─────────────────────────────────────────────────────────

/// Batch size before auto-flushing the `binance_ticks` buffer.
/// With 1/sec downsampling, ~1000 ticks ≈ ~17 minutes of data.
const TICK_FLUSH_THRESHOLD: usize = 1000;

/// Time-based flush interval (ms) — ensures buffered ticks are persisted
/// even at low ingestion rates. 60s is a good balance between durability
/// and TCP overhead.
const TICK_FLUSH_INTERVAL_MS: u64 = 60_000;

/// All other tables (book snapshots, signals, trades) flush immediately because
/// they are rare events and timeliness matters more than throughput.

// ─── Default Ports ────────────────────────────────────────────────────────────

/// QuestDB ILP TCP port (ingestion).
const DEFAULT_ILP_PORT: u16 = 9009;

// ─── ColdStorage ──────────────────────────────────────────────────────────────

/// QuestDB-backed cold storage for millisecond-level time-series data.
///
/// Architecture:
/// - A single QuestDB ILP `Sender` writes all tables over one TCP connection.
/// - `binance_ticks`: batched (buffer up to 1000 rows, flush at threshold).
/// - All other tables: flushed immediately after each write (low volume).
/// - Automated partition pruning is handled externally via `prune_old_partitions()`.
pub struct ColdStorage {
    sender: QuestSender,
    /// Shared write buffer — QuestDB ILP allows mixing multiple tables in one buffer.
    buffer: Buffer,
    /// Number of rows in the buffer that belong to `binance_ticks`.
    tick_count: usize,
    /// Epoch ms of last tick buffer flush — for time-based flushing.
    last_flush_ms: u64,
}

impl ColdStorage {
    /// Connect to QuestDB via ILP (InfluxDB Line Protocol) over TCP.
    ///
    /// `questdb_url` should be in the form `"host:port"` (e.g. `"127.0.0.1:9009"`).
    pub fn new(questdb_url: &str) -> Result<Self> {
        let (host, port) = parse_host_port(questdb_url)?;
        let sender = SenderBuilder::new(Protocol::Tcp, host, port)
            .build()
            .context("failed to connect to QuestDB")?;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        info!(url = questdb_url, "QuestDB sender created");
        Ok(Self {
            sender,
            buffer: Buffer::new(),
            tick_count: 0,
            last_flush_ms: now_ms,
        })
    }

    // ─── 1. binance_ticks ─────────────────────────────────────────────────────

    /// Buffer a Binance ticker snapshot into the `binance_ticks` table.
    ///
    /// Schema:
    /// - symbol (symbol): "btcusdt" or "ethusdt"
    /// - bid (f64): best bid price
    /// - ask (f64): best ask price
    /// - mid (f64): mid-price = (bid + ask) / 2
    /// - timestamp (timestamp): tick event time (designated timestamp)
    ///
    /// Auto-flushes when `tick_count` reaches `TICK_FLUSH_THRESHOLD` (1000).
    pub fn record_tick(&mut self, tick: &BinanceTick) -> Result<()> {
        let bid: f64 = tick.bid_price.try_into().unwrap_or(0.0);
        let ask: f64 = tick.ask_price.try_into().unwrap_or(0.0);
        let mid: f64 = tick.mid_price().try_into().unwrap_or(0.0);

        self.buffer
            .table("binance_ticks")?
            .symbol("symbol", &tick.symbol)?
            .column_f64("bid", bid)?
            .column_f64("ask", ask)?
            .column_f64("mid", mid)?
            .column_ts(
                "event_time",
                TimestampMicros::new(tick.timestamp_ms as i64 * 1000),
            )?
            .at_now()?;

        self.tick_count += 1;

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let time_since_flush = now_ms.saturating_sub(self.last_flush_ms);

        if self.tick_count >= TICK_FLUSH_THRESHOLD || time_since_flush >= TICK_FLUSH_INTERVAL_MS {
            self.flush_ticks()?;
        }
        Ok(())
    }

    /// Flush the accumulated `binance_ticks` buffer to QuestDB.
    ///
    /// Called automatically at threshold and on `Drop`.
    /// Note: the buffer may also contain rows from non-tick tables that were
    /// already flushed individually; `tick_count` tracks only pending tick rows.
    pub fn flush_ticks(&mut self) -> Result<()> {
        if self.tick_count == 0 {
            return Ok(());
        }
        self.sender
            .flush(&mut self.buffer)
            .context("QuestDB flush (binance_ticks) failed")?;
        info!(count = self.tick_count, "flushed binance_ticks to QuestDB");
        self.tick_count = 0;
        self.last_flush_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Ok(())
    }

    // ─── 2. poly_book_snapshots ───────────────────────────────────────────────

    /// Write a Polymarket orderbook snapshot to `poly_book_snapshots`.
    ///
    /// Intended to be called every 5 seconds for the active market tokens.
    ///
    /// Schema:
    /// - token_id (symbol): Polymarket YES or NO token ID
    /// - best_bid (f64): best bid price
    /// - best_ask (f64): best ask price
    /// - bid_depth (f64): total bid-side depth (USDC)
    /// - ask_depth (f64): total ask-side depth (USDC)
    /// - spread (f64): spread percentage
    /// - timestamp (designated timestamp): wall-clock time of snapshot
    ///
    /// Flushes immediately (low volume, one row per 5s per token).
    pub fn record_book_snapshot(
        &mut self,
        token_id: &str,
        best_bid: Decimal,
        best_ask: Decimal,
        bid_depth: Decimal,
        ask_depth: Decimal,
        spread: Decimal,
    ) -> Result<()> {
        self.buffer
            .table("poly_book_snapshots")?
            .symbol("token_id", token_id)?
            .column_f64("best_bid", best_bid.try_into().unwrap_or(0.0))?
            .column_f64("best_ask", best_ask.try_into().unwrap_or(0.0))?
            .column_f64("bid_depth", bid_depth.try_into().unwrap_or(0.0))?
            .column_f64("ask_depth", ask_depth.try_into().unwrap_or(0.0))?
            .column_f64("spread", spread.try_into().unwrap_or(0.0))?
            .at_now()?;

        // Flush immediately — low volume, timeliness matters.
        self.sender
            .flush(&mut self.buffer)
            .context("QuestDB flush (poly_book_snapshots) failed")?;
        Ok(())
    }

    // ─── 3. trade_signals ─────────────────────────────────────────────────────

    /// Write a trade signal record to the `trade_signals` table.
    ///
    /// Called for every signal the engine generates — whether it results in an
    /// order or not. The `action` field records the outcome.
    ///
    /// Schema:
    /// - market_id (symbol): Polymarket condition ID
    /// - direction (symbol): "YES" or "NO" (directional entry side)
    /// - confidence (f64): expected repricing percentage [0.0, 1.0]
    /// - spike_magnitude (f64): spike size relative to ATR
    /// - atr (f64): current ATR value at signal time
    /// - book_depth (f64): Polymarket book depth at signal time (USDC)
    /// - time_remaining (i64): seconds to market expiry
    /// - alloc_amount (f64): USDC allocated for this signal
    /// - action (symbol): "entered" | "aborted_spread" | "aborted_liquidity" |
    ///                     "unfilled_postonly" | "skipped_reprice"
    /// - timestamp (designated timestamp): signal generation time
    ///
    /// Flushes immediately.
    pub fn record_signal(
        &mut self,
        market_id: &str,
        direction: &str,
        confidence: Decimal,
        spike_magnitude: Decimal,
        atr: Decimal,
        book_depth: Decimal,
        time_remaining_secs: i64,
        alloc_amount: Decimal,
        action: &str,
        spike_detected_ms: u64,
        composite_score: Decimal,
        // Metric normalized values [0.0, 1.0]
        cvd_norm: f64,
        basis_norm: f64,
        spot_flow_norm: f64,
        obi_norm: f64,
        liq_norm: f64,
        atr_norm: f64,
        // Metric freshness (ms since last update)
        cvd_age_ms: u64,
        basis_age_ms: u64,
        spot_flow_age_ms: u64,
        obi_age_ms: u64,
        liq_age_ms: u64,
        atr_age_ms: u64,
        dissenter_count: u32,
    ) -> Result<()> {
        self.buffer
            .table("trade_signals")?
            .symbol("market_id", market_id)?
            .symbol("direction", direction)?
            .symbol("action", action)?
            .column_f64("confidence", confidence.try_into().unwrap_or(0.0))?
            .column_f64("spike_magnitude", spike_magnitude.try_into().unwrap_or(0.0))?
            .column_f64("atr", atr.try_into().unwrap_or(0.0))?
            .column_f64("book_depth", book_depth.try_into().unwrap_or(0.0))?
            .column_i64("time_remaining", time_remaining_secs)?
            .column_f64("alloc_amount", alloc_amount.try_into().unwrap_or(0.0))?
            .column_i64("spike_detected_ms", spike_detected_ms as i64)?
            .column_f64("composite_score", composite_score.try_into().unwrap_or(0.0))?
            .column_i64("dissenter_count", i64::from(dissenter_count))?
            // Metric normalized values (0.0-1.0)
            .column_f64("cvd_norm", cvd_norm)?
            .column_f64("basis_norm", basis_norm)?
            .column_f64("spot_flow_norm", spot_flow_norm)?
            .column_f64("obi_norm", obi_norm)?
            .column_f64("liq_norm", liq_norm)?
            .column_f64("atr_norm", atr_norm)?
            // Metric freshness (ms since last update)
            .column_i64("cvd_age_ms", cvd_age_ms as i64)?
            .column_i64("basis_age_ms", basis_age_ms as i64)?
            .column_i64("spot_flow_age_ms", spot_flow_age_ms as i64)?
            .column_i64("obi_age_ms", obi_age_ms as i64)?
            .column_i64("liq_age_ms", liq_age_ms as i64)?
            .column_i64("atr_age_ms", atr_age_ms as i64)?
            .at_now()?;

        // Flush immediately — signals are rare, we want them durable right away.
        self.sender
            .flush(&mut self.buffer)
            .context("QuestDB flush (trade_signals) failed")?;
        Ok(())
    }

    // ─── 4. executed_trades ───────────────────────────────────────────────────

    /// Write a completed live trade record to the `executed_trades` table.
    ///
    /// Schema:
    /// - market_id (symbol): Polymarket condition ID
    /// - direction (symbol): "YES" or "NO"
    /// - leg1_price (f64): Leg 1 fill price
    /// - leg2_price (f64): Leg 2 fill price (0.0 if unhedged)
    /// - leg1_size (f64): shares filled on Leg 1
    /// - leg2_size (f64): shares filled on Leg 2 (0.0 if unhedged)
    /// - pair_cost (f64): leg1_price + leg2_price
    /// - gross_profit (f64): 1.0 - pair_cost
    /// - taker_fee (f64): taker fee paid (0 in normal flow; non-zero for emergency FOK)
    /// - net_profit (f64): gross_profit - taker_fee
    /// - profit_pct (f64): net_profit / pair_cost * 100
    /// - confidence (f64): expected repricing percentage
    /// - profit_tier (symbol): "HIGH" | "MED" | "LOW"
    /// - alloc_amount (f64): USDC allocated
    /// - hedge_phase (i64): hedge phase at fill (0=Phase1, 1=Phase2)
    /// - leg2_was_taker (bool): true if Leg 2 used emergency FOK
    /// - bot_contested (bool): true if a competitor depth wall was detected
    /// - favorable_taker (bool): true if Leg 2 filled via favorable taker crossing
    /// - emergency_maker (bool): true if Leg 2 filled as maker during emergency chase
    /// - spike_magnitude (f64): spike size relative to ATR at entry
    /// - maker_rebate (f64): estimated total maker rebate earned (both legs)
    /// - exit_reason (symbol): "NormalHedge" | "BreakEvenBreach" | "Phase2Timeout" |
    ///                         "Phase2PriceBreach" | "MarketExpiry" | "FavorableTaker" | "Phase1Breach" | "WhipsawReversal"
    /// - leg1_order_id (symbol): CLOB order ID for Leg 1
    /// - leg2_order_id (symbol): CLOB order ID for Leg 2 ("" if unhedged)
    /// - timestamp (designated timestamp): Leg 1 fill time
    ///
    /// Flushes immediately — trades are rare, durability matters more than throughput.
    #[allow(clippy::too_many_arguments)]
    pub fn record_trade(
        &mut self,
        market_id: &str,
        direction: &str,
        leg1_price: Decimal,
        leg2_price: Option<Decimal>,
        leg1_size: Decimal,
        leg2_size: Option<Decimal>,
        pair_cost: Decimal,
        gross_profit: Decimal,
        taker_fee: Decimal,
        net_profit: Decimal,
        profit_pct: Decimal,
        confidence: Decimal,
        profit_tier: &str,
        alloc_amount: Decimal,
        hedge_phase: u8,
        leg2_was_taker: bool,
        bot_contested: bool,
        leg1_order_id: &str,
        leg2_order_id: Option<&str>,
        leg1_fill_timestamp_ms: u64,
        exit_reason: Option<ExitReason>,
        favorable_taker: bool,
        emergency_maker: bool,
        spike_magnitude: Decimal,
        maker_rebate: Decimal,
    ) -> Result<()> {
        let leg2_price_f64: f64 = leg2_price.and_then(|d| d.try_into().ok()).unwrap_or(0.0);
        let leg2_size_f64: f64 = leg2_size.and_then(|d| d.try_into().ok()).unwrap_or(0.0);
        let leg2_order_id_str = leg2_order_id.unwrap_or("");

        let exit_reason_str = match exit_reason {
            Some(ExitReason::BreakEvenBreach) => "BreakEvenBreach",
            Some(ExitReason::Phase2Timeout) => "Phase2Timeout",
            Some(ExitReason::Phase2PriceBreach) => "Phase2PriceBreach",
            Some(ExitReason::MarketExpiry) => "MarketExpiry",
            Some(ExitReason::FavorableTaker) => "FavorableTaker",
            Some(ExitReason::Phase1Breach) => "Phase1Breach",
            Some(ExitReason::WhipsawReversal) => "WhipsawReversal",
            Some(ExitReason::FlowCollapse) => "FlowCollapse",
            None => "NormalHedge",
        };

        self.buffer
            .table("executed_trades")?
            .symbol("market_id", market_id)?
            .symbol("direction", direction)?
            .symbol("profit_tier", profit_tier)?
            .symbol("exit_reason", exit_reason_str)?
            .symbol("leg1_order_id", leg1_order_id)?
            .symbol("leg2_order_id", leg2_order_id_str)?
            .column_f64("leg1_price", leg1_price.try_into().unwrap_or(0.0))?
            .column_f64("leg2_price", leg2_price_f64)?
            .column_f64("leg1_size", leg1_size.try_into().unwrap_or(0.0))?
            .column_f64("leg2_size", leg2_size_f64)?
            .column_f64("pair_cost", pair_cost.try_into().unwrap_or(0.0))?
            .column_f64("gross_profit", gross_profit.try_into().unwrap_or(0.0))?
            .column_f64("taker_fee", taker_fee.try_into().unwrap_or(0.0))?
            .column_f64("net_profit", net_profit.try_into().unwrap_or(0.0))?
            .column_f64("profit_pct", profit_pct.try_into().unwrap_or(0.0))?
            .column_f64("confidence", confidence.try_into().unwrap_or(0.0))?
            .column_f64("alloc_amount", alloc_amount.try_into().unwrap_or(0.0))?
            .column_i64("hedge_phase", i64::from(hedge_phase))?
            .column_bool("leg2_was_taker", leg2_was_taker)?
            .column_bool("bot_contested", bot_contested)?
            .column_bool("favorable_taker", favorable_taker)?
            .column_bool("emergency_maker", emergency_maker)?
            .column_f64("spike_magnitude", spike_magnitude.try_into().unwrap_or(0.0))?
            .column_f64("maker_rebate", maker_rebate.try_into().unwrap_or(0.0))?
            .column_ts(
                "leg1_fill_time",
                TimestampMicros::new(leg1_fill_timestamp_ms as i64 * 1000),
            )?
            .at_now()?;

        // Flush immediately — every executed trade is critical data.
        self.sender
            .flush(&mut self.buffer)
            .context("QuestDB flush (executed_trades) failed")?;
        Ok(())
    }

    // ─── 5. Explicit Flush ─────────────────────────────────────────────────────

    /// Flush any remaining buffered `binance_ticks` data.
    ///
    /// Call this on graceful shutdown. Non-tick tables always flush immediately
    /// and have no pending data at this point.
    pub fn flush(&mut self) -> Result<()> {
        self.flush_ticks()
    }
}

impl Drop for ColdStorage {
    fn drop(&mut self) {
        if self.tick_count > 0 {
            if let Err(e) = self.flush_ticks() {
                warn!(error = %e, "failed to flush remaining ticks on drop");
            }
        }
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn parse_host_port(url: &str) -> Result<(String, u16)> {
    match url.rsplit_once(':') {
        Some((host, port_str)) => {
            let port: u16 = port_str.parse().context("invalid port in QuestDB URL")?;
            Ok((host.to_string(), port))
        }
        None => Ok((url.to_string(), DEFAULT_ILP_PORT)),
    }
}
