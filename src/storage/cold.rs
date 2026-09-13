use anyhow::{Context, Result};
use questdb::ingress::{Buffer, Protocol, Sender as QuestSender, SenderBuilder, TimestampMicros};
use tracing::{info, warn};

use crate::types::BinanceTick;

// ─── Flush Thresholds ─────────────────────────────────────────────────────────

/// Batch size before auto-flushing the `binance_ticks` buffer.
/// With 1/sec downsampling, ~1000 ticks ≈ ~17 minutes of data.
const TICK_FLUSH_THRESHOLD: usize = 1000;

/// Time-based flush interval (ms) — ensures buffered ticks are persisted
/// even at low ingestion rates. 60s is a good balance between durability
/// and TCP overhead.
const TICK_FLUSH_INTERVAL_MS: u64 = 60_000;

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

    // ─── 2. v2_fills ────────────────────────────────────────────────────────────

    /// Record a fill event to the `v2_fills` table. Flushed immediately (low volume).
    pub fn record_fill(&mut self, record: &FillRecord) -> Result<()> {
        self.buffer
            .table("v2_fills")?
            .symbol("condition_id", &record.condition_id)?
            .symbol("side", &record.side)?
            .column_f64("price", record.price)?
            .column_f64("size", record.size)?
            .column_bool("was_taker", record.was_taker)?
            .column_f64("fee", record.fee)?
            .column_f64("pair_cost", record.pair_cost)?
            .column_f64("paired", record.paired)?
            .column_f64("locked_profit", record.locked_profit)?
            .column_f64("fair_value_yes", record.fair_value_yes)?
            .column_f64("strike", record.strike)?
            .at(TimestampMicros::new(record.timestamp_ms as i64 * 1000))?;

        self.sender
            .flush(&mut self.buffer)
            .context("QuestDB flush (v2_fills) failed")?;
        Ok(())
    }

    // ─── 3. v2_market_summaries ───────────────────────────────────────────────

    /// Record end-of-market summary to `v2_market_summaries`. Flushed immediately.
    pub fn record_market_summary(&mut self, record: &MarketSummaryRecord) -> Result<()> {
        self.buffer
            .table("v2_market_summaries")?
            .symbol("condition_id", &record.condition_id)?
            .column_f64("yes_shares", record.yes_shares)?
            .column_f64("no_shares", record.no_shares)?
            .column_f64("yes_avg", record.yes_avg)?
            .column_f64("no_avg", record.no_avg)?
            .column_f64("paired", record.paired)?
            .column_f64("pair_cost", record.pair_cost)?
            .column_f64("locked_profit", record.locked_profit)?
            .column_f64("taker_fees", record.taker_fees)?
            .column_i64("fill_count", record.fill_count)?
            .column_f64("strike", record.strike)?
            .column_f64("final_fv_yes", record.final_fv_yes)?
            .column_f64("unpaired_yes", record.unpaired_yes)?
            .column_f64("unpaired_no", record.unpaired_no)?
            .column_i64("rebalance_count", record.rebalance_count)?
            .at(TimestampMicros::new(record.timestamp_ms as i64 * 1000))?;

        self.sender
            .flush(&mut self.buffer)
            .context("QuestDB flush (v2_market_summaries) failed")?;
        Ok(())
    }

    // ─── 4. v2_risk_scores ─────────────────────────────────────────────────────

    /// Record a risk score snapshot to `v2_risk_scores`. Flushed immediately.
    pub fn record_risk_score(&mut self, record: &RiskScoreRecord) -> Result<()> {
        self.buffer
            .table("v2_risk_scores")?
            .symbol("condition_id", &record.condition_id)?
            .column_f64("rebalance_risk", record.rebalance_risk)?
            .column_f64("dynamic_max_post", record.dynamic_max_post)?
            .column_f64("conviction", record.conviction)?
            .column_f64("time_pressure", record.time_pressure)?
            .column_f64("momentum_alignment", record.momentum_alignment)?
            .column_f64("fv_yes", record.fv_yes)?
            .column_f64("yes_shares", record.yes_shares)?
            .column_f64("no_shares", record.no_shares)?
            .column_f64("paired", record.paired)?
            .at(TimestampMicros::new(record.timestamp_ms as i64 * 1000))?;

        self.sender
            .flush(&mut self.buffer)
            .context("QuestDB flush (v2_risk_scores) failed")?;
        Ok(())
    }

    // ─── 5. Explicit Flush ──────────────────────────────────────────────────────

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

// ─── Record Types ────────────────────────────────────────────────────────────

/// A single fill event for QuestDB recording.
pub struct FillRecord {
    pub condition_id: String,
    pub side: String,
    pub price: f64,
    pub size: f64,
    pub was_taker: bool,
    pub fee: f64,
    pub pair_cost: f64,
    pub paired: f64,
    pub locked_profit: f64,
    pub fair_value_yes: f64,
    pub strike: f64,
    pub timestamp_ms: u64,
}

/// End-of-market summary for QuestDB recording.
pub struct MarketSummaryRecord {
    pub condition_id: String,
    pub yes_shares: f64,
    pub no_shares: f64,
    pub yes_avg: f64,
    pub no_avg: f64,
    pub paired: f64,
    pub pair_cost: f64,
    pub locked_profit: f64,
    pub taker_fees: f64,
    pub fill_count: i64,
    pub strike: f64,
    pub final_fv_yes: f64,
    pub unpaired_yes: f64,
    pub unpaired_no: f64,
    pub rebalance_count: i64,
    pub timestamp_ms: u64,
}

/// Dynamic risk score snapshot for QuestDB recording (every 5s during quoting).
pub struct RiskScoreRecord {
    pub condition_id: String,
    pub rebalance_risk: f64,
    pub dynamic_max_post: f64,
    pub conviction: f64,
    pub time_pressure: f64,
    pub momentum_alignment: f64,
    pub fv_yes: f64,
    pub yes_shares: f64,
    pub no_shares: f64,
    pub paired: f64,
    pub timestamp_ms: u64,
}

// ─── Retention Policy ────────────────────────────────────────────────────────

/// Drop QuestDB partitions older than `days` for the given table.
///
/// Uses QuestDB's HTTP query endpoint (`/exec`) to issue an ALTER TABLE command.
/// This is safe to call even if partitions don't exist (QuestDB ignores the no-op).
pub async fn drop_old_partitions(questdb_http_url: &str, table: &str, days: u32) -> Result<()> {
    let query = format!(
        "ALTER TABLE {table} DROP PARTITION WHERE timestamp < dateadd('d', -{days}, now())"
    );
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{questdb_http_url}/exec"))
        .query(&[("query", &query)])
        .send()
        .await
        .context("QuestDB retention HTTP request failed")?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        // "no partitions" errors are expected for fresh/empty tables
        if !body.contains("no partitions") {
            anyhow::bail!("QuestDB retention query failed: {body}");
        }
    }
    Ok(())
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
