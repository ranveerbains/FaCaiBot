use anyhow::{Context, Result};
use questdb::ingress::{Buffer, Protocol, Sender as QuestSender, SenderBuilder, TimestampMicros};
use tracing::{info, warn};

use crate::types::BinanceTick;

/// Batch size before flushing to QuestDB.
const FLUSH_THRESHOLD: usize = 1000;

/// Default QuestDB ILP port.
const DEFAULT_PORT: u16 = 9009;

/// QuestDB-backed cold storage for millisecond-level tick history.
pub struct ColdStorage {
    sender: QuestSender,
    buffer: Buffer,
    buffered_count: usize,
}

impl ColdStorage {
    /// Connect to QuestDB via ILP (InfluxDB Line Protocol) over TCP.
    ///
    /// `questdb_url` should be in the form "host:port" (e.g. "127.0.0.1:9009").
    pub fn new(questdb_url: &str) -> Result<Self> {
        let (host, port) = parse_host_port(questdb_url)?;
        let sender = SenderBuilder::new(Protocol::Tcp, host, port)
            .build()
            .context("failed to connect to QuestDB")?;
        info!(url = questdb_url, "QuestDB sender created");
        Ok(Self {
            sender,
            buffer: Buffer::new(),
            buffered_count: 0,
        })
    }

    /// Buffer a Binance tick. Automatically flushes at FLUSH_THRESHOLD.
    pub fn record_tick(&mut self, tick: &BinanceTick) -> Result<()> {
        self.buffer
            .table("binance_ticks")?
            .symbol("symbol", &tick.symbol)?
            .column_f64("bid_price", tick.bid_price.try_into().unwrap_or(0.0))?
            .column_f64("ask_price", tick.ask_price.try_into().unwrap_or(0.0))?
            .column_f64("bid_qty", tick.bid_qty.try_into().unwrap_or(0.0))?
            .column_f64("ask_qty", tick.ask_qty.try_into().unwrap_or(0.0))?
            .column_ts(
                "event_time",
                TimestampMicros::new(tick.timestamp_ms as i64 * 1000),
            )?
            .at_now()?;

        self.buffered_count += 1;

        if self.buffered_count >= FLUSH_THRESHOLD {
            self.flush()?;
        }
        Ok(())
    }

    /// Flush buffered data to QuestDB.
    pub fn flush(&mut self) -> Result<()> {
        if self.buffered_count == 0 {
            return Ok(());
        }
        self.sender
            .flush(&mut self.buffer)
            .context("QuestDB flush failed")?;
        info!(count = self.buffered_count, "flushed ticks to QuestDB");
        self.buffered_count = 0;
        Ok(())
    }
}

impl Drop for ColdStorage {
    fn drop(&mut self) {
        if self.buffered_count > 0
            && let Err(e) = self.flush()
        {
            warn!(error = %e, "failed to flush remaining ticks on drop");
        }
    }
}

fn parse_host_port(url: &str) -> Result<(String, u16)> {
    match url.rsplit_once(':') {
        Some((host, port_str)) => {
            let port: u16 = port_str.parse().context("invalid port in QuestDB URL")?;
            Ok((host.to_string(), port))
        }
        None => Ok((url.to_string(), DEFAULT_PORT)),
    }
}
