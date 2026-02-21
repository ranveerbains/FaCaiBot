use anyhow::{Context, Result};
use redis::AsyncCommands;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::types::OrderBook;

// ─── TTL Constants ────────────────────────────────────────────────────────────

/// Orderbook snapshot TTL: 30 seconds.
const TTL_BOOK_SECS: u64 = 30;
/// Active market IDs TTL: 960 seconds (16 minutes — one 15-min window + buffer).
const TTL_ACTIVE_SECS: u64 = 960;
/// Tick size cache TTL: 960 seconds (refreshed at each market rotation).
const TTL_TICK_SECS: u64 = 960;
/// Fee rate cache TTL: 960 seconds (refreshed at each market rotation).
const TTL_FEE_SECS: u64 = 960;
/// Cumulative allocation TTL: 960 seconds (one market window).
const TTL_ALLOC_SECS: u64 = 960;
/// Daily PnL TTL: 86400 seconds (24 hours).
const TTL_DAILY_PNL_SECS: u64 = 86_400;

// ─── Resolution Status ────────────────────────────────────────────────────────

/// Serializable resolution status stored in Redis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolutionStatus {
    /// Resolution state string (e.g., "pending", "proposed", "disputed", "confirmed").
    pub status: String,
    /// Epoch ms when this status was recorded.
    pub timestamp_ms: u64,
    /// Winning outcome if resolved (e.g., "YES" or "NO"). `None` if still pending.
    pub winning_outcome: Option<String>,
}

// ─── HotStorage ───────────────────────────────────────────────────────────────

/// Redis-backed hot storage for fast read/write of volatile state.
///
/// Uses `get_multiplexed_async_connection()` for all operations so that a single
/// underlying TCP connection is shared (multiplexed) across concurrent callers.
pub struct HotStorage {
    client: redis::Client,
}

impl HotStorage {
    pub fn new(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url).context("failed to create Redis client")?;
        info!(url = redis_url, "Redis client created");
        Ok(Self { client })
    }

    // ─── 1. Orderbook Snapshots ───────────────────────────────────────────────

    /// Cache an order book snapshot, keyed by `book:{asset_id}`. TTL: 30s.
    ///
    /// The key uses `asset_id` (= token_id) from the `OrderBook` struct.
    pub async fn cache_orderbook(&self, book: &OrderBook) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let value = serde_json::to_string(book)?;
        conn.set_ex::<_, _, ()>(&format!("book:{}", book.asset_id), value, TTL_BOOK_SECS)
            .await?;
        debug!(token_id = %book.asset_id, "cached orderbook snapshot");
        Ok(())
    }

    /// Retrieve the cached order book for a token. Returns `None` on cache miss.
    pub async fn get_orderbook(&self, token_id: &str) -> Result<Option<OrderBook>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let raw: Option<String> = conn.get(format!("book:{token_id}")).await?;
        match raw {
            Some(s) => Ok(Some(serde_json::from_str(&s)?)),
            None => Ok(None),
        }
    }

    // ─── 2. Active Market ─────────────────────────────────────────────────────

    /// Store the currently active 15-minute market token IDs.
    ///
    /// Keys: `active:condition_id`, `active:yes_token_id`, `active:no_token_id`.
    /// TTL: 960s (one market window + buffer).
    pub async fn set_active_market(
        &self,
        condition_id: &str,
        yes_token_id: &str,
        no_token_id: &str,
    ) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.set_ex::<_, _, ()>("active:condition_id", condition_id, TTL_ACTIVE_SECS)
            .await?;
        conn.set_ex::<_, _, ()>("active:yes_token_id", yes_token_id, TTL_ACTIVE_SECS)
            .await?;
        conn.set_ex::<_, _, ()>("active:no_token_id", no_token_id, TTL_ACTIVE_SECS)
            .await?;
        info!(
            condition_id,
            yes_token_id, no_token_id, "set active market in Redis"
        );
        Ok(())
    }

    /// Get the currently active market IDs.
    ///
    /// Returns `(condition_id, yes_token_id, no_token_id)`.
    /// Any element is `None` on a cache miss (e.g., after a Redis restart).
    pub async fn get_active_market(
        &self,
    ) -> Result<(Option<String>, Option<String>, Option<String>)> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let cid: Option<String> = conn.get("active:condition_id").await?;
        let yes: Option<String> = conn.get("active:yes_token_id").await?;
        let no: Option<String> = conn.get("active:no_token_id").await?;
        Ok((cid, yes, no))
    }

    // ─── 3. Resolution Tracking ───────────────────────────────────────────────

    /// Persist the resolution status for a market.
    ///
    /// Key: `resolution:{condition_id}`. No TTL — resolution data is kept until
    /// the operator manually flushes (or until the key is overwritten on next update).
    pub async fn set_resolution_status(
        &self,
        condition_id: &str,
        status: &str,
        timestamp_ms: u64,
        winning_outcome: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let record = ResolutionStatus {
            status: status.to_string(),
            timestamp_ms,
            winning_outcome: winning_outcome.map(|s| s.to_string()),
        };
        let value = serde_json::to_string(&record)?;
        // No TTL — keep until explicitly overwritten or flushed.
        conn.set::<_, _, ()>(format!("resolution:{condition_id}"), value)
            .await?;
        info!(condition_id, status, "updated resolution status");
        Ok(())
    }

    /// Retrieve the cached resolution status for a market.
    /// Returns `None` on cache miss.
    pub async fn get_resolution_status(
        &self,
        condition_id: &str,
    ) -> Result<Option<ResolutionStatus>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let raw: Option<String> = conn.get(format!("resolution:{condition_id}")).await?;
        match raw {
            Some(s) => Ok(Some(serde_json::from_str(&s)?)),
            None => Ok(None),
        }
    }

    // ─── 4. Cumulative Allocation ─────────────────────────────────────────────

    /// Get the cumulative USDC used so far in the current market window.
    ///
    /// Key: `alloc:{condition_id}`. Returns `Decimal::ZERO` on cache miss.
    pub async fn get_cumulative_used(&self, condition_id: &str) -> Result<Decimal> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let raw: Option<String> = conn.get(format!("alloc:{condition_id}")).await?;
        match raw {
            Some(s) => {
                let val: Decimal = s
                    .parse()
                    .context("failed to parse cumulative_used from Redis")?;
                Ok(val)
            }
            None => Ok(Decimal::ZERO),
        }
    }

    /// Atomically add `amount` to the cumulative allocation for this market.
    ///
    /// Uses a GET-then-SET pattern (acceptable at our low write frequency).
    /// Refreshes TTL on every write to ensure the key outlives the market window.
    pub async fn add_cumulative_used(&self, condition_id: &str, amount: Decimal) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = format!("alloc:{condition_id}");
        let raw: Option<String> = conn.get(&key).await?;
        let current: Decimal = match raw {
            Some(s) => s.parse().unwrap_or(Decimal::ZERO),
            None => Decimal::ZERO,
        };
        let updated = current + amount;
        conn.set_ex::<_, _, ()>(&key, updated.to_string(), TTL_ALLOC_SECS)
            .await?;
        debug!(condition_id, %updated, "updated cumulative allocation");
        Ok(())
    }

    /// Reset the cumulative allocation counter for a market (called on market rotation).
    pub async fn reset_cumulative_used(&self, condition_id: &str) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.del::<_, ()>(format!("alloc:{condition_id}")).await?;
        info!(
            condition_id,
            "reset cumulative allocation for market rotation"
        );
        Ok(())
    }

    // ─── 5. Tick Size Cache ───────────────────────────────────────────────────

    /// Cache the tick size for a token, fetched once per market rotation.
    ///
    /// Key: `tick:{token_id}`. TTL: 960s.
    /// Only refreshed on rare `tick_size_change` WS events at price extremes.
    pub async fn set_tick_size(&self, token_id: &str, tick_size: Decimal) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.set_ex::<_, _, ()>(
            format!("tick:{token_id}"),
            tick_size.to_string(),
            TTL_TICK_SECS,
        )
        .await?;
        debug!(token_id, %tick_size, "cached tick size");
        Ok(())
    }

    /// Retrieve the cached tick size for a token. Returns `None` on cache miss.
    pub async fn get_tick_size(&self, token_id: &str) -> Result<Option<Decimal>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let raw: Option<String> = conn.get(format!("tick:{token_id}")).await?;
        match raw {
            Some(s) => {
                let val: Decimal = s.parse().context("failed to parse tick_size from Redis")?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    // ─── 6. Fee Rate Cache ────────────────────────────────────────────────────

    /// Cache the taker fee rate (in basis points) for a token, fetched once per rotation.
    ///
    /// Key: `fee:{token_id}`. TTL: 960s.
    /// Used only for emergency taker fee calculations (Leg 2 FOK fills).
    pub async fn set_fee_rate(&self, token_id: &str, fee_rate_bps: u16) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.set_ex::<_, _, ()>(
            format!("fee:{token_id}"),
            fee_rate_bps.to_string(),
            TTL_FEE_SECS,
        )
        .await?;
        debug!(token_id, fee_rate_bps, "cached fee rate");
        Ok(())
    }

    /// Retrieve the cached taker fee rate (bps) for a token. Returns `None` on cache miss.
    pub async fn get_fee_rate(&self, token_id: &str) -> Result<Option<u16>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let raw: Option<String> = conn.get(format!("fee:{token_id}")).await?;
        match raw {
            Some(s) => {
                let val: u16 = s
                    .parse()
                    .context("failed to parse fee_rate_bps from Redis")?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    // ─── 7. Daily PnL Tracking ────────────────────────────────────────────────

    /// Get the running daily PnL (USDC, positive = profit). Returns `Decimal::ZERO` on miss.
    ///
    /// Key: `daily_pnl`. TTL: 86400s (auto-expires at end of day).
    pub async fn get_daily_pnl(&self) -> Result<Decimal> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let raw: Option<String> = conn.get("daily_pnl").await?;
        match raw {
            Some(s) => {
                let val: Decimal = s.parse().context("failed to parse daily_pnl from Redis")?;
                Ok(val)
            }
            None => Ok(Decimal::ZERO),
        }
    }

    /// Add `amount` (positive for profit, negative for loss) to the daily PnL.
    ///
    /// Refreshes TTL on each write so the key survives intraday restarts.
    pub async fn add_daily_pnl(&self, amount: Decimal) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let raw: Option<String> = conn.get("daily_pnl").await?;
        let current: Decimal = match raw {
            Some(s) => s.parse().unwrap_or(Decimal::ZERO),
            None => Decimal::ZERO,
        };
        let updated = current + amount;
        conn.set_ex::<_, _, ()>("daily_pnl", updated.to_string(), TTL_DAILY_PNL_SECS)
            .await?;
        debug!(%updated, "updated daily PnL");
        Ok(())
    }

    /// Reset the daily PnL counter (call at UTC midnight or on session start).
    pub async fn reset_daily_pnl(&self) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.del::<_, ()>("daily_pnl").await?;
        info!("reset daily PnL");
        Ok(())
    }
}
