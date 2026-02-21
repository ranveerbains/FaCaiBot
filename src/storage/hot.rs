use anyhow::{Context, Result};
use redis::AsyncCommands;
use tracing::info;

use crate::types::OrderBook;

/// Redis-backed hot storage for fast read/write of volatile state.
pub struct HotStorage {
    client: redis::Client,
}

impl HotStorage {
    pub fn new(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url).context("failed to create Redis client")?;
        info!(url = redis_url, "Redis client created");
        Ok(Self { client })
    }

    /// Cache an order book snapshot, keyed by token ID.
    pub async fn cache_orderbook(&self, book: &OrderBook) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let value = serde_json::to_string(book)?;
        conn.set_ex::<_, _, ()>(&format!("book:{}", book.token_id), value, 30)
            .await?;
        Ok(())
    }

    /// Retrieve the cached order book for a token.
    pub async fn get_orderbook(&self, token_id: &str) -> Result<Option<OrderBook>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let raw: Option<String> = conn.get(format!("book:{token_id}")).await?;
        match raw {
            Some(s) => Ok(Some(serde_json::from_str(&s)?)),
            None => Ok(None),
        }
    }

    /// Store the currently active 15-minute market token ID.
    pub async fn set_active_market(&self, condition_id: &str, token_id: &str) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.set_ex::<_, _, ()>("active:condition_id", condition_id, 960)
            .await?;
        conn.set_ex::<_, _, ()>("active:token_id", token_id, 960)
            .await?;
        Ok(())
    }

    /// Get the currently active market IDs.
    pub async fn get_active_market(&self) -> Result<(Option<String>, Option<String>)> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let cid: Option<String> = conn.get("active:condition_id").await?;
        let tid: Option<String> = conn.get("active:token_id").await?;
        Ok((cid, tid))
    }
}
