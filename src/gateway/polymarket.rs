use anyhow::Result;
use crossbeam_channel::Sender;
use tracing::{info, warn};

use crate::config::Config;
use crate::types::{IngestorEvent, OrderBook, OrderRequest, OrderResponse, OrderStatus};

/// Wrapper around the Polymarket CLOB client SDK.
pub struct PolymarketGateway {
    config: Config,
}

impl PolymarketGateway {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Fetch the current order book for a token from the CLOB REST API.
    pub async fn get_orderbook(&self, token_id: &str) -> Result<OrderBook> {
        // TODO: Use polymarket-client-sdk to fetch order book.
        // let client = ClobClient::new(...);
        // let book = client.get_order_book(token_id).await?;
        info!(token_id, "fetching orderbook from CLOB");
        let _ = &self.config;

        Ok(OrderBook {
            token_id: token_id.to_string(),
            bids: Vec::new(),
            asks: Vec::new(),
            timestamp_ms: now_ms(),
        })
    }

    /// Get the mid-point price for a token.
    pub async fn get_midpoint(&self, token_id: &str) -> Result<rust_decimal::Decimal> {
        // TODO: Use polymarket-client-sdk GET /midpoint
        info!(token_id, "fetching midpoint from CLOB");
        Ok(rust_decimal::Decimal::ZERO)
    }

    /// Get the current price for a token.
    pub async fn get_price(&self, token_id: &str) -> Result<rust_decimal::Decimal> {
        // TODO: Use polymarket-client-sdk GET /price
        info!(token_id, "fetching price from CLOB");
        Ok(rust_decimal::Decimal::ZERO)
    }

    /// Place an order on the CLOB.
    pub async fn place_order(&self, order: &OrderRequest) -> Result<OrderResponse> {
        // TODO: Use polymarket-client-sdk POST /order with EIP-712 signed payload.
        info!(token_id = %order.token_id, side = ?order.side, price = %order.price, size = %order.size, "placing order");
        Ok(OrderResponse {
            order_id: String::new(),
            status: OrderStatus::Placed,
        })
    }

    /// Cancel an active order.
    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        // TODO: Use polymarket-client-sdk DELETE /order
        info!(order_id, "cancelling order");
        Ok(())
    }

    /// Stream order book updates via WebSocket, pushing events to the ingestor channel.
    pub async fn stream_orderbook(&self, token_id: &str, tx: Sender<IngestorEvent>) -> Result<()> {
        // TODO: Use polymarket-client-sdk WebSocket subscription.
        // The SDK manages the WS connection internally.
        // On each update, construct an OrderBook and send via:
        //   tx.send(IngestorEvent::PolymarketBook(book))?;
        info!(token_id, "starting CLOB WebSocket stream");
        let _ = tx;
        warn!("polymarket WS stream not yet implemented");
        Ok(())
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
