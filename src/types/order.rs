use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Side of a trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

/// Signal emitted by the strategy engine to the executor.
#[derive(Debug, Clone)]
pub struct TradeSignal {
    pub side: Side,
    pub token_id: String,
    pub price: Decimal,
    pub size: Decimal,
    /// Binance reference price at signal generation time.
    pub reference_price: Decimal,
    pub timestamp_ms: u64,
}

/// Request submitted to the Polymarket CLOB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub token_id: String,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
}

/// Response from the Polymarket CLOB after order placement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResponse {
    pub order_id: String,
    pub status: OrderStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderStatus {
    Placed,
    Filled,
    PartiallyFilled,
    Cancelled,
    Rejected,
}
