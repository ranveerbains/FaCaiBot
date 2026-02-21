use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Snapshot of a single price level in an order book.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub size: Decimal,
}

/// Aggregated order book for a Polymarket token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBook {
    pub token_id: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub timestamp_ms: u64,
}

impl OrderBook {
    pub fn best_bid(&self) -> Option<&PriceLevel> {
        self.bids.first()
    }

    pub fn best_ask(&self) -> Option<&PriceLevel> {
        self.asks.first()
    }

    pub fn spread(&self) -> Option<Decimal> {
        match (self.best_ask(), self.best_bid()) {
            (Some(ask), Some(bid)) => Some(ask.price - bid.price),
            _ => None,
        }
    }
}

/// A tick from Binance spot feeds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceTick {
    pub symbol: String,
    pub bid_price: Decimal,
    pub bid_qty: Decimal,
    pub ask_price: Decimal,
    pub ask_qty: Decimal,
    pub timestamp_ms: u64,
}

/// Consolidated view of the current market state for the strategy engine.
#[derive(Debug, Clone)]
pub struct MarketState {
    /// Current Polymarket order book for the active 15m YES token.
    pub poly_book: Option<OrderBook>,
    /// Latest Binance spot price (reference).
    pub binance_price: Option<Decimal>,
    /// Active Polymarket 15-minute market condition ID.
    pub active_condition_id: Option<String>,
    /// Active YES token ID for the current 15m window.
    pub active_token_id: Option<String>,
    /// Timestamp of last update (epoch ms).
    pub last_update_ms: u64,
}

impl MarketState {
    pub fn new() -> Self {
        Self {
            poly_book: None,
            binance_price: None,
            active_condition_id: None,
            active_token_id: None,
            last_update_ms: 0,
        }
    }
}

impl Default for MarketState {
    fn default() -> Self {
        Self::new()
    }
}

/// Unified inbound event pushed from the ingestor to the engine.
#[derive(Debug, Clone)]
pub enum IngestorEvent {
    PolymarketBook(OrderBook),
    BinanceTick(BinanceTick),
    MarketRotation {
        condition_id: String,
        token_id: String,
    },
}
