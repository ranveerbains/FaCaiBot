use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

// ─── Direction & Data Source ─────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Up,
    Down,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataSource {
    Binance,
    BinanceFutures,
    PolymarketMarket,
    PolymarketUser,
}

/// Trade lifecycle status as reported by the Polymarket User WS channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeStatus {
    Matched,
    Mined,
    Confirmed,
    Retrying,
    Failed,
    Canceled,
}

// ─── Price Level & Order Book ────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub size: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBook {
    pub asset_id: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub timestamp_ms: u64,
}

impl OrderBook {
    #[allow(dead_code)]
    pub fn best_bid(&self) -> Option<&PriceLevel> {
        self.bids.first()
    }

    pub fn best_ask(&self) -> Option<&PriceLevel> {
        self.asks.first()
    }

}

// ─── Binance Structs ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceTick {
    pub symbol: &'static str,
    pub bid_price: Decimal,
    pub bid_qty: Decimal,
    pub ask_price: Decimal,
    pub ask_qty: Decimal,
    pub timestamp_ms: u64,
}

impl BinanceTick {
    pub fn mid_price(&self) -> Decimal {
        (self.bid_price + self.ask_price) / Decimal::TWO
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceDepth {
    pub symbol: &'static str,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub timestamp_ms: u64,
}

impl BinanceDepth {
    pub fn best_bid(&self) -> Option<&PriceLevel> {
        self.bids.first()
    }

    pub fn best_ask(&self) -> Option<&PriceLevel> {
        self.asks.first()
    }

    pub fn mid_price(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some((bid.price + ask.price) / Decimal::TWO),
            _ => None,
        }
    }

    pub fn obi(&self) -> Option<Decimal> {
        let bid_depth: Decimal = self.bids.iter().map(|l| l.size).sum();
        let ask_depth: Decimal = self.asks.iter().map(|l| l.size).sum();
        let total = bid_depth + ask_depth;
        if total.is_zero() {
            return None;
        }
        Some((bid_depth - ask_depth) / total)
    }
}

// ─── Futures / Spot Trade Types ──────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FuturesAggTrade {
    #[allow(dead_code)]
    pub price: Decimal,
    pub quantity: Decimal,
    pub is_buyer_maker: bool,
    #[allow(dead_code)]
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone)]
pub struct FuturesBookTicker {
    pub bid_price: Decimal,
    #[allow(dead_code)]
    pub bid_qty: Decimal,
    pub ask_price: Decimal,
    #[allow(dead_code)]
    pub ask_qty: Decimal,
    #[allow(dead_code)]
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone)]
pub struct FuturesForceOrder {
    pub side: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone)]
pub struct SpotTrade {
    pub price: Decimal,
    pub quantity: Decimal,
    pub is_buyer_maker: bool,
    pub timestamp_ms: u64,
}

// ─── Market State (v2: simplified) ──────────────────────────────────────────

/// Consolidated view of the current market data.
/// v2: contains only raw market data. Strategy state lives in V2StrategyEngine.
#[derive(Debug, Clone)]
pub struct MarketState {
    // ── Polymarket books ──
    pub poly_book: Option<OrderBook>,
    pub poly_yes_book: Option<OrderBook>,
    pub poly_no_book: Option<OrderBook>,

    // ── Binance ──
    pub binance_price: Option<Decimal>,

    // ── Active market ──
    pub active_condition_id: Option<String>,
    pub active_yes_token_id: Option<String>,
    pub active_no_token_id: Option<String>,
    pub tick_size: Decimal,
    pub market_end_timestamp_ms: u64,

    // ── Staleness ──
    pub last_update_ms: u64,
}

impl MarketState {
    pub fn new() -> Self {
        Self {
            poly_book: None,
            poly_yes_book: None,
            poly_no_book: None,
            binance_price: None,
            active_condition_id: None,
            active_yes_token_id: None,
            active_no_token_id: None,
            tick_size: Decimal::new(1, 2),
            market_end_timestamp_ms: 0,
            last_update_ms: 0,
        }
    }

}

impl Default for MarketState {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Ingestor Event ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum IngestorEvent {
    PolymarketBook(OrderBook),
    PolymarketPriceChange {
        asset_id: String,
        #[allow(dead_code)]
        price: Decimal,
        #[allow(dead_code)]
        size: Decimal,
        #[allow(dead_code)]
        side: crate::types::order::Side,
        #[allow(dead_code)]
        best_bid: Decimal,
        #[allow(dead_code)]
        best_ask: Decimal,
    },
    PolymarketBestBidAsk {
        asset_id: String,
        best_bid: Decimal,
        best_ask: Decimal,
    },
    PolymarketTickSizeChange {
        #[allow(dead_code)]
        asset_id: String,
        #[allow(dead_code)]
        old_tick_size: Decimal,
        new_tick_size: Decimal,
    },
    PolymarketMarketResolved {
        #[allow(dead_code)]
        market: String,
        #[allow(dead_code)]
        winning_asset_id: String,
    },
    TradeStatusUpdate {
        order_id: String,
        status: TradeStatus,
        size_matched: Option<Decimal>,
        #[allow(dead_code)]
        original_size: Option<Decimal>,
    },
    BinanceTick(BinanceTick),
    BinanceDepth(BinanceDepth),
    FuturesAggTrade(FuturesAggTrade),
    FuturesBookTicker(FuturesBookTicker),
    FuturesForceOrder(FuturesForceOrder),
    SpotTrade(SpotTrade),
    MarketRotation {
        condition_id: String,
        yes_token_id: String,
        no_token_id: String,
        end_timestamp_ms: u64,
        tick_size: Decimal,
    },
    HeartbeatStatus { success: bool, latency_ms: u64 },
    WsStatus {
        #[allow(dead_code)]
        source: DataSource,
        #[allow(dead_code)]
        connected: bool,
    },
    Shutdown,
    DrainAndRestart,
    PauseTrading,
    ResumeTrading,
}
