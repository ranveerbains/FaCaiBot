use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

// ─── Direction & Data Source ─────────────────────────────────────────────────

/// Price movement direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Up,
    Down,
}

/// Identifies the origin of a WebSocket or data feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataSource {
    Binance,
    PolymarketMarket,
    PolymarketUser,
}

/// Trade lifecycle status as reported by the Polymarket User WS channel.
/// Progression: Matched → Mined → Confirmed (terminal) | Retrying | Failed (terminal).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeStatus {
    /// Trade matched off-chain by operator, pending on-chain settlement.
    Matched,
    /// Transaction mined on Polygon, awaiting finality.
    Mined,
    /// Trade finalized successfully (terminal).
    Confirmed,
    /// Operator handling resubmission (revert or reorg) — bot should wait.
    Retrying,
    /// Trade failed permanently (terminal) — log and evaluate re-entry.
    Failed,
}

// ─── Order State ─────────────────────────────────────────────────────────────

/// Tracks the lifecycle of a single order (used for Leg 1 / Leg 2 state in Engine).
#[derive(Debug, Clone)]
pub enum OrderState {
    /// No order submitted.
    None,
    /// Order has been posted to the CLOB and is resting on the book.
    Posted {
        order_id: String,
        price: Decimal,
        size: Decimal,
        timestamp_ms: u64,
    },
    /// Order has been fully or partially filled.
    Filled {
        #[allow(dead_code)] // stored for QuestDB trade recording
        order_id: String,
        price: Decimal,
        size: Decimal,
        #[allow(dead_code)] // stored for QuestDB trade recording
        fill_timestamp_ms: u64,
    },
}

// ─── Price Level & Order Book ────────────────────────────────────────────────

/// Snapshot of a single price level in an order book.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub size: Decimal,
}

/// Aggregated order book for a Polymarket token.
///
/// **Invariant**: `bids` are sorted highest-price-first (descending).
/// `asks` are sorted lowest-price-first (ascending).
/// Consumers may rely on `bids[0]` being the best bid and `asks[0]` being the best ask.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBook {
    pub asset_id: String,
    /// Bid levels, sorted highest-price-first (best bid at index 0).
    pub bids: Vec<PriceLevel>,
    /// Ask levels, sorted lowest-price-first (best ask at index 0).
    pub asks: Vec<PriceLevel>,
    pub timestamp_ms: u64,
}

impl OrderBook {
    /// Returns the best (highest) bid, or `None` if the bid side is empty.
    pub fn best_bid(&self) -> Option<&PriceLevel> {
        self.bids.first()
    }

    /// Returns the best (lowest) ask, or `None` if the ask side is empty.
    pub fn best_ask(&self) -> Option<&PriceLevel> {
        self.asks.first()
    }

    /// Total depth (sum of sizes) on the bid side.
    pub fn total_bid_depth(&self) -> Decimal {
        self.bids.iter().map(|l| l.size).sum()
    }

    /// Total depth (sum of sizes) on the ask side.
    pub fn total_ask_depth(&self) -> Decimal {
        self.asks.iter().map(|l| l.size).sum()
    }
}

// ─── Binance Structs ─────────────────────────────────────────────────────────

/// A best bid/ask snapshot from Binance SBE `@bestBidAsk` stream (real-time).
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
    /// Mid-price: `(bid + ask) / 2`.
    pub fn mid_price(&self) -> Decimal {
        (self.bid_price + self.ask_price) / Decimal::TWO
    }
}

/// Top-20 depth snapshot from Binance SBE `@depth20` stream (50ms cadence).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceDepth {
    pub symbol: &'static str,
    /// Bid levels, sorted highest-price-first (best bid at index 0).
    pub bids: Vec<PriceLevel>,
    /// Ask levels, sorted lowest-price-first (best ask at index 0).
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

    /// Mid-price from the depth snapshot.
    pub fn mid_price(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some((bid.price + ask.price) / Decimal::TWO),
            _ => None,
        }
    }
}

// ─── Spike Info ──────────────────────────────────────────────────────────────

/// Describes a detected Binance price spike that survived the sustain + momentum filters.
#[derive(Debug, Clone, Copy)]
pub struct SpikeInfo {
    /// Whether price spiked up or down.
    pub direction: Direction,
    /// Magnitude of the spike as a ratio (e.g., 0.0042 = 0.42%).
    pub magnitude: Decimal,
    /// How long the spike has been sustained (ms).
    pub sustained_ms: u64,
    /// Epoch ms when the spike was first detected.
    pub timestamp_ms: u64,
}

// ─── Market State ────────────────────────────────────────────────────────────

/// Consolidated view of the current market state maintained by the Strategy Engine.
///
/// Updated incrementally via `IngestorEvent` messages. All pricing fields use
/// `Decimal` — never f32/f64.
#[derive(Debug, Clone)]
pub struct MarketState {
    // ── Polymarket book ──────────────────────────────────────────────────
    /// Current Polymarket order book (last received, YES or NO token).
    /// Kept for backward compatibility — use `poly_yes_book`/`poly_no_book` for
    /// direction-aware fill simulation.
    pub poly_book: Option<OrderBook>,
    /// Order book for the active YES token specifically.
    pub poly_yes_book: Option<OrderBook>,
    /// Order book for the active NO token specifically.
    pub poly_no_book: Option<OrderBook>,

    // ── Binance reference ────────────────────────────────────────────────
    /// Latest Binance spot mid-price (reference signal).
    pub binance_price: Option<Decimal>,

    // ── Active market identifiers ────────────────────────────────────────
    /// Active Polymarket 15-minute market condition ID.
    pub active_condition_id: Option<String>,
    /// Active YES token ID for the current 15-min window.
    pub active_yes_token_id: Option<String>,
    /// Active NO token ID for the current 15-min window.
    pub active_no_token_id: Option<String>,

    // ── Market parameters (cached at rotation) ───────────────────────────
    /// Tick size for the current market token (dynamic, cached once per rotation).
    /// Updated on rare `tick_size_change` WS events.
    pub tick_size: Decimal,
    /// Epoch ms when the current 15-min market expires.
    pub market_end_timestamp_ms: u64,

    // ── Order tracking ───────────────────────────────────────────────────
    /// Current Leg 1 order lifecycle.
    pub leg1_state: OrderState,
    /// Current Leg 2 order lifecycle (populated after Leg 1 fill).
    pub leg2_state: OrderState,

    // ── Spike / volatility ───────────────────────────────────────────────
    /// Rolling EMA-ATR (1-minute window, alpha=0.1). `None` until enough data.
    pub atr: Option<Decimal>,
    /// Whether a spike has been detected and survived sustain + momentum filters.
    pub spike_detected: bool,
    /// Details of the latest surviving spike (cleared on market rotation).
    pub last_spike: Option<SpikeInfo>,

    // ── Capital tracking ─────────────────────────────────────────────────
    /// Total USDC allocated in the current market window.
    pub cumulative_used: Decimal,
    /// Available capital = total_capital - locked_in_resolution.
    pub available_capital: Decimal,

    // ── Staleness ────────────────────────────────────────────────────────
    /// Timestamp of the most recent update (epoch ms).
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
            tick_size: Decimal::new(1, 2), // 0.01 default; overwritten at rotation
            market_end_timestamp_ms: 0,
            leg1_state: OrderState::None,
            leg2_state: OrderState::None,
            atr: None,
            spike_detected: false,
            last_spike: None,
            cumulative_used: Decimal::ZERO,
            available_capital: Decimal::ZERO,
            last_update_ms: 0,
        }
    }

    /// Time remaining (ms) until the current market expires.
    /// Returns 0 if `market_end_timestamp_ms` is in the past or not set.
    pub fn time_remaining_ms(&self, now_ms: u64) -> u64 {
        self.market_end_timestamp_ms.saturating_sub(now_ms)
    }

    /// Available allocation for the next signal in the current market:
    /// `min(FIXED_ALLOC, available_capital) - cumulative_used`.
    /// Caller must pass `fixed_alloc` (from config).
    #[allow(dead_code)] // used in tests
    pub fn remaining_alloc(&self, fixed_alloc: Decimal) -> Decimal {
        let cap = fixed_alloc.min(self.available_capital);
        (cap - self.cumulative_used).max(Decimal::ZERO)
    }
}

impl Default for MarketState {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Ingestor Event ──────────────────────────────────────────────────────────

/// Unified inbound event pushed from the Ingestor layer to the Engine layer
/// via a crossbeam SPSC channel.
///
/// Every variant must be `Send + 'static` (required by crossbeam channels).
/// Variants with `String` fields are not `Copy`; prefer cloning where needed.
#[derive(Debug, Clone)]
pub enum IngestorEvent {
    // ── Polymarket Market WS ─────────────────────────────────────────────
    /// Full orderbook snapshot from the `book` channel.
    PolymarketBook(OrderBook),

    /// Individual price level update from `price_change` channel.
    PolymarketPriceChange {
        asset_id: String,
        price: Decimal,
        size: Decimal,
        side: crate::types::order::Side,
        best_bid: Decimal,
        best_ask: Decimal,
    },

    /// Top-of-book update from `best_bid_ask` channel
    /// (requires `custom_feature_enabled: true` on subscription).
    PolymarketBestBidAsk {
        asset_id: String,
        best_bid: Decimal,
        best_ask: Decimal,
    },

    /// Dynamic tick size change at price extremes (>0.96 or <0.04).
    PolymarketTickSizeChange {
        asset_id: String,
        old_tick_size: Decimal,
        new_tick_size: Decimal,
    },

    /// Market resolution event.
    PolymarketMarketResolved {
        /// Condition ID of the resolved market.
        market: String,
        /// Token ID of the winning outcome.
        winning_asset_id: String,
    },

    // ── Polymarket User WS ───────────────────────────────────────────────
    /// Trade status update from the authenticated User WS channel.
    TradeStatusUpdate {
        order_id: String,
        status: TradeStatus,
    },

    // ── Binance ──────────────────────────────────────────────────────────
    /// Best bid/ask update from SBE `@bestBidAsk` stream (real-time).
    BinanceTick(BinanceTick),

    /// Depth snapshot from SBE `@depth20` stream (50ms cadence).
    BinanceDepth(BinanceDepth),

    /// Spike candidate from the Binance spike detector (ATR + magnitude passed).
    /// Emitted immediately on the initial big tick — triggers speculative Leg 1 posting.
    SpikeCandidate(SpikeInfo),

    /// Spike confirmed after sustain + momentum check passed.
    /// Gates sim Leg 1 fills; live mode no-op (fills come from User WS).
    SpikeConfirmed(SpikeInfo),

    /// Spike candidate failed momentum/sustain check — cancel speculative Leg 1.
    SpikeFailed { timestamp_ms: u64 },

    // ── Lifecycle ────────────────────────────────────────────────────────
    /// Emitted when the active 15-min market rotates (anticipatory loading complete).
    MarketRotation {
        condition_id: String,
        yes_token_id: String,
        no_token_id: String,
        end_timestamp_ms: u64,
    },

    /// Result of a heartbeat POST to the CLOB.
    HeartbeatStatus { success: bool, latency_ms: u64 },

    /// WebSocket connection status change.
    WsStatus { source: DataSource, connected: bool },
}
