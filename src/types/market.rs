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
    BinanceFutures,
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
    /// Order cancelled by CLOB (heartbeat failure, user cancel, or admin action).
    Canceled,
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
        order_id: String,
        price: Decimal,
        size: Decimal,
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

    /// Order Book Imbalance: `(bid_depth - ask_depth) / (bid_depth + ask_depth)`.
    /// Range [-1, +1]: positive = bid-heavy (bullish), negative = ask-heavy (bearish).
    /// Returns `None` if total depth is zero.
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

/// Aggregated trade event from Binance Futures @aggTrade stream.
#[derive(Debug, Clone)]
pub struct FuturesAggTrade {
    #[allow(dead_code)] // constructed by gateway, used for logging/diagnostics
    pub price: Decimal,
    pub quantity: Decimal,
    pub is_buyer_maker: bool, // false = taker buy (aggressor bought)
    #[allow(dead_code)] // constructed by gateway, used for logging/diagnostics
    pub timestamp_ms: u64,
}

/// Best bid/ask from Binance Futures @bookTicker stream.
#[derive(Debug, Clone)]
pub struct FuturesBookTicker {
    pub bid_price: Decimal,
    #[allow(dead_code)] // constructed by gateway, used for logging/diagnostics
    pub bid_qty: Decimal,
    pub ask_price: Decimal,
    #[allow(dead_code)] // constructed by gateway, used for logging/diagnostics
    pub ask_qty: Decimal,
    #[allow(dead_code)] // constructed by gateway, used for logging/diagnostics
    pub timestamp_ms: u64,
}

/// Forced liquidation event from Binance Futures @forceOrder stream.
#[derive(Debug, Clone)]
pub struct FuturesForceOrder {
    pub side: String, // "SELL" (long liquidated) or "BUY" (short liquidated)
    #[allow(dead_code)] // constructed by gateway, used for logging/diagnostics
    pub price: Decimal,
    pub quantity: Decimal,
    #[allow(dead_code)] // constructed by gateway, used for logging/diagnostics
    pub timestamp_ms: u64,
}

/// Individual trade from Binance Spot SBE @trade stream.
#[derive(Debug, Clone)]
pub struct SpotTrade {
    #[allow(dead_code)] // constructed by gateway, used for logging/diagnostics
    pub price: Decimal,
    pub quantity: Decimal,
    pub is_buyer_maker: bool,
    pub timestamp_ms: u64,
}

// ─── Buildup Info ───────────────────────────────────────────────────────────

/// Buildup signal emitted by the BuildupDetector.
/// Replaces SpikeInfo as the trigger for Leg 1 entry.
#[derive(Debug, Clone)]
pub struct BuildupInfo {
    /// Composite buildup score [0.0, 1.0].
    pub composite_score: Decimal,
    /// Predicted direction of imminent spike.
    pub direction: Direction,
    /// Individual metric values (for diagnostics/logging).
    #[allow(dead_code)] // diagnostic field, will be logged/reported in future phase
    pub cvd_accel: Decimal,
    #[allow(dead_code)] // diagnostic field, will be logged/reported in future phase
    pub spot_flow: Decimal,
    #[allow(dead_code)] // diagnostic field, will be logged/reported in future phase
    pub obi_velocity: Decimal,
    #[allow(dead_code)] // diagnostic field, will be logged/reported in future phase
    pub basis_delta: Decimal,
    #[allow(dead_code)] // diagnostic field, will be logged/reported in future phase
    pub liq_pressure: Decimal,
    pub atr_displacement: Decimal,
    /// Raw ATR displacement ratio (abs_displacement / ema_atr) — same semantics as
    /// SpikeInfo.atr_ratio. Used by the repricing model until Phase 7 reworks it.
    pub signal_atr_ratio: Decimal,
    /// Current Binance order book imbalance [-1, +1] at detection time.
    /// Used by the OBI alignment gate in the evaluator.
    pub obi: Decimal,
    /// Epoch ms when buildup threshold was crossed.
    pub timestamp_ms: u64,
    /// Normalized metric values [0.0, 1.0] at detection time (for tuning diagnostics).
    pub cvd_norm: f64,
    pub basis_norm: f64,
    pub spot_flow_norm: f64,
    pub obi_norm: f64,
    pub liq_norm: f64,
    pub atr_norm: f64,
    /// Metric freshness: age in ms since last update at detection time (for freshness gate tuning).
    pub cvd_age_ms: u64,
    pub basis_age_ms: u64,
    pub spot_flow_age_ms: u64,
    pub obi_age_ms: u64,
    pub liq_age_ms: u64,
    pub atr_age_ms: u64,
}

// ─── Spike Info ──────────────────────────────────────────────────────────────

/// Entry metadata container — synthesized from BuildupInfo for backward compatibility.
/// Carried on TradeSignal, HedgeState, HedgeSnap for reporting and QuestDB recording.
#[derive(Debug, Clone, Copy)]
pub struct SpikeInfo {
    /// Direction at entry time. Used for struct construction only.
    #[allow(dead_code)]
    pub direction: Direction,
    /// Magnitude of the spike as a ratio (e.g., 0.0042 = 0.42%).
    pub magnitude: Decimal,
    /// Deprecated — always 0. Kept for struct compatibility.
    #[allow(dead_code)]
    pub sustained_ms: u64,
    /// Epoch ms when the spike was first detected.
    pub timestamp_ms: u64,
    /// ATR ratio at entry. Synthesized from BuildupInfo.signal_atr_ratio.
    #[allow(dead_code)]
    pub atr_ratio: Decimal,
    /// OBI at entry. Synthesized from BuildupInfo.obi.
    #[allow(dead_code)]
    pub obi: Decimal,
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
    /// Active Polymarket 5-minute market condition ID.
    pub active_condition_id: Option<String>,
    /// Active YES token ID for the current 5-min window.
    pub active_yes_token_id: Option<String>,
    /// Active NO token ID for the current 5-min window.
    pub active_no_token_id: Option<String>,

    // ── Market parameters (cached at rotation) ───────────────────────────
    /// Tick size for the current market token (dynamic, cached once per rotation).
    /// Updated on rare `tick_size_change` WS events.
    pub tick_size: Decimal,
    /// Epoch ms when the current 5-min market expires.
    pub market_end_timestamp_ms: u64,

    // ── Order tracking ───────────────────────────────────────────────────
    /// Current Leg 1 order lifecycle.
    pub leg1_state: OrderState,
    /// Current Leg 2 order lifecycle (populated after Leg 1 fill).
    pub leg2_state: OrderState,
    /// Best ask at the time Leg 1 was posted (for adaptive repost detection).
    /// Cleared on market rotation or when Leg 1 is fully reset.
    pub leg1_posted_ask: Option<Decimal>,

    // ── Volatility ────────────────────────────────────────────────────────
    /// Rolling EMA-ATR (1-minute window, alpha=0.1). `None` until enough data.
    pub atr: Option<Decimal>,

    // ── Buildup detection (Phase 6+) ─────────────────────────────────────
    /// Whether a buildup signal has been detected (replaces `spike_detected` as
    /// the primary entry trigger). Set by BuildupDetector or ATR backstop.
    pub buildup_detected: bool,
    /// Details of the latest buildup signal (cleared on market rotation).
    pub last_buildup: Option<BuildupInfo>,
    /// Current composite buildup score (updated on every event for flow monitoring).
    pub current_composite_score: Decimal,
    /// Current composite direction (updated on every event for flow monitoring).
    pub current_composite_direction: Option<Direction>,
    /// Timestamp of the last composite update.
    pub composite_update_ms: u64,

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
            leg1_posted_ask: None,
            atr: None,
            buildup_detected: false,
            last_buildup: None,
            current_composite_score: Decimal::ZERO,
            current_composite_direction: None,
            composite_update_ms: 0,
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
        /// Shares matched so far (from User WS `size_matched` field).
        size_matched: Option<Decimal>,
        /// Original order size at placement (from User WS `original_size` field).
        original_size: Option<Decimal>,
    },

    // ── Binance ──────────────────────────────────────────────────────────
    /// Best bid/ask update from SBE `@bestBidAsk` stream (real-time).
    BinanceTick(BinanceTick),

    /// Depth snapshot from SBE `@depth20` stream (50ms cadence).
    BinanceDepth(BinanceDepth),

    // ── Binance Futures ──────────────────────────────────────────────────
    /// Futures aggregated trade (for CVD acceleration).
    FuturesAggTrade(FuturesAggTrade),

    /// Futures best bid/ask (for basis delta computation).
    FuturesBookTicker(FuturesBookTicker),

    /// Futures forced liquidation (for liquidation pressure).
    FuturesForceOrder(FuturesForceOrder),

    // ── Binance Spot @trade ─────────────────────────────────────────────
    /// Spot individual trade from SBE @trade (for spot trade flow).
    SpotTrade(SpotTrade),

    // ── Lifecycle ────────────────────────────────────────────────────────
    /// Emitted when the active 5-min market rotates (anticipatory loading complete).
    MarketRotation {
        condition_id: String,
        yes_token_id: String,
        no_token_id: String,
        end_timestamp_ms: u64,
        tick_size: Decimal,
    },

    /// Result of a heartbeat POST to the CLOB.
    HeartbeatStatus { success: bool, latency_ms: u64 },

    /// WebSocket connection status change.
    WsStatus { source: DataSource, connected: bool },

    // ── Control ───────────────────────────────────────────────────────
    /// Graceful shutdown (from /shutdown). Drain open position, then exit 0.
    Shutdown,
    /// Config changed (from /set). Drain open position, then exit 42 for systemd restart.
    DrainAndRestart,
    /// Pause trading (from /stop). Block new entries, complete open Leg 2, stay alive.
    PauseTrading,
    /// Resume trading (from /resume).
    ResumeTrading,
}
