use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::market::{Direction, OrderBook, SpikeInfo};

// ─── Exit Reason ─────────────────────────────────────────────────────────────

/// Reason for an emergency Leg 2 exit (post-only first, FOK fallback).
///
/// Set by the evaluator when generating emergency signals so the executor
/// can accurately categorize the exit for session statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExitReason {
    /// Binance price reversed beyond adverse_threshold.
    AdverseMovement,
    /// Opposing ask worsened beyond break-even tolerance.
    BreakEvenBreach,
    /// Market rotation arrived while Leg 1 was filled but Leg 2 incomplete.
    /// Emergency FOK to close the position before state reset.
    MarketExpiry,
    /// Opposing ask dropped below posted bid — market-take at ask price.
    /// Taker fee is acceptable insurance vs leaving Leg 1 unhedged.
    FavorableTaker,
}

// ─── Side ────────────────────────────────────────────────────────────────────

/// Side of a trade on the Polymarket CLOB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

// ─── Order Type ──────────────────────────────────────────────────────────────

/// Order time-in-force / execution type for the Polymarket CLOB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    /// Good-Til-Cancelled — rests on book until filled or cancelled.
    /// Used for normal post-only maker orders.
    Gtc,
    /// Good-Til-Date — rests on book until `expiration` timestamp.
    Gtd,
    /// Fill-Or-Kill — must fill entirely or cancel. Crosses the spread (taker).
    /// Reserved for emergency hedges only.
    Fok,
    /// Fill-And-Kill — fills as much as possible, cancels remainder.
    Fak,
}

// ─── Profit Tier ─────────────────────────────────────────────────────────────

/// Confidence-based profit target tier.
///
/// Determines the initial profit target percentage, erosion step size,
/// and allocation percentage for a trade signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProfitTier {
    /// Confidence >= 0.8 → 2.5% target, 30% allocation.
    High,
    /// Confidence >= 0.5 → 1.5% target, 20% allocation.
    Med,
    /// Confidence < 0.5  → 1.0% target, 10% allocation.
    Low,
}

impl ProfitTier {
    /// Initial profit target as a decimal fraction (e.g., 0.025 for 2.5%).
    #[allow(dead_code)] // used in tests
    pub fn target_pct(&self) -> Decimal {
        match self {
            ProfitTier::High => Decimal::new(25, 3), // 0.025
            ProfitTier::Med => Decimal::new(15, 3),  // 0.015
            ProfitTier::Low => Decimal::new(10, 3),  // 0.010
        }
    }

    /// Determine the tier from a confidence score (0.0–1.0) using configurable thresholds.
    pub fn from_confidence(
        confidence: Decimal,
        high_threshold: Decimal,
        med_threshold: Decimal,
    ) -> Self {
        if confidence >= high_threshold {
            ProfitTier::High
        } else if confidence >= med_threshold {
            ProfitTier::Med
        } else {
            ProfitTier::Low
        }
    }

    /// Human-readable label for logging and Telegram messages.
    pub fn label(&self) -> &'static str {
        match self {
            ProfitTier::High => "HIGH",
            ProfitTier::Med => "MED",
            ProfitTier::Low => "LOW",
        }
    }
}

// ─── Trade Signal ────────────────────────────────────────────────────────────

/// Signal emitted by the Strategy Engine to the Executor layer.
///
/// Contains all information the Executor needs to submit (or simulate) an order.
/// Must be `Send + 'static` for crossbeam channel transport.
#[derive(Debug, Clone)]
pub struct TradeSignal {
    // ── Order parameters ─────────────────────────────────────────────────
    /// Emergency exit reason (set only for Leg 2 emergency signals).
    pub exit_reason: Option<ExitReason>,
    /// Buy or Sell.
    pub side: Side,
    /// Polymarket token ID to trade (YES or NO token).
    pub token_id: String,
    /// Polymarket condition ID (market-level identifier, same for YES and NO tokens).
    pub condition_id: String,
    /// Target price for this order (already rounded to tick_size).
    pub price: Decimal,
    /// Number of shares to trade.
    pub size: Decimal,

    // ── Binance reference ────────────────────────────────────────────────
    /// Binance mid-price at the moment the signal was generated.
    pub reference_price: Decimal,

    // ── Confidence & allocation ──────────────────────────────────────────
    /// Composite confidence score (0.0–1.0), computed from spike/ATR, sustain,
    /// depth, and time remaining.
    pub confidence: Decimal,
    /// Profit target tier derived from confidence.
    pub profit_target_tier: ProfitTier,
    /// Initial profit target as a decimal fraction (e.g., 0.025).
    pub profit_target_pct: Decimal,
    /// USDC amount allocated to this signal (after confidence weighting).
    pub alloc_amount: Decimal,

    // ── Directional context ──────────────────────────────────────────────
    /// Spike direction that triggered this signal.
    pub direction: Direction,
    /// Full spike details (magnitude, sustain duration, etc.).
    pub spike_info: SpikeInfo,

    // ── Leg context ──────────────────────────────────────────────────────
    /// `false` = Leg 1 (directional entry), `true` = Leg 2 (hedge).
    pub is_leg2: bool,
    /// If this is a Leg 2 signal, the fill price of Leg 1. `None` for Leg 1 signals.
    #[allow(dead_code)] // set on Leg 2 signals for QuestDB recording
    pub leg1_fill_price: Option<Decimal>,

    // ── Market context ───────────────────────────────────────────────────
    /// Epoch ms when this signal was generated.
    pub entry_timestamp_ms: u64,
    /// Epoch ms when the market expires.
    pub market_end_timestamp_ms: u64,
    /// Tick size for price rounding.
    pub tick_size: Decimal,

    // ── Analytics flags ──────────────────────────────────────────────────
    /// Current ATR at signal time (for QuestDB analytics). `Decimal::ZERO` if ATR not yet warmed up.
    pub atr: Decimal,
    /// Whether a competitor depth wall was detected during signal generation.
    pub bot_contested: bool,

    // ── Book snapshot ────────────────────────────────────────────────────
    /// Snapshot of the Polymarket orderbook at signal generation time.
    /// Used by the executor to simulate fills without a separate book feed.
    pub book_snapshot: Option<OrderBook>,

    /// Simulation only: `true` when this signal is a confirmed fill from
    /// `advance_simulation()`. Executor records it directly without re-checking.
    pub sim_confirmed_fill: bool,

    /// Simulation only: `true` when this emergency fill crossed the spread
    /// (FOK fallback). `false` when it rested as a post-only maker fill.
    pub sim_was_taker: bool,
}

// ─── Executor Command ────────────────────────────────────────────────────────

/// Commands sent from the Engine layer to the Executor layer.
/// Wraps trade signals plus control events (market rotation).
#[derive(Debug, Clone)]
pub enum ExecutorCommand {
    /// A trade signal (Leg 1 or Leg 2).
    Signal(TradeSignal),
    /// Market rotation — executor should close open positions and send summary.
    MarketRotation {
        /// Condition ID of the market that just rotated.
        condition_id: String,
        /// YES token ID for the new market.
        yes_token_id: String,
        /// NO token ID for the new market.
        no_token_id: String,
        /// Tick size for the new market.
        tick_size: Decimal,
    },
    /// x-minute entry cutoff window entered — executor should send market summary.
    MarketCutoff {
        /// Condition ID of the current market.
        condition_id: String,
        /// Epoch ms when the market ends.
        market_end_ms: u64,
    },
    /// Cancel a stale Leg 1 order that wasn't filled in time.
    CancelLeg1 { order_id: String },
}

// ─── Order Request ───────────────────────────────────────────────────────────

/// Request submitted to the Polymarket CLOB (or simulated).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub token_id: String,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    /// Order time-in-force type. Default: `Gtc`.
    pub order_type: OrderType,
    /// If `true`, the order is rejected (not filled) if it would cross the spread.
    /// Default: `true` for all normal orders. `false` only for emergency FOK fills.
    pub post_only: bool,
    /// Optional expiration timestamp (epoch ms). Used with `Gtd` orders.
    pub expiration: Option<u64>,
}

impl OrderRequest {
    /// Convenience constructor for a standard post-only GTC order.
    pub fn post_only_gtc(token_id: String, side: Side, price: Decimal, size: Decimal) -> Self {
        Self {
            token_id,
            side,
            price,
            size,
            order_type: OrderType::Gtc,
            post_only: true,
            expiration: None,
        }
    }

    /// Convenience constructor for an emergency FOK taker order (crosses spread).
    pub fn emergency_fok(token_id: String, side: Side, price: Decimal, size: Decimal) -> Self {
        Self {
            token_id,
            side,
            price,
            size,
            order_type: OrderType::Fok,
            post_only: false,
            expiration: None,
        }
    }

    /// Convenience constructor for an aggressive post-only GTC order used in
    /// emergency code paths. Functionally identical to `post_only_gtc()` but
    /// named distinctly so call sites communicate intent.
    pub fn aggressive_post_only(
        token_id: String,
        side: Side,
        price: Decimal,
        size: Decimal,
    ) -> Self {
        Self {
            token_id,
            side,
            price,
            size,
            order_type: OrderType::Gtc,
            post_only: true,
            expiration: None,
        }
    }
}

// ─── Order Response ──────────────────────────────────────────────────────────

/// Response from the Polymarket CLOB after order placement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResponse {
    pub order_id: String,
    pub status: OrderStatus,
    /// Epoch ms when the CLOB accepted the order.
    pub timestamp_ms: u64,
}

// ─── Order Status ────────────────────────────────────────────────────────────

/// Status of an order on the Polymarket CLOB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    /// Order accepted and resting on book.
    Placed,
    /// Order fully filled.
    Filled,
    /// Order cancelled (by user or system).
    Cancelled,
    /// Order rejected by the CLOB (e.g., post-only would cross spread, tick size violation).
    Rejected,
}

// ─── Executor Feedback ──────────────────────────────────────────────────

/// Feedback from the live executor to the engine (reverse channel).
///
/// The live executor sends these after placing or failing to place orders
/// on the CLOB, so the engine can update its `OrderState` with the real
/// CLOB order ID (needed for matching User WS `TradeStatusUpdate` events).
#[derive(Debug, Clone)]
pub enum ExecutorFeedback {
    /// CLOB accepted the order — update `OrderState::Posted` with the real order ID.
    OrderPosted {
        is_leg2: bool,
        order_id: String,
        price: Decimal,
        size: Decimal,
    },
    /// Order placement failed — reset the leg state to `OrderState::None`.
    OrderFailed { is_leg2: bool },
    /// Periodic diagnostic snapshot from the live executor (sent every 60s).
    /// Forwarded to the engine so it can be included in the Telegram diagnostic.
    DiagSnapshot {
        placed: u64,
        cancelled: u64,
        failed: u64,
        emergency_foks: u64,
        emergency_makers: u64,
        favorable_takers: u64,
    },
}
