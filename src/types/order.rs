use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::market::{Direction, OrderBook, SpikeInfo};

// ─── Exit Reason ─────────────────────────────────────────────────────────────

/// Reason for an emergency Leg 2 exit (immediate FOK taker).
///
/// Set by the evaluator when generating emergency signals so the executor
/// can accurately categorize the exit for session statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExitReason {
    /// Opposing ask worsened beyond break-even tolerance.
    BreakEvenBreach,
    /// Market rotation arrived while Leg 1 was filled but Leg 2 incomplete.
    /// Emergency FOK to close the position before state reset.
    MarketExpiry,
    /// Opposing ask dropped below posted bid — market-take at ask price.
    /// Taker fee is acceptable insurance vs leaving Leg 1 unhedged.
    FavorableTaker,
    /// Phase 1 breach: pair cost exceeded threshold before phase transition.
    /// Triggers immediate transition to Phase 2 break-even pursuit.
    Phase1Breach,
    /// Opposite spike detected after Leg 1 fill — immediate FOK hedge, no erosion.
    WhipsawReversal,
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

    /// Determine the tier from expected repricing percentage.
    /// HIGH: pct >= reprice_scale, MED: pct >= reprice_scale/2, LOW: below.
    pub fn from_expected_reprice(pct: Decimal, reprice_scale: Decimal) -> Self {
        let half_scale = reprice_scale / Decimal::TWO;
        if pct >= reprice_scale {
            ProfitTier::High
        } else if pct >= half_scale {
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
    /// Best ask price on the hedge book at signal generation time.
    /// Used by the executor for favorable maker try-first pricing.
    pub best_ask: Option<Decimal>,

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
        /// Condition ID of the new market.
        condition_id: String,
        /// YES token ID for the new market.
        yes_token_id: String,
        /// NO token ID for the new market.
        no_token_id: String,
        /// Tick size for the new market.
        tick_size: Decimal,
        /// Condition ID of the outgoing market (None on first rotation after startup).
        outgoing_condition_id: Option<String>,
        /// End timestamp of the outgoing market (epoch ms). 0 if no prior market.
        outgoing_end_timestamp_ms: u64,
    },
    /// Cancel a stale Leg 1 order that wasn't filled in time.
    CancelLeg1 { order_id: String },
    /// Post a Phase 2 order alongside the existing Phase 1 order (dual-order).
    /// Does NOT cancel Phase 1.
    PostLeg2Phase2 { signal: TradeSignal },
    /// Cancel a specific Leg 2 order by ID (used when one of two dual orders fills).
    CancelLeg2Order { order_id: String },
    /// Rebalance FOK: buy Leg 1 side after a double-fill race condition.
    RebalanceLeg1 { signal: TradeSignal },
    /// Tick size changed mid-market — update SDK cache for both tokens.
    TickSizeChanged {
        yes_token_id: String,
        no_token_id: String,
        new_tick_size: Decimal,
    },
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

// ─── Fill Method ────────────────────────────────────────────────────────

/// How the executor actually filled a Leg 2 order. Set when the executor
/// autonomously converts a normal erosion signal into a favorable exit
/// (e.g., "crosses book" → immediate FOK taker).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillMethod {
    /// "crosses book" → try maker first, then FOK fallback. This variant means
    /// the maker order filled within the favorable_maker_timeout window.
    FavorableMaker,
    /// "crosses book" → immediate FOK taker at the favorable ask.
    FavorableTaker,
    /// Emergency FOK taker fill (deadline FOK or emergency post-only rejected → FOK fallback).
    EmergencyTaker,
}

/// Tag identifying which Leg 2 order a feedback message refers to.
/// Used to route `OrderPosted` and fill events to the correct order slot
/// when two Leg 2 orders rest simultaneously (dual-order Phase 1 + Phase 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderTag {
    /// The initial Phase 1 hedge order (profit target price).
    Leg2Phase1,
    /// The Phase 2 hedge order (ask-1tick, posted alongside Phase 1).
    Leg2Phase2,
    /// Rebalance FOK order after a double-fill race condition.
    Rebalance,
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
        /// How the executor filled this order. `None` for normal post-only orders.
        /// `Some` when the executor autonomously converted to a favorable exit.
        fill_method: Option<FillMethod>,
        /// `true` when a FOK order returned `Filled` synchronously from the REST API.
        /// The engine should skip waiting for User WS MATCHED and transition directly
        /// to `Filled` state. Prevents the double-fill bug where the engine keeps
        /// evaluating and dispatching more FOK signals.
        already_filled: bool,
        /// Identifies which Leg 2 order this feedback refers to (Phase 1, Phase 2,
        /// or Rebalance). `None` for Leg 1 or legacy Leg 2 initial posts.
        order_tag: Option<OrderTag>,
    },
    /// Order placement failed — reset the leg state to `OrderState::None`.
    OrderFailed { is_leg2: bool },
    /// Cancel response from CLOB. `was_cancelled = false` means the order may
    /// have filled before the cancel reached the CLOB.
    CancelResult {
        order_id: String,
        was_cancelled: bool,
        is_leg2: bool,
    },
    /// REST poll detected a Leg 1 fill before the User WS MATCHED event.
    /// Primary fill detection path (~200ms deterministic). User WS is backup.
    RestFillDetected {
        order_id: String,
        price: Decimal,
        size: Decimal,
        size_matched: Decimal,
        original_size: Decimal,
    },
    /// Result of cancelling a specific Leg 2 order (from dual-order system).
    Leg2OrderCancelResult {
        order_id: String,
        was_cancelled: bool,
    },
    /// Result of a rebalance FOK after double-fill race condition.
    RebalanceResult {
        success: bool,
        price: Decimal,
        size: Decimal,
        order_id: Option<String>,
    },
    /// Leg 2 placement failed due to insufficient balance/allowance.
    /// Executor halts further Leg 2 attempts until rotation. Engine sends
    /// a critical Telegram alert with position details.
    BalanceExhausted,
}
