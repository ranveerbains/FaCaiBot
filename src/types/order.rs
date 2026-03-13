use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::market::{BuildupInfo, Direction, OrderBook, SpikeInfo};

// ─── Exit Reason ─────────────────────────────────────────────────────────────

/// Reason for an emergency Leg 2 exit (immediate FOK taker).
///
/// Set by the evaluator when generating emergency signals so the executor
/// can accurately categorize the exit for session statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExitReason {
    /// Phase 2 entry guard: ask-1tick > breakeven at Phase 2 transition.
    BreakEvenBreach,
    /// Phase 2 resting timed out — FOK at ask.
    Phase2Timeout,
    /// Phase 2 breach: ask rose above Phase 2 posted price (order staleness).
    Phase2PriceBreach,
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
    /// Composite score dropped below cancel threshold while Leg 2 in Phase 1.
    FlowCollapse,
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
}

// ─── Profit Tier ─────────────────────────────────────────────────────────────

/// Repricing-model profit target tier (display-only label).
///
/// Determined by `from_expected_reprice()` based on the repricing model output.
/// Used for Telegram reporting and QuestDB analytics — does not affect execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProfitTier {
    /// expected_pct >= reprice_scale → HIGH tier.
    High,
    /// expected_pct >= reprice_scale/2 → MED tier.
    Med,
    /// expected_pct < reprice_scale/2 → LOW tier.
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
    #[allow(dead_code)] // will be used for analytics/logging in future phase
    pub reference_price: Decimal,

    // ── Repricing & allocation ──────────────────────────────────────────
    /// Expected repricing percentage from the repricing model.
    pub expected_pct: Decimal,
    /// Profit target tier derived from expected repricing.
    pub profit_target_tier: ProfitTier,
    /// Initial profit target as a decimal fraction (e.g., 0.025).
    pub profit_target_pct: Decimal,
    /// USDC amount allocated to this signal (after repricing-scaled allocation).
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

    /// Leg 1 fee per share. Negative = maker rebate (post-only Leg 1). Leg 1 is always maker.
    /// Used by init_leg2() for breakeven computation.
    pub leg1_fee: Decimal,

    // ── Book snapshot ────────────────────────────────────────────────────
    /// Best ask price on the hedge book at signal generation time.
    /// Used by the executor for favorable maker try-first pricing.
    pub best_ask: Option<Decimal>,

    /// Snapshot of the Polymarket orderbook at signal generation time.
    /// Used by the executor to simulate fills without a separate book feed.
    pub book_snapshot: Option<OrderBook>,

    /// Buildup detector snapshot at signal generation time (normalized metrics + ages).
    /// Used by the executor for QuestDB analytics recording.
    pub buildup_info: Option<BuildupInfo>,

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
        #[allow(dead_code)] // will be used by executor for position reconciliation (future phase)
        outgoing_condition_id: Option<String>,
        /// End timestamp of the outgoing market (epoch ms). 0 if no prior market.
        #[allow(dead_code)] // will be used by executor for position reconciliation (future phase)
        outgoing_end_timestamp_ms: u64,
    },
    /// Post a Phase 2 order alongside the existing Phase 1 order (dual-order).
    /// Does NOT cancel Phase 1.
    PostLeg2Phase2 { signal: TradeSignal },
    /// Cancel a specific Leg 2 order by ID (used when one of two dual orders fills).
    CancelLeg2Order { order_id: String },
    /// Rebalance FOK: buy Leg 1 side after a double-fill race condition.
    RebalanceLeg1 { signal: TradeSignal },
    /// Cancel unfilled Leg 1 maker order (flow-based sustain failure or timeout).
    /// Engine tracks repost intent internally via `leg1_last_cancel_repost`.
    CancelLeg1Order { order_id: String },
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
    /// Shares actually filled, from SDK `taking_amount` (taker) or `making_amount` (maker).
    /// Zero if the order went on the book without immediate fill.
    pub size_matched: Decimal,
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

// ─── Fill Info (replaces SimFill) ────────────────────────────────────────────

/// A single fill (either Leg 1 or Leg 2) for live trade reporting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillInfo {
    /// Which side was traded (Buy YES or Buy NO).
    pub side: Side,
    /// Fill price.
    pub price: Decimal,
    /// Number of shares filled.
    pub size: Decimal,
    /// Epoch ms when the fill occurred.
    pub timestamp_ms: u64,
    /// Whether this was a partial fill (remaining size could not be matched).
    pub was_partial: bool,
    /// `true` if the fill crossed the spread (taker).
    pub was_taker: bool,
    /// Taker fee paid. `Decimal::ZERO` for maker fills.
    pub taker_fee: Decimal,
    /// Estimated maker rebate earned. `Decimal::ZERO` for taker fills.
    pub maker_rebate: Decimal,
}

// ─── Live Trade Report (replaces SimTrade) ───────────────────────────────────

/// A completed trade with full PnL accounting.
/// Used for Telegram reporting, QuestDB recording, and market/session summaries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveTradeReport {
    /// Polymarket condition ID.
    pub market_id: String,
    /// Spike direction (Up/Down → YES/NO entry).
    pub direction: Direction,
    /// Leg 1 fill details.
    pub leg1: FillInfo,
    /// Leg 2 fill details. `None` if unhedged at expiry.
    pub leg2: Option<FillInfo>,

    // ── Signal context ───────────────────────────────────────────────────
    /// Expected repricing percentage that generated this trade.
    pub expected_pct: Decimal,
    /// Profit target tier (HIGH / MED / LOW).
    pub profit_target_tier: ProfitTier,
    /// USDC allocated to this trade.
    pub alloc_amount: Decimal,

    // ── PnL ──────────────────────────────────────────────────────────────
    /// `leg1.price + leg2.price` per share (or just `leg1.price` if unhedged).
    pub pair_cost: Decimal,
    /// `(1.0 - pair_cost) * size` in USDC. Negative if pair cost > 1.0.
    pub gross_profit: Decimal,
    /// Taker fee paid on Leg 2 in USDC (0 in normal flow; non-zero only for emergency taker).
    pub taker_fee: Decimal,
    /// Estimated maker rebate earned on this trade (sum of both legs' maker rebates).
    pub maker_rebate: Decimal,
    /// `gross_profit - taker_fee + maker_rebate` in USDC.
    pub net_profit: Decimal,
    /// `net_profit / (pair_cost * size) * 100` — return on capital deployed.
    pub profit_pct: Decimal,

    // ── Execution metadata ───────────────────────────────────────────────
    /// Hedge phase at trade close: 0=Phase1, 1=Phase2.
    pub hedge_phase: u8,
    /// Whether Leg 2 executed as an emergency taker (FOK).
    pub leg2_was_taker: bool,
    /// Whether a competitor depth wall was detected during this trade.
    pub bot_contested: bool,
    /// Whether Leg 2 filled as a favorable taker (ask < posted bid).
    pub favorable_taker: bool,
    /// Whether Leg 2 filled via the favorable maker try-first path (no taker fee + rebate).
    pub favorable_maker: bool,
    /// Whether Leg 2 was an emergency exit that filled as post-only maker (zero fee).
    pub emergency_maker: bool,
    /// Why the trade exited. `None` for normal erosion fills.
    pub exit_reason: Option<ExitReason>,
    /// Spike magnitude that triggered this trade (ratio, e.g., 0.005 = 0.5%).
    pub spike_magnitude: Decimal,
    /// Deprecated — always `false`. Kept for QuestDB backward compatibility.
    pub leg1_cancel_race: bool,
    /// Whether Leg 2 was triggered by Phase 1 breach (pair cost exceeded threshold).
    pub phase1_breach: bool,
    /// Whether Leg 2 filled from the Phase 1 order while Phase 2 was also active (dual-order).
    pub phase1_dual_fill: bool,
    /// Whether Leg 2 was triggered by whipsaw reversal (opposite spike → immediate FOK).
    pub whipsaw_reversal: bool,

    // ── Timestamps ───────────────────────────────────────────────────────
    /// Epoch ms when the trade was opened (Leg 1 fill).
    pub open_timestamp_ms: u64,
    /// Epoch ms when the trade was closed (Leg 2 fill or market expiry).
    pub close_timestamp_ms: u64,
}

// ─── Market Summary ──────────────────────────────────────────────────────────

/// Aggregated stats for a single 5-minute market window.
/// Used for Telegram reporting.
#[derive(Debug, Clone)]
pub struct MarketSummary {
    /// Polymarket condition ID.
    pub market_id: String,
    /// UTC period string (e.g. "12:30 - 12:45 UTC").
    pub period_label: String,
    /// Resolution outcome: "YES", "NO", or "pending".
    pub resolution: String,
    /// Estimated time remaining in UMA challenge period (hours), None if resolved.
    pub uma_hours_remaining: Option<u32>,
    /// Total signals generated by the engine in this market.
    pub signals_detected: u32,
    /// Leg 1 fills in this market.
    pub leg1_fills: u32,
    /// Successfully hedged trades.
    pub trades_hedged: u32,
    /// Total trades (filled Leg 1, whether or not hedged).
    #[allow(dead_code)] // set for Telegram reporting
    pub total_trades: u32,
    /// Smart outbidding events in this market.
    pub walls_outbid: u32,
    /// Emergency taker fills in this market.
    pub emergency_taker_fills: u32,
    /// Emergency post-only maker fills in this market (zero fee).
    pub emergency_maker_fills: u32,
    /// Favorable taker fills in this market (ask < posted bid).
    pub favorable_taker_fills: u32,
    /// Favorable maker fills in this market (try-maker-first succeeded).
    pub favorable_maker_fills: u32,
    /// All closed trades in this market (for detail lines).
    pub trades: Vec<LiveTradeReport>,
    /// Total USDC allocated this market.
    pub allocation_used: Decimal,
    /// FIXED_ALLOC cap for this session.
    pub allocation_cap: Decimal,
    /// Taker fees paid in this market.
    pub taker_fees_paid: Decimal,
    /// Estimated maker rebates earned in this market.
    pub maker_rebates_earned: Decimal,
    /// Gross PnL for this market (sum of gross_profit).
    pub gross_market_pnl: Decimal,
    /// Net PnL for this market (sum of net_profit).
    pub net_market_pnl: Decimal,
    /// Capital still locked in UMA resolution from this market.
    pub capital_locked: Decimal,
}

// ─── Session Summary ─────────────────────────────────────────────────────────

/// Aggregated stats for the entire session so far.
/// Used for Telegram reporting (session shutdown / hourly).
#[derive(Debug, Clone)]
pub struct SessionSummary {
    /// Uptime in seconds since session_start.
    pub uptime_secs: u64,
    /// Number of 5-min markets observed.
    pub markets_observed: u32,
    /// Total signals generated.
    pub signals_detected: u32,
    /// Leg 1 fills (post-only filled).
    pub leg1_fills: u32,
    /// Trades where Leg 2 hedge filled.
    pub trades_hedged: u32,
    /// Total closed trades.
    pub total_trades: u32,
    /// Smart outbidding events.
    pub walls_outbid: u32,
    /// Emergency taker fills breakdown: price breach FOKs (BE, Phase2PriceBreach, Phase1Breach).
    pub breach_fok: u32,
    /// Emergency taker fills breakdown: timeout FOKs (Phase2Timeout, MarketExpiry).
    pub timeout_fok: u32,
    /// Total emergency taker fills.
    pub emergency_taker_fills: u32,
    /// Emergency post-only maker fills (zero fee).
    pub emergency_maker_fills: u32,
    /// Favorable taker fills (ask < posted bid).
    pub favorable_taker_fills: u32,
    /// Favorable maker fills (try-maker-first succeeded).
    pub favorable_maker_fills: u32,

    // ── Repricing tier breakdown ──────────────────────────────────────────
    pub high_tier_trades: u32,
    pub high_tier_avg_alloc: Decimal,
    pub med_tier_trades: u32,
    pub med_tier_avg_alloc: Decimal,
    pub low_tier_trades: u32,
    pub low_tier_avg_alloc: Decimal,
    pub avg_expected_pct: Decimal,

    // ── PnL ──────────────────────────────────────────────────────────────
    pub gross_pnl: Decimal,
    pub emergency_taker_fees: Decimal,
    pub est_maker_rebates: Decimal,
    pub net_pnl: Decimal,

    // ── Win stats ─────────────────────────────────────────────────────────
    /// Winning trades as a fraction (0-100).
    pub win_rate_pct: Decimal,
    /// Average net profit % per closed trade.
    pub avg_net_profit_pct: Decimal,
    /// Best net_profit trade description.
    pub best_trade_pct: Decimal,
    pub best_trade_market: String,
    pub best_trade_reprice: Decimal,
    /// Worst net_profit trade description.
    pub worst_trade_pct: Decimal,
    pub worst_trade_market: String,
    pub worst_trade_reprice: Decimal,

    // ── Unfilled signal breakdown ──────────────────────────────────────────
    pub unfilled_signals: u32,
    pub unfilled_post_only: u32,
    pub unfilled_liquidity: u32,
    pub unfilled_spread_wide: u32,

    // ── Capital state ─────────────────────────────────────────────────────
    pub capital_locked: Decimal,
    pub virtual_balance: Decimal,
    pub starting_balance: Decimal,
}
