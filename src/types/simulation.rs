use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::market::Direction;
use super::order::{ProfitTier, Side};

// ════════════════════════════════════════════════════════════════════════════
// Data Structs
// ════════════════════════════════════════════════════════════════════════════

// ─── Position Status ─────────────────────────────────────────────────────────

/// Lifecycle status of a simulated position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionStatus {
    /// Leg 1 filled, waiting for Leg 2 hedge.
    Open,
    /// Both legs filled — paired position locked in.
    Hedged,
    /// Market expired before Leg 2 could fill.
    Expired,
    /// Market expired; position awaiting UMA resolution (~2h challenge period).
    AwaitingResolution,
    /// UMA resolution confirmed; PnL finalized.
    Resolved,
}

// ─── Simulated Fill ──────────────────────────────────────────────────────────

/// A single simulated fill (either Leg 1 or Leg 2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimFill {
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
    /// `true` if the fill crossed the spread (taker). Always `false` for Leg 1
    /// and for normal Leg 2 erosion. Only `true` for emergency FOK fills.
    pub was_taker: bool,
    /// Taker fee paid. `Decimal::ZERO` for maker fills.
    /// Formula (when taker): `shares * 0.25 * (price * (1 - price))^2`.
    pub taker_fee: Decimal,
}

impl SimFill {
    /// Compute taker fee for a fill at this price and size.
    /// Returns `Decimal::ZERO` if `was_taker` is false.
    ///
    /// Pure function — no side effects.
    pub fn compute_taker_fee(price: Decimal, size: Decimal) -> Decimal {
        // fee = size * 0.25 * (price * (1 - price))^2
        let one = Decimal::ONE;
        let factor = Decimal::new(25, 2); // 0.25
        let inner = price * (one - price);
        size * factor * inner * inner
    }
}

// ─── Simulated Position ──────────────────────────────────────────────────────

/// A live simulated position: one Leg 1 fill plus an optional Leg 2 hedge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimPosition {
    /// Polymarket condition ID.
    pub market_id: String,
    /// Spike direction that triggered this position.
    pub direction: Direction,
    /// Signal confidence score (0.0–1.0).
    pub confidence: Decimal,
    /// Profit target tier (HIGH / MED / LOW).
    pub profit_target_tier: ProfitTier,
    /// USDC allocated to this trade.
    pub alloc_amount: Decimal,
    /// Leg 1 (directional entry) fill details.
    pub leg1: SimFill,
    /// Leg 2 (hedge) fill details. `None` if not yet hedged.
    pub leg2: Option<SimFill>,
    /// Current lifecycle status.
    pub status: PositionStatus,
    /// Number of erosion steps applied to Leg 2 price so far.
    pub erosion_steps: u32,
    /// Whether a competitor depth wall was detected during this trade.
    pub bot_contested: bool,
    /// Whether Leg 2 was triggered by adverse Binance price movement.
    pub adverse_movement_hedge: bool,
}

// ─── Simulated Trade (closed) ────────────────────────────────────────────────

/// A completed (closed) simulated trade with full PnL accounting.
/// Written to QuestDB `simulated_trades` table and used for Telegram reporting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimTrade {
    /// Polymarket condition ID.
    pub market_id: String,
    /// Spike direction (Up/Down → YES/NO entry).
    pub direction: Direction,
    /// Leg 1 fill details.
    pub leg1: SimFill,
    /// Leg 2 fill details. `None` if unhedged at expiry.
    pub leg2: Option<SimFill>,

    // ── Signal context ───────────────────────────────────────────────────
    /// Confidence score that generated this trade (0.0–1.0).
    pub confidence: Decimal,
    /// Profit target tier (HIGH / MED / LOW).
    pub profit_target_tier: ProfitTier,
    /// USDC allocated to this trade.
    pub alloc_amount: Decimal,

    // ── PnL ──────────────────────────────────────────────────────────────
    /// `leg1.price + leg2.price` (or just `leg1.price` if unhedged).
    pub pair_cost: Decimal,
    /// `1.0 - pair_cost` for hedged trades. Negative if pair cost > 1.0.
    pub gross_profit: Decimal,
    /// Taker fee paid on Leg 2 (0 in normal flow; non-zero only for emergency taker).
    pub taker_fee: Decimal,
    /// `gross_profit - taker_fee`.
    pub net_profit: Decimal,
    /// `net_profit / pair_cost * 100` (percentage).
    pub profit_pct: Decimal,

    // ── Resolution ───────────────────────────────────────────────────────
    /// Market outcome: "YES" or "NO", or `None` if still pending.
    pub resolution: Option<String>,
    /// Epoch ms when UMA resolution was confirmed. `None` if pending.
    pub resolution_timestamp_ms: Option<u64>,

    // ── Execution metadata ───────────────────────────────────────────────
    /// Number of erosion steps applied to Leg 2 before fill.
    pub erosion_steps: u32,
    /// Whether Leg 2 executed as an emergency taker (FOK).
    pub leg2_was_taker: bool,
    /// Whether Leg 2 was triggered by adverse Binance price movement.
    pub adverse_movement_hedge: bool,
    /// Whether a competitor depth wall was detected during this trade.
    pub bot_contested: bool,

    // ── Timestamps ───────────────────────────────────────────────────────
    /// Epoch ms when the trade was opened (Leg 1 fill).
    pub open_timestamp_ms: u64,
    /// Epoch ms when the trade was closed (Leg 2 fill or market expiry).
    pub close_timestamp_ms: u64,
}

// ─── Helper Structs for Telegram Reporting ───────────────────────────────────

/// Aggregated stats for a single 15-minute market window.
/// Used for Tier 2 Telegram messages.
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
    pub total_trades: u32,
    /// Smart outbidding events in this market.
    pub walls_outbid: u32,
    /// Emergency taker fills in this market.
    pub emergency_taker_fills: u32,
    /// All closed trades in this market (for detail lines).
    pub trades: Vec<SimTrade>,
    /// Total USDC allocated this market.
    pub allocation_used: Decimal,
    /// FIXED_ALLOC cap for this session.
    pub allocation_cap: Decimal,
    /// Taker fees paid in this market.
    pub taker_fees_paid: Decimal,
    /// Gross PnL for this market (sum of gross_profit).
    pub gross_market_pnl: Decimal,
    /// Net PnL for this market (sum of net_profit).
    pub net_market_pnl: Decimal,
    /// Capital still locked in UMA resolution from this market.
    pub capital_locked: Decimal,
}

/// Aggregated stats for the entire session so far.
/// Used for Tier 3 hourly Telegram messages.
#[derive(Debug, Clone)]
pub struct SessionSummary {
    /// Uptime in seconds since session_start.
    pub uptime_secs: u64,
    /// Number of 15-min markets observed.
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
    /// Emergency taker fills breakdown.
    pub adverse_movement_fok: u32,
    pub break_even_fok: u32,
    pub timer_deadline_fok: u32,
    /// Total emergency taker fills.
    pub emergency_taker_fills: u32,

    // ── Confidence tier breakdown ─────────────────────────────────────────
    pub high_conf_trades: u32,
    pub high_conf_avg_alloc: Decimal,
    pub med_conf_trades: u32,
    pub med_conf_avg_alloc: Decimal,
    pub low_conf_trades: u32,
    pub low_conf_avg_alloc: Decimal,
    pub avg_confidence: Decimal,

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
    pub best_trade_conf: Decimal,
    /// Worst net_profit trade description.
    pub worst_trade_pct: Decimal,
    pub worst_trade_market: String,
    pub worst_trade_conf: Decimal,

    // ── Unfilled signal breakdown ──────────────────────────────────────────
    pub unfilled_signals: u32,
    pub unfilled_post_only: u32,   // Normal unfilled — zero cost
    pub unfilled_liquidity: u32,   // Insufficient liquidity
    pub unfilled_spread_wide: u32, // Spread too wide

    // ── Capital state ─────────────────────────────────────────────────────
    pub capital_locked: Decimal,
    pub virtual_balance: Decimal,
    pub starting_balance: Decimal,
}

// ─── Simulation State ────────────────────────────────────────────────────────

/// Virtual portfolio and session statistics for simulation mode.
///
/// Maintained by `SimulationExecutor`. All monetary values in USDC (Decimal).
#[derive(Debug, Clone)]
pub struct SimulationState {
    // ── Portfolio ─────────────────────────────────────────────────────────
    /// Virtual USDC balance (starts at `FIXED_ALLOC`, e.g. $100).
    pub virtual_balance: Decimal,
    /// Positions currently open (Leg 1 filled, Leg 2 pending).
    pub open_positions: Vec<SimPosition>,
    /// All closed trades this session.
    pub closed_trades: Vec<SimTrade>,

    // ── Session counters ─────────────────────────────────────────────────
    /// Total signals generated by the engine (including unfilled).
    pub signals_detected: u32,
    /// Signals where Leg 1 post-only order filled.
    pub leg1_fills: u32,
    /// Trades where Leg 2 hedge filled (maker).
    pub trades_hedged: u32,
    /// Trades hedged via adverse movement protocol (emergency taker).
    pub trades_adverse_hedged: u32,
    /// Total Leg 2 fills that executed as taker (any emergency reason).
    pub trades_emergency_taker: u32,
    /// Smart outbidding events (depth walls detected and outbid).
    pub walls_outbid: u32,

    // ── Unfilled signal breakdown ────────────────────────────────────────
    /// Signals that did not fill as post-only (normal — zero cost).
    pub unfilled_post_only: u32,
    /// Signals aborted due to insufficient liquidity.
    pub unfilled_liquidity: u32,
    /// Signals aborted due to spread too wide.
    pub unfilled_spread_wide: u32,

    // ── Financial tracking ───────────────────────────────────────────────
    /// Running net PnL for the session.
    pub total_pnl: Decimal,
    /// Cumulative taker fees paid (emergency fills only).
    pub total_taker_fees_paid: Decimal,
    /// Estimated maker rebates earned (20% of taker fees on maker fills).
    pub total_maker_rebates_earned: Decimal,
    /// USDC locked in positions awaiting UMA resolution.
    pub locked_in_resolution: Decimal,
    /// USDC allocated in the current market window.
    pub cumulative_used: Decimal,

    // ── Session metadata ─────────────────────────────────────────────────
    /// Number of 15-min markets observed this session.
    pub markets_observed: u32,
    /// Epoch ms when this simulation session started.
    pub session_start: u64,
    /// Starting virtual balance (for summary reporting).
    pub starting_balance: Decimal,
}

// ════════════════════════════════════════════════════════════════════════════
// Simple Accessors (stateless reads, no computation)
// ════════════════════════════════════════════════════════════════════════════

impl SimulationState {
    /// Create a new simulation state with the given starting balance.
    pub fn new(starting_balance: Decimal, now_ms: u64) -> Self {
        Self {
            virtual_balance: starting_balance,
            open_positions: Vec::new(),
            closed_trades: Vec::new(),
            signals_detected: 0,
            leg1_fills: 0,
            trades_hedged: 0,
            trades_adverse_hedged: 0,
            trades_emergency_taker: 0,
            walls_outbid: 0,
            unfilled_post_only: 0,
            unfilled_liquidity: 0,
            unfilled_spread_wide: 0,
            total_pnl: Decimal::ZERO,
            total_taker_fees_paid: Decimal::ZERO,
            total_maker_rebates_earned: Decimal::ZERO,
            locked_in_resolution: Decimal::ZERO,
            cumulative_used: Decimal::ZERO,
            markets_observed: 0,
            session_start: now_ms,
            starting_balance,
        }
    }

    /// Leg 1 fill rate as a percentage (0–100).
    pub fn fill_rate_pct(&self) -> Decimal {
        if self.signals_detected == 0 {
            return Decimal::ZERO;
        }
        Decimal::from(self.leg1_fills) / Decimal::from(self.signals_detected) * Decimal::ONE_HUNDRED
    }

    /// Win rate: fraction of hedged trades that were profitable (percentage 0–100).
    pub fn win_rate_pct(&self) -> Decimal {
        if self.closed_trades.is_empty() {
            return Decimal::ZERO;
        }
        let wins = self
            .closed_trades
            .iter()
            .filter(|t| t.net_profit > Decimal::ZERO)
            .count();
        Decimal::from(wins as u64) / Decimal::from(self.closed_trades.len() as u64)
            * Decimal::ONE_HUNDRED
    }

    /// Reset per-market counters on market rotation.
    pub fn on_market_rotation(&mut self) {
        self.cumulative_used = Decimal::ZERO;
        self.markets_observed += 1;
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Business Logic — Counter Mutations
// (called by SimulationExecutor to update session state)
// ════════════════════════════════════════════════════════════════════════════

impl SimulationState {
    /// Increment the signals_detected counter. Called by SimulationExecutor
    /// for every TradeSignal received from the Engine.
    pub fn record_signal(&mut self) {
        self.signals_detected += 1;
    }

    /// Record an unfilled signal (post-only bid not matched — zero cost).
    pub fn record_unfilled_post_only(&mut self) {
        self.unfilled_post_only += 1;
    }

    /// Record a signal aborted due to insufficient liquidity.
    pub fn record_unfilled_liquidity(&mut self) {
        self.unfilled_liquidity += 1;
    }

    /// Record a signal aborted due to spread too wide.
    pub fn record_unfilled_spread_wide(&mut self) {
        self.unfilled_spread_wide += 1;
    }

    /// Record a smart outbidding event (depth wall detected and outbid by 1 tick).
    pub fn record_wall_outbid(&mut self) {
        self.walls_outbid += 1;
    }

    /// Increment the erosion step counter for an open position.
    /// Should be called each time the Leg 2 target price is raised.
    pub fn record_erosion_step(&mut self, position_idx: usize) {
        if let Some(pos) = self.open_positions.get_mut(position_idx) {
            pos.erosion_steps += 1;
        }
    }

    /// Mark a position as bot_contested (depth wall was detected during this trade).
    pub fn mark_bot_contested(&mut self, position_idx: usize) {
        if let Some(pos) = self.open_positions.get_mut(position_idx) {
            pos.bot_contested = true;
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Business Logic — Fill & PnL Accounting
// (more complex methods that mutate portfolio state and compute PnL)
// ════════════════════════════════════════════════════════════════════════════

impl SimulationState {
    /// Record a Leg 1 fill and create the corresponding open position.
    ///
    /// Increments `leg1_fills`, deducts `fill.price * fill.size` from
    /// `virtual_balance`, and appends a new `SimPosition` to `open_positions`.
    ///
    /// Returns the index of the newly created position in `open_positions`.
    pub fn record_leg1_fill(
        &mut self,
        fill: SimFill,
        market_id: String,
        direction: Direction,
        confidence: Decimal,
        profit_tier: ProfitTier,
        alloc: Decimal,
    ) -> usize {
        self.leg1_fills += 1;
        self.cumulative_used += alloc;

        // Deduct allocated capital from virtual balance.
        let cost = fill.price * fill.size;
        self.virtual_balance -= cost;

        let position = SimPosition {
            market_id,
            direction,
            confidence,
            profit_target_tier: profit_tier,
            alloc_amount: alloc,
            leg1: fill,
            leg2: None,
            status: PositionStatus::Open,
            erosion_steps: 0,
            bot_contested: false,
            adverse_movement_hedge: false,
        };

        self.open_positions.push(position);
        self.open_positions.len() - 1
    }

    /// Record a Leg 2 maker fill (normal erosion cascade — zero fee).
    ///
    /// Updates the position at `position_idx`, sets status to `Hedged`,
    /// and increments `trades_hedged`.
    pub fn record_leg2_fill(&mut self, position_idx: usize, fill: SimFill) {
        if let Some(pos) = self.open_positions.get_mut(position_idx) {
            pos.leg2 = Some(fill);
            pos.status = PositionStatus::Hedged;
            self.trades_hedged += 1;
        }
    }

    /// Record an emergency taker Leg 2 fill (deadline, adverse, break-even breach).
    ///
    /// Updates the position at `position_idx`, sets status to `Hedged`,
    /// increments both `trades_hedged`, `trades_emergency_taker`, and
    /// accumulates `total_taker_fees_paid`.
    pub fn record_emergency_taker(&mut self, position_idx: usize, fill: SimFill) {
        if let Some(pos) = self.open_positions.get_mut(position_idx) {
            self.total_taker_fees_paid += fill.taker_fee;
            pos.leg2 = Some(fill);
            pos.status = PositionStatus::Hedged;
            self.trades_hedged += 1;
            self.trades_emergency_taker += 1;
        }
    }

    /// Record a Leg 2 fill triggered by adverse Binance price movement (FOK taker).
    ///
    /// Updates the position at `position_idx`, sets status to `Hedged`, and
    /// increments `trades_hedged`, `trades_emergency_taker`, and
    /// `trades_adverse_hedged`. Also accumulates `total_taker_fees_paid`.
    pub fn record_adverse_hedge(&mut self, position_idx: usize, fill: SimFill) {
        if let Some(pos) = self.open_positions.get_mut(position_idx) {
            pos.adverse_movement_hedge = true;
            self.total_taker_fees_paid += fill.taker_fee;
            pos.leg2 = Some(fill);
            pos.status = PositionStatus::Hedged;
            self.trades_hedged += 1;
            self.trades_emergency_taker += 1;
            self.trades_adverse_hedged += 1;
        }
    }

    /// Close the position at `position_idx`: remove it from `open_positions`,
    /// compute PnL, push to `closed_trades`, and return the resulting `SimTrade`.
    ///
    /// The caller is responsible for providing `close_timestamp_ms`.
    /// Returns `None` if the index is out of bounds.
    pub fn close_trade(
        &mut self,
        position_idx: usize,
        close_timestamp_ms: u64,
    ) -> Option<SimTrade> {
        if position_idx >= self.open_positions.len() {
            return None;
        }

        let pos = self.open_positions.remove(position_idx);

        let one = Decimal::ONE;
        let hundred = Decimal::ONE_HUNDRED;

        let (pair_cost, gross_profit, taker_fee, net_profit, profit_pct, leg2_was_taker) =
            if let Some(ref leg2) = pos.leg2 {
                let pc = pos.leg1.price + leg2.price;
                let gp = one - pc;
                let tf = leg2.taker_fee;
                let np = gp - tf;
                let pct = if pc.is_zero() {
                    Decimal::ZERO
                } else {
                    np / pc * hundred
                };
                (pc, gp, tf, np, pct, leg2.was_taker)
            } else {
                // Unhedged: PnL determined by resolution outcome later.
                // Pessimistic: treat as full loss of leg1 cost.
                let pc = pos.leg1.price;
                let gp = -pc;
                (pc, gp, Decimal::ZERO, gp, Decimal::ZERO, false)
            };

        // Update running session PnL.
        self.total_pnl += net_profit;

        // Return the cost basis to virtual_balance (net of profit/loss).
        let leg1_cost = pos.leg1.price * pos.leg1.size;
        self.virtual_balance += leg1_cost + net_profit * pos.leg1.size;

        let trade = SimTrade {
            market_id: pos.market_id,
            direction: pos.direction,
            leg1: pos.leg1.clone(),
            leg2: pos.leg2.clone(),
            confidence: pos.confidence,
            profit_target_tier: pos.profit_target_tier,
            alloc_amount: pos.alloc_amount,
            pair_cost,
            gross_profit,
            taker_fee,
            net_profit,
            profit_pct,
            resolution: None,
            resolution_timestamp_ms: None,
            erosion_steps: pos.erosion_steps,
            leg2_was_taker,
            adverse_movement_hedge: pos.adverse_movement_hedge,
            bot_contested: pos.bot_contested,
            open_timestamp_ms: pos.leg1.timestamp_ms,
            close_timestamp_ms,
        };

        self.closed_trades.push(trade.clone());
        Some(trade)
    }

    /// Record a UMA market resolution event.
    ///
    /// Finds all positions (open or closed) with the given `market_id`,
    /// updates their resolution fields, transitions `AwaitingResolution` positions
    /// to `Resolved`, and releases `locked_in_resolution` capital.
    pub fn record_resolution(&mut self, market_id: &str, outcome: &str, timestamp_ms: u64) {
        // Update any open positions still AwaitingResolution for this market.
        for pos in &mut self.open_positions {
            if pos.market_id == market_id && pos.status == PositionStatus::AwaitingResolution {
                pos.status = PositionStatus::Resolved;
                // Release the locked capital (leg1 cost).
                let locked = pos.leg1.price * pos.leg1.size;
                self.locked_in_resolution = (self.locked_in_resolution - locked).max(Decimal::ZERO);
                // Compute resolution PnL: winning side returns $1/share.
                let won = outcome == "YES" && pos.direction == Direction::Up
                    || outcome == "NO" && pos.direction == Direction::Down;
                let resolved_profit = if won {
                    (Decimal::ONE - pos.leg1.price) * pos.leg1.size
                } else {
                    -pos.leg1.price * pos.leg1.size
                };
                self.virtual_balance += pos.leg1.price * pos.leg1.size + resolved_profit;
                self.total_pnl += resolved_profit;
            }
        }

        // Update any matching closed trades (hedge was already done but resolution outstanding).
        for trade in &mut self.closed_trades {
            if trade.market_id == market_id && trade.resolution.is_none() {
                trade.resolution = Some(outcome.to_owned());
                trade.resolution_timestamp_ms = Some(timestamp_ms);
            }
        }
    }

    /// Lock capital for a position awaiting UMA resolution (unhedged at market expiry).
    /// Transitions the position status to `AwaitingResolution`.
    pub fn lock_for_resolution(&mut self, position_idx: usize) {
        if let Some(pos) = self.open_positions.get_mut(position_idx) {
            let cost = pos.leg1.price * pos.leg1.size;
            self.locked_in_resolution += cost;
            pos.status = PositionStatus::AwaitingResolution;
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Business Logic — Summary Aggregation
// (builds MarketSummary / SessionSummary for Telegram reporting)
// ════════════════════════════════════════════════════════════════════════════

impl SimulationState {
    /// Compute a `SessionSummary` for Tier 3 Telegram reporting (hourly / shutdown).
    ///
    /// `now_ms` is the current epoch timestamp in milliseconds.
    pub fn hourly_summary(&self, now_ms: u64) -> SessionSummary {
        let uptime_secs = (now_ms.saturating_sub(self.session_start)) / 1_000;

        let total_trades = self.closed_trades.len() as u32;
        let unfilled_signals = self.signals_detected.saturating_sub(self.leg1_fills);

        // Confidence tier breakdown across all closed trades.
        let mut high_alloc_sum = Decimal::ZERO;
        let mut med_alloc_sum = Decimal::ZERO;
        let mut low_alloc_sum = Decimal::ZERO;
        let mut high_count: u32 = 0;
        let mut med_count: u32 = 0;
        let mut low_count: u32 = 0;
        let mut conf_sum = Decimal::ZERO;

        let mut gross_pnl = Decimal::ZERO;
        let mut profit_pct_sum = Decimal::ZERO;
        let mut best_pct = Decimal::MIN;
        let mut best_market = String::new();
        let mut best_conf = Decimal::ZERO;
        let mut worst_pct = Decimal::MAX;
        let mut worst_market = String::new();
        let mut worst_conf = Decimal::ZERO;

        for trade in &self.closed_trades {
            gross_pnl += trade.gross_profit;
            profit_pct_sum += trade.profit_pct;
            conf_sum += trade.confidence;

            match trade.profit_target_tier {
                ProfitTier::High => {
                    high_count += 1;
                    high_alloc_sum += trade.alloc_amount;
                }
                ProfitTier::Med => {
                    med_count += 1;
                    med_alloc_sum += trade.alloc_amount;
                }
                ProfitTier::Low => {
                    low_count += 1;
                    low_alloc_sum += trade.alloc_amount;
                }
            }

            if trade.profit_pct > best_pct {
                best_pct = trade.profit_pct;
                best_market = trade.market_id.clone();
                best_conf = trade.confidence;
            }
            if trade.profit_pct < worst_pct {
                worst_pct = trade.profit_pct;
                worst_market = trade.market_id.clone();
                worst_conf = trade.confidence;
            }
        }

        let avg_confidence = if total_trades > 0 {
            conf_sum / Decimal::from(total_trades)
        } else {
            Decimal::ZERO
        };

        let avg_net_profit_pct = if total_trades > 0 {
            profit_pct_sum / Decimal::from(total_trades)
        } else {
            Decimal::ZERO
        };

        let high_conf_avg_alloc = if high_count > 0 {
            high_alloc_sum / Decimal::from(high_count)
        } else {
            Decimal::ZERO
        };
        let med_conf_avg_alloc = if med_count > 0 {
            med_alloc_sum / Decimal::from(med_count)
        } else {
            Decimal::ZERO
        };
        let low_conf_avg_alloc = if low_count > 0 {
            low_alloc_sum / Decimal::from(low_count)
        } else {
            Decimal::ZERO
        };

        // Maker rebates estimated as 20% of emergency taker fees paid.
        let est_maker_rebates = self.total_maker_rebates_earned;
        let net_pnl = gross_pnl - self.total_taker_fees_paid + est_maker_rebates;

        // Handle the degenerate case where no trades closed yet.
        if total_trades == 0 {
            best_pct = Decimal::ZERO;
            worst_pct = Decimal::ZERO;
        }

        SessionSummary {
            uptime_secs,
            markets_observed: self.markets_observed,
            signals_detected: self.signals_detected,
            leg1_fills: self.leg1_fills,
            trades_hedged: self.trades_hedged,
            total_trades,
            walls_outbid: self.walls_outbid,
            // Emergency taker breakdown — we have total and adverse.
            // Break-even and timer deadline are derived from the remainder.
            adverse_movement_fok: self.trades_adverse_hedged,
            break_even_fok: 0, // tracked separately when implemented in executor
            timer_deadline_fok: self
                .trades_emergency_taker
                .saturating_sub(self.trades_adverse_hedged),
            emergency_taker_fills: self.trades_emergency_taker,
            high_conf_trades: high_count,
            high_conf_avg_alloc,
            med_conf_trades: med_count,
            med_conf_avg_alloc,
            low_conf_trades: low_count,
            low_conf_avg_alloc,
            avg_confidence,
            gross_pnl,
            emergency_taker_fees: self.total_taker_fees_paid,
            est_maker_rebates,
            net_pnl,
            win_rate_pct: self.win_rate_pct(),
            avg_net_profit_pct,
            best_trade_pct: best_pct,
            best_trade_market: best_market,
            best_trade_conf: best_conf,
            worst_trade_pct: worst_pct,
            worst_trade_market: worst_market,
            worst_trade_conf: worst_conf,
            unfilled_signals,
            unfilled_post_only: self.unfilled_post_only,
            unfilled_liquidity: self.unfilled_liquidity,
            unfilled_spread_wide: self.unfilled_spread_wide,
            capital_locked: self.locked_in_resolution,
            virtual_balance: self.virtual_balance,
            starting_balance: self.starting_balance,
        }
    }

    /// Compute a `MarketSummary` for Tier 2 Telegram reporting (per market expiry).
    ///
    /// Filters `closed_trades` and the relevant open position counters to
    /// produce per-market aggregates. `allocation_cap` should be FIXED_ALLOC
    /// from config.
    pub fn market_summary(
        &self,
        market_id: &str,
        period_label: String,
        allocation_cap: Decimal,
    ) -> MarketSummary {
        let trades: Vec<SimTrade> = self
            .closed_trades
            .iter()
            .filter(|t| t.market_id == market_id)
            .cloned()
            .collect();

        let signals_detected = self.signals_detected; // simplification: all belong to current market
        let leg1_fills = trades.len() as u32;
        let trades_hedged = trades.iter().filter(|t| t.leg2.is_some()).count() as u32;
        let emergency_taker_fills = trades.iter().filter(|t| t.leg2_was_taker).count() as u32;
        let walls_outbid = self.walls_outbid; // scoped to current market by executor

        let mut allocation_used = Decimal::ZERO;
        let mut taker_fees_paid = Decimal::ZERO;
        let mut gross_market_pnl = Decimal::ZERO;
        let mut net_market_pnl = Decimal::ZERO;

        for trade in &trades {
            allocation_used += trade.alloc_amount;
            taker_fees_paid += trade.taker_fee;
            gross_market_pnl += trade.gross_profit;
            net_market_pnl += trade.net_profit;
        }

        // Locked capital: positions still awaiting resolution for this market.
        let capital_locked: Decimal = self
            .open_positions
            .iter()
            .filter(|p| p.market_id == market_id && p.status == PositionStatus::AwaitingResolution)
            .map(|p| p.leg1.price * p.leg1.size)
            .sum();

        // Determine resolution label.
        let resolution = trades
            .first()
            .and_then(|t| t.resolution.clone())
            .unwrap_or_else(|| "pending".to_owned());

        let uma_hours_remaining = if resolution == "pending" {
            Some(2)
        } else {
            None
        };

        MarketSummary {
            market_id: market_id.to_owned(),
            period_label,
            resolution,
            uma_hours_remaining,
            signals_detected,
            leg1_fills,
            trades_hedged,
            total_trades: leg1_fills,
            walls_outbid,
            emergency_taker_fills,
            trades,
            allocation_used,
            allocation_cap,
            taker_fees_paid,
            gross_market_pnl,
            net_market_pnl,
            capital_locked,
        }
    }
}
