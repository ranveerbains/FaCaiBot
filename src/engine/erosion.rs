//! Leg 2 erosion state machine.
//!
//! Tracks the progressive erosion of the profit target after Leg 1 fills.
//! Uses front-loaded step sizes (triangle weights \[5,4,3,2,1\]) and exponential
//! decay intervals (base × decay^step). Early steps give up more margin and wait
//! longer (market has time to fill at a good price); later steps give up less
//! and fire faster (urgency increases). 5 steps total to reach break-even.

use rust_decimal::Decimal;

use crate::types::market::{Direction, SpikeInfo};
use crate::types::order::{ExitReason, ProfitTier};

/// Triangle weights for 5 erosion steps: [5, 4, 3, 2, 1].
/// Step 1 = 33.3% of margin, step 5 = 6.7%.
const EROSION_WEIGHTS: [u32; 5] = [5, 4, 3, 2, 1];
const EROSION_WEIGHT_SUM: u32 = 15;

/// Maximum number of erosion steps. After this, the cascade is exhausted.
pub(crate) const MAX_EROSION_STEPS: u32 = EROSION_WEIGHTS.len() as u32;

// ─── Erosion State ──────────────────────────────────────────────────────────

/// Tracks progressive erosion of the Leg 2 profit target after Leg 1 fills.
#[derive(Debug, Clone)]
pub(crate) struct ErosionState {
    pub leg1_fill_ms: u64,
    pub initial_profit_target: Decimal,
    pub steps_applied: u32,
    pub leg1_fill_price: Decimal,
    #[allow(dead_code)] // stored for QuestDB trade recording
    pub leg1_fill_size: Decimal,
    pub tier: ProfitTier,
    pub direction: Direction,
    pub spike_info: SpikeInfo,
    pub confidence: Decimal,
    pub binance_at_fill: Option<Decimal>,
    pub emergency_submitted: bool,
    /// Set when an emergency FOK is confirmed — carries the reason for executor categorization.
    pub exit_reason: Option<ExitReason>,
    /// Epoch ms when the first emergency order was posted (for hard deadline).
    pub emergency_first_post_ms: Option<u64>,
    /// Price of the currently resting emergency order (for price-chase comparison).
    pub emergency_posted_price: Option<Decimal>,
    /// Set once a deadline-triggered FOK signal has been emitted to the executor.
    /// Prevents the evaluator from re-emitting FOK on every cycle (~2-50ms).
    pub fok_emitted: bool,
}

impl ErosionState {
    pub fn new(
        leg1_fill_ms: u64,
        leg1_fill_price: Decimal,
        leg1_fill_size: Decimal,
        tier: ProfitTier,
        initial_profit_target: Decimal,
        direction: Direction,
        spike_info: SpikeInfo,
        confidence: Decimal,
        binance_at_fill: Option<Decimal>,
    ) -> Self {
        Self {
            leg1_fill_ms,
            initial_profit_target,
            steps_applied: 0,
            leg1_fill_price,
            leg1_fill_size,
            tier,
            direction,
            spike_info,
            confidence,
            binance_at_fill,
            emergency_submitted: false,
            exit_reason: None,
            emergency_first_post_ms: None,
            emergency_posted_price: None,
            fok_emitted: false,
        }
    }

    /// Step size for the Nth step (0-indexed) using triangle weights [5,4,3,2,1].
    pub fn step_size_for_step(&self, step: u32) -> Decimal {
        let w = EROSION_WEIGHTS.get(step as usize).copied().unwrap_or(1);
        self.initial_profit_target * Decimal::from(w) / Decimal::from(EROSION_WEIGHT_SUM)
    }

    /// Cumulative erosion after N steps.
    pub fn cumulative_erosion(&self, steps: u32) -> Decimal {
        (0..steps).map(|s| self.step_size_for_step(s)).sum()
    }

    pub fn current_profit_target(&self) -> Decimal {
        let eroded = self.cumulative_erosion(self.steps_applied);
        (self.initial_profit_target - eroded).max(Decimal::ZERO)
    }

    pub fn break_even(&self) -> Decimal {
        Decimal::ONE - self.leg1_fill_price
    }

    #[allow(dead_code)] // used in tests
    pub fn current_leg2_target(&self) -> Decimal {
        Decimal::ONE - self.current_profit_target() - self.leg1_fill_price
    }

    /// Interval (ms) for the Nth step: `base × decay^step`, min 200ms.
    pub fn interval_for_step(step: u32, base_ms: u64, decay: f64) -> u64 {
        (base_ms as f64 * decay.powi(step as i32)).max(200.0) as u64
    }

    /// Returns `true` when all erosion steps have been applied and the
    /// cascade is exhausted (profit target is at or below zero).
    pub fn is_exhausted(&self) -> bool {
        self.steps_applied >= MAX_EROSION_STEPS
    }
}

impl ErosionSnap {
    /// Returns `true` when all erosion steps have been applied and the
    /// cascade is exhausted (profit target is at or below zero).
    pub fn is_exhausted(&self) -> bool {
        self.steps_applied >= MAX_EROSION_STEPS
    }
}

// ─── Connectivity State ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub(crate) struct ConnectivityState {
    pub binance_connected: bool,
    pub polymarket_market_connected: bool,
    pub polymarket_user_connected: bool,
    pub heartbeat_healthy: bool,
    pub consecutive_heartbeat_failures: u32,
}

// ─── Erosion Snapshot ───────────────────────────────────────────────────────

/// Borrow-free snapshot of `ErosionState` fields needed in `evaluate_leg2`.
/// Avoids holding `&mut self.erosion` while calling methods that borrow `self`.
pub(crate) struct ErosionSnap {
    pub emergency_submitted: bool,
    pub break_even: Decimal,
    pub current_profit_target: Decimal,
    /// Initial profit target (from config) — used to recalculate erosion steps.
    pub initial_profit_target: Decimal,
    pub direction: Direction,
    pub binance_at_fill: Option<Decimal>,
    pub fill_ms: u64,
    pub steps_applied: u32,
    pub tier: ProfitTier,
    pub confidence: Decimal,
    pub spike_info: SpikeInfo,
    /// Leg 1 fill price — needed for building repost signals.
    pub leg1_fill_price: Decimal,
    /// Exit reason from the erosion state — carried for emergency reposts.
    pub exit_reason: Option<ExitReason>,
    /// Epoch ms when the first emergency order was posted (for hard deadline).
    pub emergency_first_post_ms: Option<u64>,
    /// Price of the currently resting emergency order (for price-chase comparison).
    pub emergency_posted_price: Option<Decimal>,
    /// `true` once a deadline-triggered FOK signal has been emitted. Prevents
    /// the evaluator from flooding the executor with duplicate FOK signals.
    pub fok_emitted: bool,
}

// ─── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_erosion(steps: u32) -> ErosionState {
        let mut e = ErosionState::new(
            0,
            Decimal::new(50, 2),
            Decimal::new(100, 0),
            ProfitTier::High,
            Decimal::new(25, 3), // 0.025 initial profit target
            Direction::Up,
            SpikeInfo {
                direction: Direction::Up,
                magnitude: Decimal::new(5, 3),
                sustained_ms: 300,
                timestamp_ms: 0,
            },
            Decimal::new(7, 1),
            None,
        );
        e.steps_applied = steps;
        e
    }

    #[test]
    fn test_max_erosion_steps_constant() {
        assert_eq!(MAX_EROSION_STEPS, 5);
    }

    #[test]
    fn test_is_exhausted() {
        assert!(!make_erosion(0).is_exhausted());
        assert!(!make_erosion(4).is_exhausted());
        assert!(make_erosion(5).is_exhausted());
        assert!(make_erosion(6).is_exhausted());
    }

    #[test]
    fn test_cumulative_erosion_at_max_equals_target() {
        let e = make_erosion(0);
        let cum = e.cumulative_erosion(MAX_EROSION_STEPS);
        assert_eq!(
            cum, e.initial_profit_target,
            "5 steps should erode exactly 100% of initial target"
        );
    }

    #[test]
    fn test_current_profit_target_zero_at_max() {
        let e = make_erosion(5);
        assert_eq!(e.current_profit_target(), Decimal::ZERO);
    }

    #[test]
    fn test_emergency_fields_initialize_to_none() {
        let e = ErosionState::new(
            0,
            Decimal::new(50, 2),
            Decimal::new(100, 0),
            ProfitTier::High,
            Decimal::new(25, 3),
            Direction::Up,
            SpikeInfo {
                direction: Direction::Up,
                magnitude: Decimal::new(5, 3),
                sustained_ms: 300,
                timestamp_ms: 0,
            },
            Decimal::new(7, 1),
            None,
        );
        assert!(e.emergency_first_post_ms.is_none());
        assert!(e.emergency_posted_price.is_none());
    }
}
