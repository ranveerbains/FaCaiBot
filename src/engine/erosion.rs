//! Leg 2 hedge state machine.
//!
//! Tracks the 2-phase hedge system after Leg 1 fills.
//! Phase 1: Rest at profit target price, wait for fill or timeout.
//! Phase 2: Rest at break-even pursuit (ask-1tick), monitor for BE breach.
//! Single cancel/repost at phase transition — reduces off-book time from ~1s to ~200ms.

use rust_decimal::Decimal;

use crate::types::market::{Direction, SpikeInfo};
use crate::types::order::{ExitReason, ProfitTier};

/// Which phase of the 2-phase hedge system we're in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HedgePhase {
    /// Resting at profit target price.
    Phase1,
    /// Resting at break-even pursuit (ask-1tick).
    Phase2,
}

// ─── Hedge State ──────────────────────────────────────────────────────────

/// Tracks the 2-phase hedge of the Leg 2 position after Leg 1 fills.
#[derive(Debug, Clone)]
pub(crate) struct HedgeState {
    pub leg1_fill_ms: u64,
    pub initial_profit_target: Decimal,
    pub leg1_fill_price: Decimal,
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
    /// Current hedge phase.
    pub phase: HedgePhase,
    /// The initial Phase 1 post price (for skip-guard comparison).
    pub phase1_target_price: Decimal,
    /// Tracks the currently resting Phase 2 price (for improvement check).
    pub phase2_posted_price: Option<Decimal>,
}

impl HedgeState {
    pub fn new(
        leg1_fill_ms: u64,
        leg1_fill_price: Decimal,
        tier: ProfitTier,
        initial_profit_target: Decimal,
        direction: Direction,
        spike_info: SpikeInfo,
        confidence: Decimal,
        binance_at_fill: Option<Decimal>,
        phase1_target_price: Decimal,
    ) -> Self {
        Self {
            leg1_fill_ms,
            initial_profit_target,
            leg1_fill_price,
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
            phase: HedgePhase::Phase1,
            phase1_target_price,
            phase2_posted_price: None,
        }
    }

    pub fn break_even(&self) -> Decimal {
        Decimal::ONE - self.leg1_fill_price
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

// ─── Hedge Snapshot ───────────────────────────────────────────────────────

/// Borrow-free snapshot of `HedgeState` fields needed in `evaluate_leg2`.
/// Avoids holding `&mut self.hedge` while calling methods that borrow `self`.
pub(crate) struct HedgeSnap {
    pub emergency_submitted: bool,
    #[allow(dead_code)] // populated for diagnostic logging
    pub break_even: Decimal,
    pub initial_profit_target: Decimal,
    pub direction: Direction,
    pub binance_at_fill: Option<Decimal>,
    pub fill_ms: u64,
    pub tier: ProfitTier,
    pub confidence: Decimal,
    pub spike_info: SpikeInfo,
    /// Leg 1 fill price — needed for building repost signals.
    pub leg1_fill_price: Decimal,
    /// Exit reason from the hedge state — carried for emergency reposts.
    pub exit_reason: Option<ExitReason>,
    /// Epoch ms when the first emergency order was posted (for hard deadline).
    pub emergency_first_post_ms: Option<u64>,
    /// Price of the currently resting emergency order (for price-chase comparison).
    pub emergency_posted_price: Option<Decimal>,
    /// `true` once a deadline-triggered FOK signal has been emitted. Prevents
    /// the evaluator from flooding the executor with duplicate FOK signals.
    pub fok_emitted: bool,
    /// Current hedge phase.
    pub phase: HedgePhase,
    /// The initial Phase 1 post price (for skip-guard comparison).
    pub phase1_target_price: Decimal,
    /// Tracks the currently resting Phase 2 price (for improvement check).
    pub phase2_posted_price: Option<Decimal>,
}

// ─── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_hedge() -> HedgeState {
        HedgeState::new(
            0,
            Decimal::new(50, 2),    // leg1_fill_price = 0.50
            ProfitTier::High,
            Decimal::new(25, 3),     // initial_profit_target = 0.025
            Direction::Up,
            SpikeInfo {
                direction: Direction::Up,
                magnitude: Decimal::new(5, 3),
                sustained_ms: 300,
                timestamp_ms: 0,
                atr_ratio: Decimal::ZERO,
            },
            Decimal::new(7, 1),      // confidence = 0.7
            None,
            Decimal::new(475, 3),    // phase1_target_price = 0.475
        )
    }

    #[test]
    fn test_initial_phase_is_phase1() {
        let h = make_hedge();
        assert_eq!(h.phase, HedgePhase::Phase1);
    }

    #[test]
    fn test_break_even() {
        let h = make_hedge();
        assert_eq!(h.break_even(), Decimal::new(50, 2)); // 1.0 - 0.50 = 0.50
    }

    #[test]
    fn test_phase_transition() {
        let mut h = make_hedge();
        h.phase = HedgePhase::Phase2;
        h.phase2_posted_price = Some(Decimal::new(49, 2));
        assert_eq!(h.phase, HedgePhase::Phase2);
        assert_eq!(h.phase2_posted_price, Some(Decimal::new(49, 2)));
    }

    #[test]
    fn test_emergency_fields_initialize_to_none() {
        let h = make_hedge();
        assert!(h.emergency_first_post_ms.is_none());
        assert!(h.emergency_posted_price.is_none());
        assert!(!h.emergency_submitted);
        assert!(!h.fok_emitted);
    }

    #[test]
    fn test_phase1_target_price_stored() {
        let h = make_hedge();
        assert_eq!(h.phase1_target_price, Decimal::new(475, 3));
    }
}
