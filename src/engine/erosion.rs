//! Leg 2 erosion state machine.
//!
//! Tracks the progressive erosion of the profit target after Leg 1 fills.
//! Each 2-second step raises the Leg 2 bid by `step_size` until break-even
//! is reached (exactly 5 steps for all tiers).

use rust_decimal::Decimal;

use crate::types::market::{Direction, SpikeInfo};
use crate::types::order::ProfitTier;

// ─── Erosion State ──────────────────────────────────────────────────────────

/// Tracks progressive erosion of the Leg 2 profit target after Leg 1 fills.
#[derive(Debug, Clone)]
pub(crate) struct ErosionState {
    pub leg1_fill_ms: u64,
    pub initial_profit_target: Decimal,
    pub step_size: Decimal,
    pub steps_applied: u32,
    pub leg1_fill_price: Decimal,
    pub leg1_fill_size: Decimal,
    pub tier: ProfitTier,
    pub direction: Direction,
    pub spike_info: SpikeInfo,
    pub confidence: Decimal,
    pub binance_at_fill: Option<Decimal>,
    pub adverse_grace_expiry_ms: u64,
    pub emergency_submitted: bool,
}

impl ErosionState {
    pub fn new(
        leg1_fill_ms: u64,
        leg1_fill_price: Decimal,
        leg1_fill_size: Decimal,
        tier: ProfitTier,
        direction: Direction,
        spike_info: SpikeInfo,
        confidence: Decimal,
        binance_at_fill: Option<Decimal>,
        adverse_grace_ms: u64,
    ) -> Self {
        Self {
            leg1_fill_ms,
            initial_profit_target: tier.target_pct(),
            step_size: tier.step_size(),
            steps_applied: 0,
            leg1_fill_price,
            leg1_fill_size,
            tier,
            direction,
            spike_info,
            confidence,
            binance_at_fill,
            adverse_grace_expiry_ms: leg1_fill_ms + adverse_grace_ms,
            emergency_submitted: false,
        }
    }

    pub fn current_profit_target(&self) -> Decimal {
        let eroded = self.step_size * Decimal::from(self.steps_applied);
        (self.initial_profit_target - eroded).max(Decimal::ZERO)
    }

    pub fn break_even(&self) -> Decimal {
        Decimal::ONE - self.leg1_fill_price
    }

    pub fn current_leg2_target(&self) -> Decimal {
        Decimal::ONE - self.current_profit_target() - self.leg1_fill_price
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
    pub direction: Direction,
    pub adverse_grace_expiry_ms: u64,
    pub binance_at_fill: Option<Decimal>,
    pub fill_ms: u64,
    pub steps_applied: u32,
    pub tier: ProfitTier,
    pub confidence: Decimal,
    pub spike_info: SpikeInfo,
}
