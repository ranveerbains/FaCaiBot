//! Composite buildup detector — combines 6 metrics into a single score.
//!
//! Evaluation pipeline:
//! 1. Compute normalized [0,1] value for each metric (0 if stale)
//! 2. Direction consensus: dominant direction from fresh metrics; veto if any disagrees
//! 3. Causal ordering: at least 1 leading (CVD, basis) AND 1 confirming (spot flow, OBI)
//! 4. Weighted sum: composite = sum(weight_i × normalized_i)
//! 5. Return (composite, direction) or (0, None) if vetoed

use rust_decimal::Decimal;

use crate::types::market::{
    BuildupInfo, Direction, FuturesAggTrade, FuturesBookTicker, FuturesForceOrder, SpotTrade,
};

use super::metrics::{
    AtrDisplacementTracker, BasisDeltaTracker, CvdAccelTracker, LiqPressureTracker,
    ObiVelocityTracker, SpotFlowTracker,
};

// ─── BuildupDetector ─────────────────────────────────────────────────────────

pub struct BuildupDetector {
    /// Maximum dissenting directional metrics allowed in consensus vote.
    max_dissenters: u32,
    // Individual metrics
    pub(crate) cvd: CvdAccelTracker,
    pub(crate) spot_flow: SpotFlowTracker,
    pub(crate) obi_velocity: ObiVelocityTracker,
    pub(crate) basis_delta: BasisDeltaTracker,
    pub(crate) liq_pressure: LiqPressureTracker,
    pub(crate) atr_displacement: AtrDisplacementTracker,

    // Weights
    w_cvd: f64,
    w_spot_flow: f64,
    w_obi: f64,
    w_basis: f64,
    w_liq: f64,
    w_atr: f64,

    // Thresholds
    entry_threshold: f64,
    #[cfg_attr(not(test), allow(dead_code))]
    cancel_threshold: f64,

    // Spot reference for Phase B
    current_spot_mid: f64,

    /// Last raw OBI value from Binance depth (for BuildupInfo.obi).
    last_obi: f64,

    /// Tracks whether any feed method has been called since the last `tick()`.
    dirty: bool,

    /// Set when composite crosses above `entry_threshold`; cleared when it drops below
    /// OR when the direction flips while above threshold (new opportunity).
    /// Prevents emitting a new BuildupInfo on every dirty tick while above threshold —
    /// only the first crossing per episode fires (edge-triggered, not level-triggered).
    above_threshold: bool,

    /// Tracks the last confirmed direction for direction-flip detection.
    /// Reset to `None` when direction is vetoed.
    last_direction: Option<Direction>,

    // Diagnostic counters
    diag_signals_emitted: u64,
    diag_direction_vetoes: u64,
    diag_causal_vetoes: u64,
}

/// Configuration for the BuildupDetector.
pub struct BuildupConfig {
    /// Maximum dissenting directional metrics allowed in consensus vote.
    pub max_dissenters: u32,
    // Weights
    pub w_cvd: f64,
    pub w_spot_flow: f64,
    pub w_obi: f64,
    pub w_basis: f64,
    pub w_liq: f64,
    pub w_atr: f64,

    // Thresholds
    pub entry_threshold: f64,
    pub cancel_threshold: f64,

    // CVD params
    pub cvd_fast_halflife_ms: f64,
    pub cvd_slow_halflife_ms: f64,
    pub freshness_cvd_ms: u64,
    pub cvd_min: f64,
    pub cvd_saturation: f64,

    // Spot flow params
    pub spot_flow_halflife_ms: f64,
    pub freshness_spot_flow_ms: u64,
    pub spot_flow_min: f64,
    pub spot_flow_saturation: f64,

    // OBI velocity params
    pub obi_velocity_halflife_ms: f64,
    pub freshness_obi_ms: u64,
    pub obi_min: f64,
    pub obi_saturation: f64,

    // Basis delta params
    pub basis_halflife_ms: f64,
    pub freshness_basis_ms: u64,
    pub basis_min: f64,
    pub basis_saturation: f64,

    // Liquidation params
    pub liq_half_life_ms: f64,
    pub freshness_liq_ms: u64,
    pub liq_min: f64,
    pub liq_saturation: f64,

    // ATR displacement params
    pub atr_alpha: f64,
    pub freshness_atr_ms: u64,
    pub atr_min: f64,
    pub atr_saturation: f64,
    pub atr_warmup: u32,
}

impl BuildupConfig {
    /// Create from the TOML config section.
    pub fn from_toml(cfg: &crate::config::BuildupTomlConfig) -> Self {
        Self {
            max_dissenters: cfg.max_dissenters,
            w_cvd: cfg.w_cvd,
            w_spot_flow: cfg.w_spot_flow,
            w_obi: cfg.w_obi,
            w_basis: cfg.w_basis,
            w_liq: cfg.w_liq,
            w_atr: cfg.w_atr,
            entry_threshold: cfg.entry_threshold,
            cancel_threshold: cfg.cancel_threshold,
            cvd_fast_halflife_ms: cfg.cvd_fast_halflife_ms,
            cvd_slow_halflife_ms: cfg.cvd_slow_halflife_ms,
            freshness_cvd_ms: cfg.freshness_cvd_ms,
            cvd_min: cfg.cvd_min,
            cvd_saturation: cfg.cvd_saturation,
            spot_flow_halflife_ms: cfg.spot_flow_halflife_ms,
            freshness_spot_flow_ms: cfg.freshness_spot_flow_ms,
            spot_flow_min: cfg.spot_flow_min,
            spot_flow_saturation: cfg.spot_flow_saturation,
            obi_velocity_halflife_ms: cfg.obi_velocity_halflife_ms,
            freshness_obi_ms: cfg.freshness_obi_ms,
            obi_min: cfg.obi_min,
            obi_saturation: cfg.obi_saturation,
            basis_halflife_ms: cfg.basis_halflife_ms,
            freshness_basis_ms: cfg.freshness_basis_ms,
            basis_min: cfg.basis_min,
            basis_saturation: cfg.basis_saturation,
            // Liq and ATR params not exposed in TOML yet — use defaults.
            liq_half_life_ms: 2000.0,
            freshness_liq_ms: cfg.freshness_liq_ms,
            liq_min: cfg.liq_min,
            liq_saturation: cfg.liq_saturation,
            atr_alpha: 0.002,
            freshness_atr_ms: cfg.freshness_atr_ms,
            atr_min: cfg.atr_min,
            atr_saturation: cfg.atr_saturation,
            atr_warmup: 50,
        }
    }
}

impl Default for BuildupConfig {
    fn default() -> Self {
        Self {
            max_dissenters: 1,
            w_cvd: 0.30,
            w_spot_flow: 0.15,
            w_obi: 0.20,
            w_basis: 0.20,
            w_liq: 0.05,
            w_atr: 0.10,
            entry_threshold: 0.40,
            cancel_threshold: 0.25,
            cvd_fast_halflife_ms: 150.0,
            cvd_slow_halflife_ms: 700.0,
            freshness_cvd_ms: 300,
            cvd_min: 0.0,
            cvd_saturation: 1.0,
            spot_flow_halflife_ms: 300.0,
            freshness_spot_flow_ms: 200,
            spot_flow_min: 0.0,
            spot_flow_saturation: 1.0,
            obi_velocity_halflife_ms: 300.0,
            freshness_obi_ms: 100,
            obi_min: 0.0,
            obi_saturation: 0.5,
            basis_halflife_ms: 300.0,
            freshness_basis_ms: 300,
            basis_min: 0.0,
            basis_saturation: 2.0,
            liq_half_life_ms: 2000.0,
            freshness_liq_ms: 3000,
            liq_min: 0.0,
            liq_saturation: 10.0,
            atr_alpha: 0.002,
            freshness_atr_ms: 100,
            atr_min: 0.0,
            atr_saturation: 15.0,
            atr_warmup: 50,
        }
    }
}

impl BuildupDetector {
    pub fn new(cfg: &BuildupConfig) -> Self {
        Self {
            max_dissenters: cfg.max_dissenters,
            cvd: CvdAccelTracker::new(
                cfg.cvd_fast_halflife_ms,
                cfg.cvd_slow_halflife_ms,
                cfg.freshness_cvd_ms,
                cfg.cvd_min,
                cfg.cvd_saturation,
            ),
            spot_flow: SpotFlowTracker::new(
                cfg.spot_flow_halflife_ms,
                cfg.freshness_spot_flow_ms,
                cfg.spot_flow_min,
                cfg.spot_flow_saturation,
            ),
            obi_velocity: ObiVelocityTracker::new(
                cfg.obi_velocity_halflife_ms,
                cfg.freshness_obi_ms,
                cfg.obi_min,
                cfg.obi_saturation,
            ),
            basis_delta: BasisDeltaTracker::new(
                cfg.basis_halflife_ms,
                cfg.freshness_basis_ms,
                cfg.basis_min,
                cfg.basis_saturation,
            ),
            liq_pressure: LiqPressureTracker::new(
                cfg.liq_half_life_ms,
                cfg.freshness_liq_ms,
                cfg.liq_min,
                cfg.liq_saturation,
            ),
            atr_displacement: AtrDisplacementTracker::new(
                cfg.atr_alpha,
                cfg.freshness_atr_ms,
                cfg.atr_min,
                cfg.atr_saturation,
                cfg.atr_warmup,
            ),
            w_cvd: cfg.w_cvd,
            w_spot_flow: cfg.w_spot_flow,
            w_obi: cfg.w_obi,
            w_basis: cfg.w_basis,
            w_liq: cfg.w_liq,
            w_atr: cfg.w_atr,
            entry_threshold: cfg.entry_threshold,
            cancel_threshold: cfg.cancel_threshold,
            current_spot_mid: 0.0,
            last_obi: 0.0,
            dirty: false,
            above_threshold: false,
            last_direction: None,
            diag_signals_emitted: 0,
            diag_direction_vetoes: 0,
            diag_causal_vetoes: 0,
        }
    }

    // ── Feed methods ─────────────────────────────────────────────────────

    pub fn on_futures_agg_trade(&mut self, trade: &FuturesAggTrade, now_ms: u64) {
        let qty = trade.quantity.to_string().parse::<f64>().unwrap_or(0.0);
        self.cvd.update(qty, trade.is_buyer_maker, now_ms);
        self.dirty = true;
    }

    pub fn on_futures_book_ticker(&mut self, ticker: &FuturesBookTicker, now_ms: u64) {
        let bid = ticker.bid_price.to_string().parse::<f64>().unwrap_or(0.0);
        let ask = ticker.ask_price.to_string().parse::<f64>().unwrap_or(0.0);
        self.basis_delta.update_futures_mid(bid, ask, now_ms);
        self.dirty = true;
    }

    pub fn on_futures_force_order(&mut self, order: &FuturesForceOrder, now_ms: u64) {
        let qty = order.quantity.to_string().parse::<f64>().unwrap_or(0.0);
        self.liq_pressure.update(&order.side, qty, now_ms);
        self.dirty = true;
    }

    pub fn on_spot_trade(&mut self, trade: &SpotTrade, now_ms: u64) {
        let qty = trade.quantity.to_string().parse::<f64>().unwrap_or(0.0);
        self.spot_flow.update(qty, trade.is_buyer_maker, now_ms);
        self.dirty = true;
    }

    pub fn on_spot_depth(&mut self, mid: f64, obi: f64, now_ms: u64) {
        self.obi_velocity.update(obi, now_ms);
        self.atr_displacement.update(mid, now_ms);
        self.current_spot_mid = mid;
        self.last_obi = obi;
        self.basis_delta.update_spot_mid(mid, now_ms);
        self.dirty = true;
    }

    pub fn on_spot_bba(&mut self, mid: f64, _now_ms: u64) {
        self.current_spot_mid = mid;
        self.dirty = true;
    }

    // ── Evaluation ───────────────────────────────────────────────────────

    /// Whether any feed method has been called since the last `tick()`.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Evaluate after feeding. Returns (score, direction, optional entry signal).
    /// Clears the dirty flag. Only call when `is_dirty()` to avoid redundant work.
    pub fn tick(&mut self, now_ms: u64) -> (f64, Option<Direction>, Option<BuildupInfo>) {
        self.dirty = false;
        let (score, direction) = self.evaluate(now_ms);

        // Direction flip → reset edge-trigger so a new signal can fire immediately.
        if let Some(dir) = direction {
            if let Some(last) = self.last_direction
                && dir != last
                && self.above_threshold
            {
                self.above_threshold = false;
            }
            self.last_direction = Some(dir);
        } else {
            self.last_direction = None;
        }

        let entry = match direction {
            Some(dir) if score >= self.entry_threshold => {
                if !self.above_threshold {
                    // First crossing above threshold — emit signal (edge-triggered).
                    self.above_threshold = true;
                    self.diag_signals_emitted += 1;
                    Some(self.build_info(score, dir, now_ms))
                } else {
                    // Already above threshold — suppress until score drops and recovers.
                    None
                }
            }
            _ => {
                // Score dropped below threshold or no direction — reset for next episode.
                self.above_threshold = false;
                None
            }
        };
        (score, direction, entry)
    }

    /// Build a `BuildupInfo` from the current detector state.
    fn build_info(&self, score: f64, direction: Direction, now_ms: u64) -> BuildupInfo {
        let d = |v: f64| Decimal::try_from(v).unwrap_or(Decimal::ZERO);
        BuildupInfo {
            composite_score: d(score),
            direction,
            cvd_accel: d(self.cvd.raw()),
            spot_flow: d(self.spot_flow.raw()),
            obi_velocity: d(self.obi_velocity.raw()),
            basis_delta: d(self.basis_delta.raw()),
            liq_pressure: d(self.liq_pressure.raw()),
            atr_displacement: d(self.atr_displacement.raw()),
            signal_atr_ratio: d(self.atr_displacement.raw()),
            obi: d(self.last_obi),
            timestamp_ms: now_ms,
        }
    }

    /// Compute composite score. Returns (score, direction) or (0, None) if vetoed.
    pub fn evaluate(&mut self, now_ms: u64) -> (f64, Option<Direction>) {
        // 1. Collect normalized values and directions from fresh metrics.
        let metrics: [(f64, f64, Option<Direction>); 6] = [
            (self.w_cvd, self.cvd.normalized(now_ms), self.cvd.direction()),
            (self.w_spot_flow, self.spot_flow.normalized(now_ms), self.spot_flow.direction()),
            (self.w_obi, self.obi_velocity.normalized(now_ms), self.obi_velocity.direction()),
            (self.w_basis, self.basis_delta.normalized(now_ms), self.basis_delta.direction()),
            (self.w_liq, self.liq_pressure.normalized(now_ms), self.liq_pressure.direction()),
            (self.w_atr, self.atr_displacement.normalized(now_ms), self.atr_displacement.direction()),
        ];

        // 2. Direction consensus from directional metrics (indices 0-4: CVD, spot_flow, OBI, basis, liq).
        // ATR (index 5) is excluded — it measures volatility magnitude only, not direction.
        let mut up_count = 0u32;
        let mut down_count = 0u32;
        for i in 0..5 {
            let (_, norm, dir) = metrics[i];
            if norm > 0.0 {
                match dir {
                    Some(Direction::Up) => up_count += 1,
                    Some(Direction::Down) => down_count += 1,
                    None => {}
                }
            }
        }

        // Majority consensus: require at least 3 directional metrics to agree AND
        // allow at most `max_dissenters` dissenters. Veto ties, weak majorities (< 3 votes),
        // and cases where minority exceeds the configured limit.
        let minority = std::cmp::min(up_count, down_count);
        let (dominant, majority) = if up_count >= down_count {
            (Direction::Up, up_count)
        } else {
            (Direction::Down, down_count)
        };

        if majority < 3 || minority > self.max_dissenters {
            self.diag_direction_vetoes += 1;
            return (0.0, None);
        }

        // 3. Causal ordering: at least 1 leading AND 1 confirming must be fresh + non-zero.
        // Leading: CVD (index 0), basis delta (index 3) — futures-derived.
        let has_leading = metrics[0].1 > 0.0 || metrics[3].1 > 0.0;
        // Confirming: spot flow (index 1), OBI velocity (index 2) — spot-derived.
        let has_confirming = metrics[1].1 > 0.0 || metrics[2].1 > 0.0;

        if !has_leading || !has_confirming {
            self.diag_causal_vetoes += 1;
            return (0.0, None);
        }

        // 4. Weighted sum (entry_threshold check happens downstream).
        let composite: f64 = metrics.iter().map(|(w, n, _)| w * n).sum();

        (composite, Some(dominant))
    }

    /// Check if composite exceeds entry threshold (used by tests).
    #[cfg(test)]
    pub fn check_entry(&mut self, now_ms: u64) -> Option<BuildupInfo> {
        let (score, direction) = self.evaluate(now_ms);
        let direction = direction?;

        if score < self.entry_threshold {
            return None;
        }

        self.diag_signals_emitted += 1;

        let d = |v: f64| Decimal::try_from(v).unwrap_or(Decimal::ZERO);

        Some(BuildupInfo {
            composite_score: d(score),
            direction,
            cvd_accel: d(self.cvd.raw()),
            spot_flow: d(self.spot_flow.raw()),
            obi_velocity: d(self.obi_velocity.raw()),
            basis_delta: d(self.basis_delta.raw()),
            liq_pressure: d(self.liq_pressure.raw()),
            atr_displacement: d(self.atr_displacement.raw()),
            signal_atr_ratio: d(self.atr_displacement.raw()),
            obi: d(self.last_obi),
            timestamp_ms: now_ms,
        })
    }

    /// Check if composite has dropped below cancel threshold (used by tests).
    #[cfg(test)]
    pub fn below_cancel_threshold(&mut self, now_ms: u64) -> bool {
        let (score, _) = self.evaluate(now_ms);
        score < self.cancel_threshold
    }

    /// Diagnostic counters.
    pub fn diag_signals_emitted(&self) -> u64 {
        self.diag_signals_emitted
    }
    pub fn diag_direction_vetoes(&self) -> u64 {
        self.diag_direction_vetoes
    }
    pub fn diag_causal_vetoes(&self) -> u64 {
        self.diag_causal_vetoes
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "tests/detector_tests.rs"]
mod tests;
