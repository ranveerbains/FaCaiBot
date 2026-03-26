//! Fair value estimator — continuous P(BTC_expiry > BTC_open) model.
//!
//! Uses a corrected binary option model: log-displacement / scaled realized vol
//! through the normal CDF, plus momentum adjustments from Binance metrics.
//! The normal CDF does the heavy lifting; momentum is a secondary correction.

use rust_decimal::Decimal;

use crate::engine::buildup::metrics::{
    BasisDeltaTracker, BasisLevelTracker, CvdAccelTracker, CvdLevelTracker,
    LiquidationTracker, ObiLevelTracker, ObiVelocityTracker, RealizedVolTracker,
    SpotCvdTracker,
};

// ─── Normal CDF approximation ───────────────────────────────────────────────

/// Standard normal CDF approximation (Abramowitz & Stegun 26.2.17).
/// Maximum error: 7.5e-8.
fn phi(x: f64) -> f64 {
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let ax = x.abs() / std::f64::consts::SQRT_2;
    let t = 1.0 / (1.0 + p * ax);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-ax * ax).exp();
    0.5 * (1.0 + sign * y)
}

/// Logistic sigmoid: 1 / (1 + exp(-x))
fn sigmoid(x: f64) -> f64 {
    1.0 / (1.0 + (-x).exp())
}

/// Logit (log-odds): ln(p / (1 - p)). Caller must ensure 0 < p < 1.
fn logit(p: f64) -> f64 {
    (p / (1.0 - p)).ln()
}

// ─── Config ─────────────────────────────────────────────────────────────────

/// Configuration for the fair value model (from config.toml [fair_value]).
#[derive(Debug, Clone)]
pub struct FairValueConfig {
    // Momentum signal weights
    pub momentum_weight_basis: f64,
    pub momentum_weight_cvd: f64,
    pub momentum_weight_obi: f64,
    pub max_momentum_adj: f64,
    // Edge scaling
    pub vol_edge_scale: f64,
    pub time_edge_scale: f64,
    pub baseline_vol: f64,
    // Vol tracker params
    pub vol_ring_capacity: usize,
    pub vol_session_capacity: usize,
    pub vol_freshness_ms: u64,
    pub vol_min_warmup: usize,
    pub vol_default: f64,
    pub vol_ticks_per_sec: f64,
    // Model params
    pub vol_floor: f64,
    pub tail_compression_factor: f64,
    pub stale_data_edge_penalty: f64,
    pub regime_spike_threshold: f64,
    pub regime_spike_penalty: f64,
    pub strike_warmup_count: usize,
    // Metric tracker params
    pub basis_halflife_ms: f64,
    pub basis_freshness_ms: u64,
    pub basis_min: f64,
    pub basis_saturation: f64,
    pub cvd_fast_halflife_ms: f64,
    pub cvd_slow_halflife_ms: f64,
    pub cvd_freshness_ms: u64,
    pub cvd_min: f64,
    pub cvd_saturation: f64,
    pub obi_halflife_ms: f64,
    pub obi_freshness_ms: u64,
    pub obi_min: f64,
    pub obi_saturation: f64,
    // Level-based signal weights
    pub momentum_weight_cvd_level: f64,
    pub momentum_weight_obi_level: f64,
    pub momentum_weight_basis_level: f64,
    pub momentum_weight_spot_cvd: f64,
    pub momentum_weight_liquidation: f64,
    // Level tracker params
    pub cvd_level_halflife_ms: f64,
    pub cvd_level_freshness_ms: u64,
    pub cvd_level_saturation: f64,
    pub obi_level_halflife_ms: f64,
    pub obi_level_freshness_ms: u64,
    pub obi_level_saturation: f64,
    pub basis_level_halflife_ms: f64,
    pub basis_level_freshness_ms: u64,
    pub basis_level_saturation: f64,
    // Spot CVD tracker
    pub spot_cvd_fast_halflife_ms: f64,
    pub spot_cvd_slow_halflife_ms: f64,
    pub spot_cvd_freshness_ms: u64,
    pub spot_cvd_saturation: f64,
    // Liquidation tracker
    pub liquidation_halflife_ms: f64,
    pub liquidation_freshness_ms: u64,
    pub liquidation_saturation: f64,
    // Momentum mode
    pub use_logit_momentum: bool,
    pub momentum_time_decay: bool,
}

impl Default for FairValueConfig {
    fn default() -> Self {
        Self {
            momentum_weight_basis: 0.10,
            momentum_weight_cvd: 0.05,
            momentum_weight_obi: 0.05,
            max_momentum_adj: 0.08,
            vol_edge_scale: 0.5,
            time_edge_scale: 0.02,
            baseline_vol: 0.00003,
            vol_ring_capacity: 600,
            vol_session_capacity: 6000,
            vol_freshness_ms: 500,
            vol_min_warmup: 10,
            vol_default: 0.00003,
            vol_ticks_per_sec: 20.0,
            vol_floor: 0.00002,
            tail_compression_factor: 0.45,
            stale_data_edge_penalty: 0.01,
            regime_spike_threshold: 2.0,
            regime_spike_penalty: 0.01,
            strike_warmup_count: 5,
            basis_halflife_ms: 250.0,
            basis_freshness_ms: 150,
            basis_min: 0.0,
            basis_saturation: 0.05,
            cvd_fast_halflife_ms: 200.0,
            cvd_slow_halflife_ms: 500.0,
            cvd_freshness_ms: 150,
            cvd_min: 0.0,
            cvd_saturation: 0.6,
            obi_halflife_ms: 250.0,
            obi_freshness_ms: 120,
            obi_min: 0.0,
            obi_saturation: 0.2,
            // Level-based signal weights
            momentum_weight_cvd_level: 0.04,
            momentum_weight_obi_level: 0.03,
            momentum_weight_basis_level: 0.05,
            momentum_weight_spot_cvd: 0.04,
            momentum_weight_liquidation: 0.03,
            // Level tracker params
            cvd_level_halflife_ms: 3000.0,
            cvd_level_freshness_ms: 500,
            cvd_level_saturation: 5.0,
            obi_level_halflife_ms: 1500.0,
            obi_level_freshness_ms: 300,
            obi_level_saturation: 0.3,
            basis_level_halflife_ms: 2000.0,
            basis_level_freshness_ms: 500,
            basis_level_saturation: 3.0,
            // Spot CVD tracker
            spot_cvd_fast_halflife_ms: 300.0,
            spot_cvd_slow_halflife_ms: 800.0,
            spot_cvd_freshness_ms: 300,
            spot_cvd_saturation: 0.6,
            // Liquidation tracker
            liquidation_halflife_ms: 1000.0,
            liquidation_freshness_ms: 3000,
            liquidation_saturation: 10.0,
            // Momentum mode
            use_logit_momentum: true,
            momentum_time_decay: true,
        }
    }
}

// ─── FairValueEstimator ─────────────────────────────────────────────────────

/// Continuously estimates P(BTC > strike at expiry) using Binance data.
///
/// Corrected model:
/// 1. Log-displacement = ln(current_btc / strike)
/// 2. Scaled vol = realized_vol × √(ticks_per_sec × remaining_secs)
/// 3. d = displacement / max(scaled_vol, 0.0001)
/// 4. d_adjusted = d × tail_compression_factor (fatten tails)
/// 5. P(YES) = Φ(d_adjusted)
/// 6. Momentum adjustment ±0.04 max
pub struct FairValueEstimator {
    config: FairValueConfig,
    strike_price: f64,
    current_btc: f64,
    yes_fair_value: f64,
    no_fair_value: f64,
    edge: f64,
    last_update_ms: u64,
    last_momentum: f64,
    // Vol trackers
    vol_tracker: RealizedVolTracker,
    session_vol_tracker: RealizedVolTracker,
    // Momentum trackers (velocity)
    basis_tracker: BasisDeltaTracker,
    cvd_tracker: CvdAccelTracker,
    obi_tracker: ObiVelocityTracker,
    // Level trackers
    cvd_level_tracker: CvdLevelTracker,
    obi_level_tracker: ObiLevelTracker,
    basis_level_tracker: BasisLevelTracker,
    // Additional signal trackers
    spot_cvd_tracker: SpotCvdTracker,
    liquidation_tracker: LiquidationTracker,
    // Strike warmup buffer
    strike_warmup_buffer: Vec<f64>,
}

impl FairValueEstimator {
    pub fn new(config: &FairValueConfig) -> Self {
        let vol_tracker = RealizedVolTracker::new(
            config.vol_ring_capacity,
            config.vol_min_warmup,
            config.vol_default,
            config.vol_ticks_per_sec,
            config.vol_freshness_ms,
        );
        let session_vol_tracker = RealizedVolTracker::new(
            config.vol_session_capacity,
            config.vol_min_warmup,
            config.vol_default,
            config.vol_ticks_per_sec,
            config.vol_freshness_ms,
        );
        let basis_tracker = BasisDeltaTracker::new(
            config.basis_halflife_ms,
            config.basis_freshness_ms,
            config.basis_min,
            config.basis_saturation,
        );
        let cvd_tracker = CvdAccelTracker::new(
            config.cvd_fast_halflife_ms,
            config.cvd_slow_halflife_ms,
            config.cvd_freshness_ms,
            config.cvd_min,
            config.cvd_saturation,
        );
        let obi_tracker = ObiVelocityTracker::new(
            config.obi_halflife_ms,
            config.obi_freshness_ms,
            config.obi_min,
            config.obi_saturation,
        );
        let cvd_level_tracker = CvdLevelTracker::new(
            config.cvd_level_halflife_ms,
            config.cvd_level_freshness_ms,
            config.cvd_level_saturation,
        );
        let obi_level_tracker = ObiLevelTracker::new(
            config.obi_level_halflife_ms,
            config.obi_level_freshness_ms,
            config.obi_level_saturation,
        );
        let basis_level_tracker = BasisLevelTracker::new(
            config.basis_level_halflife_ms,
            config.basis_level_freshness_ms,
            config.basis_level_saturation,
        );
        let spot_cvd_tracker = SpotCvdTracker::new(
            config.spot_cvd_fast_halflife_ms,
            config.spot_cvd_slow_halflife_ms,
            config.spot_cvd_freshness_ms,
            config.spot_cvd_saturation,
        );
        let liquidation_tracker = LiquidationTracker::new(
            config.liquidation_halflife_ms,
            config.liquidation_freshness_ms,
            config.liquidation_saturation,
        );

        Self {
            config: config.clone(),
            strike_price: 0.0,
            current_btc: 0.0,
            yes_fair_value: 0.5,
            no_fair_value: 0.5,
            edge: 0.05,
            last_update_ms: 0,
            last_momentum: 0.0,
            vol_tracker,
            session_vol_tracker,
            basis_tracker,
            cvd_tracker,
            obi_tracker,
            cvd_level_tracker,
            obi_level_tracker,
            basis_level_tracker,
            spot_cvd_tracker,
            liquidation_tracker,
            strike_warmup_buffer: Vec::with_capacity(config.strike_warmup_count),
        }
    }

    /// Try to set strike from warmup buffer. Returns true if strike was set.
    /// Collects N prices and uses the median to avoid anomalous first tick.
    pub fn try_set_strike(&mut self, price: f64) -> bool {
        if self.strike_price > 0.0 {
            return true; // already set
        }
        self.strike_warmup_buffer.push(price);
        if self.strike_warmup_buffer.len() >= self.config.strike_warmup_count {
            self.strike_warmup_buffer.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let mid = self.strike_warmup_buffer.len() / 2;
            self.strike_price = self.strike_warmup_buffer[mid];
            true
        } else {
            false
        }
    }

    /// Update current BTC spot price and feed vol trackers.
    pub fn update_btc_price(&mut self, price: f64, now_ms: u64) {
        self.current_btc = price;
        self.last_update_ms = now_ms;
    }

    /// Feed price to both vol trackers.
    pub fn update_vol(&mut self, price: f64, now_ms: u64) {
        self.vol_tracker.update(price, now_ms);
        self.session_vol_tracker.update(price, now_ms);
    }

    /// Update spot mid for basis trackers (delta + level).
    pub fn update_spot_mid(&mut self, mid: f64, now_ms: u64) {
        self.basis_tracker.update_spot_mid(mid, now_ms);
        self.basis_level_tracker.update_spot_mid(mid, now_ms);
    }

    /// Update futures mid (from FuturesBookTicker). Feeds both basis trackers.
    pub fn update_futures_mid(&mut self, bid: f64, ask: f64, now_ms: u64) {
        self.basis_tracker.update_futures_mid(bid, ask, now_ms);
        self.basis_level_tracker.update_futures_mid(bid, ask, now_ms);
    }

    /// Update spot CVD (from SpotTrade).
    pub fn update_spot_cvd(&mut self, quantity: f64, is_buyer_maker: bool, now_ms: u64) {
        self.spot_cvd_tracker.update(quantity, is_buyer_maker, now_ms);
    }

    /// Update liquidation pressure (from FuturesForceOrder).
    pub fn update_liquidation(&mut self, side: &str, quantity: f64, now_ms: u64) {
        self.liquidation_tracker.update(side, quantity, now_ms);
    }

    /// Update CVD (from FuturesAggTrade). Feeds both accel and level trackers.
    pub fn update_cvd(&mut self, quantity: f64, is_buyer_maker: bool, now_ms: u64) {
        self.cvd_tracker.update(quantity, is_buyer_maker, now_ms);
        self.cvd_level_tracker.update(quantity, is_buyer_maker, now_ms);
    }

    /// Update OBI (from BinanceDepth). Feeds both velocity and level trackers.
    pub fn update_obi(&mut self, obi: f64, now_ms: u64) {
        self.obi_tracker.update(obi, now_ms);
        self.obi_level_tracker.update(obi, now_ms);
    }

    /// Whether vol data is too stale to quote safely.
    pub fn is_stale(&self, now_ms: u64) -> bool {
        !self.vol_tracker.is_fresh(now_ms)
    }

    /// Recompute fair value and edge. Call after updating inputs.
    pub fn recompute(&mut self, market_end_ms: u64, min_edge: f64, now_ms: u64) {
        if self.strike_price <= 0.0 || self.current_btc <= 0.0 {
            return;
        }

        let time_remaining_s = market_end_ms.saturating_sub(now_ms) as f64 / 1000.0;
        let time_fraction = (time_remaining_s / 300.0).clamp(0.0, 1.0);

        // ── Base model: corrected binary option ──
        let displacement = (self.current_btc / self.strike_price).ln();
        // Vol floor prevents overconfidence during calm periods
        let sigma_remaining = self.vol_tracker
            .scaled_vol(time_remaining_s, self.config.vol_floor)
            .max(0.0001);
        let d = displacement / sigma_remaining;
        let d_adjusted = d * self.config.tail_compression_factor;
        let base_fv = phi(d_adjusted);

        // ── Momentum adjustment (all 8 signals) ──
        let dir = |d: Option<crate::types::market::Direction>| -> f64 {
            match d {
                Some(crate::types::market::Direction::Up) => 1.0,
                Some(crate::types::market::Direction::Down) => -1.0,
                None => 0.0,
            }
        };

        // Velocity signals (existing)
        let basis_signed = self.basis_tracker.normalized(now_ms) * dir(self.basis_tracker.direction());
        let cvd_signed = self.cvd_tracker.normalized(now_ms) * dir(self.cvd_tracker.direction());
        let obi_signed = self.obi_tracker.normalized(now_ms) * dir(self.obi_tracker.direction());
        // Level signals (new)
        let cvd_level_signed = self.cvd_level_tracker.normalized(now_ms) * dir(self.cvd_level_tracker.direction());
        let obi_level_signed = self.obi_level_tracker.normalized(now_ms) * dir(self.obi_level_tracker.direction());
        let basis_level_signed = self.basis_level_tracker.normalized(now_ms) * dir(self.basis_level_tracker.direction());
        // Additional signals (new)
        let spot_cvd_signed = self.spot_cvd_tracker.normalized(now_ms) * dir(self.spot_cvd_tracker.direction());
        let liq_signed = self.liquidation_tracker.normalized(now_ms) * dir(self.liquidation_tracker.direction());

        // Time-adaptive momentum: momentum matters more early, less near expiry
        let momentum_time_factor = if self.config.momentum_time_decay {
            time_fraction // already = (time_remaining_s / 300.0).clamp(0, 1)
        } else {
            1.0
        };

        let raw_momentum = self.config.momentum_weight_basis * basis_signed
            + self.config.momentum_weight_cvd * cvd_signed
            + self.config.momentum_weight_obi * obi_signed
            + self.config.momentum_weight_cvd_level * cvd_level_signed
            + self.config.momentum_weight_obi_level * obi_level_signed
            + self.config.momentum_weight_basis_level * basis_level_signed
            + self.config.momentum_weight_spot_cvd * spot_cvd_signed
            + self.config.momentum_weight_liquidation * liq_signed;

        let momentum_clamped = (raw_momentum * momentum_time_factor).clamp(
            -self.config.max_momentum_adj,
            self.config.max_momentum_adj,
        );

        self.last_momentum = momentum_clamped;

        // Apply momentum: log-odds space (correct for probability updates) or additive fallback
        if self.config.use_logit_momentum {
            let base_clamped = base_fv.clamp(0.02, 0.98);
            let logit_adj = logit(base_clamped) + momentum_clamped;
            self.yes_fair_value = sigmoid(logit_adj).clamp(0.02, 0.98);
        } else {
            self.yes_fair_value = (base_fv + momentum_clamped).clamp(0.02, 0.98);
        }
        self.no_fair_value = 1.0 - self.yes_fair_value;

        // ── Edge sizing ──
        let realized = self.vol_tracker.realized_vol();
        let vol_normalized = (realized / self.config.baseline_vol).min(2.0);
        let time_factor = time_fraction.sqrt();

        let mut edge = min_edge
            + self.config.vol_edge_scale * vol_normalized
            + self.config.time_edge_scale * time_factor;

        // Regime spike: short vol much higher than session vol
        let short_vol = self.vol_tracker.realized_vol();
        let session_vol = self.session_vol_tracker.realized_vol();
        if session_vol > 0.0 && short_vol / session_vol > self.config.regime_spike_threshold {
            edge += self.config.regime_spike_penalty;
        }

        // Stale momentum feed penalties (exclude liquidation — sparse by nature)
        let mut stale_count = 0u32;
        if !self.basis_tracker.is_fresh(now_ms) { stale_count += 1; }
        if !self.cvd_tracker.is_fresh(now_ms) { stale_count += 1; }
        if !self.obi_tracker.is_fresh(now_ms) { stale_count += 1; }
        if !self.cvd_level_tracker.is_fresh(now_ms) { stale_count += 1; }
        if !self.obi_level_tracker.is_fresh(now_ms) { stale_count += 1; }
        if !self.basis_level_tracker.is_fresh(now_ms) { stale_count += 1; }
        if !self.spot_cvd_tracker.is_fresh(now_ms) { stale_count += 1; }
        edge += stale_count as f64 * self.config.stale_data_edge_penalty;

        self.edge = edge;
        self.last_update_ms = now_ms;
    }

    // ── Accessors ──

    pub fn yes_fair_value(&self) -> Decimal {
        Decimal::try_from(self.yes_fair_value).unwrap_or(Decimal::new(50, 2))
    }

    pub fn no_fair_value(&self) -> Decimal {
        Decimal::try_from(self.no_fair_value).unwrap_or(Decimal::new(50, 2))
    }

    pub fn yes_target_price(&self, edge: Decimal) -> Decimal {
        (self.yes_fair_value() - edge).max(Decimal::new(1, 2))
    }

    pub fn no_target_price(&self, edge: Decimal) -> Decimal {
        (self.no_fair_value() - edge).max(Decimal::new(1, 2))
    }

    pub fn edge(&self) -> Decimal {
        Decimal::try_from(self.edge).unwrap_or(Decimal::new(5, 2))
    }

    pub fn last_momentum(&self) -> f64 {
        self.last_momentum
    }

    pub fn strike_price(&self) -> Decimal {
        Decimal::try_from(self.strike_price).unwrap_or(Decimal::ZERO)
    }

    pub fn is_warm(&self) -> bool {
        self.strike_price > 0.0 && self.current_btc > 0.0
    }

    /// Whether the vol tracker has enough samples for valid volatility computation.
    pub fn is_vol_warm(&self) -> bool {
        self.vol_tracker.is_warm()
    }

    /// Reset for a new market.
    pub fn reset(&mut self, config: &FairValueConfig) {
        *self = Self::new(config);
    }

    // ── Signal getters for buildup guard ──

    /// CVD acceleration: (normalized [0,1], direction).
    pub fn cvd_signal(&self, now_ms: u64) -> (f64, Option<crate::types::market::Direction>) {
        (self.cvd_tracker.normalized(now_ms), self.cvd_tracker.direction())
    }

    /// OBI velocity: (normalized [0,1], direction).
    pub fn obi_signal(&self, now_ms: u64) -> (f64, Option<crate::types::market::Direction>) {
        (self.obi_tracker.normalized(now_ms), self.obi_tracker.direction())
    }

    /// Basis delta: (normalized [0,1], direction).
    pub fn basis_signal(&self, now_ms: u64) -> (f64, Option<crate::types::market::Direction>) {
        (self.basis_tracker.normalized(now_ms), self.basis_tracker.direction())
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_phi_at_zero() {
        let result = phi(0.0);
        assert!((result - 0.5).abs() < 1e-7, "phi(0) should be 0.5, got {result}");
    }

    #[test]
    fn test_phi_large_positive() {
        let result = phi(4.0);
        assert!(result > 0.99996, "phi(4) should be ~1.0, got {result}");
    }

    #[test]
    fn test_phi_large_negative() {
        let result = phi(-4.0);
        assert!(result < 0.00004, "phi(-4) should be ~0.0, got {result}");
    }

    #[test]
    fn test_phi_symmetry() {
        let pos = phi(1.5);
        let neg = phi(-1.5);
        assert!((pos + neg - 1.0).abs() < 1e-7, "phi(x) + phi(-x) should = 1.0");
    }

    #[test]
    fn test_fair_value_at_strike() {
        let config = FairValueConfig::default();
        let mut fv = FairValueEstimator::new(&config);
        fv.strike_price = 100_000.0; // set directly for test

        // Feed vol tracker with small movements to warm up
        for i in 0..20 {
            let price = 100_000.0 + (i as f64 * 0.5);
            fv.update_vol(price, i * 50);
        }
        fv.update_btc_price(100_000.0, 1000);
        fv.recompute(300_000, 0.03, 1000);

        // At strike, should be close to 0.50
        let yes_fv = fv.yes_fair_value;
        assert!(
            (yes_fv - 0.5).abs() < 0.1,
            "at strike, yes_fv should be ~0.5, got {yes_fv}"
        );
    }

    #[test]
    fn test_fair_value_above_strike() {
        let config = FairValueConfig::default();
        let mut fv = FairValueEstimator::new(&config);
        fv.strike_price = 100_000.0;

        // Warm up vol with some movement
        for i in 0..20 {
            let price = 100_000.0 + (i as f64 * 2.0);
            fv.update_vol(price, i * 50);
        }
        // BTC $100 above strike
        fv.update_btc_price(100_100.0, 1000);
        fv.recompute(300_000, 0.03, 1000);

        assert!(
            fv.yes_fair_value > 0.5,
            "above strike, YES should be > 0.5, got {}",
            fv.yes_fair_value
        );
    }

    #[test]
    fn test_fair_value_below_strike() {
        let config = FairValueConfig::default();
        let mut fv = FairValueEstimator::new(&config);
        fv.strike_price = 100_000.0;

        for i in 0..20 {
            let price = 100_000.0 - (i as f64 * 2.0);
            fv.update_vol(price, i * 50);
        }
        fv.update_btc_price(99_900.0, 1000);
        fv.recompute(300_000, 0.03, 1000);

        assert!(
            fv.yes_fair_value < 0.5,
            "below strike, YES should be < 0.5, got {}",
            fv.yes_fair_value
        );
    }

    #[test]
    fn test_fair_value_realistic_range() {
        // BTC $50 above strike with 200s remaining should give ~0.55-0.75
        let config = FairValueConfig::default();
        let mut fv = FairValueEstimator::new(&config);
        fv.strike_price = 100_000.0;

        // Warm up with realistic vol (small tick-to-tick moves)
        for i in 0..30 {
            let price = 100_000.0 + (i as f64 * 1.5);
            fv.update_vol(price, i * 50);
        }
        fv.update_btc_price(100_050.0, 1500);
        // market_end at 301500ms → 200s remaining
        fv.recompute(201_500, 0.03, 1500);

        assert!(
            fv.yes_fair_value > 0.52 && fv.yes_fair_value < 0.85,
            "BTC +$50 above strike should give ~0.55-0.75 YES, got {}",
            fv.yes_fair_value
        );
    }

    #[test]
    fn test_edge_wider_early() {
        let config = FairValueConfig::default();
        let mut fv = FairValueEstimator::new(&config);
        fv.strike_price = 100_000.0;
        for i in 0..20 {
            fv.update_vol(100_000.0 + (i as f64 * 0.5), i * 50);
        }
        fv.update_btc_price(100_000.0, 1000);

        // Early in market (280s remaining)
        fv.recompute(300_000, 0.03, 20_000);
        let edge_early = fv.edge;

        // Late in market (30s remaining)
        fv.recompute(300_000, 0.03, 270_000);
        let edge_late = fv.edge;

        assert!(
            edge_early > edge_late,
            "edge should be wider early ({edge_early}) than late ({edge_late})"
        );
    }

    #[test]
    fn test_fair_value_clamped() {
        let config = FairValueConfig::default();
        let mut fv = FairValueEstimator::new(&config);
        fv.strike_price = 100_000.0;

        for i in 0..20 {
            fv.update_vol(100_000.0 + (i as f64 * 100.0), i * 50);
        }
        fv.update_btc_price(200_000.0, 1000);
        fv.recompute(300_000, 0.03, 1000);

        assert!(fv.yes_fair_value <= 0.98);
        assert!(fv.yes_fair_value >= 0.02);
        assert!(fv.no_fair_value <= 0.98);
        assert!(fv.no_fair_value >= 0.02);
    }

    #[test]
    fn test_strike_warmup() {
        let config = FairValueConfig::default();
        let mut fv = FairValueEstimator::new(&config);

        // Need 5 samples (default strike_warmup_count)
        assert!(!fv.try_set_strike(100_010.0));
        assert!(!fv.try_set_strike(100_000.0));
        assert!(!fv.try_set_strike(99_990.0));
        assert!(!fv.try_set_strike(100_005.0));
        // 5th sample triggers strike
        assert!(fv.try_set_strike(99_995.0));

        // Median of [99990, 99995, 100000, 100005, 100010] = 100000
        assert!(
            (fv.strike_price - 100_000.0).abs() < 1.0,
            "strike should be median ~100000, got {}",
            fv.strike_price
        );
    }

    #[test]
    fn test_is_stale() {
        let config = FairValueConfig::default();
        let mut fv = FairValueEstimator::new(&config);

        // Never updated → stale
        assert!(fv.is_stale(1000));

        // Feed some data
        for i in 0..15 {
            fv.update_vol(100_000.0 + i as f64, i * 50);
        }
        // Fresh right after update (last update at 700ms)
        assert!(!fv.is_stale(800));
        // Stale after freshness window (500ms)
        assert!(fv.is_stale(1500));
    }

    #[test]
    fn test_reset() {
        let config = FairValueConfig::default();
        let mut fv = FairValueEstimator::new(&config);
        fv.strike_price = 100_000.0;
        fv.update_btc_price(100_050.0, 1000);
        fv.reset(&config);
        assert_eq!(fv.strike_price, 0.0);
        assert!(!fv.is_warm());
        assert!(fv.strike_warmup_buffer.is_empty());
    }

    // ── Sigmoid/Logit tests ──

    #[test]
    fn test_sigmoid_at_zero() {
        assert!((sigmoid(0.0) - 0.5).abs() < 1e-10);
    }

    #[test]
    fn test_sigmoid_large_positive() {
        assert!(sigmoid(10.0) > 0.999);
    }

    #[test]
    fn test_sigmoid_large_negative() {
        assert!(sigmoid(-10.0) < 0.001);
    }

    #[test]
    fn test_logit_inverse() {
        let p = 0.3;
        let recovered = sigmoid(logit(p));
        assert!((recovered - p).abs() < 1e-10, "sigmoid(logit({p})) should = {p}, got {recovered}");
    }

    #[test]
    fn test_logit_symmetry() {
        let l1 = logit(0.3);
        let l2 = logit(0.7);
        assert!((l1 + l2).abs() < 1e-10, "logit(0.3) + logit(0.7) should = 0");
    }

    #[test]
    fn test_logit_momentum_smaller_at_extremes() {
        // Same logit shift should produce smaller probability change at P=0.90 vs P=0.50
        let shift = 0.15;
        let delta_at_50 = sigmoid(logit(0.50) + shift) - 0.50;
        let delta_at_90 = sigmoid(logit(0.90) + shift) - 0.90;
        assert!(
            delta_at_50.abs() > delta_at_90.abs(),
            "logit momentum should have less effect at extremes: delta@0.50={delta_at_50:.4}, delta@0.90={delta_at_90:.4}"
        );
    }
}
