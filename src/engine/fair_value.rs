//! Fair value estimator — continuous P(BTC_expiry > BTC_open) model.
//!
//! Uses a corrected binary option model: log-displacement / scaled realized vol
//! through the normal CDF, plus momentum adjustments from Binance metrics.
//! The normal CDF does the heavy lifting; momentum is a secondary correction.

use rust_decimal::Decimal;

use crate::engine::buildup::metrics::{
    BasisDeltaTracker, CvdAccelTracker, ObiVelocityTracker, RealizedVolTracker,
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
            tail_compression_factor: 0.85,
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
    // Momentum trackers
    basis_tracker: BasisDeltaTracker,
    cvd_tracker: CvdAccelTracker,
    obi_tracker: ObiVelocityTracker,
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

    /// Update spot mid for basis tracker.
    pub fn update_spot_mid(&mut self, mid: f64, now_ms: u64) {
        self.basis_tracker.update_spot_mid(mid, now_ms);
    }

    /// Update futures mid (from FuturesBookTicker).
    pub fn update_futures_mid(&mut self, bid: f64, ask: f64, now_ms: u64) {
        self.basis_tracker.update_futures_mid(bid, ask, now_ms);
    }

    /// Update CVD (from FuturesAggTrade).
    pub fn update_cvd(&mut self, quantity: f64, is_buyer_maker: bool, now_ms: u64) {
        self.cvd_tracker.update(quantity, is_buyer_maker, now_ms);
    }

    /// Update OBI velocity (from BinanceDepth).
    pub fn update_obi(&mut self, obi: f64, now_ms: u64) {
        self.obi_tracker.update(obi, now_ms);
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
        let sigma_remaining = self.vol_tracker.scaled_vol(time_remaining_s).max(0.0001);
        let d = displacement / sigma_remaining;
        let d_adjusted = d * self.config.tail_compression_factor;
        let base_fv = phi(d_adjusted);

        // ── Momentum adjustment ──
        let basis_norm = self.basis_tracker.normalized(now_ms);
        let cvd_norm = self.cvd_tracker.normalized(now_ms);
        let obi_norm = self.obi_tracker.normalized(now_ms);

        let basis_signed = basis_norm * direction_sign(&self.basis_tracker);
        let cvd_signed = cvd_norm * direction_sign_cvd(&self.cvd_tracker);
        let obi_signed = obi_norm * direction_sign_obi(&self.obi_tracker);

        let momentum = self.config.momentum_weight_basis * basis_signed
            + self.config.momentum_weight_cvd * cvd_signed
            + self.config.momentum_weight_obi * obi_signed;

        let momentum_clamped = momentum.clamp(
            -self.config.max_momentum_adj,
            self.config.max_momentum_adj,
        );

        self.last_momentum = momentum_clamped;
        self.yes_fair_value = (base_fv + momentum_clamped).clamp(0.02, 0.98);
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

        // Stale momentum feed penalties
        let mut stale_count = 0u32;
        if !self.basis_tracker.is_fresh(now_ms) {
            stale_count += 1;
        }
        if !self.cvd_tracker.is_fresh(now_ms) {
            stale_count += 1;
        }
        if !self.obi_tracker.is_fresh(now_ms) {
            stale_count += 1;
        }
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

fn direction_sign(tracker: &BasisDeltaTracker) -> f64 {
    match tracker.direction() {
        Some(crate::types::market::Direction::Up) => 1.0,
        Some(crate::types::market::Direction::Down) => -1.0,
        None => 0.0,
    }
}

fn direction_sign_cvd(tracker: &CvdAccelTracker) -> f64 {
    match tracker.direction() {
        Some(crate::types::market::Direction::Up) => 1.0,
        Some(crate::types::market::Direction::Down) => -1.0,
        None => 0.0,
    }
}

fn direction_sign_obi(tracker: &ObiVelocityTracker) -> f64 {
    match tracker.direction() {
        Some(crate::types::market::Direction::Up) => 1.0,
        Some(crate::types::market::Direction::Down) => -1.0,
        None => 0.0,
    }
}

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
}
