//! Individual metric trackers for the buildup detection system.
//!
//! Each tracker follows a common pattern:
//! - `update()` — feed new data, update internal EMA/state
//! - `normalized()` — return [0, 1] value (0 if stale)
//! - `direction()` — sign of raw value → `Option<Direction>`
//! - `is_fresh()` — whether the metric has been updated within its freshness window
//! - `raw()` — current raw (unnormalized) value

use std::collections::VecDeque;

use crate::types::market::Direction;

// ─── Common helpers ──────────────────────────────────────────────────────────

/// Normalize a value into [0, 1] given min threshold and saturation point.
/// Values below min → 0, above saturation → 1.
#[inline]
fn normalize(value: f64, min_threshold: f64, saturation: f64) -> f64 {
    if saturation <= min_threshold {
        return 0.0;
    }
    let abs = value.abs();
    if abs < min_threshold {
        return 0.0;
    }
    ((abs - min_threshold) / (saturation - min_threshold)).clamp(0.0, 1.0)
}

/// Compute time-based EMA alpha from elapsed time and half-life.
/// At dt = halflife_ms, alpha = 0.5 (old value halved).
/// Uses base-2 exponential matching LiqPressureTracker's decay formula.
#[inline]
fn time_alpha(dt_ms: f64, halflife_ms: f64) -> f64 {
    1.0 - 2.0_f64.powf(-dt_ms / halflife_ms)
}

/// Convert a signed f64 to a direction.
#[inline]
fn sign_to_direction(value: f64) -> Option<Direction> {
    if value > 0.0 {
        Some(Direction::Up)
    } else if value < 0.0 {
        Some(Direction::Down)
    } else {
        None
    }
}

// ─── 1. CVD Acceleration Tracker ─────────────────────────────────────────────

/// Cumulative Volume Delta acceleration from Binance Futures @aggTrade.
///
/// Two EMAs (fast/slow) of signed trade quantity. Acceleration = fast - slow.
/// Positive acceleration → buying accelerating → bullish.
/// Uses time-based EMA: alpha = 1 - 2^(-dt/halflife_ms) for consistent
/// smoothing regardless of data arrival rate.
pub struct CvdAccelTracker {
    cvd_fast_ema: f64,
    cvd_slow_ema: f64,
    accel: f64,
    halflife_fast_ms: f64,
    halflife_slow_ms: f64,
    last_update_ms: u64,
    freshness_max_ms: u64,
    min_threshold: f64,
    saturation: f64,
    samples: u32,
}

impl CvdAccelTracker {
    pub fn new(
        halflife_fast_ms: f64,
        halflife_slow_ms: f64,
        freshness_max_ms: u64,
        min_threshold: f64,
        saturation: f64,
    ) -> Self {
        Self {
            cvd_fast_ema: 0.0,
            cvd_slow_ema: 0.0,
            accel: 0.0,
            halflife_fast_ms,
            halflife_slow_ms,
            last_update_ms: 0,
            freshness_max_ms,
            min_threshold,
            saturation,
            samples: 0,
        }
    }

    /// Feed a futures aggregated trade.
    /// `is_buyer_maker`: true = seller is aggressor (bearish), false = buyer is aggressor (bullish).
    pub fn update(&mut self, quantity: f64, is_buyer_maker: bool, now_ms: u64) {
        let signed_qty = if is_buyer_maker { -quantity } else { quantity };
        if self.samples == 0 {
            self.cvd_fast_ema = signed_qty;
            self.cvd_slow_ema = signed_qty;
        } else {
            let dt = now_ms.saturating_sub(self.last_update_ms) as f64;
            let af = time_alpha(dt, self.halflife_fast_ms);
            let a_s = time_alpha(dt, self.halflife_slow_ms);
            self.cvd_fast_ema = af * signed_qty + (1.0 - af) * self.cvd_fast_ema;
            self.cvd_slow_ema = a_s * signed_qty + (1.0 - a_s) * self.cvd_slow_ema;
        }
        self.accel = self.cvd_fast_ema - self.cvd_slow_ema;
        self.last_update_ms = now_ms;
        self.samples += 1;
    }

    pub fn normalized(&self, now_ms: u64) -> f64 {
        if !self.is_fresh(now_ms) || self.samples < 10 {
            return 0.0;
        }
        normalize(self.accel, self.min_threshold, self.saturation)
    }

    pub fn direction(&self) -> Option<Direction> {
        if self.samples < 10 {
            return None;
        }
        sign_to_direction(self.accel)
    }

    pub fn is_fresh(&self, now_ms: u64) -> bool {
        self.last_update_ms > 0 && now_ms.saturating_sub(self.last_update_ms) <= self.freshness_max_ms
    }

    pub fn raw(&self) -> f64 {
        self.accel
    }
}

// ─── 2. Spot Trade Flow Tracker ──────────────────────────────────────────────

/// Net spot trade flow from Binance Spot SBE @trade.
///
/// Separate EMAs for buy vs sell volume. Flow = buy_ema - sell_ema.
/// Positive flow → net buying → bullish.
/// Uses time-based EMA for consistent smoothing regardless of trade frequency.
pub struct SpotFlowTracker {
    buy_vol_ema: f64,
    sell_vol_ema: f64,
    flow: f64,
    halflife_ms: f64,
    last_update_ms: u64,
    freshness_max_ms: u64,
    min_threshold: f64,
    saturation: f64,
    samples: u32,
}

impl SpotFlowTracker {
    pub fn new(halflife_ms: f64, freshness_max_ms: u64, min_threshold: f64, saturation: f64) -> Self {
        Self {
            buy_vol_ema: 0.0,
            sell_vol_ema: 0.0,
            flow: 0.0,
            halflife_ms,
            last_update_ms: 0,
            freshness_max_ms,
            min_threshold,
            saturation,
            samples: 0,
        }
    }

    /// Feed a spot trade.
    pub fn update(&mut self, quantity: f64, is_buyer_maker: bool, now_ms: u64) {
        let alpha = if self.samples == 0 {
            1.0
        } else {
            let dt = now_ms.saturating_sub(self.last_update_ms) as f64;
            time_alpha(dt, self.halflife_ms)
        };
        if is_buyer_maker {
            // Seller is aggressor.
            self.sell_vol_ema = alpha * quantity + (1.0 - alpha) * self.sell_vol_ema;
        } else {
            // Buyer is aggressor.
            self.buy_vol_ema = alpha * quantity + (1.0 - alpha) * self.buy_vol_ema;
        }
        self.flow = self.buy_vol_ema - self.sell_vol_ema;
        self.last_update_ms = now_ms;
        self.samples += 1;
    }

    pub fn normalized(&self, now_ms: u64) -> f64 {
        if !self.is_fresh(now_ms) || self.samples < 10 {
            return 0.0;
        }
        normalize(self.flow, self.min_threshold, self.saturation)
    }

    pub fn direction(&self) -> Option<Direction> {
        if self.samples < 10 {
            return None;
        }
        sign_to_direction(self.flow)
    }

    pub fn is_fresh(&self, now_ms: u64) -> bool {
        self.last_update_ms > 0 && now_ms.saturating_sub(self.last_update_ms) <= self.freshness_max_ms
    }

    pub fn raw(&self) -> f64 {
        self.flow
    }
}

// ─── 3. OBI Velocity Tracker ─────────────────────────────────────────────────

/// Order Book Imbalance velocity from Binance Spot @depth20.
///
/// Tracks the rate of change of OBI (not OBI itself). Accelerating bid-heavy → bullish.
/// Uses time-based EMA for consistent smoothing at any depth snapshot cadence.
pub struct ObiVelocityTracker {
    prev_obi: f64,
    obi_delta_ema: f64,
    halflife_ms: f64,
    last_update_ms: u64,
    freshness_max_ms: u64,
    min_threshold: f64,
    saturation: f64,
    initialized: bool,
}

impl ObiVelocityTracker {
    pub fn new(halflife_ms: f64, freshness_max_ms: u64, min_threshold: f64, saturation: f64) -> Self {
        Self {
            prev_obi: 0.0,
            obi_delta_ema: 0.0,
            halflife_ms,
            last_update_ms: 0,
            freshness_max_ms,
            min_threshold,
            saturation,
            initialized: false,
        }
    }

    /// Feed a depth snapshot OBI value (range [-1, +1]).
    pub fn update(&mut self, obi: f64, now_ms: u64) {
        if self.initialized {
            let delta = obi - self.prev_obi;
            let dt = now_ms.saturating_sub(self.last_update_ms) as f64;
            let alpha = time_alpha(dt, self.halflife_ms);
            self.obi_delta_ema = alpha * delta + (1.0 - alpha) * self.obi_delta_ema;
        }
        self.prev_obi = obi;
        self.last_update_ms = now_ms;
        self.initialized = true;
    }

    pub fn normalized(&self, now_ms: u64) -> f64 {
        if !self.is_fresh(now_ms) || !self.initialized {
            return 0.0;
        }
        normalize(self.obi_delta_ema, self.min_threshold, self.saturation)
    }

    pub fn direction(&self) -> Option<Direction> {
        if !self.initialized {
            return None;
        }
        sign_to_direction(self.obi_delta_ema)
    }

    pub fn is_fresh(&self, now_ms: u64) -> bool {
        self.last_update_ms > 0 && now_ms.saturating_sub(self.last_update_ms) <= self.freshness_max_ms
    }

    pub fn raw(&self) -> f64 {
        self.obi_delta_ema
    }
}

// ─── 4. Basis Delta Tracker ──────────────────────────────────────────────────

/// Futures-spot basis rate of change from @bookTicker + spot mid.
///
/// basis_bps = ((futures_mid - spot_mid) / spot_mid) * 10000.
/// Tracks delta of basis via EMA. Rising basis → futures leading up → bullish.
/// Uses time-based EMA for consistent smoothing across variable update rates.
pub struct BasisDeltaTracker {
    futures_mid: f64,
    spot_mid: f64,
    prev_basis_bps: f64,
    basis_delta_ema: f64,
    halflife_ms: f64,
    last_update_ms: u64,
    freshness_max_ms: u64,
    min_threshold: f64,
    saturation: f64,
    initialized: bool,
}

impl BasisDeltaTracker {
    pub fn new(halflife_ms: f64, freshness_max_ms: u64, min_threshold: f64, saturation: f64) -> Self {
        Self {
            futures_mid: 0.0,
            spot_mid: 0.0,
            prev_basis_bps: 0.0,
            basis_delta_ema: 0.0,
            halflife_ms,
            last_update_ms: 0,
            freshness_max_ms,
            min_threshold,
            saturation,
            initialized: false,
        }
    }

    /// Update futures mid price (from @bookTicker).
    pub fn update_futures_mid(&mut self, bid: f64, ask: f64, now_ms: u64) {
        self.futures_mid = (bid + ask) / 2.0;
        self.recompute(now_ms);
    }

    /// Update spot mid price (from BinanceTick or BinanceDepth).
    pub fn update_spot_mid(&mut self, mid: f64, now_ms: u64) {
        self.spot_mid = mid;
        self.recompute(now_ms);
    }

    fn recompute(&mut self, now_ms: u64) {
        if self.futures_mid <= 0.0 || self.spot_mid <= 0.0 {
            return;
        }
        let basis_bps = ((self.futures_mid - self.spot_mid) / self.spot_mid) * 10_000.0;
        if self.initialized {
            let delta = basis_bps - self.prev_basis_bps;
            let dt = now_ms.saturating_sub(self.last_update_ms) as f64;
            let alpha = time_alpha(dt, self.halflife_ms);
            self.basis_delta_ema = alpha * delta + (1.0 - alpha) * self.basis_delta_ema;
        }
        self.prev_basis_bps = basis_bps;
        self.last_update_ms = now_ms;
        self.initialized = true;
    }

    pub fn normalized(&self, now_ms: u64) -> f64 {
        if !self.is_fresh(now_ms) || !self.initialized {
            return 0.0;
        }
        normalize(self.basis_delta_ema, self.min_threshold, self.saturation)
    }

    pub fn direction(&self) -> Option<Direction> {
        if !self.initialized {
            return None;
        }
        sign_to_direction(self.basis_delta_ema)
    }

    pub fn is_fresh(&self, now_ms: u64) -> bool {
        self.last_update_ms > 0 && now_ms.saturating_sub(self.last_update_ms) <= self.freshness_max_ms
    }

    pub fn raw(&self) -> f64 {
        self.basis_delta_ema
    }
}

// ─── 5. Liquidation Pressure Tracker ─────────────────────────────────────────

/// Time-decaying sum of forced liquidation volume from @forceOrder.
///
/// Each liquidation contributes signed_qty × 2^(-(now - event_time) / half_life).
/// Positive = short liquidations dominating → bullish.
pub struct LiqPressureTracker {
    events: VecDeque<(f64, u64)>, // (signed_qty, timestamp_ms)
    half_life_ms: f64,
    last_update_ms: u64,
    freshness_max_ms: u64,
    min_threshold: f64,
    saturation: f64,
}

impl LiqPressureTracker {
    pub fn new(half_life_ms: f64, freshness_max_ms: u64, min_threshold: f64, saturation: f64) -> Self {
        Self {
            events: VecDeque::new(),
            half_life_ms,
            last_update_ms: 0,
            freshness_max_ms,
            min_threshold,
            saturation,
        }
    }

    /// Feed a forced liquidation event.
    /// `side`: "SELL" = long liquidated (bearish), "BUY" = short liquidated (bullish).
    pub fn update(&mut self, side: &str, quantity: f64, now_ms: u64) {
        let signed_qty = if side == "BUY" { quantity } else { -quantity };
        self.events.push_back((signed_qty, now_ms));
        self.last_update_ms = now_ms;
        // Prune events older than 5 × half_life (contribution < 3%).
        let cutoff = now_ms.saturating_sub((self.half_life_ms * 5.0) as u64);
        while self.events.front().is_some_and(|(_, ts)| *ts < cutoff) {
            self.events.pop_front();
        }
    }

    /// Compute the time-decaying sum.
    fn decaying_sum(&self, now_ms: u64) -> f64 {
        self.events
            .iter()
            .map(|(qty, ts)| {
                let age_ms = now_ms.saturating_sub(*ts) as f64;
                qty * 2.0_f64.powf(-age_ms / self.half_life_ms)
            })
            .sum()
    }

    pub fn normalized(&self, now_ms: u64) -> f64 {
        if !self.is_fresh(now_ms) || self.events.is_empty() {
            return 0.0;
        }
        normalize(self.decaying_sum(now_ms), self.min_threshold, self.saturation)
    }

    pub fn direction(&self) -> Option<Direction> {
        if self.events.is_empty() {
            return None;
        }
        // Use last_update_ms as "now" for direction check.
        sign_to_direction(self.decaying_sum(self.last_update_ms))
    }

    pub fn is_fresh(&self, now_ms: u64) -> bool {
        self.last_update_ms > 0 && now_ms.saturating_sub(self.last_update_ms) <= self.freshness_max_ms
    }

    pub fn raw(&self) -> f64 {
        self.decaying_sum(self.last_update_ms)
    }
}

// ─── 6. ATR Displacement Tracker ─────────────────────────────────────────────

/// Price displacement in ATR multiples from Binance Spot @depth20.
///
/// Adapted from the existing SpikeDetector's EMA-ATR logic but as a continuous
/// metric rather than a binary spike trigger.
pub struct AtrDisplacementTracker {
    ema_atr: f64,
    prev_mid: f64,
    displacement_ratio: f64,
    atr_alpha: f64,
    last_update_ms: u64,
    freshness_max_ms: u64,
    min_threshold: f64,
    saturation: f64,
    samples: u32,
    min_warmup: u32,
}

impl AtrDisplacementTracker {
    pub fn new(
        atr_alpha: f64,
        freshness_max_ms: u64,
        min_threshold: f64,
        saturation: f64,
        min_warmup: u32,
    ) -> Self {
        Self {
            ema_atr: 0.0,
            prev_mid: 0.0,
            displacement_ratio: 0.0,
            atr_alpha,
            last_update_ms: 0,
            freshness_max_ms,
            min_threshold,
            saturation,
            samples: 0,
            min_warmup,
        }
    }

    /// Feed a spot depth mid-price.
    pub fn update(&mut self, mid: f64, now_ms: u64) {
        if self.samples > 0 && self.prev_mid > 0.0 {
            let abs_delta = (mid - self.prev_mid).abs();
            self.ema_atr = self.atr_alpha * abs_delta + (1.0 - self.atr_alpha) * self.ema_atr;
            if self.ema_atr > 0.0 {
                self.displacement_ratio = (mid - self.prev_mid) / self.ema_atr;
            }
        }
        self.prev_mid = mid;
        self.last_update_ms = now_ms;
        self.samples += 1;
    }

    pub fn normalized(&self, now_ms: u64) -> f64 {
        if !self.is_fresh(now_ms) || self.samples < self.min_warmup {
            return 0.0;
        }
        normalize(self.displacement_ratio, self.min_threshold, self.saturation)
    }

    pub fn direction(&self) -> Option<Direction> {
        if self.samples < self.min_warmup {
            return None;
        }
        sign_to_direction(self.displacement_ratio)
    }

    pub fn is_fresh(&self, now_ms: u64) -> bool {
        self.last_update_ms > 0 && now_ms.saturating_sub(self.last_update_ms) <= self.freshness_max_ms
    }

    pub fn raw(&self) -> f64 {
        self.displacement_ratio
    }

    /// Current EMA ATR value (for Phase B observed displacement normalization).
    pub fn ema_atr(&self) -> f64 {
        self.ema_atr
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── time_alpha helper ────────────────────────────────────────────────

    #[test]
    fn test_time_alpha_at_halflife() {
        let alpha = time_alpha(200.0, 200.0);
        assert!((alpha - 0.5).abs() < 1e-10);
    }

    #[test]
    fn test_time_alpha_at_zero() {
        let alpha = time_alpha(0.0, 200.0);
        assert!((alpha - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_time_alpha_large_dt() {
        // 10x halflife → alpha ≈ 0.999
        let alpha = time_alpha(2000.0, 200.0);
        assert!(alpha > 0.99);
    }

    // ── Normalize helper ─────────────────────────────────────────────────

    #[test]
    fn test_normalize_below_min() {
        assert_eq!(normalize(0.05, 0.1, 1.0), 0.0);
    }

    #[test]
    fn test_normalize_at_saturation() {
        assert_eq!(normalize(1.0, 0.0, 1.0), 1.0);
    }

    #[test]
    fn test_normalize_above_saturation() {
        assert_eq!(normalize(2.0, 0.0, 1.0), 1.0);
    }

    #[test]
    fn test_normalize_midpoint() {
        let v = normalize(0.5, 0.0, 1.0);
        assert!((v - 0.5).abs() < 1e-10);
    }

    #[test]
    fn test_normalize_negative() {
        // Uses abs, so negative values work symmetrically.
        assert_eq!(normalize(-1.0, 0.0, 1.0), 1.0);
    }

    // ── CVD Acceleration ─────────────────────────────────────────────────

    #[test]
    fn test_cvd_warmup_returns_zero() {
        let tracker = CvdAccelTracker::new(150.0, 700.0, 300, 0.0, 1.0);
        assert_eq!(tracker.normalized(100), 0.0);
        assert!(tracker.direction().is_none());
    }

    #[test]
    fn test_cvd_bullish_acceleration() {
        let mut tracker = CvdAccelTracker::new(150.0, 700.0, 300, 0.0, 1.0);
        for i in 0..50 {
            // Aggressive buying (is_buyer_maker=false → positive).
            tracker.update(1.0, false, i * 10);
        }
        assert!(tracker.raw() > 0.0);
        assert_eq!(tracker.direction(), Some(Direction::Up));
        assert!(tracker.normalized(500) > 0.0);
    }

    #[test]
    fn test_cvd_bearish_acceleration() {
        let mut tracker = CvdAccelTracker::new(150.0, 700.0, 300, 0.0, 1.0);
        for i in 0..50 {
            // Aggressive selling (is_buyer_maker=true → negative).
            tracker.update(1.0, true, i * 10);
        }
        assert!(tracker.raw() < 0.0);
        assert_eq!(tracker.direction(), Some(Direction::Down));
    }

    #[test]
    fn test_cvd_staleness() {
        let mut tracker = CvdAccelTracker::new(150.0, 700.0, 300, 0.0, 1.0);
        for i in 0..50 {
            tracker.update(1.0, false, i * 10);
        }
        // 500ms is within freshness window (300ms), but last update at 490.
        assert!(tracker.is_fresh(500));
        // Way past freshness.
        assert!(!tracker.is_fresh(1000));
        assert_eq!(tracker.normalized(1000), 0.0);
    }

    // ── Spot Flow ────────────────────────────────────────────────────────

    #[test]
    fn test_spot_flow_buy_dominant() {
        let mut tracker = SpotFlowTracker::new(300.0, 200, 0.0, 1.0);
        for i in 0..50 {
            tracker.update(1.0, false, i * 10); // buyer aggressor
        }
        assert!(tracker.raw() > 0.0);
        assert_eq!(tracker.direction(), Some(Direction::Up));
    }

    #[test]
    fn test_spot_flow_sell_dominant() {
        let mut tracker = SpotFlowTracker::new(300.0, 200, 0.0, 1.0);
        for i in 0..50 {
            tracker.update(1.0, true, i * 10); // seller aggressor
        }
        assert!(tracker.raw() < 0.0);
        assert_eq!(tracker.direction(), Some(Direction::Down));
    }

    // ── OBI Velocity ─────────────────────────────────────────────────────

    #[test]
    fn test_obi_velocity_rising() {
        let mut tracker = ObiVelocityTracker::new(300.0, 100, 0.0, 0.5);
        // OBI rising: 0.0 → 0.1 → 0.2 → 0.3
        for i in 0..4 {
            tracker.update(i as f64 * 0.1, i as u64 * 50);
        }
        assert!(tracker.raw() > 0.0);
        assert_eq!(tracker.direction(), Some(Direction::Up));
    }

    #[test]
    fn test_obi_velocity_falling() {
        let mut tracker = ObiVelocityTracker::new(300.0, 100, 0.0, 0.5);
        // OBI falling: 0.3 → 0.2 → 0.1 → 0.0
        for i in 0..4 {
            tracker.update(0.3 - i as f64 * 0.1, i as u64 * 50);
        }
        assert!(tracker.raw() < 0.0);
        assert_eq!(tracker.direction(), Some(Direction::Down));
    }

    // ── Basis Delta ──────────────────────────────────────────────────────

    #[test]
    fn test_basis_delta_futures_premium_rising() {
        let mut tracker = BasisDeltaTracker::new(300.0, 300, 0.0, 2.0);
        tracker.update_spot_mid(50000.0, 100);
        // Futures premium increasing: 50001, 50002, 50003
        tracker.update_futures_mid(50000.5, 50001.5, 100);
        tracker.update_futures_mid(50001.5, 50002.5, 200);
        tracker.update_futures_mid(50002.5, 50003.5, 300);
        // Basis is increasing → delta should be positive.
        assert!(tracker.raw() > 0.0);
        assert_eq!(tracker.direction(), Some(Direction::Up));
    }

    // ── Liquidation Pressure ─────────────────────────────────────────────

    #[test]
    fn test_liq_pressure_short_squeezes() {
        let mut tracker = LiqPressureTracker::new(2000.0, 3000, 0.0, 10.0);
        // Short liquidations (side="BUY") → bullish pressure.
        tracker.update("BUY", 5.0, 1000);
        tracker.update("BUY", 3.0, 1500);
        assert!(tracker.raw() > 0.0);
        assert_eq!(tracker.direction(), Some(Direction::Up));
        assert!(tracker.normalized(1500) > 0.0);
    }

    #[test]
    fn test_liq_pressure_long_liquidations() {
        let mut tracker = LiqPressureTracker::new(2000.0, 3000, 0.0, 10.0);
        // Long liquidations (side="SELL") → bearish pressure.
        tracker.update("SELL", 5.0, 1000);
        assert!(tracker.raw() < 0.0);
        assert_eq!(tracker.direction(), Some(Direction::Down));
    }

    #[test]
    fn test_liq_pressure_decay() {
        let mut tracker = LiqPressureTracker::new(2000.0, 3000, 0.0, 10.0);
        tracker.update("BUY", 10.0, 0);
        let raw_at_0 = tracker.decaying_sum(0);
        let raw_at_2000 = tracker.decaying_sum(2000); // 1 half-life later
        // Should be approximately half.
        assert!((raw_at_2000 - raw_at_0 / 2.0).abs() < 0.01);
    }

    #[test]
    fn test_liq_pressure_pruning() {
        let mut tracker = LiqPressureTracker::new(2000.0, 3000, 0.0, 10.0);
        tracker.update("BUY", 10.0, 0);
        // Update at 11000 (> 5 × half_life = 10000) → old event pruned.
        tracker.update("BUY", 1.0, 11000);
        assert_eq!(tracker.events.len(), 1);
    }

    // ── ATR Displacement ─────────────────────────────────────────────────

    #[test]
    fn test_atr_displacement_warmup() {
        let tracker = AtrDisplacementTracker::new(0.002, 100, 0.0, 15.0, 50);
        assert_eq!(tracker.normalized(0), 0.0);
        assert!(tracker.direction().is_none());
    }

    #[test]
    fn test_atr_displacement_spike_up() {
        let mut tracker = AtrDisplacementTracker::new(0.1, 100, 0.0, 5.0, 10);
        // Warm up with stable prices.
        for i in 0..20 {
            tracker.update(50000.0 + (i as f64 * 0.1), i as u64 * 50);
        }
        // Big jump.
        tracker.update(50010.0, 1050);
        assert!(tracker.raw() > 0.0);
        assert_eq!(tracker.direction(), Some(Direction::Up));
    }

    #[test]
    fn test_atr_displacement_spike_down() {
        let mut tracker = AtrDisplacementTracker::new(0.1, 100, 0.0, 5.0, 10);
        for i in 0..20 {
            tracker.update(50000.0 + (i as f64 * 0.1), i as u64 * 50);
        }
        // Big drop.
        tracker.update(49990.0, 1050);
        assert!(tracker.raw() < 0.0);
        assert_eq!(tracker.direction(), Some(Direction::Down));
    }
}
