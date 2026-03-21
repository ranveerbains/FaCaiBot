//! Individual metric trackers for the buildup detection system.
//!
//! Each tracker follows a common pattern:
//! - `update()` — feed new data, update internal EMA/state
//! - `normalized()` — return [0, 1] value (0 if stale)
//! - `direction()` — sign of raw value → `Option<Direction>`
//! - `is_fresh()` — whether the metric has been updated within its freshness window
//! - `raw()` — current raw (unnormalized) value

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
/// Uses base-2 exponential decay formula.
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

// ─── 2. OBI Velocity Tracker ─────────────────────────────────────────────────

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

// ─── 3. Basis Delta Tracker ──────────────────────────────────────────────────

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

// ─── 4. Realized Volatility Tracker ─────────────────────────────────────────

/// Rolling realized volatility from BinanceTick mid-price log-returns.
///
/// Maintains a ring buffer of log-returns and computes per-tick standard
/// deviation (zero-mean assumption, valid for short windows like 5 minutes).
/// Two instances are used: short-window (~30s) for the base model, and
/// session-window (full 5-min) for regime detection.
pub struct RealizedVolTracker {
    /// Ring buffer of log-returns.
    returns: Vec<f64>,
    /// Write position in ring buffer.
    head: usize,
    /// Number of samples collected (may exceed capacity).
    count: usize,
    /// Running sum of squared returns for fast vol computation.
    sum_sq: f64,
    /// Previous price for computing log-returns.
    prev_price: f64,
    /// Maximum capacity of the ring buffer.
    capacity: usize,
    /// Minimum samples before returning a valid vol.
    min_warmup: usize,
    /// Default vol to return when not warmed up.
    default_vol: f64,
    /// Estimated ticks per second (for time scaling).
    ticks_per_sec: f64,
    /// Timestamp of last update.
    last_update_ms: u64,
    /// Maximum age before considered stale.
    freshness_max_ms: u64,
}

impl RealizedVolTracker {
    pub fn new(
        capacity: usize,
        min_warmup: usize,
        default_vol: f64,
        ticks_per_sec: f64,
        freshness_max_ms: u64,
    ) -> Self {
        Self {
            returns: vec![0.0; capacity],
            head: 0,
            count: 0,
            sum_sq: 0.0,
            prev_price: 0.0,
            capacity,
            min_warmup,
            default_vol,
            ticks_per_sec,
            last_update_ms: 0,
            freshness_max_ms,
        }
    }

    /// Feed a new mid-price. Computes log-return and updates ring buffer.
    pub fn update(&mut self, price: f64, now_ms: u64) {
        if price <= 0.0 {
            return;
        }
        if self.prev_price > 0.0 {
            let log_return = (price / self.prev_price).ln();
            // Evict oldest if buffer is full
            if self.count >= self.capacity {
                let old = self.returns[self.head];
                self.sum_sq -= old * old;
            }
            self.returns[self.head] = log_return;
            self.sum_sq += log_return * log_return;
            self.head = (self.head + 1) % self.capacity;
            self.count += 1;
        }
        self.prev_price = price;
        self.last_update_ms = now_ms;
    }

    /// Per-tick standard deviation of log-returns.
    pub fn realized_vol(&self) -> f64 {
        let n = self.count.min(self.capacity);
        if n < self.min_warmup {
            return self.default_vol;
        }
        // Clamp sum_sq to avoid negative due to floating-point drift
        let variance = (self.sum_sq / n as f64).max(0.0);
        variance.sqrt()
    }

    /// Volatility scaled to remaining time: σ_tick × √(ticks_per_sec × remaining_secs).
    pub fn scaled_vol(&self, remaining_secs: f64) -> f64 {
        let ticks_remaining = self.ticks_per_sec * remaining_secs;
        self.realized_vol() * ticks_remaining.max(1.0).sqrt()
    }

    /// Whether the tracker has been updated recently.
    pub fn is_fresh(&self, now_ms: u64) -> bool {
        self.last_update_ms > 0
            && now_ms.saturating_sub(self.last_update_ms) <= self.freshness_max_ms
    }

    /// Number of samples collected so far.
    pub fn sample_count(&self) -> usize {
        self.count
    }

    /// Whether the tracker has enough samples for valid vol computation.
    pub fn is_warm(&self) -> bool {
        self.count >= self.min_warmup
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "tests/metrics_tests.rs"]
mod tests;
