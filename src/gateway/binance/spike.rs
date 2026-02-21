//! EMA-ATR spike detector for Binance price feeds.
//!
//! Detects large, sustained mid-price movements ("spikes") in the BTC/USDT
//! depth stream. Uses a rolling EMA-ATR with phantom-reversion filtering to
//! reduce false positives from momentary liquidity gaps.

use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use tracing::{debug, info};

use crate::config::SpikeDetectionConfig;
use crate::types::market::{Direction, SpikeInfo};

// ─── Constants ────────────────────────────────────────────────────────────────

/// Minimum number of depth samples before the fast ATR is considered reliable.
/// Below this, the slow 5-min fallback EMA is used.
pub(super) const MIN_ATR_SAMPLES: usize = 10;

// ─── Internal structs ─────────────────────────────────────────────────────────

/// Pending spike confirmed by sustain but not yet emitted (phantom window active).
pub(super) struct PendingSpike {
    pub(super) info: SpikeInfo,
    /// Mid-price at the moment sustain was confirmed (for phantom reversion check).
    pub(super) sustain_mid: f64,
    /// Epoch ms when the sustain check passed.
    pub(super) sustain_confirmed_ms: u64,
}

// ─── SpikeDetector ────────────────────────────────────────────────────────────

/// Rolling EMA-ATR spike detector.
pub struct SpikeDetector {
    // ── Config ────────────────────────────────────────────────────────
    multiplier: f64,
    atr_alpha: f64,
    window_ms: u64,
    sustain_ms: u64,
    sustain_low_vol_ext_ms: u64,
    phantom_revert_fraction: f64,
    phantom_check_ms: u64,

    // ── ATR state ─────────────────────────────────────────────────────
    /// Fast 1-minute EMA of absolute mid-price deltas.
    ema_atr: f64,
    /// Slow ~5-minute fallback EMA (alpha ≈ 0.002).
    ema_atr_slow: f64,
    /// Very slow daily-average proxy (alpha ≈ 0.0001), used for low-vol detection.
    daily_avg_atr: f64,
    /// Number of samples seen; determines when fast ATR is reliable.
    sample_count: usize,
    /// Previous depth snapshot's mid-price.
    prev_mid: Option<f64>,
    /// Epoch ms of the previous depth snapshot.
    prev_ts_ms: u64,

    // ── Spike candidate state ─────────────────────────────────────────
    /// Epoch ms when the current spike candidate first appeared.
    spike_start_ms: Option<u64>,
    /// Direction of the spike candidate.
    spike_direction: Option<Direction>,
    /// Mid-price when the spike candidate started.
    spike_origin_mid: f64,
    /// Signed peak delta during the spike window.
    spike_peak_delta: f64,

    // ── Pending (post-sustain) confirmation ───────────────────────────
    /// Spike that passed sustain, now in the phantom-check window.
    pub(super) pending: Option<PendingSpike>,

    // ── Stale-event telemetry ─────────────────────────────────────────
    stale_count: u64,
    last_stale_log_ms: u64,
}

impl SpikeDetector {
    pub fn new(config: &SpikeDetectionConfig) -> Self {
        Self {
            multiplier: config.multiplier,
            atr_alpha: config.atr_alpha,
            window_ms: config.window_ms,
            sustain_ms: config.sustain_ms,
            sustain_low_vol_ext_ms: config.sustain_low_vol_ext_ms,
            phantom_revert_fraction: config.phantom_revert_fraction,
            phantom_check_ms: config.phantom_check_ms,
            ema_atr: 0.0,
            ema_atr_slow: 0.0,
            daily_avg_atr: 0.0,
            sample_count: 0,
            prev_mid: None,
            prev_ts_ms: 0,
            spike_start_ms: None,
            spike_direction: None,
            spike_origin_mid: 0.0,
            spike_peak_delta: 0.0,
            pending: None,
            stale_count: 0,
            last_stale_log_ms: 0,
        }
    }

    /// Feed a new mid-price sample from a `@depth20` snapshot.
    ///
    /// Returns `Some(SpikeInfo)` when a spike passes all filters and is ready
    /// to emit. Returns `None` if no spike is confirmed yet.
    pub fn update(&mut self, mid: f64, now_ms: u64) -> Option<SpikeInfo> {
        // ── Initialise on first sample ─────────────────────────────────
        let Some(prev_mid) = self.prev_mid else {
            self.prev_mid = Some(mid);
            self.prev_ts_ms = now_ms;
            return None;
        };

        let delta = mid - prev_mid;
        let abs_delta = delta.abs();

        // ── Update ATR estimates ───────────────────────────────────────
        self.sample_count += 1;
        if self.sample_count == 1 {
            self.ema_atr = abs_delta;
            self.ema_atr_slow = abs_delta;
            self.daily_avg_atr = abs_delta;
        } else {
            self.ema_atr = self.atr_alpha * abs_delta + (1.0 - self.atr_alpha) * self.ema_atr;
            self.ema_atr_slow = 0.002 * abs_delta + (1.0 - 0.002) * self.ema_atr_slow;
            self.daily_avg_atr = 0.0001 * abs_delta + (1.0 - 0.0001) * self.daily_avg_atr;
        }

        // Effective ATR: use slow fallback until we have enough fast samples.
        let atr = if self.sample_count < MIN_ATR_SAMPLES {
            self.ema_atr_slow.max(1e-10)
        } else {
            self.ema_atr.max(1e-10)
        };

        // ── Phantom reversion check (must run before spike candidate logic) ──
        if let Some(ref pending) = self.pending {
            let time_since_sustain = now_ms.saturating_sub(pending.sustain_confirmed_ms);

            if time_since_sustain <= self.phantom_check_ms {
                // Still within phantom-check window — check for reversion.
                let revert_delta = (mid - pending.sustain_mid).abs();
                let spike_delta = pending.info.magnitude.to_f64().unwrap_or(0.0) * prev_mid.abs();
                let revert_fraction = if spike_delta > 1e-12 {
                    revert_delta / spike_delta
                } else {
                    0.0
                };

                if revert_fraction > self.phantom_revert_fraction {
                    debug!(
                        revert_fraction,
                        "phantom spike discarded (>{:.0}% reversion in 100ms)",
                        self.phantom_revert_fraction * 100.0
                    );
                    self.pending = None;
                    // Spike is gone — fall through to normal candidate logic.
                } else {
                    // Still holding — wait for phantom window to close.
                    self.prev_mid = Some(mid);
                    self.prev_ts_ms = now_ms;
                    return None;
                }
            } else {
                // Phantom check window elapsed without reversion — emit spike.
                let confirmed_spike = self.pending.take().unwrap().info;
                debug!(
                    direction = ?confirmed_spike.direction,
                    magnitude_pct = %(confirmed_spike.magnitude.to_f64().unwrap_or(0.0) * 100.0),
                    sustained_ms = confirmed_spike.sustained_ms,
                    "spike confirmed — phantom window elapsed without reversion"
                );
                self.prev_mid = Some(mid);
                self.prev_ts_ms = now_ms;
                return Some(confirmed_spike);
            }
        }

        // ── Spike candidate detection ──────────────────────────────────
        let threshold = self.multiplier * atr;

        if let Some(spike_start) = self.spike_start_ms {
            // Active spike candidate — evaluate based on total displacement from
            // origin, not per-tick delta. After the initial spike tick the ATR is
            // inflated, so per-tick deltas (which are small during sustain) would
            // never exceed the threshold again.
            let displacement = mid - self.spike_origin_mid;
            let abs_displacement = displacement.abs();
            let elapsed = now_ms.saturating_sub(spike_start);

            if elapsed > self.window_ms {
                // Candidate timed out without reaching sustain threshold.
                debug!(elapsed, "spike candidate expired (window exceeded)");
                self.reset_candidate();
            } else if abs_displacement > threshold {
                // Price is still significantly displaced from origin — spike holds.
                // Track the largest displacement as peak delta.
                if abs_displacement > self.spike_peak_delta.abs() {
                    self.spike_peak_delta = displacement;
                }

                // Check sustain: dynamic window based on volatility.
                let sustain_window = if atr < self.daily_avg_atr {
                    self.sustain_ms + self.sustain_low_vol_ext_ms // low-vol: slower build
                } else {
                    self.sustain_ms
                };

                if elapsed >= sustain_window {
                    // Spike has sustained long enough — move to phantom check.
                    let magnitude = if self.spike_origin_mid.abs() > 1e-12 {
                        (self.spike_peak_delta.abs() / self.spike_origin_mid.abs())
                            .clamp(0.0, 1.0)
                    } else {
                        0.0
                    };

                    let spike_info = SpikeInfo {
                        direction: self.spike_direction.unwrap_or(Direction::Up),
                        magnitude: Decimal::from_f64(magnitude).unwrap_or(Decimal::ZERO),
                        sustained_ms: elapsed,
                        timestamp_ms: spike_start,
                    };

                    debug!(
                        elapsed_ms = elapsed,
                        sustain_window_ms = sustain_window,
                        magnitude_pct = %(magnitude * 100.0),
                        "spike sustained — entering phantom check"
                    );

                    self.pending = Some(PendingSpike {
                        info: spike_info,
                        sustain_mid: mid,
                        sustain_confirmed_ms: now_ms,
                    });
                    self.reset_candidate();
                }
            }
            // else: displacement below threshold but within window — allow brief dips.
        } else if abs_delta > threshold {
            // No active candidate — start a new one from a large per-tick jump.
            let candidate_dir = if delta > 0.0 {
                Direction::Up
            } else {
                Direction::Down
            };

            self.spike_start_ms = Some(now_ms);
            self.spike_direction = Some(candidate_dir);
            self.spike_origin_mid = prev_mid;
            self.spike_peak_delta = delta;
            debug!(
                direction = ?candidate_dir,
                abs_delta,
                atr,
                threshold,
                "spike candidate started"
            );
        }

        self.prev_mid = Some(mid);
        self.prev_ts_ms = now_ms;
        None
    }

    pub(super) fn reset_candidate(&mut self) {
        self.spike_start_ms = None;
        self.spike_direction = None;
        self.spike_origin_mid = 0.0;
        self.spike_peak_delta = 0.0;
    }

    /// Record a discarded stale event; log a summary every 60 seconds.
    pub(super) fn record_stale(&mut self, now_ms: u64) {
        self.stale_count += 1;
        if now_ms.saturating_sub(self.last_stale_log_ms) >= 60_000 {
            if self.stale_count > 0 {
                info!(
                    stale_count = self.stale_count,
                    "stale Binance events discarded in the last 60s"
                );
            }
            self.stale_count = 0;
            self.last_stale_log_ms = now_ms;
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_spike_config() -> SpikeDetectionConfig {
        SpikeDetectionConfig {
            multiplier: 1.5,
            atr_alpha: 0.1,
            window_ms: 400,
            sustain_ms: 200,
            sustain_low_vol_ext_ms: 300,
            phantom_revert_fraction: 0.5,
            phantom_check_ms: 100,
        }
    }

    #[test]
    fn test_spike_detector_no_spike_on_noise() {
        let mut det = SpikeDetector::new(&test_spike_config());
        let base = 52000.0_f64;
        let mut ts = 1_700_000_000_000_u64;

        // Feed 40 small-delta samples to establish ATR.
        for i in 0..40 {
            // Tiny oscillation — well below 1.5x ATR threshold.
            let price = base + (i as f64 % 3.0) * 0.3;
            let result = det.update(price, ts);
            ts += 100;
            assert!(result.is_none(), "noise tick {i} should not trigger spike");
        }

        // A modest move that is still within 1.5× ATR.
        let result = det.update(base + 5.0, ts);
        assert!(
            result.is_none(),
            "sub-threshold move should not trigger spike"
        );
    }

    #[test]
    fn test_spike_detector_confirms_spike() {
        let cfg = test_spike_config();
        let mut det = SpikeDetector::new(&cfg);
        let base = 52000.0_f64;
        let mut ts = 1_700_000_000_000_u64;

        // Warm ATR with small stable moves.
        for i in 0..30 {
            det.update(base + (i as f64 % 2.0) * 1.0, ts);
            ts += 100;
        }

        // Large spike — far exceeds 1.5x ATR threshold.
        let spike_price = base + 600.0;

        // Tick 1: starts the spike candidate.
        det.update(spike_price, ts);
        ts += 100;
        // Tick 2: continues.
        det.update(spike_price + 10.0, ts);
        ts += 100;
        // Tick 3: > 200ms elapsed → sustain check passes, pending spike created.
        det.update(spike_price + 10.0, ts);
        ts += 100;
        // Tick 4: within 100ms phantom window — no reversion.
        det.update(spike_price + 8.0, ts);
        ts += 110; // now past phantom window (>100ms since sustain)
        // Tick 5: phantom window elapsed → spike should be confirmed here.
        let confirmed = det.update(spike_price + 8.0, ts);

        assert!(
            confirmed.is_some(),
            "spike should be confirmed after sustain + no phantom reversion"
        );
        let spike = confirmed.unwrap();
        assert_eq!(spike.direction, Direction::Up);
        assert!(spike.magnitude > Decimal::ZERO);
        assert!(spike.sustained_ms >= cfg.sustain_ms);
    }

    #[test]
    fn test_spike_detector_phantom_rejected() {
        let mut det = SpikeDetector::new(&test_spike_config());
        let base = 52000.0_f64;
        let mut ts = 1_700_000_000_000_u64;

        // Warm ATR.
        for i in 0..30 {
            det.update(base + (i as f64 % 2.0) * 1.0, ts);
            ts += 100;
        }

        let spike_price = base + 500.0;
        // Start spike candidate.
        det.update(spike_price, ts);
        ts += 100;
        det.update(spike_price, ts);
        ts += 100;
        // Sustain check fires (200ms elapsed).
        det.update(spike_price, ts);
        ts += 50; // 50ms after sustain — still within phantom window

        // Revert > 50% of the spike delta.
        let revert_price = base + 80.0; // reverts ~84% of the 500-pt spike
        let result = det.update(revert_price, ts);
        assert!(
            result.is_none(),
            "phantom reversion should discard the spike"
        );
    }

    #[test]
    fn test_spike_detector_candidate_times_out() {
        let cfg = test_spike_config();
        let mut det = SpikeDetector::new(&cfg);
        let base = 52000.0_f64;
        let mut ts = 1_700_000_000_000_u64;

        // Warm ATR.
        for i in 0..20 {
            det.update(base + (i as f64 % 2.0), ts);
            ts += 100;
        }

        // Single big spike tick — starts candidate.
        det.update(base + 500.0, ts);
        ts += cfg.window_ms + 50; // past the 400ms spike window

        // Price back to base — candidate should time out, no spike.
        let result = det.update(base, ts);
        assert!(
            result.is_none(),
            "spike candidate that timed out should not emit a spike"
        );
    }
}
