//! EMA-ATR spike detector for Binance price feeds.
//!
//! Detects large, sustained mid-price movements ("spikes") in the BTC/USDT
//! depth stream. Uses a rolling EMA-ATR with sustain and momentum filtering.

use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use tracing::{debug, info};

use crate::config::SpikeDetectionConfig;
use crate::types::market::{Direction, SpikeInfo};

// ─── Constants ────────────────────────────────────────────────────────────────

/// ATR warmup period: no spikes emitted until this many samples have been seen.
/// Prevents false positives while the EMA initialises from a single data point.
/// At 100ms/tick this is ~1 second.
const MIN_ATR_SAMPLES: usize = 10;

// ─── SpikeDetector ────────────────────────────────────────────────────────────

/// Rolling EMA-ATR spike detector.
pub struct SpikeDetector {
    // ── Config ────────────────────────────────────────────────────────
    multiplier: f64,
    atr_alpha: f64,
    window_ms: u64,
    sustain_ms: u64,
    min_magnitude_pct: f64,
    momentum_ratio_min: f64,

    // ── ATR state ─────────────────────────────────────────────────────
    /// EMA of absolute mid-price deltas.
    ema_atr: f64,
    /// Number of samples seen; gates spike emission during warmup.
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

    // ── Stale-event telemetry ─────────────────────────────────────────
    stale_count: u64,
    last_stale_log_ms: u64,

    // ── Diagnostic counters (cumulative, logged periodically) ──────
    diag_candidates_started: u64,
    diag_expired_window: u64,
    diag_fading_momentum: u64,
    diag_below_magnitude: u64,
    diag_confirmed: u64,
    last_diag_log_ms: u64,
}

impl SpikeDetector {
    pub fn new(config: &SpikeDetectionConfig) -> Self {
        Self {
            multiplier: config.multiplier,
            atr_alpha: config.atr_alpha,
            window_ms: config.window_ms,
            sustain_ms: config.sustain_ms,
            min_magnitude_pct: config.min_magnitude_pct,
            momentum_ratio_min: config.momentum_ratio_min,
            ema_atr: 0.0,
            sample_count: 0,
            prev_mid: None,
            prev_ts_ms: 0,
            spike_start_ms: None,
            spike_direction: None,
            spike_origin_mid: 0.0,
            spike_peak_delta: 0.0,
            stale_count: 0,
            last_stale_log_ms: 0,
            diag_candidates_started: 0,
            diag_expired_window: 0,
            diag_fading_momentum: 0,
            diag_below_magnitude: 0,
            diag_confirmed: 0,
            last_diag_log_ms: 0,
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

        // ── Update ATR ────────────────────────────────────────────────
        self.sample_count += 1;
        if self.sample_count == 1 {
            self.ema_atr = abs_delta;
        } else {
            self.ema_atr = self.atr_alpha * abs_delta + (1.0 - self.atr_alpha) * self.ema_atr;
        }

        // ── Periodic diagnostic log (every 60s) ─────────────────────
        if now_ms.saturating_sub(self.last_diag_log_ms) >= 60_000 {
            info!(
                atr = format!("{:.2}", self.ema_atr),
                threshold = format!("{:.2}", self.multiplier * self.ema_atr.max(1e-10)),
                mid = format!("{:.2}", mid),
                candidates = self.diag_candidates_started,
                rej_window = self.diag_expired_window,
                rej_momentum = self.diag_fading_momentum,
                rej_magnitude = self.diag_below_magnitude,
                confirmed = self.diag_confirmed,
                "spike 60s"
            );
            self.last_diag_log_ms = now_ms;
        }

        // ── ATR warmup gate ───────────────────────────────────────────
        // Hold off spike detection until the EMA has enough samples to be reliable.
        if self.sample_count < MIN_ATR_SAMPLES {
            self.prev_mid = Some(mid);
            self.prev_ts_ms = now_ms;
            return None;
        }

        let atr = self.ema_atr.max(1e-10);

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
                self.diag_expired_window += 1;
                debug!(
                    elapsed,
                    window_ms = self.window_ms,
                    displacement = format!("{:.2}", abs_displacement),
                    threshold = format!("{:.2}", threshold),
                    "spike REJECTED: window expired before sustain"
                );
                self.reset_candidate();
            } else if abs_displacement > threshold {
                // Price is still significantly displaced from origin — spike holds.
                // Track the largest displacement as peak delta.
                if abs_displacement > self.spike_peak_delta.abs() {
                    self.spike_peak_delta = displacement;
                }

                if elapsed >= self.sustain_ms {
                    // Momentum check: current displacement must be ≥ momentum_ratio_min of peak.
                    // Filters fading spikes that retrace most of the move during sustain.
                    let peak_abs = self.spike_peak_delta.abs().max(1e-12);
                    let momentum_ratio = abs_displacement / peak_abs;
                    if momentum_ratio < self.momentum_ratio_min {
                        self.diag_fading_momentum += 1;
                        debug!(
                            momentum_ratio = format!("{:.3}", momentum_ratio),
                            min = self.momentum_ratio_min,
                            displacement = format!("{:.2}", abs_displacement),
                            peak = format!("{:.2}", peak_abs),
                            "spike REJECTED: fading momentum"
                        );
                        self.reset_candidate();
                    } else {
                        // Use sustain-time displacement (not peak) for honest magnitude.
                        let magnitude = if self.spike_origin_mid.abs() > 1e-12 {
                            (abs_displacement / self.spike_origin_mid.abs()).clamp(0.0, 1.0)
                        } else {
                            0.0
                        };

                        // Minimum magnitude gate.
                        let min_mag = self.min_magnitude_pct / 100.0;
                        if magnitude < min_mag {
                            self.diag_below_magnitude += 1;
                            debug!(
                                magnitude_pct = format!("{:.4}", magnitude * 100.0),
                                min_pct = self.min_magnitude_pct,
                                "spike REJECTED: below min magnitude"
                            );
                            self.reset_candidate();
                        } else {
                            // All gates passed — emit spike immediately.
                            self.diag_confirmed += 1;
                            let spike_info = SpikeInfo {
                                direction: self.spike_direction.unwrap_or(Direction::Up),
                                magnitude: Decimal::from_f64(magnitude).unwrap_or(Decimal::ZERO),
                                sustained_ms: elapsed,
                                timestamp_ms: spike_start,
                            };
                            info!(
                                direction = ?spike_info.direction,
                                magnitude_pct = %(magnitude * 100.0),
                                sustained_ms = elapsed,
                                "spike CONFIRMED — all gates passed"
                            );
                            self.reset_candidate();
                            self.prev_mid = Some(mid);
                            self.prev_ts_ms = now_ms;
                            return Some(spike_info);
                        }
                    }
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
            self.diag_candidates_started += 1;
            debug!(
                direction = ?candidate_dir,
                abs_delta = format!("{:.2}", abs_delta),
                atr = format!("{:.2}", atr),
                threshold = format!("{:.2}", threshold),
                mid = format!("{:.2}", mid),
                "spike candidate STARTED"
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
            min_magnitude_pct: 0.0, // no min filter in tests (test-specific)
            momentum_ratio_min: 0.5,
        }
    }

    #[test]
    fn test_spike_detector_no_spike_on_noise() {
        let mut det = SpikeDetector::new(&test_spike_config());
        let base = 52000.0_f64;
        let mut ts = 1_700_000_000_000_u64;

        // Feed 40 alternating samples: deltas are always ±0.3.
        // ATR converges to ~0.3, threshold = 1.5 × 0.3 = 0.45.
        // All deltas (0.3) are below threshold — no candidates start.
        for i in 0..40 {
            let price = base + ((i % 2) as f64) * 0.3;
            let result = det.update(price, ts);
            ts += 100;
            assert!(result.is_none(), "noise tick {i} should not trigger spike");
        }

        // A single large tick starts a candidate but cannot confirm (no sustain yet).
        let result = det.update(base + 500.0, ts);
        assert!(
            result.is_none(),
            "single large tick without sustain should not confirm"
        );
    }

    #[test]
    fn test_spike_detector_confirms_spike() {
        let cfg = test_spike_config();
        let mut det = SpikeDetector::new(&cfg);
        let base = 52000.0_f64;
        let mut ts = 1_700_000_000_000_u64;

        // Warm ATR with small stable moves (must exceed MIN_ATR_SAMPLES=10).
        for i in 0..30 {
            det.update(base + (i as f64 % 2.0) * 1.0, ts);
            ts += 100;
        }

        // Large spike — far exceeds 1.5x ATR threshold.
        let spike_price = base + 600.0;

        // Tick 1: starts the spike candidate.
        det.update(spike_price, ts);
        ts += 100;
        // Tick 2: continues — 100ms elapsed, below sustain_ms=200ms.
        det.update(spike_price + 10.0, ts);
        ts += 100;
        // Tick 3: 200ms elapsed — sustain passes, momentum+magnitude pass → spike confirmed immediately.
        let confirmed = det.update(spike_price + 10.0, ts);

        assert!(
            confirmed.is_some(),
            "spike should be confirmed immediately after sustain"
        );
        let spike = confirmed.unwrap();
        assert_eq!(spike.direction, Direction::Up);
        assert!(spike.magnitude > Decimal::ZERO);
        assert!(spike.sustained_ms >= cfg.sustain_ms);
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
