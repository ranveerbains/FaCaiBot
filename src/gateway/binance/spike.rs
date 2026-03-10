//! EMA-ATR spike detector for Binance price feeds.
//!
//! Detects large mid-price movements ("spikes") in the BTC/USDT depth stream.
//! Uses a rolling EMA-ATR with magnitude filtering. Emits immediately on
//! threshold breach — no sustain window or momentum check.

use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use tracing::{debug, info};

use crate::config::SpikeDetectionConfig;
use crate::types::market::{Direction, SpikeInfo};

// ─── SpikeEvent ──────────────────────────────────────────────────────────────

/// Result of feeding a tick to [`SpikeDetector::update`].
///
/// - `None` — normal tick, no action.
/// - `Confirmed` — ATR + magnitude passed, spike confirmed immediately.
#[derive(Debug, Clone)]
pub enum SpikeEvent {
    /// Normal tick — no spike activity.
    None,
    /// ATR threshold + magnitude passed — spike confirmed immediately.
    Confirmed(SpikeInfo),
}

// ─── SpikeDiagSnapshot ───────────────────────────────────────────────────────

/// Snapshot of spike detector diagnostics, emitted every 60s.
#[derive(Debug, Clone)]
pub struct SpikeDiagSnapshot {
    pub atr: f64,
    pub threshold: f64,
    pub mid: f64,
    pub rej_magnitude: u64,
    pub confirmed: u64,
    pub stale: u64,
}

// ─── Constants ────────────────────────────────────────────────────────────────

/// ATR warmup period: no spikes emitted until this many samples have been seen.
/// Prevents false positives while the EMA initialises from a single data point.
/// At 50ms/tick (SBE @depth20) this is ~0.5 seconds.
const MIN_ATR_SAMPLES: usize = 10;

// ─── SpikeDetector ────────────────────────────────────────────────────────────

/// Rolling EMA-ATR spike detector.
pub struct SpikeDetector {
    // ── Config ────────────────────────────────────────────────────────
    multiplier: f64,
    atr_alpha: f64,
    min_magnitude_pct: f64,

    // ── ATR state ─────────────────────────────────────────────────────
    /// EMA of absolute mid-price deltas.
    ema_atr: f64,
    /// Number of samples seen; gates spike emission during warmup.
    sample_count: usize,
    /// Previous depth snapshot's mid-price.
    prev_mid: Option<f64>,
    /// Epoch ms of the previous depth snapshot.
    prev_ts_ms: u64,

    // ── Stale-event telemetry ─────────────────────────────────────────
    stale_count: u64,

    // ── Diagnostic counters (cumulative, logged periodically) ──────
    diag_below_magnitude: u64,
    diag_confirmed: u64,
    last_diag_log_ms: u64,

    /// Pending diagnostic snapshot, set when the 60s gate fires in `update()`.
    pending_diag: Option<SpikeDiagSnapshot>,

    /// Latest OBI from Binance @depth20 (updated each tick).
    last_obi: Option<Decimal>,
}

impl SpikeDetector {
    pub fn new(config: &SpikeDetectionConfig) -> Self {
        Self {
            multiplier: config.multiplier,
            atr_alpha: config.atr_alpha,
            min_magnitude_pct: config.min_magnitude_pct,
            ema_atr: 0.0,
            sample_count: 0,
            prev_mid: None,
            prev_ts_ms: 0,
            stale_count: 0,
            diag_below_magnitude: 0,
            diag_confirmed: 0,
            last_diag_log_ms: 0,
            pending_diag: None,
            last_obi: None,
        }
    }

    /// Feed a new mid-price sample from a `@depth20` snapshot.
    ///
    /// Returns a [`SpikeEvent`]:
    /// - `Confirmed` — ATR + magnitude passed, spike confirmed immediately.
    /// - `None` — normal tick, no action.
    pub fn update(&mut self, mid: f64, now_ms: u64, obi: Option<Decimal>) -> SpikeEvent {
        self.last_obi = obi;

        // ── Initialise on first sample ─────────────────────────────────
        let Some(prev_mid) = self.prev_mid else {
            self.prev_mid = Some(mid);
            self.prev_ts_ms = now_ms;
            return SpikeEvent::None;
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
            let atr_val = self.ema_atr;
            let threshold_val = self.multiplier * atr_val.max(1e-10);
            info!(
                atr = format!("{:.2}", atr_val),
                threshold = format!("{:.2}", threshold_val),
                mid = format!("{:.2}", mid),
                rej_magnitude = self.diag_below_magnitude,
                confirmed = self.diag_confirmed,
                stale = self.stale_count,
                "spike 60s"
            );
            self.pending_diag = Some(SpikeDiagSnapshot {
                atr: atr_val,
                threshold: threshold_val,
                mid,
                rej_magnitude: self.diag_below_magnitude,
                confirmed: self.diag_confirmed,
                stale: self.stale_count,
            });
            self.last_diag_log_ms = now_ms;
        }

        // ── ATR warmup gate ───────────────────────────────────────────
        // Hold off spike detection until the EMA has enough samples to be reliable.
        if self.sample_count < MIN_ATR_SAMPLES {
            self.prev_mid = Some(mid);
            self.prev_ts_ms = now_ms;
            return SpikeEvent::None;
        }

        let atr = self.ema_atr.max(1e-10);

        // ── Spike detection (immediate confirmation) ────────────────────
        let threshold = self.multiplier * atr;

        if abs_delta > threshold {
            let direction = if delta > 0.0 {
                Direction::Up
            } else {
                Direction::Down
            };

            // Magnitude check: reject if the tick doesn't meet the minimum
            // magnitude threshold.
            let magnitude = if prev_mid.abs() > 1e-12 {
                (abs_delta / prev_mid.abs()).clamp(0.0, 1.0)
            } else {
                0.0
            };
            let min_mag = self.min_magnitude_pct / 100.0;
            if magnitude < min_mag {
                self.diag_below_magnitude += 1;
                debug!(
                    magnitude_pct = %(magnitude * 100.0),
                    min_pct = self.min_magnitude_pct,
                    "spike REJECTED: below min magnitude"
                );
                self.prev_mid = Some(mid);
                self.prev_ts_ms = now_ms;
                return SpikeEvent::None;
            }

            self.diag_confirmed += 1;
            let atr_ratio = if self.ema_atr > 1e-12 { abs_delta / self.ema_atr } else { 0.0 };
            let spike_info = SpikeInfo {
                direction,
                magnitude: Decimal::from_f64(magnitude).unwrap_or(Decimal::ZERO),
                sustained_ms: 0,
                timestamp_ms: now_ms,
                atr_ratio: Decimal::from_f64(atr_ratio).unwrap_or(Decimal::ZERO),
                obi: self.last_obi.unwrap_or(Decimal::ZERO),
            };
            info!(
                direction = ?direction,
                %abs_delta,
                %atr,
                %threshold,
                %mid,
                magnitude_pct = %(magnitude * 100.0),
                "spike CONFIRMED — ATR + magnitude passed"
            );
            self.prev_mid = Some(mid);
            self.prev_ts_ms = now_ms;
            return SpikeEvent::Confirmed(spike_info);
        }

        self.prev_mid = Some(mid);
        self.prev_ts_ms = now_ms;
        SpikeEvent::None
    }

    /// Take the pending diagnostic snapshot (if the 60s gate fired since the last call).
    pub fn take_diagnostic(&mut self) -> Option<SpikeDiagSnapshot> {
        self.pending_diag.take()
    }

    /// Record a discarded stale event. Count is included in the next "spike 60s" log.
    pub(super) fn record_stale(&mut self, _now_ms: u64) {
        self.stale_count += 1;
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
            min_magnitude_pct: 0.0,       // no min filter in tests (test-specific)
        }
    }

    #[test]
    fn test_spike_detector_no_spike_on_noise() {
        let mut det = SpikeDetector::new(&test_spike_config());
        let base = 52000.0_f64;
        let mut ts = 1_700_000_000_000_u64;

        // Feed 40 alternating samples: deltas are always ±0.3.
        // ATR converges to ~0.3, threshold = 1.5 × 0.3 = 0.45.
        // All deltas (0.3) are below threshold — no spikes.
        for i in 0..40 {
            let price = base + ((i % 2) as f64) * 0.3;
            let result = det.update(price, ts, None);
            ts += 100;
            assert!(
                matches!(result, SpikeEvent::None),
                "noise tick {i} should not trigger spike"
            );
        }
    }

    #[test]
    fn test_spike_detector_confirms_immediately() {
        let cfg = test_spike_config();
        let mut det = SpikeDetector::new(&cfg);
        let base = 52000.0_f64;
        let mut ts = 1_700_000_000_000_u64;

        // Warm ATR with small stable moves (must exceed MIN_ATR_SAMPLES=10).
        for i in 0..30 {
            det.update(base + (i as f64 % 2.0) * 1.0, ts, None);
            ts += 100;
        }

        // Large spike — far exceeds 1.5x ATR threshold.
        // Should emit Confirmed immediately on the spike tick.
        let spike_price = base + 600.0;
        let result = det.update(spike_price, ts, None);

        match result {
            SpikeEvent::Confirmed(spike) => {
                assert_eq!(spike.direction, Direction::Up);
                assert!(spike.magnitude > Decimal::ZERO);
                assert_eq!(spike.sustained_ms, 0);
            }
            other => panic!("expected Confirmed, got {other:?}"),
        }
    }

    #[test]
    fn test_spike_confirmed_with_correct_fields() {
        let cfg = test_spike_config();
        let mut det = SpikeDetector::new(&cfg);
        let base = 52000.0_f64;
        let mut ts = 1_700_000_000_000_u64;

        // Warm ATR.
        for i in 0..20 {
            det.update(base + (i as f64 % 2.0), ts, None);
            ts += 100;
        }

        // Big upward tick — should return Confirmed with correct direction and magnitude.
        let result = det.update(base + 600.0, ts, None);
        match result {
            SpikeEvent::Confirmed(spike) => {
                assert_eq!(spike.direction, Direction::Up);
                assert!(spike.magnitude > Decimal::ZERO);
                assert_eq!(spike.sustained_ms, 0);
                assert!(spike.atr_ratio > Decimal::ZERO);
            }
            other => panic!("expected Confirmed, got {other:?}"),
        }
    }

    #[test]
    fn test_spike_down_direction() {
        let cfg = test_spike_config();
        let mut det = SpikeDetector::new(&cfg);
        let base = 52000.0_f64;
        let mut ts = 1_700_000_000_000_u64;

        // Warm ATR.
        for i in 0..20 {
            det.update(base + (i as f64 % 2.0), ts, None);
            ts += 100;
        }

        // Big downward tick.
        let result = det.update(base - 600.0, ts, None);
        match result {
            SpikeEvent::Confirmed(spike) => {
                assert_eq!(spike.direction, Direction::Down);
                assert!(spike.magnitude > Decimal::ZERO);
            }
            other => panic!("expected Confirmed(Down), got {other:?}"),
        }
    }

    #[test]
    fn test_spike_magnitude_filter() {
        // Use a config with a high min_magnitude_pct so the tick exceeds ATR but
        // fails magnitude.
        let cfg = SpikeDetectionConfig {
            multiplier: 1.5,
            atr_alpha: 0.1,
            min_magnitude_pct: 5.0,       // 5% — very high for this test
        };
        let mut det = SpikeDetector::new(&cfg);
        let base = 52000.0_f64;
        let mut ts = 1_700_000_000_000_u64;

        // Warm ATR with small moves.
        for i in 0..20 {
            det.update(base + (i as f64 % 2.0), ts, None);
            ts += 100;
        }

        // Tick exceeds ATR threshold (1.5 × ~1.0 ≈ 1.5) but magnitude is
        // only ~10/52000 ≈ 0.019% — far below the 5% minimum.
        let result = det.update(base + 10.0, ts, None);
        assert!(
            matches!(result, SpikeEvent::None),
            "tick exceeding ATR but failing magnitude should return None"
        );
    }

    #[test]
    fn test_consecutive_spikes_each_confirmed() {
        let cfg = test_spike_config();
        let mut det = SpikeDetector::new(&cfg);
        let base = 52000.0_f64;
        let mut ts = 1_700_000_000_000_u64;

        // Warm ATR.
        for i in 0..20 {
            det.update(base + (i as f64 % 2.0), ts, None);
            ts += 100;
        }

        // First spike.
        let r1 = det.update(base + 600.0, ts, None);
        assert!(matches!(r1, SpikeEvent::Confirmed(_)));
        ts += 100;

        // Second spike from new level.
        let r2 = det.update(base + 1200.0, ts, None);
        assert!(matches!(r2, SpikeEvent::Confirmed(_)));
    }
}
