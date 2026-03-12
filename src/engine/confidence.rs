//! Repricing model and tick-rounding utilities.
//!
//! `compute_expected_repricing()` estimates expected YES/NO price movement as a
//! percentage from four factors: spike quality, binary option sensitivity,
//! time amplification, and a calibration scale. The output directly becomes
//! the Phase 1 profit target.

use rust_decimal::Decimal;

use crate::types::market::Direction;

/// Compute expected repricing percentage from signal strength and market context.
///
/// Output is a decimal fraction (e.g., 0.015 = 1.5% expected movement).
/// Tick-size independent — quantization happens later via `round_to_tick()`.
///
/// # Components
/// 1. `norm_signal` [0,1]: signal strength normalized to [min_strength, strong_strength]
/// 2. `adjusted_sensitivity`: 4P(1-P) × consensus alignment
/// 3. `time_factor`: (300/max(T,10))^time_exponent
/// 4. `scale`: calibration ceiling
///
/// # Signal strength semantics
/// - Phase A (entry): composite buildup score [0,1]
/// - Phase B (hedge targeting): max(observed_norm, composite) [0,1]
/// - ATR backstop: normalized spike ATR ratio [0,1]
#[allow(clippy::too_many_arguments)]
pub fn compute_expected_repricing(
    signal_strength: Decimal,
    min_strength: Decimal,
    strong_strength: Decimal,
    yes_mid: Decimal,
    spike_direction: Direction,
    time_remaining_secs: u64,
    scale: Decimal,
    time_exponent: f64,
    max_time_factor: f64,
) -> Decimal {
    // norm_signal: [0,1]
    let range = strong_strength - min_strength;
    let norm_signal = if range.is_zero() {
        Decimal::ONE
    } else {
        ((signal_strength - min_strength) / range)
            .max(Decimal::ZERO)
            .min(Decimal::ONE)
    };

    // 4P(1-P) × consensus alignment
    let four = Decimal::new(4, 0);
    let base = four * yes_mid * (Decimal::ONE - yes_mid);
    let half = Decimal::new(5, 1);
    let dist = (yes_mid - half).abs();
    let with_consensus = matches!(
        (spike_direction, yes_mid > half),
        (Direction::Up, true) | (Direction::Down, false)
    );
    let alignment = if with_consensus {
        Decimal::ONE + dist
    } else {
        Decimal::ONE - dist
    };

    // (300 / max(T, 10)) ^ time_exponent
    let t = time_remaining_secs.max(10) as f64;
    let time_factor =
        Decimal::try_from((300.0_f64 / t).powf(time_exponent).min(max_time_factor))
            .unwrap_or(Decimal::ONE);

    (norm_signal * base * alignment * time_factor * scale).max(Decimal::ZERO)
}

/// Round `price` down to the nearest `tick_size` multiple (floor rounding).
///
/// All order prices must pass through this function before submission to the CLOB.
pub fn round_to_tick(price: Decimal, tick_size: Decimal) -> Decimal {
    if tick_size.is_zero() {
        return price;
    }
    (price / tick_size).floor() * tick_size
}

#[cfg(test)]
#[path = "tests/confidence_tests.rs"]
mod tests;
