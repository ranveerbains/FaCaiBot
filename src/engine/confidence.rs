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
mod tests {
    use super::*;

    #[test]
    fn test_balanced_market_strong_spike() {
        // YES=0.50, strong spike, full time → max sensitivity
        let pct = compute_expected_repricing(
            Decimal::new(75, 0),  // atr_ratio = 75 (at strong ceiling)
            Decimal::new(20, 0),  // min
            Decimal::new(75, 0),  // strong
            Decimal::new(50, 2),  // yes_mid = 0.50
            Direction::Up,
            300,                  // full 5 min
            Decimal::new(15, 3),  // scale = 0.015
            0.5,                  // time_exponent
            f64::INFINITY,        // no cap
        );
        // norm=1.0, 4*0.5*0.5=1.0, alignment=1.0 (dist=0), time=1.0
        // → 1.0 * 1.0 * 1.0 * 1.0 * 0.015 = 0.015
        assert_eq!(pct, Decimal::new(15, 3));
    }

    #[test]
    fn test_decided_market_with_consensus() {
        // YES=0.70, spike UP (with consensus)
        let pct = compute_expected_repricing(
            Decimal::new(75, 0),
            Decimal::new(20, 0),
            Decimal::new(75, 0),
            Decimal::new(70, 2),  // yes_mid = 0.70
            Direction::Up,        // with consensus (up + yes>0.5)
            300,
            Decimal::new(15, 3),
            0.5,
            f64::INFINITY,
        );
        // norm=1.0, base=4*0.7*0.3=0.84, dist=0.2, alignment=1.2
        // → 1.0 * 0.84 * 1.2 * 1.0 * 0.015 = 0.01512
        assert!(pct > Decimal::new(15, 3), "with-consensus should amplify");
    }

    #[test]
    fn test_decided_market_against_consensus() {
        // YES=0.70, spike DOWN (against consensus)
        let pct = compute_expected_repricing(
            Decimal::new(75, 0),
            Decimal::new(20, 0),
            Decimal::new(75, 0),
            Decimal::new(70, 2),  // yes_mid = 0.70
            Direction::Down,      // against consensus (down + yes>0.5)
            300,
            Decimal::new(15, 3),
            0.5,
            f64::INFINITY,
        );
        // norm=1.0, base=0.84, dist=0.2, alignment=0.8
        // → 1.0 * 0.84 * 0.8 * 1.0 * 0.015 = 0.01008
        assert!(pct < Decimal::new(15, 3), "against-consensus should penalize");
    }

    #[test]
    fn test_extreme_skew() {
        // YES=0.90, spike DOWN (against consensus)
        let pct = compute_expected_repricing(
            Decimal::new(75, 0),
            Decimal::new(20, 0),
            Decimal::new(75, 0),
            Decimal::new(90, 2),  // yes_mid = 0.90
            Direction::Down,
            300,
            Decimal::new(15, 3),
            0.5,
            f64::INFINITY,
        );
        // base=4*0.9*0.1=0.36, dist=0.4, alignment=0.6
        // → 1.0 * 0.36 * 0.6 * 0.015 = 0.00324
        assert!(pct < Decimal::new(5, 3), "extreme skew should produce low output");
    }

    #[test]
    fn test_time_amplification() {
        // 30s left → should amplify significantly
        let pct_30s = compute_expected_repricing(
            Decimal::new(50, 0),
            Decimal::new(20, 0),
            Decimal::new(75, 0),
            Decimal::new(50, 2),
            Direction::Up,
            30,
            Decimal::new(15, 3),
            0.5,
            f64::INFINITY,
        );
        let pct_300s = compute_expected_repricing(
            Decimal::new(50, 0),
            Decimal::new(20, 0),
            Decimal::new(75, 0),
            Decimal::new(50, 2),
            Direction::Up,
            300,
            Decimal::new(15, 3),
            0.5,
            f64::INFINITY,
        );
        assert!(pct_30s > pct_300s * Decimal::new(2, 0), "30s should be >2x of 300s");
    }

    #[test]
    fn test_weak_spike_produces_zero() {
        // atr_ratio at minimum → norm_spike = 0 → output = 0
        let pct = compute_expected_repricing(
            Decimal::new(20, 0),  // at min
            Decimal::new(20, 0),
            Decimal::new(75, 0),
            Decimal::new(50, 2),
            Direction::Up,
            300,
            Decimal::new(15, 3),
            0.5,
            f64::INFINITY,
        );
        assert_eq!(pct, Decimal::ZERO);
    }

    #[test]
    fn test_time_exponent_zero_disables_amplification() {
        let pct_30s = compute_expected_repricing(
            Decimal::new(50, 0),
            Decimal::new(20, 0),
            Decimal::new(75, 0),
            Decimal::new(50, 2),
            Direction::Up,
            30,
            Decimal::new(15, 3),
            0.0, // disabled
            f64::INFINITY,
        );
        let pct_300s = compute_expected_repricing(
            Decimal::new(50, 0),
            Decimal::new(20, 0),
            Decimal::new(75, 0),
            Decimal::new(50, 2),
            Direction::Up,
            300,
            Decimal::new(15, 3),
            0.0,
            f64::INFINITY,
        );
        assert_eq!(pct_30s, pct_300s, "time_exponent=0 should disable amplification");
    }

    #[test]
    fn test_max_time_factor_caps_amplification() {
        // 10s remaining: uncapped time_factor = (300/10)^0.5 = sqrt(30) ≈ 5.47
        // With max_time_factor=2.0: capped to 2.0
        let capped = compute_expected_repricing(
            Decimal::new(50, 0),
            Decimal::new(20, 0),
            Decimal::new(75, 0),
            Decimal::new(50, 2),
            Direction::Up,
            10,
            Decimal::new(15, 3),
            0.5,
            2.0,  // cap at 2x
        );
        let uncapped = compute_expected_repricing(
            Decimal::new(50, 0),
            Decimal::new(20, 0),
            Decimal::new(75, 0),
            Decimal::new(50, 2),
            Direction::Up,
            10,
            Decimal::new(15, 3),
            0.5,
            f64::INFINITY,
        );
        // Uncapped should be ~5.47x baseline, capped should be exactly 2x
        assert!(uncapped > capped, "uncapped should exceed capped");
        // Capped at 10s should equal the same result as 75s (where natural factor = 2.0)
        let at_75s = compute_expected_repricing(
            Decimal::new(50, 0),
            Decimal::new(20, 0),
            Decimal::new(75, 0),
            Decimal::new(50, 2),
            Direction::Up,
            75,
            Decimal::new(15, 3),
            0.5,
            f64::INFINITY, // natural factor at 75s = (300/75)^0.5 = 2.0
        );
        assert_eq!(capped, at_75s, "capped at 2.0 should match natural 2.0x at 75s");
    }

    #[test]
    fn test_round_to_tick_exact() {
        let tick = Decimal::new(1, 2);
        assert_eq!(
            round_to_tick(Decimal::new(48, 2), tick),
            Decimal::new(48, 2)
        );
    }

    #[test]
    fn test_round_to_tick_floors() {
        let tick = Decimal::new(1, 2);
        assert_eq!(
            round_to_tick(Decimal::new(489, 3), tick),
            Decimal::new(48, 2)
        );
    }

    #[test]
    fn test_round_to_tick_zero_tick_is_identity() {
        let price = Decimal::new(50, 2);
        assert_eq!(round_to_tick(price, Decimal::ZERO), price);
    }

    #[test]
    fn test_round_to_tick_small_tick() {
        let tick = Decimal::new(1, 4);
        assert_eq!(
            round_to_tick(Decimal::new(48023, 5), tick),
            Decimal::new(4802, 4)
        );
    }
}
