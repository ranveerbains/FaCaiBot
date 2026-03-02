//! Confidence scoring for trade signals.
//!
//! Computes a confidence score in [0.0, 0.8] from three factors.
//! Sustain is excluded — all confirmed spikes already passed the sustain gate,
//! so it adds a constant offset with no discriminative value.

use rust_decimal::Decimal;

const WEIGHT_SPIKE: Decimal = Decimal::from_parts(4, 0, 0, false, 1); // 0.4
const WEIGHT_DEPTH: Decimal = Decimal::from_parts(2, 0, 0, false, 1); // 0.2
const WEIGHT_TIME: Decimal = Decimal::from_parts(2, 0, 0, false, 1); // 0.2
const MARKET_DURATION: Decimal = Decimal::from_parts(300, 0, 0, false, 0); // 300

/// Compute the 3-factor confidence score in [0.0, 0.8].
///
/// ```text
/// confidence = 0.4 * min(spike_magnitude / ATR, 1.0)   [spike quality vs recent volatility]
///            + 0.2 * min(poly_book_depth / avg_book_depth, 1.0)
///            + 0.2 * (time_remaining_secs / 300.0)
/// ```
///
/// Thresholds: HIGH >= 0.6 | MED >= 0.3 | LOW < 0.3
pub fn compute_confidence(
    spike_magnitude: Decimal,
    atr: Decimal,
    poly_book_depth: Decimal,
    avg_book_depth: Decimal,
    time_remaining_secs: u64,
) -> Decimal {
    let f1 = if atr.is_zero() {
        Decimal::ONE
    } else {
        (spike_magnitude / atr).min(Decimal::ONE)
    };
    let f3 = if avg_book_depth.is_zero() {
        Decimal::ONE
    } else {
        (poly_book_depth / avg_book_depth).min(Decimal::ONE)
    };
    let f4 = (Decimal::from(time_remaining_secs) / MARKET_DURATION).min(Decimal::ONE);

    (WEIGHT_SPIKE * f1 + WEIGHT_DEPTH * f3 + WEIGHT_TIME * f4)
        .min(Decimal::ONE)
        .max(Decimal::ZERO)
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
    fn test_confidence_all_max() {
        let c = compute_confidence(
            Decimal::new(1, 0), // spike = 1
            Decimal::new(1, 0), // atr = 1 → f1 = 1.0
            Decimal::new(100, 0),
            Decimal::new(100, 0),
            300, // full 5 min
        );
        // f1=1.0, f3=1.0, f4=1.0 → 0.4 + 0.2 + 0.2 = 0.8
        assert_eq!(c, Decimal::new(8, 1));
    }

    #[test]
    fn test_confidence_all_zero() {
        let c = compute_confidence(Decimal::ZERO, Decimal::ONE, Decimal::ZERO, Decimal::ONE, 0);
        // f1=0, f3=0, f4=0 → 0.0
        assert_eq!(c, Decimal::ZERO);
    }

    #[test]
    fn test_confidence_clamped() {
        let c = compute_confidence(
            Decimal::new(10, 0), // 10x the ATR → f1 clamped to 1.0
            Decimal::ONE,
            Decimal::new(500, 0),
            Decimal::new(100, 0),
            1800,
        );
        // f1=1.0, f3=1.0 (capped), f4=1.0 (capped) → 0.4 + 0.2 + 0.2 = 0.8
        assert_eq!(c, Decimal::new(8, 1));
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
