//! Confidence scoring for trade signals.
//!
//! Computes a 4-factor confidence score in [0.0, 1.0] from spike magnitude,
//! sustain duration, book depth, and time remaining.

use rust_decimal::Decimal;

/// Compute the 4-factor confidence score in [0.0, 1.0].
///
/// ```text
/// confidence = 0.4 * min(spike_magnitude / ATR, 1.0)
///            + 0.2 * min(sustained_ms / SUSTAIN_WINDOW_MS, 1.0)
///            + 0.2 * min(poly_book_depth / avg_book_depth, 1.0)
///            + 0.2 * (time_remaining_secs / 900.0)
/// ```
pub fn compute_confidence(
    spike_magnitude: Decimal,
    atr: Decimal,
    sustained_ms: u64,
    poly_book_depth: Decimal,
    avg_book_depth: Decimal,
    time_remaining_secs: u64,
    sustain_window_ms: u64,
) -> Decimal {
    let f1 = if atr.is_zero() {
        Decimal::ONE
    } else {
        (spike_magnitude / atr).min(Decimal::ONE)
    };
    let f2 = {
        let w = Decimal::from(sustain_window_ms);
        if w.is_zero() {
            Decimal::ONE
        } else {
            (Decimal::from(sustained_ms) / w).min(Decimal::ONE)
        }
    };
    let f3 = if avg_book_depth.is_zero() {
        Decimal::ONE
    } else {
        (poly_book_depth / avg_book_depth).min(Decimal::ONE)
    };
    let f4 = (Decimal::from(time_remaining_secs) / Decimal::from(900u64)).min(Decimal::ONE);

    (Decimal::new(4, 1) * f1
        + Decimal::new(2, 1) * f2
        + Decimal::new(2, 1) * f3
        + Decimal::new(2, 1) * f4)
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
            Decimal::new(1, 0),  // spike = 1
            Decimal::new(1, 0),  // atr = 1 → factor = 1.0
            1000,                // sustained = 1000ms
            Decimal::new(100, 0),
            Decimal::new(100, 0),
            900,                 // full 15 min
            1000,
        );
        assert_eq!(c, Decimal::ONE);
    }

    #[test]
    fn test_confidence_all_zero() {
        let c = compute_confidence(
            Decimal::ZERO,
            Decimal::ONE,
            0,
            Decimal::ZERO,
            Decimal::ONE,
            0,
            1000,
        );
        assert_eq!(c, Decimal::ZERO);
    }

    #[test]
    fn test_confidence_clamped() {
        let c = compute_confidence(
            Decimal::new(10, 0), // 10x the ATR
            Decimal::ONE,
            5000,
            Decimal::new(500, 0),
            Decimal::new(100, 0),
            1800,
            1000,
        );
        assert_eq!(c, Decimal::ONE);
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
