//! Confidence scoring for trade signals.
//!
//! Computes a confidence score in [0.0, 0.8] from three factors:
//!
//! - **f1 (spike quality):** `clamp((atr_ratio - min_atr_ratio) / (strong_atr_ratio - min_atr_ratio), 0, 1)`
//!   Measures spike displacement in ATR multiples. Dimensionless — automatically adapts to
//!   volatility regime. Replaces the old magnitude-based formula which had a unit mismatch
//!   (magnitudes ~0.01% vs thresholds ~1.5%) producing f1 ≈ 0 for all spikes.
//! - **f2 (depth):** `min(book_depth / avg_depth, 1.0)`
//! - **f3 (time):** `time_remaining / 300.0`

use rust_decimal::Decimal;

const WEIGHT_SPIKE: Decimal = Decimal::from_parts(4, 0, 0, false, 1); // 0.4
const WEIGHT_DEPTH: Decimal = Decimal::from_parts(2, 0, 0, false, 1); // 0.2
const WEIGHT_TIME: Decimal = Decimal::from_parts(2, 0, 0, false, 1); // 0.2
const MARKET_DURATION: Decimal = Decimal::from_parts(300, 0, 0, false, 0); // 300

/// Compute the 3-factor confidence score in [0.0, 0.8].
///
/// ```text
/// confidence = 0.4 * clamp((atr_ratio - min_atr_ratio) / (strong_atr_ratio - min_atr_ratio), 0, 1)
///            + 0.2 * min(poly_book_depth / avg_book_depth, 1.0)
///            + 0.2 * (time_remaining_secs / 300.0)
/// ```
///
/// Thresholds: HIGH >= 0.5 | MED >= 0.30 | LOW < 0.30
pub fn compute_confidence(
    atr_ratio: Decimal,
    min_atr_ratio: Decimal,
    strong_atr_ratio: Decimal,
    poly_book_depth: Decimal,
    avg_book_depth: Decimal,
    time_remaining_secs: u64,
) -> Decimal {
    let range = strong_atr_ratio - min_atr_ratio;
    let f1 = if range.is_zero() {
        Decimal::ONE
    } else {
        ((atr_ratio - min_atr_ratio) / range)
            .max(Decimal::ZERO)
            .min(Decimal::ONE)
    };
    let f2 = if avg_book_depth.is_zero() {
        Decimal::ONE
    } else {
        (poly_book_depth / avg_book_depth).min(Decimal::ONE)
    };
    let f3 = (Decimal::from(time_remaining_secs) / MARKET_DURATION).min(Decimal::ONE);

    (WEIGHT_SPIKE * f1 + WEIGHT_DEPTH * f2 + WEIGHT_TIME * f3)
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
        // atr_ratio at strong ceiling → f1 = 1.0
        let c = compute_confidence(
            Decimal::new(75, 0),  // atr_ratio = 75 (at strong ceiling)
            Decimal::new(25, 0),  // min = 25
            Decimal::new(75, 0),  // strong = 75
            Decimal::new(100, 0),
            Decimal::new(100, 0),
            300, // full 5 min
        );
        // f1=1.0, f2=1.0, f3=1.0 → 0.4 + 0.2 + 0.2 = 0.8
        assert_eq!(c, Decimal::new(8, 1));
    }

    #[test]
    fn test_confidence_all_zero() {
        // atr_ratio at minimum floor → f1 = 0.0
        let c = compute_confidence(
            Decimal::new(25, 0),  // atr_ratio = 25 (at floor)
            Decimal::new(25, 0),  // min = 25
            Decimal::new(75, 0),  // strong = 75
            Decimal::ZERO,
            Decimal::ONE,
            0,
        );
        // f1=0, f2=0, f3=0 → 0.0
        assert_eq!(c, Decimal::ZERO);
    }

    #[test]
    fn test_confidence_clamped() {
        // atr_ratio well above strong ceiling → f1 clamped to 1.0
        let c = compute_confidence(
            Decimal::new(200, 0), // atr_ratio = 200 (way above strong)
            Decimal::new(25, 0),  // min = 25
            Decimal::new(75, 0),  // strong = 75
            Decimal::new(500, 0),
            Decimal::new(100, 0),
            1800,
        );
        // f1=1.0 (capped), f2=1.0 (capped), f3=1.0 (capped) → 0.4 + 0.2 + 0.2 = 0.8
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
