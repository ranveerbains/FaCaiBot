// Fill simulation helpers — pure utility functions used by SimulationExecutor
// and StrategyEngine::advance_simulation().

use rust_decimal::Decimal;

use crate::types::order::Side;

// ─── Module-level helpers (pub(crate) so simulation.rs can re-use) ────────────

/// Compute fill size in shares: `alloc / price`. Rounds to 2 decimal places.
/// Returns `Decimal::ZERO` if price is zero (guard against division by zero).
#[allow(dead_code)] // used in tests
pub(crate) fn compute_fill_size(alloc: Decimal, price: Decimal) -> Decimal {
    if price.is_zero() {
        return Decimal::ZERO;
    }
    let size = alloc / price;
    // Round to 2 decimal places (Polymarket minimum precision 0.01 shares).
    size.round_dp(2)
}

/// Return the opposite side (Leg 2 buys the complementary token).
pub(crate) fn opposite_side(side: Side) -> Side {
    match side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

/// Round a price to the nearest tick_size multiple.
/// Same logic as `confidence.rs::round_to_tick()`.
pub(crate) fn round_to_tick(price: Decimal, tick: Decimal) -> Decimal {
    if tick.is_zero() {
        return price;
    }
    (price / tick).round() * tick
}

/// Current epoch time in milliseconds.
pub(crate) fn epoch_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ─── Unit Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse::<Decimal>()
            .expect("invalid decimal literal in test")
    }

    // ─── compute_fill_size ────────────────────────────────────────────────

    #[test]
    fn test_compute_fill_size() {
        // $30 alloc at $0.45 → 66.666... → rounds to 66.67 at 2dp
        let size = compute_fill_size(Decimal::from(30), d("0.45"));
        assert!(size > Decimal::ZERO);
        // Verify it's rounded to 2 decimal places.
        assert_eq!(size, size.round_dp(2));

        // Zero price guard.
        let size_zero = compute_fill_size(Decimal::from(30), Decimal::ZERO);
        assert_eq!(size_zero, Decimal::ZERO);
    }

    // ─── opposite_side ────────────────────────────────────────────────────

    #[test]
    fn test_opposite_side() {
        assert_eq!(opposite_side(Side::Buy), Side::Sell);
        assert_eq!(opposite_side(Side::Sell), Side::Buy);
    }
}
