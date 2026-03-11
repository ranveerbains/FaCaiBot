// Fill helpers — pure utility functions used by StrategyEngine and LiveExecutor.

use rust_decimal::Decimal;

// ─── Module-level helpers (pub(crate)) ──────────────────────────────────────

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

/// Rounds to nearest tick_size multiple. Note: confidence.rs::round_to_tick()
/// uses floor rounding for order pricing.
pub(crate) fn round_to_tick(price: Decimal, tick: Decimal) -> Decimal {
    if tick.is_zero() {
        return price;
    }
    (price / tick).round() * tick
}


// ─── Fee computation ─────────────────────────────────────────────────────────

/// Compute taker fee for a fill at the given price and size.
///
/// Polymarket 5-min crypto: fee = C × feeRate × (p × (1 - p))^2
/// where C = shares, feeRate = 0.25, exponent = 2.
pub(crate) fn compute_taker_fee(price: Decimal, size: Decimal) -> Decimal {
    let one = Decimal::ONE;
    let factor = Decimal::new(25, 2); // 0.25
    let inner = price * (one - price);
    size * factor * inner * inner
}

/// Estimated maker rebate for a fill at the given price and size.
/// Approximation: 20% of fee-equivalent (same formula as taker fee).
/// Actual rebate depends on daily pool distribution; this is an upper-bound estimate.
pub(crate) fn compute_maker_rebate(price: Decimal, size: Decimal) -> Decimal {
    let fee_equivalent = compute_taker_fee(price, size);
    fee_equivalent * Decimal::new(20, 2) // × 0.20
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

    // ─── fee computation ──────────────────────────────────────────────────

    #[test]
    fn test_compute_taker_fee() {
        let fee = compute_taker_fee(d("0.50"), d("100.00"));
        assert!(fee > Decimal::ZERO);
        // At p=0.50: inner = 0.50 * 0.50 = 0.25, inner^2 = 0.0625
        // fee = 100 * 0.25 * 0.0625 = 1.5625
        assert_eq!(fee, d("1.5625"));
    }

    #[test]
    fn test_compute_maker_rebate() {
        let rebate = compute_maker_rebate(d("0.50"), d("100.00"));
        // 20% of taker fee: 1.5625 * 0.20 = 0.3125
        assert_eq!(rebate, d("0.312500"));
    }
}
