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
