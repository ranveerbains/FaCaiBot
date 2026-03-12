use super::*;

#[test]
fn test_format_token_normal() {
    let raw = U256::from(1_500_000u64); // 1.5 USDC.e
    assert_eq!(format_token(raw, 6), "1.500000");
}

#[test]
fn test_format_token_zero() {
    assert_eq!(format_token(U256::ZERO, 6), "0.000000");
}

#[test]
fn test_format_token_large() {
    let raw = U256::from(123_456_789u64); // 123.456789 USDC.e
    assert_eq!(format_token(raw, 6), "123.456789");
}

#[test]
fn test_format_wei() {
    // 1.5 POL = 1_500_000_000_000_000_000 wei
    let raw = U256::from(1_500_000_000_000_000_000u64);
    assert_eq!(format_wei(raw, 4), "1.5000");
}
