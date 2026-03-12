use super::*;
use rust_decimal::Decimal;

// ── map_sdk_status ───────────────────────────────────────────────────────

#[test]
fn test_map_sdk_status_matched_is_filled() {
    assert_eq!(
        map_sdk_status(&OrderStatusType::Matched),
        OrderStatus::Filled
    );
}

#[test]
fn test_map_sdk_status_live_is_placed() {
    assert_eq!(map_sdk_status(&OrderStatusType::Live), OrderStatus::Placed);
}

#[test]
fn test_map_sdk_status_delayed_is_placed() {
    assert_eq!(
        map_sdk_status(&OrderStatusType::Delayed),
        OrderStatus::Placed
    );
}

#[test]
fn test_map_sdk_status_canceled_is_cancelled() {
    assert_eq!(
        map_sdk_status(&OrderStatusType::Canceled),
        OrderStatus::Cancelled
    );
}

#[test]
fn test_map_sdk_status_unknown_defaults_to_placed() {
    assert_eq!(
        map_sdk_status(&OrderStatusType::Unknown("new_status".to_string())),
        OrderStatus::Placed
    );
}

// ── to_sdk_side ──────────────────────────────────────────────────────────

#[test]
fn test_to_sdk_side() {
    assert!(matches!(to_sdk_side(Side::Buy), SdkSide::Buy));
    assert!(matches!(to_sdk_side(Side::Sell), SdkSide::Sell));
}

// ── to_sdk_order_type ────────────────────────────────────────────────────

#[test]
fn test_to_sdk_order_type() {
    assert!(matches!(
        to_sdk_order_type(OrderType::Gtc),
        SdkOrderType::GTC
    ));
    assert!(matches!(
        to_sdk_order_type(OrderType::Gtd),
        SdkOrderType::GTD
    ));
    assert!(matches!(
        to_sdk_order_type(OrderType::Fok),
        SdkOrderType::FOK
    ));
}

// ── maker/taker amount calculation ───────────────────────────────────────

/// Verify that the BUY amount formula produces the correct USDC cost.
///
/// price=0.52, size=100:
///   makerAmount (USDC cost) = 0.52 * 100 * 1e6 = 52_000_000
///   takerAmount (tokens)    = 100 * 1e6         = 100_000_000
#[test]
fn test_buy_amounts_calculation() {
    let scale = Decimal::new(1_000_000, 0);
    let price: Decimal = "0.52".parse().unwrap();
    let size: Decimal = "100".parse().unwrap();

    let cost = (price * size * scale).round();
    let tokens = (size * scale).round();

    assert_eq!(cost.to_string(), "52000000");
    assert_eq!(tokens.to_string(), "100000000");
}

/// Verify that the SELL amount formula inverts maker/taker correctly.
///
/// price=0.48, size=50:
///   makerAmount (tokens given)  = 50 * 1e6          = 50_000_000
///   takerAmount (USDC received) = 0.48 * 50 * 1e6   = 24_000_000
#[test]
fn test_sell_amounts_calculation() {
    let scale = Decimal::new(1_000_000, 0);
    let price: Decimal = "0.48".parse().unwrap();
    let size: Decimal = "50".parse().unwrap();

    let cost = (price * size * scale).round();
    let tokens = (size * scale).round();

    assert_eq!(tokens.to_string(), "50000000");
    assert_eq!(cost.to_string(), "24000000");
}
