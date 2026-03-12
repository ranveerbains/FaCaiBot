use super::*;

#[test]
fn test_is_stale() {
    assert!(is_stale(1000, 1600, 500)); // age = 600 > 500
    assert!(!is_stale(1200, 1600, 500)); // age = 400 < 500
    assert!(!is_stale(1100, 1600, 500)); // age = 500 == 500 (not strictly greater)
}

#[test]
fn test_fastwebsockets_generate_key_length() {
    let key = handshake::generate_key();
    assert_eq!(key.len(), 24, "WS key should be 24 chars: '{key}'");
    assert!(key.chars().all(|c| c.is_ascii()));
}

// ── SBE decimal conversion ──────────────────────────────────────

#[test]
fn test_sbe_to_decimal_negative_exponent() {
    // 4250050 × 10^-2 = 42500.50
    let d = sbe_to_decimal(4250050, -2);
    assert_eq!(d, Decimal::new(4250050, 2));
    assert_eq!(d.to_string(), "42500.50");
}

#[test]
fn test_sbe_to_decimal_large_negative_exponent() {
    // 123456789 × 10^-5 = 1234.56789
    let d = sbe_to_decimal(123456789, -5);
    assert_eq!(d, Decimal::new(123456789, 5));
    assert_eq!(d.to_string(), "1234.56789");
}

#[test]
fn test_sbe_to_decimal_zero_exponent() {
    let d = sbe_to_decimal(42500, 0);
    assert_eq!(d, Decimal::new(42500, 0));
}

#[test]
fn test_sbe_to_decimal_positive_exponent() {
    // 5 × 10^2 = 500
    let d = sbe_to_decimal(5, 2);
    assert_eq!(d.to_string(), "500");
}

// ── SBE BestBidAsk parsing ──────────────────────────────────────

#[test]
fn test_parse_sbe_best_bid_ask() {
    // Build synthetic BestBidAskStreamEvent body (50 bytes root block).
    let mut body = vec![0u8; 50];
    let event_time_us: i64 = 1_700_000_000_000_000; // microseconds
    body[0..8].copy_from_slice(&event_time_us.to_le_bytes()); // eventTime
    body[8..16].copy_from_slice(&1_i64.to_le_bytes()); // bookUpdateId
    body[16] = (-2_i8) as u8; // priceExponent
    body[17] = (-5_i8) as u8; // qtyExponent
    body[18..26].copy_from_slice(&9700050_i64.to_le_bytes()); // bidPrice
    body[26..34].copy_from_slice(&150000_i64.to_le_bytes()); // bidQty
    body[34..42].copy_from_slice(&9700100_i64.to_le_bytes()); // askPrice
    body[42..50].copy_from_slice(&200000_i64.to_le_bytes()); // askQty

    let tick = parse_sbe_best_bid_ask(&body, 50).unwrap();

    assert_eq!(tick.symbol, "BTCUSDT");
    assert_eq!(tick.bid_price, Decimal::new(9700050, 2)); // 97000.50
    assert_eq!(tick.bid_qty, Decimal::new(150000, 5)); // 1.50000
    assert_eq!(tick.ask_price, Decimal::new(9700100, 2)); // 97001.00
    assert_eq!(tick.ask_qty, Decimal::new(200000, 5)); // 2.00000
    assert_eq!(tick.timestamp_ms, 1_700_000_000_000); // us → ms
}

// ── SBE DepthSnapshot parsing ───────────────────────────────────

#[test]
fn test_parse_sbe_depth_snapshot() {
    // Build synthetic DepthSnapshotStreamEvent.
    // Root block = 18 bytes, then bids group (2 levels), then asks group (2 levels).
    let price_exp: i8 = -2;
    let qty_exp: i8 = -5;
    let event_time_us: i64 = 1_700_000_000_000_000;

    let mut body = Vec::new();

    // Root block (18 bytes).
    body.extend_from_slice(&event_time_us.to_le_bytes()); // [0..8] eventTime
    body.extend_from_slice(&42_i64.to_le_bytes()); // [8..16] bookUpdateId
    body.push(price_exp as u8); // [16] priceExponent
    body.push(qty_exp as u8); // [17] qtyExponent

    // Bids group header (groupSize16Encoding: blockLength u16 + numInGroup u16).
    let entry_block_length: u16 = 16; // price(8) + qty(8)
    let num_bids: u16 = 2;
    body.extend_from_slice(&entry_block_length.to_le_bytes());
    body.extend_from_slice(&num_bids.to_le_bytes());

    // Bid 0: price=97001.00, qty=1.50000
    body.extend_from_slice(&9700100_i64.to_le_bytes());
    body.extend_from_slice(&150000_i64.to_le_bytes());
    // Bid 1: price=97000.50, qty=2.00000
    body.extend_from_slice(&9700050_i64.to_le_bytes());
    body.extend_from_slice(&200000_i64.to_le_bytes());

    // Asks group header.
    let num_asks: u16 = 2;
    body.extend_from_slice(&entry_block_length.to_le_bytes());
    body.extend_from_slice(&num_asks.to_le_bytes());

    // Ask 0: price=97001.50, qty=0.50000
    body.extend_from_slice(&9700150_i64.to_le_bytes());
    body.extend_from_slice(&50000_i64.to_le_bytes());
    // Ask 1: price=97002.00, qty=3.00000
    body.extend_from_slice(&9700200_i64.to_le_bytes());
    body.extend_from_slice(&300000_i64.to_le_bytes());

    let depth = parse_sbe_depth(&body, 18).unwrap();

    assert_eq!(depth.symbol, "BTCUSDT");
    assert_eq!(depth.timestamp_ms, 1_700_000_000_000);
    assert_eq!(depth.bids.len(), 2);
    assert_eq!(depth.asks.len(), 2);

    // Bids: highest first.
    assert_eq!(depth.bids[0].price, Decimal::new(9700100, 2)); // 97001.00
    assert_eq!(depth.bids[0].size, Decimal::new(150000, 5)); // 1.50000
    assert_eq!(depth.bids[1].price, Decimal::new(9700050, 2)); // 97000.50
    assert_eq!(depth.bids[1].size, Decimal::new(200000, 5)); // 2.00000

    // Asks: lowest first.
    assert_eq!(depth.asks[0].price, Decimal::new(9700150, 2)); // 97001.50
    assert_eq!(depth.asks[0].size, Decimal::new(50000, 5)); // 0.50000
    assert_eq!(depth.asks[1].price, Decimal::new(9700200, 2)); // 97002.00
    assert_eq!(depth.asks[1].size, Decimal::new(300000, 5)); // 3.00000
}

// ── SBE full message dispatch ───────────────────────────────────

#[test]
fn test_handle_sbe_message_depth() {
    let (tx, rx) = crossbeam_channel::bounded(64);

    // Build a full SBE message (header + body).
    let block_length: u16 = 18;
    let template_id: u16 = SBE_TEMPLATE_DEPTH_SNAPSHOT;
    let schema_id: u16 = 1;
    let version: u16 = 0;

    let mut payload = Vec::new();
    // Header.
    payload.extend_from_slice(&block_length.to_le_bytes());
    payload.extend_from_slice(&template_id.to_le_bytes());
    payload.extend_from_slice(&schema_id.to_le_bytes());
    payload.extend_from_slice(&version.to_le_bytes());

    // Body — root block.
    let now_us = (now_epoch_ms() as i64) * 1000;
    payload.extend_from_slice(&now_us.to_le_bytes()); // eventTime
    payload.extend_from_slice(&1_i64.to_le_bytes()); // bookUpdateId
    payload.push((-2_i8) as u8); // priceExponent
    payload.push((-5_i8) as u8); // qtyExponent

    // Bids group: 1 level.
    payload.extend_from_slice(&16_u16.to_le_bytes()); // blockLength
    payload.extend_from_slice(&1_u16.to_le_bytes()); // numInGroup
    payload.extend_from_slice(&9700100_i64.to_le_bytes()); // price
    payload.extend_from_slice(&150000_i64.to_le_bytes()); // qty

    // Asks group: 1 level.
    payload.extend_from_slice(&16_u16.to_le_bytes());
    payload.extend_from_slice(&1_u16.to_le_bytes());
    payload.extend_from_slice(&9700200_i64.to_le_bytes());
    payload.extend_from_slice(&50000_i64.to_le_bytes());

    handle_sbe_message(&payload, &tx, 5000).unwrap();

    // Should emit a BinanceDepth event.
    let event = rx.try_recv().expect("expected BinanceDepth event");
    match event {
        IngestorEvent::BinanceDepth(d) => {
            assert_eq!(d.bids.len(), 1);
            assert_eq!(d.asks.len(), 1);
            assert_eq!(d.bids[0].price, Decimal::new(9700100, 2));
        }
        other => panic!("expected BinanceDepth, got {other:?}"),
    }
}

#[test]
fn test_handle_sbe_message_best_bid_ask() {
    let (tx, rx) = crossbeam_channel::bounded(64);

    let block_length: u16 = 50;
    let template_id: u16 = SBE_TEMPLATE_BEST_BID_ASK;

    let mut payload = Vec::new();
    // Header.
    payload.extend_from_slice(&block_length.to_le_bytes());
    payload.extend_from_slice(&template_id.to_le_bytes());
    payload.extend_from_slice(&1_u16.to_le_bytes()); // schemaId
    payload.extend_from_slice(&0_u16.to_le_bytes()); // version

    // Body.
    let now_us = (now_epoch_ms() as i64) * 1000;
    payload.extend_from_slice(&now_us.to_le_bytes()); // eventTime
    payload.extend_from_slice(&1_i64.to_le_bytes()); // bookUpdateId
    payload.push((-2_i8) as u8); // priceExponent
    payload.push((-5_i8) as u8); // qtyExponent
    payload.extend_from_slice(&9700050_i64.to_le_bytes()); // bidPrice
    payload.extend_from_slice(&150000_i64.to_le_bytes()); // bidQty
    payload.extend_from_slice(&9700100_i64.to_le_bytes()); // askPrice
    payload.extend_from_slice(&200000_i64.to_le_bytes()); // askQty

    handle_sbe_message(&payload, &tx, 5000).unwrap();

    let event = rx.try_recv().expect("expected BinanceTick event");
    match event {
        IngestorEvent::BinanceTick(t) => {
            assert_eq!(t.bid_price, Decimal::new(9700050, 2));
            assert_eq!(t.ask_price, Decimal::new(9700100, 2));
        }
        other => panic!("expected BinanceTick, got {other:?}"),
    }
}

#[test]
fn test_handle_sbe_message_stale_dropped() {
    let (tx, rx) = crossbeam_channel::bounded(64);

    let mut payload = Vec::new();
    payload.extend_from_slice(&50_u16.to_le_bytes()); // blockLength
    payload.extend_from_slice(&SBE_TEMPLATE_BEST_BID_ASK.to_le_bytes());
    payload.extend_from_slice(&1_u16.to_le_bytes());
    payload.extend_from_slice(&0_u16.to_le_bytes());

    // Use a very old timestamp (1s ago with 500ms threshold → stale).
    let old_us = ((now_epoch_ms() - 2000) as i64) * 1000;
    payload.extend_from_slice(&old_us.to_le_bytes());
    payload.extend_from_slice(&1_i64.to_le_bytes());
    payload.push((-2_i8) as u8);
    payload.push((-5_i8) as u8);
    payload.extend_from_slice(&9700050_i64.to_le_bytes());
    payload.extend_from_slice(&150000_i64.to_le_bytes());
    payload.extend_from_slice(&9700100_i64.to_le_bytes());
    payload.extend_from_slice(&200000_i64.to_le_bytes());

    // Stale threshold = 500ms, event is 2s old → should be dropped.
    handle_sbe_message(&payload, &tx, 500).unwrap();

    assert!(rx.try_recv().is_err(), "stale event should be dropped");
}
