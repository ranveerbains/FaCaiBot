use super::*;

// ── Market WS event parsing ────────────────────────────────────────

#[test]
fn test_parse_book_event() {
    let json = serde_json::json!({
        "event_type": "book",
        "asset_id": "0xtoken123",
        "bids": [{"price": "0.48", "size": "100"}, {"price": "0.47", "size": "50"}],
        "asks": [{"price": "0.52", "size": "80"}, {"price": "0.53", "size": "30"}],
        "timestamp": "1714000000"
    });

    let book = parse_book_event(&json).expect("parse_book_event failed");
    assert_eq!(book.asset_id, "0xtoken123");
    assert_eq!(book.bids.len(), 2);
    assert_eq!(book.asks.len(), 2);
    // Bids sorted highest-first.
    assert_eq!(book.bids[0].price, "0.48".parse::<Decimal>().unwrap());
    assert_eq!(book.bids[1].price, "0.47".parse::<Decimal>().unwrap());
    // Asks sorted lowest-first.
    assert_eq!(book.asks[0].price, "0.52".parse::<Decimal>().unwrap());
}

#[test]
fn test_parse_best_bid_ask_event() {
    let json = serde_json::json!({
        "event_type": "best_bid_ask",
        "asset_id": "0xtoken456",
        "bid": "0.48",
        "ask": "0.52"
    });

    match parse_best_bid_ask_event(&json).expect("parse failed") {
        IngestorEvent::PolymarketBestBidAsk {
            asset_id,
            best_bid,
            best_ask,
        } => {
            assert_eq!(asset_id, "0xtoken456");
            assert_eq!(best_bid, "0.48".parse::<Decimal>().unwrap());
            assert_eq!(best_ask, "0.52".parse::<Decimal>().unwrap());
        }
        other => panic!("wrong variant: {:?}", other),
    }
}

#[test]
fn test_parse_tick_size_change_event() {
    let json = serde_json::json!({
        "event_type": "tick_size_change",
        "asset_id": "0xtoken789",
        "old_tick_size": "0.01",
        "new_tick_size": "0.001"
    });

    match parse_tick_size_change_event(&json).expect("parse failed") {
        IngestorEvent::PolymarketTickSizeChange {
            asset_id,
            old_tick_size,
            new_tick_size,
        } => {
            assert_eq!(asset_id, "0xtoken789");
            assert_eq!(old_tick_size, "0.01".parse::<Decimal>().unwrap());
            assert_eq!(new_tick_size, "0.001".parse::<Decimal>().unwrap());
        }
        other => panic!("wrong variant: {:?}", other),
    }
}

#[test]
fn test_parse_market_resolved_event() {
    let json = serde_json::json!({
        "event_type": "market_resolved",
        "market": "0xcond999",
        "winner": "0xyes999"
    });

    match parse_market_resolved_event(&json).expect("parse failed") {
        IngestorEvent::PolymarketMarketResolved {
            market,
            winning_asset_id,
        } => {
            assert_eq!(market, "0xcond999");
            assert_eq!(winning_asset_id, "0xyes999");
        }
        other => panic!("wrong variant: {:?}", other),
    }
}

#[test]
fn test_parse_price_change_event() {
    let json = serde_json::json!({
        "event_type": "price_change",
        "changes": [
            {
                "asset_id": "0xtoken001",
                "side": "BUY",
                "price": "0.49",
                "size": "200",
                "best_bid": "0.49",
                "best_ask": "0.51"
            },
            {
                "asset_id": "0xtoken001",
                "side": "SELL",
                "price": "0.52",
                "size": "0",
                "best_bid": "0.49",
                "best_ask": "0.51"
            }
        ]
    });

    let events = parse_price_change_event(&json).expect("parse failed");
    assert_eq!(events.len(), 2);

    match &events[0] {
        IngestorEvent::PolymarketPriceChange {
            asset_id,
            side,
            price,
            ..
        } => {
            assert_eq!(asset_id, "0xtoken001");
            assert_eq!(*side, Side::Buy);
            assert_eq!(*price, "0.49".parse::<Decimal>().unwrap());
        }
        other => panic!("wrong variant: {:?}", other),
    }

    match &events[1] {
        IngestorEvent::PolymarketPriceChange { side, .. } => {
            assert_eq!(*side, Side::Sell);
        }
        other => panic!("wrong variant: {:?}", other),
    }
}

// ── Market WS array frame dispatch ────────────────────────────────

#[test]
fn test_handle_market_message_array() {
    use crossbeam_channel::bounded;

    let (tx, rx) = bounded::<IngestorEvent>(16);
    let json = serde_json::json!([
        {
            "event_type": "best_bid_ask",
            "asset_id": "0xtokABC",
            "bid": "0.45",
            "ask": "0.55"
        }
    ])
    .to_string();

    handle_market_message(&json, &tx).expect("handle_market_message failed");

    let event = rx.try_recv().expect("should have received event");
    match event {
        IngestorEvent::PolymarketBestBidAsk { asset_id, .. } => {
            assert_eq!(asset_id, "0xtokABC");
        }
        other => panic!("wrong event: {:?}", other),
    }
}

#[test]
fn test_parse_price_levels_object_form() {
    let levels = serde_json::json!([
        {"price": "0.48", "size": "100"},
        {"price": "0.47", "size": "50"}
    ]);

    let parsed = parse_price_levels(Some(&levels)).expect("parse_price_levels failed");
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0].price, "0.48".parse::<Decimal>().unwrap());
    assert_eq!(parsed[0].size, "100".parse::<Decimal>().unwrap());
}

#[test]
fn test_parse_price_levels_array_form() {
    let levels = serde_json::json!([["0.48", "100"], ["0.47", "50"]]);

    let parsed = parse_price_levels(Some(&levels)).expect("parse_price_levels failed");
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0].price, "0.48".parse::<Decimal>().unwrap());
}

#[test]
fn test_parse_price_levels_none() {
    let parsed = parse_price_levels(None).expect("should return empty vec");
    assert!(parsed.is_empty());
}

#[test]
fn test_build_market_subscribe_msg() {
    let token_ids = vec!["0xyes".to_string(), "0xno".to_string()];
    let msg = build_market_subscribe_msg(&token_ids);
    let parsed: serde_json::Value = serde_json::from_str(&msg).expect("JSON parse failed");
    assert_eq!(parsed["type"], "market");
    assert_eq!(parsed["custom_feature_enabled"], true);
    assert_eq!(parsed["assets_ids"][0], "0xyes");
    assert_eq!(parsed["assets_ids"][1], "0xno");
}
