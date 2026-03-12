use super::*;

// ── Gamma API events response parsing ───────────────────────────────

/// BTC 5-min event with a future endDate.
const GAMMA_EVENTS_BTC: &str = r#"[
    {
        "slug": "btc-updown-5m-9999999900",
        "endDate": "2099-01-01T00:00:00Z",
        "markets": [{
            "conditionId": "0xabc123",
            "clobTokenIds": "[\"0xyes111\", \"0xno222\"]",
            "acceptingOrders": true
        }]
    }
]"#;

/// ETH 5-min event with a future endDate (not targeted, kept for mixed test).
const GAMMA_EVENTS_ETH: &str = r#"[
    {
        "slug": "eth-updown-5m-9999999900",
        "endDate": "2099-01-01T00:00:00Z",
        "markets": [{
            "conditionId": "0xeth456",
            "clobTokenIds": "[\"0xyes333\", \"0xno444\"]",
            "acceptingOrders": true
        }]
    }
]"#;

/// All events expired.
const GAMMA_EVENTS_EXPIRED: &str = r#"[
    {
        "slug": "btc-updown-5m-1",
        "endDate": "1970-01-01T00:01:00Z",
        "markets": [{
            "conditionId": "0xold",
            "clobTokenIds": "[\"0xyes000\", \"0xno000\"]",
            "acceptingOrders": true
        }]
    }
]"#;

/// Mix of BTC (expired), BTC (valid), SOL (valid but not targeted), ETH (not targeted).
const GAMMA_EVENTS_MIXED: &str = r#"[
    {
        "slug": "btc-updown-5m-1",
        "endDate": "1970-01-01T00:01:00Z",
        "markets": [{
            "conditionId": "0xexpired",
            "clobTokenIds": "[\"0xyesA\", \"0xnoA\"]",
            "acceptingOrders": true
        }]
    },
    {
        "slug": "sol-updown-5m-9999999900",
        "endDate": "2099-01-01T00:00:00Z",
        "markets": [{
            "conditionId": "0xsol_skip",
            "clobTokenIds": "[\"0xyesS\", \"0xnoS\"]",
            "acceptingOrders": true
        }]
    },
    {
        "slug": "btc-updown-5m-9999999900",
        "endDate": "2099-01-01T00:00:00Z",
        "markets": [{
            "conditionId": "0xvalid",
            "clobTokenIds": "[\"0xyesB\", \"0xnoB\"]",
            "acceptingOrders": true
        }]
    }
]"#;

#[test]
fn test_parse_gamma_events_btc() {
    let info = parse_gamma_events_response(GAMMA_EVENTS_BTC).expect("parse failed");
    assert_eq!(info.condition_id, "0xabc123");
    assert_eq!(info.yes_token_id, "0xyes111");
    assert_eq!(info.no_token_id, "0xno222");
    assert!(info.end_timestamp_ms > 0);
}

#[test]
fn test_parse_gamma_events_eth_not_targeted() {
    // ETH is no longer a target — only BTC 5m is targeted.
    let result = parse_gamma_events_response(GAMMA_EVENTS_ETH);
    assert!(result.is_err(), "ETH should not match BTC-only target slugs");
}

#[test]
fn test_parse_gamma_events_skips_expired() {
    let result = parse_gamma_events_response(GAMMA_EVENTS_EXPIRED);
    assert!(result.is_err(), "should error when all events are expired");
}

#[test]
fn test_parse_gamma_events_picks_valid_target() {
    let info =
        parse_gamma_events_response(GAMMA_EVENTS_MIXED).expect("should find valid market");
    // Should pick valid BTC, skipping expired BTC and non-target SOL.
    assert_eq!(info.condition_id, "0xvalid");
}

// ── ISO 8601 parser ────────────────────────────────────────────────

#[test]
fn test_iso8601_epoch_calculation() {
    // 1970-01-01T00:00:00Z should be epoch 0.
    let ms = parse_iso8601_to_epoch_ms("1970-01-01T00:00:00Z").expect("parse failed");
    assert_eq!(ms, 0, "Unix epoch should be 0 ms");
}

#[test]
fn test_iso8601_known_date() {
    // 2024-04-25T15:00:00Z — known epoch seconds = 1714057200
    let ms = parse_iso8601_to_epoch_ms("2024-04-25T15:00:00Z").expect("parse failed");
    // Allow ±1 day for leap year approximation in our simple parser.
    let expected = 1_714_057_200_000u64;
    let delta = (ms as i64 - expected as i64).abs();
    assert!(
        delta < 86_400_000,
        "ISO 8601 parse drift too large: got {ms}, expected ~{expected}, delta {delta}ms"
    );
}

// ── Anticipatory discovery (parse_gamma_events_response_after) ─────

/// Two BTC markets: Market A (ends 2098) and Market B (ends 2099).
/// When skip_before = Market A's end time, only Market B should be returned.
const GAMMA_EVENTS_TWO_MARKETS: &str = r#"[
    {
        "slug": "btc-updown-5m-1111111111",
        "endDate": "2098-06-15T12:00:00Z",
        "markets": [{
            "conditionId": "0xmarketA",
            "clobTokenIds": "[\"0xyesA\", \"0xnoA\"]",
            "acceptingOrders": true
        }]
    },
    {
        "slug": "btc-updown-5m-2222222222",
        "endDate": "2099-06-15T12:00:00Z",
        "markets": [{
            "conditionId": "0xmarketB",
            "clobTokenIds": "[\"0xyesB\", \"0xnoB\"]",
            "acceptingOrders": true
        }]
    }
]"#;

#[test]
fn test_parse_after_skips_current_market() {
    // Market A ends at 2098-06-15T12:00:00Z. Use its end_ms as the cutoff.
    let market_a_end_ms =
        parse_iso8601_to_epoch_ms("2098-06-15T12:00:00Z").expect("parse A end");

    // With skip_before = market A's end, we should get Market B.
    let info = parse_gamma_events_response_after(GAMMA_EVENTS_TWO_MARKETS, market_a_end_ms)
        .expect("should find Market B");
    assert_eq!(info.condition_id, "0xmarketB");
    assert_eq!(info.yes_token_id, "0xyesB");
}

#[test]
fn test_parse_after_returns_soonest_above_cutoff() {
    // With skip_before = 0 (epoch), both markets are valid — should pick A (soonest).
    let info = parse_gamma_events_response_after(GAMMA_EVENTS_TWO_MARKETS, 0)
        .expect("should find Market A");
    assert_eq!(info.condition_id, "0xmarketA");
}

#[test]
fn test_parse_after_errors_when_all_skipped() {
    // Market B ends at 2099-06-15T12:00:00Z. If cutoff is beyond that, nothing matches.
    let beyond_b_ms = parse_iso8601_to_epoch_ms("2099-06-15T12:00:00Z").expect("parse B end");
    let result = parse_gamma_events_response_after(GAMMA_EVENTS_TWO_MARKETS, beyond_b_ms);
    assert!(
        result.is_err(),
        "should error when all markets end before cutoff"
    );
}

/// Market with `acceptingOrders=false` should still be discovered.
/// We only need token IDs for discovery; engine guards prevent premature trading.
const GAMMA_EVENTS_NOT_ACCEPTING: &str = r#"[
    {
        "slug": "btc-updown-5m-9999999900",
        "endDate": "2099-01-01T00:00:00Z",
        "markets": [{
            "conditionId": "0xnotyet",
            "clobTokenIds": "[\"0xyesNew\", \"0xnoNew\"]",
            "acceptingOrders": false
        }]
    }
]"#;

#[test]
fn test_accepting_orders_false_still_discovered() {
    let info = parse_gamma_events_response(GAMMA_EVENTS_NOT_ACCEPTING)
        .expect("should discover market even with acceptingOrders=false");
    assert_eq!(info.condition_id, "0xnotyet");
    assert_eq!(info.yes_token_id, "0xyesNew");
    assert_eq!(info.no_token_id, "0xnoNew");
}
