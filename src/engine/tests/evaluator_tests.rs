use super::*;
use crate::types::market::{BuildupInfo, PriceLevel};

fn make_book(bid: &str, ask: &str) -> OrderBook {
    OrderBook {
        asset_id: "yes".to_string(),
        bids: vec![PriceLevel {
            price: bid.parse().unwrap(),
            size: Decimal::new(500, 0),
        }],
        asks: vec![PriceLevel {
            price: ask.parse().unwrap(),
            size: Decimal::new(500, 0),
        }],
        timestamp_ms: 0,
    }
}

#[test]
fn test_make_leg2_signal_fields() {
    let spike = SpikeInfo {
        direction: Direction::Up,
        magnitude: Decimal::new(5, 3),
        sustained_ms: 200,
        timestamp_ms: 0,
        atr_ratio: Decimal::ZERO,
        obi: Decimal::ZERO,
    };
    let book = make_book("0.30", "0.32");
    let signal = make_leg2_signal(
        "no_token",
        "test_condition",
        Decimal::new(30, 2),
        Decimal::new(100, 0),
        Decimal::new(50_000, 0),
        Decimal::new(75, 2),
        ProfitTier::Med,
        Decimal::new(15, 3),
        Direction::Up,
        spike,
        Decimal::new(70, 2),
        1_000,
        2_000,
        Decimal::new(1, 2),
        Decimal::ZERO,
        false,
        None,
        Some(book),
        None,
    );
    assert!(signal.is_leg2);
    assert_eq!(signal.token_id, "no_token");
    assert_eq!(signal.price, Decimal::new(30, 2));
    assert_eq!(signal.leg1_fill_price, Some(Decimal::new(70, 2)));
}

// ── Emergency repost interval + price tests ─────────────────────────

/// Build a minimal MarketState + HedgeSnap suitable for emergency repost tests.
/// Direction::Up → hedge token is NO → evaluator reads `poly_no_book`.
fn make_leg2_test_setup(
    no_book_ask: &str,
    now_ms: u64,
) -> (MarketState, HedgeSnap, Leg2Evaluator) {
    use crate::types::market::OrderState;

    let tick = Decimal::new(1, 2); // 0.01

    let state = MarketState {
        poly_no_book: Some(OrderBook {
            asset_id: "no".to_string(),
            bids: vec![PriceLevel {
                price: Decimal::new(40, 2),
                size: Decimal::new(200, 0),
            }],
            asks: vec![PriceLevel {
                price: no_book_ask.parse().unwrap(),
                size: Decimal::new(200, 0),
            }],
            timestamp_ms: now_ms,
        }),
        poly_yes_book: Some(make_book("0.50", "0.52")),
        binance_price: Some(Decimal::new(50_000, 0)),
        active_condition_id: Some("cond".to_string()),
        active_yes_token_id: Some("yes".to_string()),
        active_no_token_id: Some("no".to_string()),
        tick_size: tick,
        market_end_timestamp_ms: now_ms + 600_000,
        leg1_state: OrderState::Filled {
            order_id: "sim-leg1".into(),
            price: Decimal::new(50, 2),
            size: Decimal::new(100, 0),
            fill_timestamp_ms: now_ms - 5_000,
        },
        leg2_state: OrderState::Posted {
            order_id: "sim-leg2".into(),
            price: Decimal::new(48, 2),
            size: Decimal::new(100, 0),
            timestamp_ms: now_ms - 1_000,
        },
        ..MarketState::default()
    };

    let snap = HedgeSnap {
        emergency_submitted: false,
        break_even: Decimal::new(50, 2),
        leg1_fee: Decimal::ZERO,
        initial_profit_target: Decimal::new(25, 3), // 2.5%
        direction: Direction::Up,
        fill_ms: now_ms - 5_000,
        tier: ProfitTier::High,
        expected_pct: Decimal::new(7, 1),
        spike_info: SpikeInfo {
            direction: Direction::Up,
            magnitude: Decimal::new(5, 3),
            sustained_ms: 300,
            timestamp_ms: now_ms - 6_000,
            atr_ratio: Decimal::ZERO,
            obi: Decimal::ZERO,
        },
        phase: HedgePhase::Phase1,
        phase1_target_price: Decimal::new(475, 3),
        phase2_start_ms: None,
        phase2_posted_price: None,
        flow_monitoring_active: false,
        last_flow_score: Decimal::ZERO,
        last_flow_direction: None,
    };

    let evaluator = Leg2Evaluator {
        phase1_timeout_ms: 2000,
        phase1_breach_threshold: Decimal::new(105, 2),
        phase2_timeout_ms: 2000,
        entry_threshold: Decimal::new(40, 2),
        cancel_threshold: Decimal::new(25, 2),
    };

    (state, snap, evaluator)
}

// ── Emergency submitted returns None (no chase) ───────────────────

#[test]
fn test_emergency_submitted_returns_none() {
    let now_ms = 100_000;
    let (_state, mut snap, evaluator) = make_leg2_test_setup("0.49", now_ms);
    snap.emergency_submitted = true;
    let state = {
        let (s, _, _) = make_leg2_test_setup("0.49", now_ms);
        s
    };
    let result = evaluator.evaluate_leg2(&state, &snap, now_ms);
    assert!(result.is_none(), "emergency_submitted should short-circuit to None");
}

// ── Phase 1 breach triggers immediate FOK ─────────────────────────

#[test]
fn test_phase1_breach_triggers_fok() {
    use crate::types::market::OrderState;
    let now_ms = 100_000;

    let (mut state, mut snap, evaluator) = make_leg2_test_setup("0.56", now_ms);
    snap.phase = HedgePhase::Phase1;
    state.leg2_state = OrderState::Posted {
        order_id: "sim-leg2".into(),
        price: Decimal::new(475, 3),
        size: Decimal::new(100, 0),
        timestamp_ms: now_ms - 1_000,
    };
    // leg1=0.50, ask=0.56 → pair=1.06 > 1.05 test threshold → breach
    let result = evaluator.evaluate_leg2(&state, &snap, now_ms);
    assert!(result.is_some(), "phase 1 breach should trigger FOK");
    let decision = result.unwrap();
    assert!(decision.is_emergency(), "breach should be emergency (FOK)");
    // Price should be at ask: 0.56
    assert_eq!(decision.price(), Decimal::new(56, 2));
    let sig = decision.into_signal();
    assert_eq!(sig.exit_reason, Some(ExitReason::Phase1Breach));

}

// ── Phase 1 timeout triggers transition ───────────────────────────

#[test]
fn test_phase1_timeout_triggers_transition() {
    use crate::types::market::OrderState;
    let now_ms = 100_000;

    let (mut state, mut snap, evaluator) = make_leg2_test_setup("0.49", now_ms);
    snap.phase = HedgePhase::Phase1;
    // fill_ms 3s ago → past 2s timeout
    snap.fill_ms = now_ms - 3_000;
    state.leg2_state = OrderState::Posted {
        order_id: "sim-leg2".into(),
        price: Decimal::new(475, 3),
        size: Decimal::new(100, 0),
        timestamp_ms: now_ms - 2_000,
    };

    let result = evaluator.evaluate_leg2(&state, &snap, now_ms);
    assert!(result.is_some(), "timeout should trigger transition");
    let decision = result.unwrap();
    assert!(matches!(decision, Leg2Decision::Phase2Alongside { reason: TransitionReason::Timeout, .. }));
}

// ── Phase 2 BE breach triggers emergency ──────────────────────────

#[test]
fn test_phase2_be_breach_triggers_emergency() {
    use crate::types::market::OrderState;
    let now_ms = 100_000;

    let (mut state, mut snap, evaluator) = make_leg2_test_setup("0.51", now_ms);
    snap.phase = HedgePhase::Phase2;
    snap.phase2_start_ms = Some(now_ms - 500);
    snap.phase2_posted_price = Some(Decimal::new(50, 2)); // ask 0.51 > posted 0.50 → breach
    state.leg2_state = OrderState::Posted {
        order_id: "sim-leg2".into(),
        price: Decimal::new(48, 2),
        size: Decimal::new(100, 0),
        timestamp_ms: now_ms - 1_000,
    };
    // ask=0.51 > phase2_posted_price=0.50 → breach
    let result = evaluator.evaluate_leg2(&state, &snap, now_ms);
    assert!(result.is_some(), "BE breach should trigger emergency");
    let decision = result.unwrap();
    assert!(decision.is_emergency());
    // Price should be at ask (FOK): 0.51
    assert_eq!(decision.price(), Decimal::new(51, 2));
    let sig = decision.into_signal();
    assert_eq!(sig.exit_reason, Some(ExitReason::Phase2PriceBreach));
}

// ── Phase 2 timeout triggers FOK ───────────────────────────────────

#[test]
fn test_phase2_timeout_triggers_fok() {
    use crate::types::market::OrderState;
    let now_ms = 100_000;

    let (mut state, mut snap, evaluator) = make_leg2_test_setup("0.49", now_ms);
    snap.phase = HedgePhase::Phase2;
    snap.phase2_start_ms = Some(now_ms - 3_000); // 3s ago > 2s timeout
    state.leg2_state = OrderState::Posted {
        order_id: "sim-leg2".into(),
        price: Decimal::new(48, 2),
        size: Decimal::new(100, 0),
        timestamp_ms: now_ms - 3_000,
    };
    let result = evaluator.evaluate_leg2(&state, &snap, now_ms);
    assert!(result.is_some(), "phase 2 timeout should trigger FOK");
    let decision = result.unwrap();
    assert!(decision.is_emergency());
    // Price should be at ask (FOK): 0.49
    assert_eq!(decision.price(), Decimal::new(49, 2));
    let sig = decision.into_signal();
    assert_eq!(sig.exit_reason, Some(ExitReason::Phase2Timeout));
}

// ── Phase 2 holds position (no repost) ──────────────────────────────

#[test]
fn test_phase2_no_repost_on_price_improvement() {
    use crate::types::market::OrderState;
    let now_ms = 100_000;

    let (mut state, mut snap, evaluator) = make_leg2_test_setup("0.49", now_ms);
    snap.phase = HedgePhase::Phase2;
    snap.phase2_start_ms = Some(now_ms - 500); // within timeout
    state.leg2_state = OrderState::Posted {
        order_id: "sim-leg2".into(),
        price: Decimal::new(46, 2),
        size: Decimal::new(100, 0),
        timestamp_ms: now_ms - 1_000,
    };
    // ask=0.49 → previously would have triggered repost. Now should hold.
    let result = evaluator.evaluate_leg2(&state, &snap, now_ms);
    assert!(result.is_none(), "Phase 2 should hold position — no reposts");
}

// ── Dampening tests ──────────────────────────────────────────────────

fn make_leg1_evaluator(dampen: &str) -> Leg1Evaluator {
    Leg1Evaluator {
        entry_cutoff_secs: 25,
        stale_book_ms: 5000,
        max_entry_spread: Decimal::new(10, 1), // 1.0 — wide enough that tests don't trigger
        max_alloc_per_trade: Decimal::new(15, 0),
        reprice_scale: Decimal::new(5, 2),    // 0.05
        min_reprice_pct: Decimal::new(1, 3),  // 0.001 — low so tests pass easily
        min_alloc_pct: Decimal::new(25, 2),   // 0.25
        hard_skew_cap: Decimal::new(90, 2),   // 0.90
        max_ask_pair_price: Decimal::new(103, 2), // 1.03
        time_exponent: 0.5,
        max_time_factor: 2.0,
        phase1_target_dampen: dampen.parse().unwrap(),
    }
}

/// Helper to build a test BuildupInfo with sensible defaults.
fn test_buildup_info(direction: Direction, now_ms: u64) -> BuildupInfo {
    BuildupInfo {
        composite_score: Decimal::new(7, 1),  // 0.70
        direction,
        cvd_accel: Decimal::ZERO,
        spot_flow: Decimal::ZERO,
        obi_velocity: Decimal::ZERO,
        basis_delta: Decimal::ZERO,
        liq_pressure: Decimal::ZERO,
        atr_displacement: Decimal::new(5, 3),
        signal_atr_ratio: Decimal::new(60, 0),  // 60x — matches old spike tests
        obi: Decimal::new(3, 1),                 // 0.3 — bid-heavy
        timestamp_ms: now_ms.saturating_sub(200),
        cvd_norm: 0.0,
        basis_norm: 0.0,
        spot_flow_norm: 0.0,
        obi_norm: 0.0,
        liq_norm: 0.0,
        atr_norm: 0.0,
        cvd_age_ms: 0,
        basis_age_ms: 0,
        spot_flow_age_ms: 0,
        obi_age_ms: 0,
        liq_age_ms: 0,
        atr_age_ms: 0,
        dissenter_count: 0,
    }
}

fn make_leg1_test_state(now_ms: u64) -> MarketState {
    let yes_book = OrderBook {
        asset_id: "yes".to_string(),
        bids: vec![PriceLevel {
            price: Decimal::new(50, 2),  // 0.50
            size: Decimal::new(500, 0),
        }],
        asks: vec![PriceLevel {
            price: Decimal::new(52, 2),  // 0.52
            size: Decimal::new(500, 0),
        }],
        timestamp_ms: now_ms,
    };
    let no_book = OrderBook {
        asset_id: "no".to_string(),
        bids: vec![PriceLevel {
            price: Decimal::new(48, 2),  // 0.48
            size: Decimal::new(500, 0),
        }],
        asks: vec![PriceLevel {
            price: Decimal::new(49, 2),  // 0.49
            size: Decimal::new(500, 0),
        }],
        timestamp_ms: now_ms,
    };
    MarketState {
        poly_yes_book: Some(yes_book.clone()),
        poly_no_book: Some(no_book),
        poly_book: Some(yes_book),
        binance_price: Some(Decimal::new(50_000, 0)),
        active_condition_id: Some("cond".to_string()),
        active_yes_token_id: Some("yes".to_string()),
        active_no_token_id: Some("no".to_string()),
        tick_size: Decimal::new(1, 2),
        market_end_timestamp_ms: now_ms + 240_000,
        leg1_state: OrderState::None,
        leg2_state: OrderState::None,
        buildup_detected: true,
        last_buildup: Some(test_buildup_info(Direction::Up, now_ms)),
        atr: Some(Decimal::new(2, 3)),
        ..MarketState::default()
    }
}

#[test]
fn test_dampening_reduces_target_not_allocation() {
    let now_ms = 100_000;
    let eval = make_leg1_evaluator("0.8");
    let state = make_leg1_test_state(now_ms);

    let outcome = eval.evaluate(&state, now_ms);
    match outcome {
        Leg1Outcome::Signal(sig) => {
            // profit_target_pct should be less than expected_pct due to dampening
            assert!(sig.profit_target_pct < sig.expected_pct,
                "dampened target ({}) should be < expected_pct ({})",
                sig.profit_target_pct, sig.expected_pct);
            // allocation should be based on raw expected_pct (>0)
            assert!(sig.alloc_amount > Decimal::ZERO);
        }
        other => panic!("expected Signal, got {other:?}"),
    }
}

#[test]
fn test_dampening_1_0_is_noop() {
    let now_ms = 100_000;
    let eval_damped = make_leg1_evaluator("1.0");
    let state = make_leg1_test_state(now_ms);

    let outcome = eval_damped.evaluate(&state, now_ms);
    match outcome {
        Leg1Outcome::Signal(sig) => {
            // With dampen=1.0, profit_target_pct should equal round_to_tick(expected_pct)
            let expected_target = round_to_tick(sig.expected_pct, Decimal::new(1, 2));
            assert_eq!(sig.profit_target_pct, expected_target,
                "dampen=1.0 should produce target = round_to_tick(expected_pct)");
        }
        other => panic!("expected Signal, got {other:?}"),
    }
}

// ── OBI computation tests ─────────────────────────────────────────────

#[test]
fn test_obi_computation() {
    use crate::types::market::BinanceDepth;
    let depth = BinanceDepth {
        symbol: "BTCUSDT",
        bids: vec![
            PriceLevel { price: Decimal::new(100, 0), size: Decimal::new(30, 0) },
            PriceLevel { price: Decimal::new(99, 0), size: Decimal::new(20, 0) },
        ],
        asks: vec![
            PriceLevel { price: Decimal::new(101, 0), size: Decimal::new(10, 0) },
            PriceLevel { price: Decimal::new(102, 0), size: Decimal::new(40, 0) },
        ],
        timestamp_ms: 0,
    };
    // bid_depth = 50, ask_depth = 50, total = 100, OBI = 0/100 = 0
    let obi = depth.obi().unwrap();
    assert_eq!(obi, Decimal::ZERO, "equal depths should give OBI = 0");
}

#[test]
fn test_obi_computation_bullish() {
    use crate::types::market::BinanceDepth;
    let depth = BinanceDepth {
        symbol: "BTCUSDT",
        bids: vec![
            PriceLevel { price: Decimal::new(100, 0), size: Decimal::new(80, 0) },
        ],
        asks: vec![
            PriceLevel { price: Decimal::new(101, 0), size: Decimal::new(20, 0) },
        ],
        timestamp_ms: 0,
    };
    // bid=80, ask=20, total=100, OBI = 60/100 = 0.6
    let obi = depth.obi().unwrap();
    assert_eq!(obi, Decimal::new(6, 1));
}

// ── Ask pair price guard tests ──────────────────────────────────────

#[test]
fn test_ask_pair_guard_blocks() {
    let now_ms = 100_000;
    let mut eval = make_leg1_evaluator("1.0");
    eval.max_ask_pair_price = Decimal::new(100, 2); // 1.00 — very tight

    // Direction::Up → leg1 buys YES (ask 0.52), hedge is NO book (ask 0.49)
    // Combined = 0.52 + 0.49 = 1.01 > 1.00 → should block
    let state = make_leg1_test_state(now_ms);
    let outcome = eval.evaluate(&state, now_ms);
    assert!(
        matches!(outcome, Leg1Outcome::Rejected(Leg1RejectReason::AskPairTooExpensive)),
        "ask pair 1.01 > max 1.00 should block, got {outcome:?}"
    );
}

#[test]
fn test_ask_pair_guard_passes() {
    let now_ms = 100_000;
    let mut eval = make_leg1_evaluator("1.0");
    eval.max_ask_pair_price = Decimal::new(103, 2); // 1.03

    // Direction::Up → leg1 buys YES (ask 0.52), hedge is NO book (ask 0.49)
    // Combined = 0.52 + 0.49 = 1.01 <= 1.03 → should pass
    let state = make_leg1_test_state(now_ms);
    let outcome = eval.evaluate(&state, now_ms);
    assert!(
        matches!(outcome, Leg1Outcome::Signal(_)),
        "ask pair 1.01 <= max 1.03 should pass, got {outcome:?}"
    );
}

