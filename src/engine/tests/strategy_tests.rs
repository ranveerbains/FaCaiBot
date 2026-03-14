use super::*;
use crate::types::market::{BinanceTick, BuildupInfo, OrderBook, PriceLevel};

const TEST_FIXED_ALLOC: Decimal = Decimal::from_parts(100, 0, 0, false, 0);
fn make_engine_with_market(secs_remaining: u64) -> StrategyEngine {
    let mut engine = StrategyEngine::new(&Config::test_defaults());
    engine.on_event(IngestorEvent::MarketRotation {
        condition_id: "cond".to_string(),
        yes_token_id: "yes".to_string(),
        no_token_id: "no".to_string(),
        end_timestamp_ms: now_epoch_ms() + secs_remaining * 1_000,
        tick_size: Decimal::new(1, 2),
    });
    engine.state.available_capital = TEST_FIXED_ALLOC;
    engine
}

fn set_book(engine: &mut StrategyEngine, bid: &str, ask: &str) {
    engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
        asset_id: "yes".to_string(),
        bids: vec![PriceLevel {
            price: bid.parse().unwrap(),
            size: Decimal::new(500, 0),
        }],
        asks: vec![PriceLevel {
            price: ask.parse().unwrap(),
            size: Decimal::new(500, 0),
        }],
        timestamp_ms: now_epoch_ms(),
    }));
}

fn inject_buildup(engine: &mut StrategyEngine, direction: Direction) {
    engine.state.buildup_detected = true;
    engine.state.last_buildup = Some(BuildupInfo {
        composite_score: Decimal::new(5, 1), // 0.5
        direction,
        cvd_accel: Decimal::ZERO,
        spot_flow: Decimal::ZERO,
        obi_velocity: Decimal::ZERO,
        basis_delta: Decimal::ZERO,
        liq_pressure: Decimal::ZERO,
        atr_displacement: Decimal::ZERO,
        signal_atr_ratio: Decimal::new(50, 0),
        obi: Decimal::ZERO,
        timestamp_ms: now_epoch_ms() - 200,
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
    });
    engine.state.atr = Some(Decimal::new(2, 3));
    engine.state.binance_price = Some(Decimal::new(50_000, 0));
}

// ── on_event: MarketRotation ──────────────────────────────────────────

#[test]
fn test_market_rotation_resets_state() {
    let mut engine = make_engine_with_market(600);
    engine.state.cumulative_used = Decimal::new(50, 0);
    engine.state.buildup_detected = true;
    engine.state.leg1_state = OrderState::Posted {
        order_id: "old".to_string(),
        price: Decimal::new(48, 2),
        size: Decimal::new(100, 0),
        timestamp_ms: 0,
    };
    engine.on_event(IngestorEvent::MarketRotation {
        condition_id: "new_cond".to_string(),
        yes_token_id: "new_yes".to_string(),
        no_token_id: "new_no".to_string(),
        end_timestamp_ms: now_epoch_ms() + 900_000,
        tick_size: Decimal::new(1, 2),
    });
    assert_eq!(engine.state.cumulative_used, Decimal::ZERO);
    assert!(!engine.state.buildup_detected);
    assert!(matches!(engine.state.leg1_state, OrderState::None));
    assert_eq!(
        engine.state.active_condition_id.as_deref(),
        Some("new_cond")
    );
}

#[test]
fn test_market_rotation_sets_tick_size() {
    let mut engine = make_engine_with_market(600);
    // Default from make_engine_with_market is 0.01.
    assert_eq!(engine.state.tick_size, Decimal::new(1, 2));

    // Rotate with a different tick_size.
    engine.on_event(IngestorEvent::MarketRotation {
        condition_id: "new_cond".to_string(),
        yes_token_id: "new_yes".to_string(),
        no_token_id: "new_no".to_string(),
        end_timestamp_ms: now_epoch_ms() + 900_000,
        tick_size: Decimal::new(1, 3), // 0.001
    });
    assert_eq!(
        engine.state.tick_size,
        Decimal::new(1, 3),
        "tick_size should be set from MarketRotation event"
    );
}

// ── on_event: TickSizeChange ──────────────────────────────────────────

#[test]
fn test_tick_size_change_updates_active_asset() {
    let mut engine = StrategyEngine::new(&Config::test_defaults());
    // Set active tokens so the asset_id filter matches.
    engine.state.active_yes_token_id = Some("yes_tok".to_string());
    engine.state.active_no_token_id = Some("no_tok".to_string());

    let new_tick = Decimal::new(1, 3);
    engine.on_event(IngestorEvent::PolymarketTickSizeChange {
        asset_id: "yes_tok".to_string(),
        old_tick_size: Decimal::new(1, 2),
        new_tick_size: new_tick,
    });
    assert_eq!(engine.state.tick_size, new_tick);
    // Should produce a pending tick_size command for the executor.
    let cmd = engine.take_tick_size_change();
    assert!(cmd.is_some());
    match cmd.unwrap() {
        ExecutorCommand::TickSizeChanged {
            yes_token_id,
            no_token_id,
            new_tick_size: ts,
        } => {
            assert_eq!(yes_token_id, "yes_tok");
            assert_eq!(no_token_id, "no_tok");
            assert_eq!(ts, new_tick);
        }
        _ => panic!("expected TickSizeChanged"),
    }
}

#[test]
fn test_tick_size_change_ignores_non_active_asset() {
    let mut engine = StrategyEngine::new(&Config::test_defaults());
    engine.state.active_yes_token_id = Some("yes_tok".to_string());
    engine.state.active_no_token_id = Some("no_tok".to_string());
    let old_tick = engine.state.tick_size;

    engine.on_event(IngestorEvent::PolymarketTickSizeChange {
        asset_id: "other_tok".to_string(),
        old_tick_size: Decimal::new(1, 2),
        new_tick_size: Decimal::new(1, 3),
    });
    // tick_size should NOT change.
    assert_eq!(engine.state.tick_size, old_tick);
    // No pending command.
    assert!(engine.take_tick_size_change().is_none());
}

// ── on_event: BinanceTick (normal) ────────────────────────────────────

#[test]
fn test_binance_tick_updates_price() {
    let mut engine = StrategyEngine::new(&Config::test_defaults());
    engine.on_event(IngestorEvent::BinanceTick(BinanceTick {
        symbol: "BTCUSDT",
        bid_price: Decimal::new(52_000_00, 2),
        bid_qty: Decimal::new(10, 0), // > 1.0 → NOT spike-encoded
        ask_price: Decimal::new(52_001_00, 2),
        ask_qty: Decimal::new(5, 0),
        timestamp_ms: now_epoch_ms(),
    }));
    let expected = (Decimal::new(52_000_00, 2) + Decimal::new(52_001_00, 2)) / Decimal::TWO;
    assert_eq!(engine.state.binance_price, Some(expected));
}

// ── evaluate ─────────────────────────────────────────────────────────

#[test]
fn test_evaluate_no_signal_without_buildup() {
    let mut engine = make_engine_with_market(600);
    set_book(&mut engine, "0.48", "0.52");
    engine.state.binance_price = Some(Decimal::new(50_000, 0));
    assert!(engine.evaluate().is_none());
}

#[test]
fn test_evaluate_aborts_near_expiry() {
    let mut engine = make_engine_with_market(60);
    set_book(&mut engine, "0.48", "0.52");
    inject_buildup(&mut engine, Direction::Up);
    assert!(engine.evaluate().is_none());
}

#[test]
fn test_evaluate_generates_leg1_signal() {
    let mut engine = make_engine_with_market(600);
    set_book(&mut engine, "0.495", "0.505");
    inject_buildup(&mut engine, Direction::Up);
    let s = engine.evaluate().expect("should generate signal");
    assert!(!s.is_leg2);
    assert_eq!(s.side, Side::Buy);
    assert_eq!(s.token_id, "yes"); // UP → YES
    // FOK taker: signal price = best ask
    assert_eq!(s.price, Decimal::new(505, 3));
}

#[test]
fn test_evaluate_no_signal_with_active_leg1() {
    let mut engine = make_engine_with_market(600);
    set_book(&mut engine, "0.48", "0.52");
    inject_buildup(&mut engine, Direction::Up);
    engine.state.leg1_state = OrderState::Posted {
        order_id: "ord1".to_string(),
        price: Decimal::new(49, 2),
        size: Decimal::new(100, 0),
        timestamp_ms: now_epoch_ms(),
    };
    assert!(engine.evaluate().is_none());
}

// ── ProfitTier ────────────────────────────────────────────────────────

#[test]
fn test_profit_tier_from_expected_reprice() {
    let scale = Decimal::new(15, 3); // 0.015
    // HIGH: pct >= scale
    assert_eq!(
        ProfitTier::from_expected_reprice(Decimal::new(20, 3), scale),
        ProfitTier::High
    );
    assert_eq!(
        ProfitTier::from_expected_reprice(Decimal::new(15, 3), scale),
        ProfitTier::High
    );
    // MED: pct >= scale/2 (0.0075)
    assert_eq!(
        ProfitTier::from_expected_reprice(Decimal::new(10, 3), scale),
        ProfitTier::Med
    );
    assert_eq!(
        ProfitTier::from_expected_reprice(Decimal::new(75, 4), scale),
        ProfitTier::Med
    );
    // LOW: below scale/2
    assert_eq!(
        ProfitTier::from_expected_reprice(Decimal::new(5, 3), scale),
        ProfitTier::Low
    );
    assert_eq!(
        ProfitTier::from_expected_reprice(Decimal::ZERO, scale),
        ProfitTier::Low
    );
}

// ── HedgeState ────────────────────────────────────────────────────────

// ── TradeStatusUpdate fills Leg 1 ─────────────────────────────────────

#[test]
fn test_leg1_fill_initialises_hedge() {
    let mut engine = make_engine_with_market(600);
    let now_ms = now_epoch_ms();
    engine.state.leg1_state = OrderState::Posted {
        order_id: "ord1".to_string(),
        price: Decimal::new(48, 2),
        size: Decimal::new(100, 0),
        timestamp_ms: now_ms - 500,
    };
    engine.state.last_buildup = Some(BuildupInfo {
        composite_score: Decimal::new(5, 1),
        direction: Direction::Up,
        cvd_accel: Decimal::ZERO,
        spot_flow: Decimal::ZERO,
        obi_velocity: Decimal::ZERO,
        basis_delta: Decimal::ZERO,
        liq_pressure: Decimal::ZERO,
        atr_displacement: Decimal::ZERO,
        signal_atr_ratio: Decimal::ZERO,
        obi: Decimal::ZERO,
        timestamp_ms: now_ms - 300,
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
    });
    engine.state.atr = Some(Decimal::new(2, 3));

    engine.on_event(IngestorEvent::TradeStatusUpdate {
        order_id: "ord1".to_string(),
        status: TradeStatus::Matched,
        size_matched: None,
        original_size: None,
    });

    assert!(matches!(engine.state.leg1_state, OrderState::Filled { .. }));
    assert!(engine.hedge.is_some());
}

// ── HeartbeatStatus ───────────────────────────────────────────────────

#[test]
fn test_heartbeat_failure_tracking() {
    let mut engine = StrategyEngine::new(&Config::test_defaults());
    engine.on_event(IngestorEvent::HeartbeatStatus {
        success: false,
        latency_ms: 0,
    });
    engine.on_event(IngestorEvent::HeartbeatStatus {
        success: false,
        latency_ms: 0,
    });
    assert_eq!(engine.connectivity.consecutive_heartbeat_failures, 2);
    engine.on_event(IngestorEvent::HeartbeatStatus {
        success: true,
        latency_ms: 3,
    });
    assert_eq!(engine.connectivity.consecutive_heartbeat_failures, 0);
    assert!(engine.connectivity.heartbeat_healthy);
}

// ── WsStatus ─────────────────────────────────────────────────────────

#[test]
fn test_ws_disconnect_clears_buildup_detected() {
    let mut engine = StrategyEngine::new(&Config::test_defaults());
    engine.state.buildup_detected = true;
    engine.on_event(IngestorEvent::WsStatus {
        source: DataSource::Binance,
        connected: false,
    });
    assert!(!engine.state.buildup_detected);
    assert!(!engine.connectivity.binance_connected);
}

// ── Self-gating & advance_simulation ────────────────────────────────

#[test]
fn test_evaluate_self_gates_after_signal() {
    let mut engine = make_engine_with_market(600);
    set_book(&mut engine, "0.495", "0.505");
    inject_buildup(&mut engine, Direction::Up);

    let s1 = engine.evaluate();
    assert!(s1.is_some(), "first evaluate should produce signal");

    let s2 = engine.evaluate();
    assert!(
        s2.is_none(),
        "second evaluate should be blocked by self-gating"
    );

    assert!(!engine.state.buildup_detected, "buildup should be cleared");
    assert!(
        matches!(engine.state.leg1_state, OrderState::Posted { .. }),
        "leg1_state should be Posted"
    );
    assert!(
        engine.state.cumulative_used > Decimal::ZERO,
        "allocation should be recorded"
    );
}

#[test]
fn test_evaluate_allows_new_signal_after_reset() {
    let mut engine = make_engine_with_market(600);
    set_book(&mut engine, "0.495", "0.505");
    inject_buildup(&mut engine, Direction::Up);

    let s1 = engine.evaluate();
    assert!(s1.is_some());

    // Simulate trade completion reset.
    engine.state.leg1_state = OrderState::None;
    engine.state.leg2_state = OrderState::None;
    engine.hedge = None;

    inject_buildup(&mut engine, Direction::Up);
    let s2 = engine.evaluate();
    assert!(
        s2.is_some(),
        "new signal should be generated after state reset"
    );
}

#[test]
fn test_advance_simulation_leg1_fill() {
    let mut engine = make_engine_with_market(600);
    set_book(&mut engine, "0.495", "0.505");
    inject_buildup(&mut engine, Direction::Up);

    let _signal = engine.evaluate().expect("should generate signal");
    assert!(matches!(engine.state.leg1_state, OrderState::Posted { .. }));

    engine.advance_simulation();
    assert!(
        matches!(engine.state.leg1_state, OrderState::Filled { .. }),
        "Leg 1 should be Filled when depth exists near bid and delay elapsed"
    );
    assert!(engine.hedge.is_some(), "hedge should be initialized");
}

// test_advance_simulation_leg1_no_fill_before_delay removed:
// fill delay is now 0 (instant fills) — no timing gate to test.

#[test]
fn test_advance_simulation_leg1_no_fill_no_depth() {
    let mut engine = make_engine_with_market(600);
    set_book(&mut engine, "0.495", "0.505");
    inject_buildup(&mut engine, Direction::Up);

    let _signal = engine.evaluate().expect("should generate signal");


    engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
        asset_id: "yes".to_string(),
        bids: vec![PriceLevel {
            price: "0.495".parse().unwrap(),
            size: Decimal::new(500, 0),
        }],
        asks: vec![PriceLevel {
            price: "0.60".parse().unwrap(),
            size: Decimal::new(500, 0),
        }],
        timestamp_ms: now_epoch_ms(),
    }));

    engine.advance_simulation();
    assert!(
        matches!(engine.state.leg1_state, OrderState::Posted { .. }),
        "should remain Posted when no depth within 2 ticks"
    );
}

#[test]
fn test_advance_simulation_full_trade_cycle() {
    let mut engine = make_engine_with_market(600);
    set_book(&mut engine, "0.495", "0.505");
    inject_buildup(&mut engine, Direction::Up);

    let _s1 = engine.evaluate().expect("Leg 1 signal");
    assert!(matches!(engine.state.leg1_state, OrderState::Posted { .. }));


    engine.advance_simulation();
    assert!(matches!(engine.state.leg1_state, OrderState::Filled { .. }));
    assert!(engine.hedge.is_some());

    let s2 = engine.evaluate_leg2();
    assert!(s2.is_some(), "Leg 2 hedge signal should be generated");
    assert!(matches!(engine.state.leg2_state, OrderState::Posted { .. }));

    set_book(&mut engine, "0.40", "0.45");
    engine.advance_simulation();

    assert!(
        matches!(engine.state.leg1_state, OrderState::None),
        "leg1 should be reset after trade completion"
    );
    assert!(
        matches!(engine.state.leg2_state, OrderState::None),
        "leg2 should be reset after trade completion"
    );
    assert!(engine.hedge.is_none());
    assert!(
        engine.state.cumulative_used > Decimal::ZERO,
        "cumulative_used should persist"
    );
}

#[test]
fn test_advance_simulation_noop_when_idle() {
    let mut engine = make_engine_with_market(600);
    engine.advance_simulation();
    assert!(matches!(engine.state.leg1_state, OrderState::None));
    assert!(matches!(engine.state.leg2_state, OrderState::None));
}

#[test]
fn test_multiple_trades_per_market() {
    let mut engine = make_engine_with_market(600);

    set_book(&mut engine, "0.495", "0.505");
    inject_buildup(&mut engine, Direction::Up);
    let _s1 = engine.evaluate().expect("Trade 1 Leg 1");


    engine.advance_simulation();
    assert!(matches!(engine.state.leg1_state, OrderState::Filled { .. }));

    let _s2 = engine.evaluate_leg2().expect("Trade 1 Leg 2");
    set_book(&mut engine, "0.40", "0.45");
    engine.advance_simulation();

    assert!(matches!(engine.state.leg1_state, OrderState::None));
    let used_after_trade1 = engine.state.cumulative_used;
    assert!(used_after_trade1 > Decimal::ZERO);

    set_book(&mut engine, "0.495", "0.505");
    inject_buildup(&mut engine, Direction::Up);
    let s3 = engine.evaluate();
    if engine.state.remaining_alloc(TEST_FIXED_ALLOC) >= TEST_FIXED_ALLOC * Decimal::new(10, 2)
    {
        assert!(s3.is_some(), "Trade 2 should generate if capital remains");
        assert!(engine.state.cumulative_used > used_after_trade1);
    }
}

#[test]
fn test_init_leg2_helper() {
    let mut engine = make_engine_with_market(600);
    set_book(&mut engine, "0.495", "0.505");
    inject_buildup(&mut engine, Direction::Up);
    engine.state.atr = Some(Decimal::new(2, 3));

    let now_ms = now_epoch_ms();
    engine.init_leg2(Decimal::new(50, 2), Decimal::new(100, 0), now_ms);

    let hedge = engine
        .hedge
        .as_ref()
        .expect("hedge should be initialized");
    assert_eq!(hedge.leg1_fill_price, Decimal::new(50, 2));
    assert_eq!(hedge.leg1_fill_ms, now_ms);
    assert!(hedge.expected_pct > Decimal::ZERO);
    assert_eq!(hedge.phase, HedgePhase::Phase1);
}

// ── Rotation emergency protection ──────────────────────────────────

/// Helper: set up an engine with a Leg 1 filled position + hedge state.
/// Returns the engine in a state where Leg 1 is Filled and hedge is initialized.
fn engine_with_filled_leg1() -> StrategyEngine {
    let mut engine = make_engine_with_market(600);
    set_book(&mut engine, "0.495", "0.505");
    inject_buildup(&mut engine, Direction::Up);

    let _signal = engine.evaluate().expect("should generate Leg 1 signal");
    assert!(matches!(engine.state.leg1_state, OrderState::Posted { .. }));

    // Simulate Leg 1 fill via advance_simulation.
    engine.advance_simulation();
    assert!(
        matches!(engine.state.leg1_state, OrderState::Filled { .. }),
        "Leg 1 should be filled"
    );
    assert!(engine.hedge.is_some(), "hedge should be initialized");
    engine
}

#[test]
fn test_rotation_with_filled_leg1_generates_emergency() {
    let mut engine = engine_with_filled_leg1();

    // Set up the NO book (opposing side for an Up spike) so the emergency
    // signal can find an opposing ask price.
    engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
        asset_id: "no".to_string(),
        bids: vec![PriceLevel {
            price: Decimal::new(45, 2),
            size: Decimal::new(100, 0),
        }],
        asks: vec![PriceLevel {
            price: Decimal::new(50, 2),
            size: Decimal::new(100, 0),
        }],
        timestamp_ms: now_epoch_ms(),
    }));

    // Rotate to a new market.
    engine.on_event(IngestorEvent::MarketRotation {
        condition_id: "new_cond".to_string(),
        yes_token_id: "new_yes".to_string(),
        no_token_id: "new_no".to_string(),
        end_timestamp_ms: now_epoch_ms() + 900_000,
        tick_size: Decimal::new(1, 2),
    });

    let emergencies = engine.take_rotation_emergencies();
    assert_eq!(
        emergencies.len(),
        1,
        "should generate exactly 1 emergency signal"
    );

    let sig = &emergencies[0];
    assert!(sig.is_leg2, "emergency should be a Leg 2 signal");
    assert_eq!(
        sig.exit_reason,
        Some(ExitReason::MarketExpiry),
        "exit reason should be MarketExpiry"
    );
    // Signal should reference the OLD market's token, not the new one.
    assert_eq!(sig.token_id, "no", "should use old market's NO token");
    assert_eq!(
        sig.condition_id, "cond",
        "should use old market's condition ID"
    );
}

#[test]
fn test_rotation_without_position_no_emergency() {
    let mut engine = make_engine_with_market(600);

    engine.on_event(IngestorEvent::MarketRotation {
        condition_id: "new_cond".to_string(),
        yes_token_id: "new_yes".to_string(),
        no_token_id: "new_no".to_string(),
        end_timestamp_ms: now_epoch_ms() + 900_000,
        tick_size: Decimal::new(1, 2),
    });

    let emergencies = engine.take_rotation_emergencies();
    assert!(
        emergencies.is_empty(),
        "no emergency when no position is open"
    );
}

#[test]
fn test_rotation_both_legs_filled_no_emergency() {
    let mut engine = engine_with_filled_leg1();

    // Set up NO book and generate a Leg 2 hedge signal.
    engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
        asset_id: "no".to_string(),
        bids: vec![PriceLevel {
            price: Decimal::new(45, 2),
            size: Decimal::new(100, 0),
        }],
        asks: vec![PriceLevel {
            price: Decimal::new(50, 2),
            size: Decimal::new(100, 0),
        }],
        timestamp_ms: now_epoch_ms(),
    }));
    let _leg2 = engine.evaluate_leg2();

    // Force-fill Leg 2 via advance_simulation (set ask <= posted price).
    engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
        asset_id: "no".to_string(),
        bids: vec![PriceLevel {
            price: Decimal::new(40, 2),
            size: Decimal::new(100, 0),
        }],
        asks: vec![PriceLevel {
            price: Decimal::new(42, 2),
            size: Decimal::new(100, 0),
        }],
        timestamp_ms: now_epoch_ms(),
    }));
    engine.advance_simulation();

    // Both legs should now be filled (or reset after trade completion).
    // Either way, no emergency should be generated.
    engine.on_event(IngestorEvent::MarketRotation {
        condition_id: "new_cond".to_string(),
        yes_token_id: "new_yes".to_string(),
        no_token_id: "new_no".to_string(),
        end_timestamp_ms: now_epoch_ms() + 900_000,
        tick_size: Decimal::new(1, 2),
    });

    let emergencies = engine.take_rotation_emergencies();
    assert!(
        emergencies.is_empty(),
        "no emergency when both legs are filled/completed"
    );
}

#[test]
fn test_rotation_posted_leg1_no_emergency() {
    let mut engine = make_engine_with_market(600);
    set_book(&mut engine, "0.495", "0.505");
    inject_buildup(&mut engine, Direction::Up);

    let _signal = engine.evaluate().expect("should generate signal");
    assert!(matches!(engine.state.leg1_state, OrderState::Posted { .. }));

    // Leg 1 is Posted but not Filled — no position to protect.
    engine.on_event(IngestorEvent::MarketRotation {
        condition_id: "new_cond".to_string(),
        yes_token_id: "new_yes".to_string(),
        no_token_id: "new_no".to_string(),
        end_timestamp_ms: now_epoch_ms() + 900_000,
        tick_size: Decimal::new(1, 2),
    });

    let emergencies = engine.take_rotation_emergencies();
    assert!(
        emergencies.is_empty(),
        "no emergency when Leg 1 is only Posted (not Filled)"
    );
}

// ── Emergency post-only vs FOK taker in advance_simulation ──────────

/// Helper: set up an engine with Leg 1 filled, hedge initialized, Leg 2 posted,
/// and emergency_submitted=true. Returns the engine ready for advance_simulation tests.
fn engine_with_emergency_leg2() -> StrategyEngine {
    let mut engine = engine_with_filled_leg1();

    // Set up the NO book (hedge for Up direction) so evaluate_leg2 can emit.
    engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
        asset_id: "no".to_string(),
        bids: vec![PriceLevel {
            price: Decimal::new(45, 2),
            size: Decimal::new(200, 0),
        }],
        asks: vec![PriceLevel {
            price: Decimal::new(50, 2),
            size: Decimal::new(200, 0),
        }],
        timestamp_ms: now_epoch_ms(),
    }));

    // Generate a Leg 2 hedge signal to set leg2_state = Posted.
    let _leg2 = engine.evaluate_leg2();
    assert!(
        matches!(engine.state.leg2_state, OrderState::Posted { .. }),
        "Leg 2 should be Posted after evaluate_leg2"
    );

    // Simulate emergency: set emergency_submitted = true on the hedge state.
    if let Some(e) = engine.hedge.as_mut() {
        e.emergency_submitted = true;
        e.exit_reason = Some(ExitReason::BreakEvenBreach);
    }

    engine
}

#[test]
fn test_sim_emergency_maker_when_ask_drops_to_posted() {
    let mut engine = engine_with_emergency_leg2();

    // Get the posted Leg 2 price.
    let posted_price = match &engine.state.leg2_state {
        OrderState::Posted { price, .. } => *price,
        _ => panic!("expected Posted"),
    };

    // Set NO book ask AT posted_price → market moved to our bid → maker fill.
    engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
        asset_id: "no".to_string(),
        bids: vec![PriceLevel {
            price: Decimal::new(40, 2),
            size: Decimal::new(200, 0),
        }],
        asks: vec![PriceLevel {
            price: posted_price,
            size: Decimal::new(200, 0),
        }],
        timestamp_ms: now_epoch_ms(),
    }));

    let signals = engine.advance_simulation();
    let leg2_fills: Vec<_> = signals
        .iter()
        .filter(|s| s.is_leg2)
        .collect();
    assert_eq!(
        leg2_fills.len(),
        1,
        "should produce exactly 1 confirmed Leg 2 fill"
    );
    assert_eq!(
        leg2_fills[0].price, posted_price,
        "maker fill should be at posted_price"
    );
}

#[test]
fn test_sim_fok_emitted_fills_immediately_at_ask() {
    let mut engine = engine_with_emergency_leg2();

    // Set fok_emitted = true → FOK orders fill immediately at ask.
    if let Some(e) = engine.hedge.as_mut() {
        e.fok_emitted = true;
    }

    // Get the posted Leg 2 price.
    let posted_price = match &engine.state.leg2_state {
        OrderState::Posted { price, .. } => *price,
        _ => panic!("expected Posted"),
    };
    let tick = engine.state.tick_size;

    // Set NO book ask ABOVE posted_price → FOK fills at ask regardless.
    let ask_above = posted_price + tick * Decimal::TWO;
    engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
        asset_id: "no".to_string(),
        bids: vec![PriceLevel {
            price: Decimal::new(40, 2),
            size: Decimal::new(200, 0),
        }],
        asks: vec![PriceLevel {
            price: ask_above,
            size: Decimal::new(200, 0),
        }],
        timestamp_ms: now_epoch_ms(),
    }));

    let signals = engine.advance_simulation();
    let leg2_fills: Vec<_> = signals
        .iter()
        .filter(|s| s.is_leg2)
        .collect();
    assert_eq!(
        leg2_fills.len(),
        1,
        "FOK should produce taker fill at ask"
    );
    assert_eq!(
        leg2_fills[0].price, ask_above,
        "taker fill should be at the ask price"
    );
}

// ── Phase 1 → Phase 2 transition via timeout ─────────────────────

#[test]
fn test_phase1_timeout_triggers_phase_transition() {
    // Test: after Phase 1 timeout, evaluate_leg2 should emit a
    // Phase2Alongside signal and advance the hedge to Phase 2.
    let mut engine = make_engine_with_market(600);
    set_book(&mut engine, "0.495", "0.505"); // YES: bid=0.495, ask=0.505
    inject_buildup(&mut engine, Direction::Up);

    // Leg 1: evaluate → posted at 0.50 (bid+tick).
    let _s = engine.evaluate().expect("Leg 1 signal");
    assert!(matches!(engine.state.leg1_state, OrderState::Posted { .. }));

    // Sim fill Leg 1.
    engine.advance_simulation();
    assert!(matches!(engine.state.leg1_state, OrderState::Filled { .. }));
    assert!(engine.hedge.is_some());

    // Set up the NO book: ask=0.49 (pair_cost = 0.50 + 0.49 = 0.99 < 1.0).
    engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
        asset_id: "no".to_string(),
        bids: vec![PriceLevel {
            price: Decimal::new(40, 2),
            size: Decimal::new(200, 0),
        }],
        asks: vec![PriceLevel {
            price: Decimal::new(49, 2),
            size: Decimal::new(200, 0),
        }],
        timestamp_ms: now_epoch_ms(),
    }));

    // Phase 1: initial post at profit target.
    let initial = engine.evaluate_leg2();
    assert!(
        initial.is_some(),
        "Phase 1 should produce initial Leg 2 signal"
    );
    assert!(matches!(engine.state.leg2_state, OrderState::Posted { .. }));
    assert_eq!(engine.hedge.as_ref().unwrap().phase, HedgePhase::Phase1);

    // Backdate the Leg 2 posted timestamp to trigger Phase 1 timeout.
    let timeout = engine.leg2.phase1_timeout_ms;
    if let OrderState::Posted { ref mut timestamp_ms, .. } = engine.state.leg2_state {
        *timestamp_ms = now_epoch_ms() - timeout - 1;
    }

    // evaluate_leg2 should trigger phase transition.
    let transition = engine.evaluate_leg2();
    assert!(
        transition.is_some(),
        "Phase 1 timeout should trigger phase transition signal"
    );
    assert_eq!(
        engine.hedge.as_ref().unwrap().phase,
        HedgePhase::Phase2,
        "hedge should be in Phase 2 after timeout"
    );
}

// ── Drain mode ────────────────────────────────────────────────────

#[test]
fn test_draining_blocks_new_leg1() {
    let mut engine = make_engine_with_market(600);
    set_book(&mut engine, "0.495", "0.505");
    inject_buildup(&mut engine, Direction::Up);

    // Without draining, evaluate would produce a signal.
    engine.set_draining();
    let signal = engine.evaluate();
    assert!(signal.is_none(), "draining should block new Leg 1 entries");
    assert!(
        !engine.state.buildup_detected,
        "buildup_detected should be cleared"
    );
}

#[test]
fn test_has_no_open_position_when_idle() {
    let engine = StrategyEngine::default();
    assert!(engine.has_no_open_position());
}

#[test]
fn test_has_open_position_when_leg1_posted() {
    let mut engine = StrategyEngine::default();
    engine.state.leg1_state = OrderState::Posted {
        order_id: "test".into(),
        price: Decimal::new(50, 2),
        size: Decimal::new(10, 0),
        timestamp_ms: 1000,
    };
    assert!(!engine.has_no_open_position());
}

#[test]
fn test_build_status_snapshot() {
    let engine = StrategyEngine::default();
    let status = engine.build_status("simulation");
    assert_eq!(status.mode, "simulation");
    assert_eq!(status.leg1_state, "None");
    assert_eq!(status.leg2_state, "None");
    assert!(!status.draining);
}

#[test]
fn test_on_event_ignores_control_variants() {
    let mut engine = StrategyEngine::default();
    // Should not panic or modify state.
    engine.on_event(IngestorEvent::Shutdown);
    engine.on_event(IngestorEvent::DrainAndRestart);
    assert!(engine.has_no_open_position());
}

// ── Opposite-direction cancel tests ──────────────────────────────────

/// Helper: set up engine with Leg 1 Posted in a given direction.
fn setup_posted_leg1(direction: Direction) -> StrategyEngine {
    let mut engine = make_engine_with_market(600);
    engine.in_quiet_period = false; // clear post-rotation quiet period for test
    engine.state.leg1_state = OrderState::Posted {
        order_id: "clob-leg1-001".to_string(),
        price: Decimal::new(50, 2),
        size: Decimal::new(100, 0),
        timestamp_ms: now_epoch_ms(),
    };
    engine.state.leg1_posted_ask = Some(Decimal::new(50, 2));
    engine.leg1_direction = Some(direction);
    set_book(&mut engine, "0.49", "0.52");
    engine
}

#[test]
fn test_opposite_direction_buildup_cancels_leg1() {
    let mut engine = setup_posted_leg1(Direction::Up);

    // Fire a buildup in the opposite direction (Down).
    let buildup = BuildupInfo {
        composite_score: Decimal::new(6, 1),
        direction: Direction::Down,
        cvd_accel: Decimal::ZERO,
        spot_flow: Decimal::ZERO,
        obi_velocity: Decimal::ZERO,
        basis_delta: Decimal::ZERO,
        liq_pressure: Decimal::ZERO,
        atr_displacement: Decimal::ZERO,
        signal_atr_ratio: Decimal::ZERO,
        obi: Decimal::ZERO,
        timestamp_ms: now_epoch_ms(),
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
    };
    let now = now_epoch_ms();
    engine.handle_buildup_confirmed(buildup, now);

    assert!(engine.pending_leg1_cancel.is_some(), "opposite-direction buildup should queue cancel");
    assert!(engine.leg1_cancel_inflight, "cancel inflight flag should be set");
    assert_eq!(engine.diag_opposite_dir_cancels, 1, "opposite_dir_cancels counter should increment");
}

#[test]
fn test_same_direction_buildup_ignored_when_posted() {
    let mut engine = setup_posted_leg1(Direction::Up);

    // Fire a buildup in the SAME direction (Up).
    let buildup = BuildupInfo {
        composite_score: Decimal::new(6, 1),
        direction: Direction::Up,
        cvd_accel: Decimal::ZERO,
        spot_flow: Decimal::ZERO,
        obi_velocity: Decimal::ZERO,
        basis_delta: Decimal::ZERO,
        liq_pressure: Decimal::ZERO,
        atr_displacement: Decimal::ZERO,
        signal_atr_ratio: Decimal::ZERO,
        obi: Decimal::ZERO,
        timestamp_ms: now_epoch_ms(),
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
    };
    let now = now_epoch_ms();
    engine.handle_buildup_confirmed(buildup, now);

    assert!(engine.pending_leg1_cancel.is_none(), "same-direction buildup should NOT queue cancel");
    assert_eq!(engine.diag_opposite_dir_cancels, 0, "opposite_dir_cancels should remain 0");
}

// ── Heartbeat system tests ──────────────────────────────────────────────

#[test]
fn test_heartbeat_gates_leg1_entry() {
    let mut engine = make_engine_with_market(600);
    set_book(&mut engine, "0.48", "0.52");
    engine.state.binance_price = Some(Decimal::new(50_000, 0));

    // Mark heartbeat as down.
    engine.connectivity.heartbeat_healthy = false;
    engine.connectivity.consecutive_heartbeat_failures = 5;

    // Inject a valid buildup.
    inject_buildup(&mut engine, Direction::Up);
    assert!(engine.state.buildup_detected);

    // evaluate() should return None and clear buildup.
    let result = engine.evaluate();
    assert!(result.is_none(), "heartbeat down should block Leg 1 entry");
    assert!(!engine.state.buildup_detected, "buildup_detected should be cleared");
    assert_eq!(engine.diag_rej_heartbeat, 1, "should count heartbeat rejection");
}

#[test]
fn test_heartbeat_proactive_reset() {
    let mut engine = make_engine_with_market(600);
    set_book(&mut engine, "0.48", "0.52");

    // Put Leg 1 in Posted state.
    engine.state.leg1_state = OrderState::Posted {
        order_id: "test-order-123".to_string(),
        price: Decimal::new(52, 2),
        size: Decimal::new(10, 0),
        timestamp_ms: now_epoch_ms(),
    };
    engine.leg1_direction = Some(Direction::Up);

    // Fire heartbeat failures up to threshold (default = 5).
    for i in 1..=5 {
        engine.on_event(IngestorEvent::HeartbeatStatus {
            success: false,
            latency_ms: 0,
        });
        if i < 5 {
            // Before threshold: Leg 1 should still be posted.
            assert!(
                matches!(engine.state.leg1_state, OrderState::Posted { .. }),
                "Leg 1 should stay Posted before threshold (failure {i})"
            );
        }
    }

    // After threshold: Leg 1 should be reset to None.
    assert!(
        matches!(engine.state.leg1_state, OrderState::None),
        "Leg 1 should be reset to None after heartbeat dead threshold"
    );
    assert!(engine.leg1_direction.is_none(), "leg1_direction should be cleared");
    assert_eq!(engine.diag_heartbeat_resets, 1, "should count one heartbeat reset");

    // Should have queued a cancel command.
    let cancel = engine.take_heartbeat_cancel();
    assert!(cancel.is_some(), "should queue CancelLeg1Order");
    match cancel.unwrap() {
        ExecutorCommand::CancelLeg1Order { order_id } => {
            assert_eq!(order_id, "test-order-123");
        }
        _ => panic!("expected CancelLeg1Order"),
    }
}

#[test]
fn test_heartbeat_recovery_logging() {
    let mut engine = make_engine_with_market(600);

    // Simulate failures (below threshold, so no reset).
    for _ in 0..3 {
        engine.on_event(IngestorEvent::HeartbeatStatus {
            success: false,
            latency_ms: 0,
        });
    }
    assert!(!engine.connectivity.heartbeat_healthy);
    assert_eq!(engine.connectivity.consecutive_heartbeat_failures, 3);

    // Recovery.
    engine.on_event(IngestorEvent::HeartbeatStatus {
        success: true,
        latency_ms: 5,
    });
    assert!(engine.connectivity.heartbeat_healthy);
    assert_eq!(engine.connectivity.consecutive_heartbeat_failures, 0);
    assert_eq!(engine.connectivity.last_heartbeat_latency_ms, 5);
}
