use super::*;

fn make_hedge() -> HedgeState {
    HedgeState::new(
        0,
        Decimal::new(50, 2),    // leg1_fill_price = 0.50
        Decimal::ZERO,          // leg1_fee
        ProfitTier::High,
        Decimal::new(25, 3),     // initial_profit_target = 0.025
        Direction::Up,
        SpikeInfo {
            direction: Direction::Up,
            magnitude: Decimal::new(5, 3),
            sustained_ms: 300,
            timestamp_ms: 0,
            atr_ratio: Decimal::ZERO,
            obi: Decimal::ZERO,
        },
        Decimal::new(7, 1),      // expected_pct = 0.7
        Decimal::new(475, 3),    // phase1_target_price = 0.475
        Decimal::ZERO,           // phase_a_pct
        Decimal::ZERO,           // phase_b_pct
        Decimal::ZERO,           // spot_mid_at_entry
        Decimal::ZERO,           // entry_ema_atr
    )
}

#[test]
fn test_break_even() {
    let h = make_hedge();
    assert_eq!(h.break_even(), Decimal::new(50, 2)); // 1.0 - 0.50 = 0.50
}

#[test]
fn test_phase_transition() {
    let mut h = make_hedge();
    h.phase = HedgePhase::Phase2;
    h.phase2_posted_price = Some(Decimal::new(49, 2));
    assert_eq!(h.phase, HedgePhase::Phase2);
    assert_eq!(h.phase2_posted_price, Some(Decimal::new(49, 2)));
}
