use super::*;

fn default_detector() -> BuildupDetector {
    BuildupDetector::new(&BuildupConfig::default())
}

#[test]
fn test_empty_returns_zero() {
    let mut det = default_detector();
    let (score, dir, _) = det.evaluate(0);
    assert_eq!(score, 0.0);
    assert!(dir.is_none());
}

#[test]
fn test_direction_veto_spot_only() {
    // Only 2 confirming metrics (OBI + spot_flow) are bullish — majority=2 < 3 → direction veto.
    // (Liq excluded from direction voting as a reactive signal.)
    let mut det = default_detector();
    for i in 0..50u64 {
        det.obi_velocity.update(0.01 * i as f64, i * 50);
        det.spot_flow.update(1.0, false, i * 50);
    }
    let (score, _, _) = det.evaluate(2500);
    // Should be 0 due to direction veto (majority < 3).
    assert_eq!(score, 0.0);
    assert!(det.diag_direction_vetoes > 0);
}

#[test]
fn test_causal_veto_futures_only() {
    // Only futures metrics → no confirming metric → veto.
    let mut det = default_detector();
    for i in 0..50u64 {
        det.cvd.update(1.0, false, i * 10);
        det.basis_delta.update_spot_mid(50000.0, i * 10);
        det.basis_delta.update_futures_mid(50000.5, 50001.5, i * 10);
    }
    let (score, _, _) = det.evaluate(500);
    assert_eq!(score, 0.0);
}

#[test]
fn test_direction_consensus_veto() {
    let mut det = default_detector();
    // Push 2 metrics bullish, 2 metrics bearish → tie (majority=2 < 3) → veto.
    for i in 0..50u64 {
        let t = i * 10;
        det.cvd.update((i + 1) as f64, false, t);       // bullish (increasing buy volume)
        det.obi_velocity.update(0.01 * i as f64, t); // bullish (rising OBI)
        // Spot flow bearish (sell-side).
        det.spot_flow.update(1.0, true, t);
        // Basis bearish (futures mid dropping) — stagger by 5ms so dt > 0.
        det.basis_delta.update_spot_mid(50000.0, t);
        det.basis_delta.update_futures_mid(49999.0 - i as f64, 49999.5 - i as f64, t + 5);
    }
    let (score, dir, _) = det.evaluate(500);
    // 2 up (CVD, OBI), 2 down (spot_flow, basis) → majority=2 < 3 → vetoed.
    assert_eq!(score, 0.0);
    assert!(dir.is_none());
    assert!(det.diag_direction_vetoes > 0);
}

#[test]
fn test_full_agreement_produces_score() {
    let mut det = default_detector();
    // All metrics bullish + both leading and confirming present.
    // CVD needs increasing quantity to produce acceleration with time-based EMA.
    for i in 0..50u64 {
        let t = i * 10;
        // Leading: CVD bullish (increasing buy volume → acceleration).
        det.cvd.update((i + 1) as f64 * 2.0, false, t);
        // Leading: basis bullish.
        det.basis_delta.update_spot_mid(50000.0, t);
        det.basis_delta.update_futures_mid(50001.0 + i as f64 * 0.1, 50002.0 + i as f64 * 0.1, t);
        // Confirming: spot flow bullish.
        det.spot_flow.update(2.0, false, t);
        // Confirming: OBI velocity bullish.
        det.obi_velocity.update(0.01 * i as f64, t);
    }
    let (score, dir, _) = det.evaluate(500);
    assert!(score > 0.0, "score should be positive: {score}");
    assert_eq!(dir, Some(Direction::Up));
}

#[test]
fn test_check_entry_below_threshold() {
    let mut det = default_detector();
    // Very weak signals → below entry threshold.
    for i in 0..15u64 {
        let t = i * 10;
        det.cvd.update(0.01, false, t);
        det.spot_flow.update(0.01, false, t);
        det.basis_delta.update_spot_mid(50000.0, t);
        det.basis_delta.update_futures_mid(50000.001, 50000.002, t);
        det.obi_velocity.update(0.0001 * i as f64, t);
    }
    assert!(det.check_entry(150).is_none());
}

#[test]
fn test_below_cancel_threshold_when_stale() {
    let mut det = default_detector();
    // No data → all metrics stale → score=0 < cancel_threshold.
    assert!(det.below_cancel_threshold(0));
}

#[test]
fn test_max_dissenters_zero() {
    // With max_dissenters=0, a 3-1 vote (1 dissenter) should be vetoed.
    let mut cfg = BuildupConfig::default();
    cfg.max_dissenters = 0;
    let mut det = BuildupDetector::new(&cfg);

    // Push 3 metrics bullish, 1 bearish → minority=1 > max_dissenters=0 → veto.
    for i in 0..50u64 {
        let t = i * 10;
        // Bullish: CVD, OBI, basis (3 metrics).
        det.cvd.update((i + 1) as f64 * 2.0, false, t);
        det.obi_velocity.update(0.01 * i as f64, t);
        det.basis_delta.update_spot_mid(50000.0, t);
        det.basis_delta.update_futures_mid(50001.0 + i as f64 * 0.1, 50002.0 + i as f64 * 0.1, t);
        // Bearish: spot_flow (sell-side).
        det.spot_flow.update(2.0, true, t);
    }
    let (score, dir, _) = det.evaluate(500);
    // 3 up, 1 down → minority=1 > max_dissenters=0 → vetoed.
    assert_eq!(score, 0.0, "should be vetoed with max_dissenters=0 and 1 dissenter");
    assert!(dir.is_none());
    assert!(det.diag_direction_vetoes > 0);
}
