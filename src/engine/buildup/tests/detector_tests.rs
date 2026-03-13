use super::*;

fn default_detector() -> BuildupDetector {
    BuildupDetector::new(&BuildupConfig::default())
}

#[test]
fn test_empty_returns_zero() {
    let mut det = default_detector();
    let (score, dir) = det.evaluate(0);
    assert_eq!(score, 0.0);
    assert!(dir.is_none());
}

#[test]
fn test_causal_veto_spot_only() {
    // 3 bullish non-leading metrics (OBI + spot_flow + liq): direction check passes
    // (majority=3, minority=0), but causal check fails (no CVD or basis_delta) → causal veto.
    let mut det = default_detector();
    for i in 0..50u64 {
        det.obi_velocity.update(0.01 * i as f64, i * 50);
        det.spot_flow.update(1.0, false, i * 50);
        det.liq_pressure.update("BUY", 1.0, i * 50); // short liquidations = bullish
    }
    let (score, _) = det.evaluate(2500);
    // Should be 0 due to causal veto (no leading).
    assert_eq!(score, 0.0);
    assert_eq!(det.diag_causal_vetoes, 1);
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
    let (score, _) = det.evaluate(500);
    assert_eq!(score, 0.0);
}

#[test]
fn test_direction_consensus_veto() {
    let mut det = default_detector();
    // Push 3 metrics bullish, 2 metrics bearish → minority=2 > 1 → veto.
    // CVD needs increasing quantity to produce acceleration with time-based EMA.
    // Basis needs staggered spot/futures updates (different timestamps) so
    // the delta EMA actually accumulates (dt=0 → alpha=0 would zero it out).
    for i in 0..50u64 {
        let t = i * 10;
        det.cvd.update((i + 1) as f64, false, t);       // bullish (increasing buy volume)
        det.spot_flow.update(1.0, false, t);  // bullish
        det.obi_velocity.update(0.01 * i as f64, t); // bullish (rising OBI)
        // Basis bearish (futures mid dropping) — stagger by 5ms so dt > 0.
        det.basis_delta.update_spot_mid(50000.0, t);
        det.basis_delta.update_futures_mid(49999.0 - i as f64, 49999.5 - i as f64, t + 5);
        // Liq bearish (long liquidations).
        det.liq_pressure.update("SELL", 1.0, t);
    }
    let (score, dir) = det.evaluate(500);
    // Majority is Up (3 up), minority is Down (2) > 1 → should be vetoed.
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
    let (score, dir) = det.evaluate(500);
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
