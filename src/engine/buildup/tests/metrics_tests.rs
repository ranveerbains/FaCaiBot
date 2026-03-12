use super::*;

// ── time_alpha helper ────────────────────────────────────────────────

#[test]
fn test_time_alpha_at_halflife() {
    let alpha = time_alpha(200.0, 200.0);
    assert!((alpha - 0.5).abs() < 1e-10);
}

#[test]
fn test_time_alpha_at_zero() {
    let alpha = time_alpha(0.0, 200.0);
    assert!((alpha - 0.0).abs() < 1e-10);
}

#[test]
fn test_time_alpha_large_dt() {
    // 10x halflife → alpha ≈ 0.999
    let alpha = time_alpha(2000.0, 200.0);
    assert!(alpha > 0.99);
}

// ── Normalize helper ─────────────────────────────────────────────────

#[test]
fn test_normalize_below_min() {
    assert_eq!(normalize(0.05, 0.1, 1.0), 0.0);
}

#[test]
fn test_normalize_at_saturation() {
    assert_eq!(normalize(1.0, 0.0, 1.0), 1.0);
}

#[test]
fn test_normalize_above_saturation() {
    assert_eq!(normalize(2.0, 0.0, 1.0), 1.0);
}

#[test]
fn test_normalize_midpoint() {
    let v = normalize(0.5, 0.0, 1.0);
    assert!((v - 0.5).abs() < 1e-10);
}

#[test]
fn test_normalize_negative() {
    // Uses abs, so negative values work symmetrically.
    assert_eq!(normalize(-1.0, 0.0, 1.0), 1.0);
}

// ── CVD Acceleration ─────────────────────────────────────────────────

#[test]
fn test_cvd_warmup_returns_zero() {
    let tracker = CvdAccelTracker::new(150.0, 700.0, 300, 0.0, 1.0);
    assert_eq!(tracker.normalized(100), 0.0);
    assert!(tracker.direction().is_none());
}

#[test]
fn test_cvd_bullish_acceleration() {
    let mut tracker = CvdAccelTracker::new(150.0, 700.0, 300, 0.0, 1.0);
    for i in 0..50 {
        // Increasing buy volume → fast EMA rises faster than slow → positive accel.
        tracker.update((i + 1) as f64, false, i * 10);
    }
    assert!(tracker.raw() > 0.0);
    assert_eq!(tracker.direction(), Some(Direction::Up));
    assert!(tracker.normalized(500) > 0.0);
}

#[test]
fn test_cvd_bearish_acceleration() {
    let mut tracker = CvdAccelTracker::new(150.0, 700.0, 300, 0.0, 1.0);
    for i in 0..50 {
        // Increasing sell volume → fast EMA drops faster than slow → negative accel.
        tracker.update((i + 1) as f64, true, i * 10);
    }
    assert!(tracker.raw() < 0.0);
    assert_eq!(tracker.direction(), Some(Direction::Down));
}

#[test]
fn test_cvd_staleness() {
    let mut tracker = CvdAccelTracker::new(150.0, 700.0, 300, 0.0, 1.0);
    for i in 0..50 {
        tracker.update(1.0, false, i * 10);
    }
    // 500ms is within freshness window (300ms), but last update at 490.
    assert!(tracker.is_fresh(500));
    // Way past freshness.
    assert!(!tracker.is_fresh(1000));
    assert_eq!(tracker.normalized(1000), 0.0);
}

// ── Spot Flow ────────────────────────────────────────────────────────

#[test]
fn test_spot_flow_buy_dominant() {
    let mut tracker = SpotFlowTracker::new(300.0, 200, 0.0, 1.0);
    for i in 0..50 {
        tracker.update(1.0, false, i * 10); // buyer aggressor
    }
    assert!(tracker.raw() > 0.0);
    assert_eq!(tracker.direction(), Some(Direction::Up));
}

#[test]
fn test_spot_flow_sell_dominant() {
    let mut tracker = SpotFlowTracker::new(300.0, 200, 0.0, 1.0);
    for i in 0..50 {
        tracker.update(1.0, true, i * 10); // seller aggressor
    }
    assert!(tracker.raw() < 0.0);
    assert_eq!(tracker.direction(), Some(Direction::Down));
}

// ── OBI Velocity ─────────────────────────────────────────────────────

#[test]
fn test_obi_velocity_rising() {
    let mut tracker = ObiVelocityTracker::new(300.0, 100, 0.0, 0.5);
    // OBI rising: 0.0 → 0.1 → 0.2 → 0.3
    for i in 0..4 {
        tracker.update(i as f64 * 0.1, i as u64 * 50);
    }
    assert!(tracker.raw() > 0.0);
    assert_eq!(tracker.direction(), Some(Direction::Up));
}

#[test]
fn test_obi_velocity_falling() {
    let mut tracker = ObiVelocityTracker::new(300.0, 100, 0.0, 0.5);
    // OBI falling: 0.3 → 0.2 → 0.1 → 0.0
    for i in 0..4 {
        tracker.update(0.3 - i as f64 * 0.1, i as u64 * 50);
    }
    assert!(tracker.raw() < 0.0);
    assert_eq!(tracker.direction(), Some(Direction::Down));
}

// ── Basis Delta ──────────────────────────────────────────────────────

#[test]
fn test_basis_delta_futures_premium_rising() {
    let mut tracker = BasisDeltaTracker::new(300.0, 300, 0.0, 2.0);
    tracker.update_spot_mid(50000.0, 100);
    // Futures premium increasing: 50001, 50002, 50003
    tracker.update_futures_mid(50000.5, 50001.5, 100);
    tracker.update_futures_mid(50001.5, 50002.5, 200);
    tracker.update_futures_mid(50002.5, 50003.5, 300);
    // Basis is increasing → delta should be positive.
    assert!(tracker.raw() > 0.0);
    assert_eq!(tracker.direction(), Some(Direction::Up));
}

// ── Liquidation Pressure ─────────────────────────────────────────────

#[test]
fn test_liq_pressure_short_squeezes() {
    let mut tracker = LiqPressureTracker::new(2000.0, 3000, 0.0, 10.0);
    // Short liquidations (side="BUY") → bullish pressure.
    tracker.update("BUY", 5.0, 1000);
    tracker.update("BUY", 3.0, 1500);
    assert!(tracker.raw() > 0.0);
    assert_eq!(tracker.direction(), Some(Direction::Up));
    assert!(tracker.normalized(1500) > 0.0);
}

#[test]
fn test_liq_pressure_long_liquidations() {
    let mut tracker = LiqPressureTracker::new(2000.0, 3000, 0.0, 10.0);
    // Long liquidations (side="SELL") → bearish pressure.
    tracker.update("SELL", 5.0, 1000);
    assert!(tracker.raw() < 0.0);
    assert_eq!(tracker.direction(), Some(Direction::Down));
}

#[test]
fn test_liq_pressure_decay() {
    let mut tracker = LiqPressureTracker::new(2000.0, 3000, 0.0, 10.0);
    tracker.update("BUY", 10.0, 0);
    let raw_at_0 = tracker.decaying_sum(0);
    let raw_at_2000 = tracker.decaying_sum(2000); // 1 half-life later
    // Should be approximately half.
    assert!((raw_at_2000 - raw_at_0 / 2.0).abs() < 0.01);
}

#[test]
fn test_liq_pressure_pruning() {
    let mut tracker = LiqPressureTracker::new(2000.0, 3000, 0.0, 10.0);
    tracker.update("BUY", 10.0, 0);
    // Update at 11000 (> 5 × half_life = 10000) → old event pruned.
    tracker.update("BUY", 1.0, 11000);
    assert_eq!(tracker.events.len(), 1);
}

// ── ATR Displacement ─────────────────────────────────────────────────

#[test]
fn test_atr_displacement_warmup() {
    let tracker = AtrDisplacementTracker::new(0.002, 100, 0.0, 15.0, 50);
    assert_eq!(tracker.normalized(0), 0.0);
    assert!(tracker.direction().is_none());
}

#[test]
fn test_atr_displacement_spike_up() {
    let mut tracker = AtrDisplacementTracker::new(0.1, 100, 0.0, 5.0, 10);
    // Warm up with stable prices.
    for i in 0..20 {
        tracker.update(50000.0 + (i as f64 * 0.1), i as u64 * 50);
    }
    // Big jump.
    tracker.update(50010.0, 1050);
    assert!(tracker.raw() > 0.0);
    assert_eq!(tracker.direction(), Some(Direction::Up));
}

#[test]
fn test_atr_displacement_spike_down() {
    let mut tracker = AtrDisplacementTracker::new(0.1, 100, 0.0, 5.0, 10);
    for i in 0..20 {
        tracker.update(50000.0 + (i as f64 * 0.1), i as u64 * 50);
    }
    // Big drop.
    tracker.update(49990.0, 1050);
    assert!(tracker.raw() < 0.0);
    assert_eq!(tracker.direction(), Some(Direction::Down));
}
