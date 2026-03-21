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

// ── Realized Vol Tracker ─────────────────────────────────────────────

#[test]
fn test_realized_vol_warmup() {
    let tracker = RealizedVolTracker::new(600, 10, 0.00003, 20.0, 500);
    // Before warmup, returns default vol
    assert!((tracker.realized_vol() - 0.00003).abs() < 1e-10);
}

#[test]
fn test_realized_vol_stable_prices() {
    let mut tracker = RealizedVolTracker::new(600, 5, 0.00003, 20.0, 500);
    // Feed stable prices → near-zero vol
    for i in 0..20 {
        tracker.update(100_000.0, i * 50);
    }
    assert!(tracker.realized_vol() < 0.0001, "stable prices should have near-zero vol, got {}", tracker.realized_vol());
}

#[test]
fn test_realized_vol_with_movement() {
    let mut tracker = RealizedVolTracker::new(600, 5, 0.00003, 20.0, 500);
    // Feed prices with alternating movement
    for i in 0..20 {
        let price = 100_000.0 + if i % 2 == 0 { 50.0 } else { -50.0 };
        tracker.update(price, i * 50);
    }
    let vol = tracker.realized_vol();
    assert!(vol > 0.0001, "moving prices should produce non-trivial vol, got {vol}");
}

#[test]
fn test_realized_vol_time_scaling() {
    let mut tracker = RealizedVolTracker::new(600, 5, 0.00003, 20.0, 500);
    for i in 0..20 {
        let price = 100_000.0 + (i as f64 * 10.0);
        tracker.update(price, i * 50);
    }
    let vol_200s = tracker.scaled_vol(200.0);
    let vol_100s = tracker.scaled_vol(100.0);
    // More time remaining → higher scaled vol
    assert!(vol_200s > vol_100s, "200s vol ({vol_200s}) should exceed 100s vol ({vol_100s})");
    // Scaling should be sqrt(2) ratio
    let ratio = vol_200s / vol_100s;
    assert!((ratio - std::f64::consts::SQRT_2).abs() < 0.01,
        "ratio should be ~sqrt(2), got {ratio}");
}

#[test]
fn test_realized_vol_freshness() {
    let mut tracker = RealizedVolTracker::new(600, 5, 0.00003, 20.0, 500);
    for i in 0..10 {
        tracker.update(100_000.0 + (i as f64), i * 50);
    }
    assert!(tracker.is_fresh(500));
    assert!(!tracker.is_fresh(1500)); // 500ms freshness, last update at 450
}

#[test]
fn test_realized_vol_ring_buffer_eviction() {
    // Small capacity to test eviction
    let mut tracker = RealizedVolTracker::new(5, 3, 0.00003, 20.0, 500);
    // Fill beyond capacity
    for i in 0..20 {
        tracker.update(100_000.0 + (i as f64 * 5.0), i * 50);
    }
    // Vol should be finite and reasonable
    let vol = tracker.realized_vol();
    assert!(vol.is_finite(), "vol should be finite after eviction");
    assert!(vol > 0.0, "vol should be positive with price movement");
}
