//! Quoter — per-side order management for bilateral quoting.
//!
//! Decides when/where to post, cancel, or replace maker orders on each side.

use rust_decimal::Decimal;

use crate::engine::position::{BilateralPosition, MarketSide};

// ─── Types ──────────────────────────────────────────────────────────────────

/// State of a resting order managed by the quoter.
#[derive(Debug, Clone)]
pub struct ManagedOrder {
    pub order_id: String,
    pub price: Decimal,
    pub size: Decimal,
    pub posted_ms: u64,
    pub fair_value_at_post: Decimal,
    /// Cumulative filled size tracked for dedup (WS + CancelResult may both report fills).
    pub size_filled: Decimal,
}

/// Action the engine should take for one side.
#[derive(Debug, Clone)]
pub enum QuoteAction {
    Post {
        side: MarketSide,
        token_id: String,
        price: Decimal,
        size: Decimal,
    },
    Cancel {
        side: MarketSide,
        order_id: String,
    },
}

/// Config for the quoter (from config.toml [quoting]).
#[derive(Debug, Clone)]
pub struct QuotingConfig {
    #[allow(dead_code)]
    pub min_edge: f64,
    pub requote_threshold: f64,
    pub min_requote_interval_ms: u64,
    pub max_order_size: Decimal,
    pub min_order_size: Decimal,
    pub emergency_requote_threshold: f64,
    pub max_imbalance_skew: f64,
}

impl Default for QuotingConfig {
    fn default() -> Self {
        Self {
            min_edge: 0.04,
            requote_threshold: 0.01,
            min_requote_interval_ms: 2000,
            max_order_size: Decimal::new(100, 0),
            min_order_size: Decimal::new(5, 0),
            emergency_requote_threshold: 0.05,
            max_imbalance_skew: 0.02,
        }
    }
}

/// Risk limits for v2 (from config.toml [risk_v2]).
#[derive(Debug, Clone)]
pub struct RiskV2Config {
    pub max_unpaired_shares: Decimal,
    pub max_unpaired_usdc: Decimal,
    pub max_capital_per_market: Decimal,
    pub closing_phase_secs: u64,
    pub rotation_quiet_ms: u64,
    pub max_closing_pair_cost: f64,
    pub closing_retry_price_increment: f64,
    pub rebalance_threshold: Decimal,
    pub rebalance_size: Decimal,
    pub rebalance_max_pair_cost: f64,
    pub min_rebalance_interval_ms: u64,
    pub stale_book_ms: u64,
    pub max_entry_spread: f64,
    pub heartbeat_dead_threshold: u32,
}

impl Default for RiskV2Config {
    fn default() -> Self {
        Self {
            max_unpaired_shares: Decimal::new(30, 0),
            max_unpaired_usdc: Decimal::new(25, 0),
            max_capital_per_market: Decimal::new(100, 0),
            closing_phase_secs: 45,
            rotation_quiet_ms: 5000,
            max_closing_pair_cost: 0.97,
            closing_retry_price_increment: 0.01,
            rebalance_threshold: Decimal::new(20, 0),
            rebalance_size: Decimal::new(10, 0),
            rebalance_max_pair_cost: 0.96,
            min_rebalance_interval_ms: 10000,
            stale_book_ms: 850,
            max_entry_spread: 0.04,
            heartbeat_dead_threshold: 5,
        }
    }
}

// ─── Cleared order (for CancelResult dedup) ─────────────────────────────────

/// Snapshot of an order that was cleared (full fill or cancel result),
/// retained so a late-arriving `CancelResult` can still dedup correctly.
#[derive(Debug, Clone)]
struct ClearedOrder {
    order_id: String,
    price: Decimal,
    size_filled: Decimal,
}

// ─── Quoter ─────────────────────────────────────────────────────────────────

/// Manages resting orders on both sides (YES and NO).
pub struct Quoter {
    pub yes_order: Option<ManagedOrder>,
    pub no_order: Option<ManagedOrder>,
    yes_last_post_ms: u64,
    no_last_post_ms: u64,
    yes_pending_cancel: bool,
    no_pending_cancel: bool,
    yes_pending_post: bool,
    no_pending_post: bool,
    /// Last cleared order per side — for CancelResult dedup when order is already gone.
    yes_last_cleared: Option<ClearedOrder>,
    no_last_cleared: Option<ClearedOrder>,
}

impl Quoter {
    pub fn new() -> Self {
        Self {
            yes_order: None,
            no_order: None,
            yes_last_post_ms: 0,
            no_last_post_ms: 0,
            yes_pending_cancel: false,
            no_pending_cancel: false,
            yes_pending_post: false,
            no_pending_post: false,
            yes_last_cleared: None,
            no_last_cleared: None,
        }
    }

    /// Evaluate quoting for one side. Returns an action if one is needed.
    pub fn evaluate_side(
        &self,
        side: MarketSide,
        target_price: Decimal,
        fair_value: Decimal,
        token_id: &str,
        position: &BilateralPosition,
        quoting: &QuotingConfig,
        risk: &RiskV2Config,
        now_ms: u64,
    ) -> Option<QuoteAction> {
        // ── Guards ──
        if self.is_pending(side) {
            return None;
        }
        if position.unpaired(side) >= risk.max_unpaired_shares {
            return None;
        }
        if position.unpaired_usdc() >= risk.max_unpaired_usdc {
            // Only block if this side is the one causing the imbalance
            if position.unpaired(side) > Decimal::ZERO {
                return None;
            }
        }
        if position.total_capital_deployed() >= risk.max_capital_per_market {
            return None;
        }

        // ── Size computation ──
        let remaining_capital = risk.max_capital_per_market - position.total_capital_deployed();
        if remaining_capital <= Decimal::ZERO || target_price <= Decimal::ZERO {
            return None;
        }
        let max_by_capital = remaining_capital / target_price;
        let max_by_imbalance = risk.max_unpaired_shares - position.unpaired(side);
        let size = quoting.max_order_size
            .min(max_by_capital)
            .min(max_by_imbalance)
            .max(Decimal::ZERO);

        if size < quoting.min_order_size {
            return None;
        }

        // $1 notional minimum guard
        if target_price * size < Decimal::new(1, 0) {
            return None;
        }

        let resting = self.order(side);

        match resting {
            None => {
                // No resting order — post new one
                Some(QuoteAction::Post {
                    side,
                    token_id: token_id.to_string(),
                    price: target_price,
                    size,
                })
            }
            Some(existing) => {
                // Check if requote needed
                let fv_drift = (fair_value - existing.fair_value_at_post).abs();
                let fv_drift_f64 = fv_drift.to_string().parse::<f64>().unwrap_or(0.0);
                let elapsed = now_ms.saturating_sub(self.last_post_ms(side));

                let is_emergency = fv_drift_f64 >= quoting.emergency_requote_threshold;
                if fv_drift_f64 >= quoting.requote_threshold
                    && (elapsed >= quoting.min_requote_interval_ms || is_emergency)
                {
                    // Cancel existing, then repost (cancel first, post on confirmation)
                    Some(QuoteAction::Cancel {
                        side,
                        order_id: existing.order_id.clone(),
                    })
                } else {
                    None // hold
                }
            }
        }
    }

    // ── State updates ──

    pub fn on_order_posted(&mut self, side: MarketSide, order: ManagedOrder) {
        match side {
            MarketSide::Yes => {
                self.yes_last_post_ms = order.posted_ms;
                self.yes_order = Some(order);
                self.yes_pending_post = false;
            }
            MarketSide::No => {
                self.no_last_post_ms = order.posted_ms;
                self.no_order = Some(order);
                self.no_pending_post = false;
            }
        }
    }

    pub fn on_order_failed(&mut self, side: MarketSide) {
        match side {
            MarketSide::Yes => {
                self.yes_pending_post = false;
            }
            MarketSide::No => {
                self.no_pending_post = false;
            }
        }
    }

    pub fn on_cancel_sent(&mut self, side: MarketSide) {
        match side {
            MarketSide::Yes => self.yes_pending_cancel = true,
            MarketSide::No => self.no_pending_cancel = true,
        }
    }

    pub fn on_cancel_result(&mut self, side: MarketSide) {
        match side {
            MarketSide::Yes => {
                if let Some(ref o) = self.yes_order {
                    self.yes_last_cleared = Some(ClearedOrder {
                        order_id: o.order_id.clone(),
                        price: o.price,
                        size_filled: o.size_filled,
                    });
                }
                self.yes_order = None;
                self.yes_pending_cancel = false;
            }
            MarketSide::No => {
                if let Some(ref o) = self.no_order {
                    self.no_last_cleared = Some(ClearedOrder {
                        order_id: o.order_id.clone(),
                        price: o.price,
                        size_filled: o.size_filled,
                    });
                }
                self.no_order = None;
                self.no_pending_cancel = false;
            }
        }
    }

    pub fn on_fill(&mut self, side: MarketSide, was_full: bool) {
        if was_full {
            match side {
                MarketSide::Yes => {
                    if let Some(ref o) = self.yes_order {
                        self.yes_last_cleared = Some(ClearedOrder {
                            order_id: o.order_id.clone(),
                            price: o.price,
                            size_filled: o.size_filled,
                        });
                    }
                    self.yes_order = None;
                }
                MarketSide::No => {
                    if let Some(ref o) = self.no_order {
                        self.no_last_cleared = Some(ClearedOrder {
                            order_id: o.order_id.clone(),
                            price: o.price,
                            size_filled: o.size_filled,
                        });
                    }
                    self.no_order = None;
                }
            }
        }
    }

    /// Record a WS fill (cumulative matched size). Returns the delta to record in position.
    /// Both User WS and CancelResult may report fills — this deduplicates by tracking
    /// cumulative filled size per order.
    pub fn record_ws_fill(&mut self, side: MarketSide, cumulative_matched: Decimal) -> Decimal {
        let order = match side {
            MarketSide::Yes => self.yes_order.as_mut(),
            MarketSide::No => self.no_order.as_mut(),
        };
        match order {
            Some(o) => {
                let delta = (cumulative_matched - o.size_filled).max(Decimal::ZERO);
                o.size_filled = o.size_filled.max(cumulative_matched);
                delta
            }
            None => Decimal::ZERO,
        }
    }

    /// Return how much has already been recorded as filled for the current order on this side.
    pub fn filled_so_far(&self, side: MarketSide) -> Decimal {
        self.order(side).map(|o| o.size_filled).unwrap_or(Decimal::ZERO)
    }

    pub fn mark_post_pending(&mut self, side: MarketSide) {
        match side {
            MarketSide::Yes => self.yes_pending_post = true,
            MarketSide::No => self.no_pending_post = true,
        }
    }

    pub fn cancel_all_actions(&self) -> Vec<QuoteAction> {
        let mut actions = Vec::new();
        if let Some(ref o) = self.yes_order {
            if !self.yes_pending_cancel {
                actions.push(QuoteAction::Cancel {
                    side: MarketSide::Yes,
                    order_id: o.order_id.clone(),
                });
            }
        }
        if let Some(ref o) = self.no_order {
            if !self.no_pending_cancel {
                actions.push(QuoteAction::Cancel {
                    side: MarketSide::No,
                    order_id: o.order_id.clone(),
                });
            }
        }
        actions
    }

    /// Look up an order by side + order_id for CancelResult dedup.
    /// Checks the current resting order first, then the last cleared order.
    /// Returns `(price, size_filled)` if found.
    pub fn lookup_order_for_cancel(&self, side: MarketSide, order_id: &str) -> Option<(Decimal, Decimal)> {
        if let Some(o) = self.order(side)
            && o.order_id == order_id
        {
            return Some((o.price, o.size_filled));
        }
        let cleared = match side {
            MarketSide::Yes => &self.yes_last_cleared,
            MarketSide::No => &self.no_last_cleared,
        };
        if let Some(c) = cleared
            && c.order_id == order_id
        {
            return Some((c.price, c.size_filled));
        }
        None
    }

    pub fn reset(&mut self) {
        self.yes_order = None;
        self.no_order = None;
        self.yes_last_post_ms = 0;
        self.no_last_post_ms = 0;
        self.yes_pending_cancel = false;
        self.no_pending_cancel = false;
        self.yes_pending_post = false;
        self.no_pending_post = false;
        self.yes_last_cleared = None;
        self.no_last_cleared = None;
    }

    // ── Accessors ──

    pub fn order(&self, side: MarketSide) -> Option<&ManagedOrder> {
        match side {
            MarketSide::Yes => self.yes_order.as_ref(),
            MarketSide::No => self.no_order.as_ref(),
        }
    }

    pub fn has_resting_orders(&self) -> bool {
        self.yes_order.is_some() || self.no_order.is_some()
    }

    pub fn is_pending(&self, side: MarketSide) -> bool {
        match side {
            MarketSide::Yes => self.yes_pending_cancel || self.yes_pending_post,
            MarketSide::No => self.no_pending_cancel || self.no_pending_post,
        }
    }

    fn last_post_ms(&self, side: MarketSide) -> u64 {
        match side {
            MarketSide::Yes => self.yes_last_post_ms,
            MarketSide::No => self.no_last_post_ms,
        }
    }
}

impl Default for Quoter {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn default_configs() -> (QuotingConfig, RiskV2Config) {
        (QuotingConfig::default(), RiskV2Config::default())
    }

    #[test]
    fn test_post_when_no_resting() {
        let quoter = Quoter::new();
        let position = BilateralPosition::new();
        let (qc, rc) = default_configs();

        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.42"), dec("0.45"), "yes_token",
            &position, &qc, &rc, 1000,
        );
        assert!(matches!(action, Some(QuoteAction::Post { .. })));
    }

    #[test]
    fn test_no_post_when_pending() {
        let mut quoter = Quoter::new();
        quoter.mark_post_pending(MarketSide::Yes);
        let position = BilateralPosition::new();
        let (qc, rc) = default_configs();

        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.42"), dec("0.45"), "yes_token",
            &position, &qc, &rc, 1000,
        );
        assert!(action.is_none());
    }

    #[test]
    fn test_no_post_when_capital_exhausted() {
        let quoter = Quoter::new();
        let mut position = BilateralPosition::new();
        // Deploy max capital
        position.record_fill(MarketSide::Yes, dec("0.50"), dec("200"), false, dec("0"));
        let (qc, rc) = default_configs();

        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.42"), dec("0.45"), "yes_token",
            &position, &qc, &rc, 1000,
        );
        assert!(action.is_none());
    }

    #[test]
    fn test_no_post_when_unpaired_limit() {
        let quoter = Quoter::new();
        let mut position = BilateralPosition::new();
        // 50 YES, 0 NO → 50 unpaired YES (at limit)
        position.record_fill(MarketSide::Yes, dec("0.40"), dec("50"), false, dec("0"));
        let (qc, rc) = default_configs();

        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.42"), dec("0.45"), "yes_token",
            &position, &qc, &rc, 1000,
        );
        assert!(action.is_none());
    }

    #[test]
    fn test_requote_on_fv_drift() {
        let mut quoter = Quoter::new();
        quoter.on_order_posted(MarketSide::Yes, ManagedOrder {
            order_id: "order1".into(),
            price: dec("0.42"),
            size: dec("50"),
            posted_ms: 1000,
            fair_value_at_post: dec("0.45"),
            size_filled: Decimal::ZERO,
        });

        let position = BilateralPosition::new();
        let (qc, rc) = default_configs();

        // Fair value shifted by 0.02 (> threshold 0.01), 3s elapsed (> 2s min)
        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.44"), dec("0.47"), "yes_token",
            &position, &qc, &rc, 7000,
        );
        assert!(matches!(action, Some(QuoteAction::Cancel { .. })));
    }

    #[test]
    fn test_hold_when_fv_stable() {
        let mut quoter = Quoter::new();
        quoter.on_order_posted(MarketSide::Yes, ManagedOrder {
            order_id: "order1".into(),
            price: dec("0.42"),
            size: dec("50"),
            posted_ms: 1000,
            fair_value_at_post: dec("0.45"),
            size_filled: Decimal::ZERO,
        });

        let position = BilateralPosition::new();
        let (qc, rc) = default_configs();

        // Fair value barely moved (0.005 < threshold 0.01)
        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.425"), dec("0.455"), "yes_token",
            &position, &qc, &rc, 7000,
        );
        assert!(action.is_none());
    }

    #[test]
    fn test_cancel_all() {
        let mut quoter = Quoter::new();
        quoter.on_order_posted(MarketSide::Yes, ManagedOrder {
            order_id: "y1".into(),
            price: dec("0.42"),
            size: dec("10"),
            posted_ms: 1000,
            fair_value_at_post: dec("0.45"),
            size_filled: Decimal::ZERO,
        });
        quoter.on_order_posted(MarketSide::No, ManagedOrder {
            order_id: "n1".into(),
            price: dec("0.48"),
            size: dec("10"),
            posted_ms: 1000,
            fair_value_at_post: dec("0.55"),
            size_filled: Decimal::ZERO,
        });

        let actions = quoter.cancel_all_actions();
        assert_eq!(actions.len(), 2);
    }

    #[test]
    fn test_size_capped_by_remaining_capital() {
        let quoter = Quoter::new();
        let mut position = BilateralPosition::new();
        // Deploy $90 of $100 max
        position.record_fill(MarketSide::Yes, dec("0.45"), dec("100"), false, dec("0"));
        position.record_fill(MarketSide::No, dec("0.45"), dec("100"), false, dec("0"));
        let (qc, rc) = default_configs();

        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.45"), dec("0.48"), "yes_token",
            &position, &qc, &rc, 1000,
        );
        // Remaining capital = 100 - 90 = 10. At 0.45, that's ~22 shares.
        // But max_order_size is 100 and min is 5, so should post with capped size.
        assert!(matches!(action, Some(QuoteAction::Post { size, .. }) if size >= dec("5")));
    }

    #[test]
    fn test_reset_clears_all() {
        let mut quoter = Quoter::new();
        quoter.mark_post_pending(MarketSide::Yes);
        quoter.on_cancel_sent(MarketSide::No);
        quoter.reset();
        assert!(!quoter.is_pending(MarketSide::Yes));
        assert!(!quoter.is_pending(MarketSide::No));
        assert!(!quoter.has_resting_orders());
    }

    #[test]
    fn test_record_ws_fill_delta() {
        let mut quoter = Quoter::new();
        quoter.on_order_posted(MarketSide::Yes, ManagedOrder {
            order_id: "o1".into(),
            price: dec("0.42"),
            size: dec("100"),
            posted_ms: 1000,
            fair_value_at_post: dec("0.45"),
            size_filled: Decimal::ZERO,
        });

        // First WS fill: cumulative 30 → delta 30
        let d1 = quoter.record_ws_fill(MarketSide::Yes, dec("30"));
        assert_eq!(d1, dec("30"));

        // Second WS fill: cumulative 50 → delta 20
        let d2 = quoter.record_ws_fill(MarketSide::Yes, dec("50"));
        assert_eq!(d2, dec("20"));

        // Duplicate: cumulative 50 again → delta 0
        let d3 = quoter.record_ws_fill(MarketSide::Yes, dec("50"));
        assert_eq!(d3, dec("0"));
    }

    #[test]
    fn test_filled_so_far() {
        let mut quoter = Quoter::new();
        quoter.on_order_posted(MarketSide::No, ManagedOrder {
            order_id: "o2".into(),
            price: dec("0.55"),
            size: dec("80"),
            posted_ms: 1000,
            fair_value_at_post: dec("0.52"),
            size_filled: Decimal::ZERO,
        });

        assert_eq!(quoter.filled_so_far(MarketSide::No), dec("0"));
        quoter.record_ws_fill(MarketSide::No, dec("25"));
        assert_eq!(quoter.filled_so_far(MarketSide::No), dec("25"));
    }

    #[test]
    fn test_emergency_requote_bypasses_interval() {
        let mut quoter = Quoter::new();
        quoter.on_order_posted(MarketSide::Yes, ManagedOrder {
            order_id: "o1".into(),
            price: dec("0.42"),
            size: dec("50"),
            posted_ms: 1000,
            fair_value_at_post: dec("0.45"),
            size_filled: Decimal::ZERO,
        });

        let position = BilateralPosition::new();
        let (qc, rc) = default_configs();

        // FV drift 0.06 (>= emergency 0.05), only 500ms elapsed (< min_requote 2000ms)
        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.48"), dec("0.51"), "yes_token",
            &position, &qc, &rc, 1500,
        );
        assert!(matches!(action, Some(QuoteAction::Cancel { .. })));
    }

    #[test]
    fn test_normal_requote_respects_interval() {
        let mut quoter = Quoter::new();
        quoter.on_order_posted(MarketSide::Yes, ManagedOrder {
            order_id: "o1".into(),
            price: dec("0.42"),
            size: dec("50"),
            posted_ms: 1000,
            fair_value_at_post: dec("0.45"),
            size_filled: Decimal::ZERO,
        });

        let position = BilateralPosition::new();
        let (qc, rc) = default_configs();

        // FV drift 0.02 (>= threshold 0.01, < emergency 0.05), only 500ms elapsed
        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.44"), dec("0.47"), "yes_token",
            &position, &qc, &rc, 1500,
        );
        assert!(action.is_none());
    }
}
