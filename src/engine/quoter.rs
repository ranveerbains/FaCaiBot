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
    #[allow(dead_code)]
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
    pub max_order_size: Decimal,
    pub min_order_size: Decimal,
    pub max_fair_value_extremity: f64,
}

impl Default for QuotingConfig {
    fn default() -> Self {
        Self {
            min_edge: 0.04,
            requote_threshold: 0.01,
            max_order_size: Decimal::new(100, 0),
            min_order_size: Decimal::new(5, 0),
            max_fair_value_extremity: 0.85,
        }
    }
}

/// Risk limits for v2 (from config.toml [risk_v2]).
#[derive(Debug, Clone)]
pub struct RiskV2Config {
    pub rotation_quiet_ms: u64,
    pub rebalance_threshold: Decimal,
    pub rebalance_size: Decimal,
    pub rebalance_max_pair_cost: f64,
    pub stale_book_ms: u64,
    pub heartbeat_dead_threshold: u32,
    pub buildup_go_dark_threshold: f64,
    pub buildup_go_live_threshold: f64,
}

impl Default for RiskV2Config {
    fn default() -> Self {
        Self {
            rotation_quiet_ms: 5000,
            rebalance_threshold: Decimal::new(20, 0),
            rebalance_size: Decimal::new(10, 0),
            rebalance_max_pair_cost: 0.96,
            stale_book_ms: 850,
            heartbeat_dead_threshold: 5,
            buildup_go_dark_threshold: 0.40,
            buildup_go_live_threshold: 0.25,
        }
    }
}

// ─── Dynamic order sizing ────────────────────────────────────────────────────

/// Precalculate the maximum total shares per side (sum of all batch sizes).
/// After this many shares on either side, maker orders stop.
///
/// Example with max=20, min=5: 20 + 10 + 5 = 35
/// Example with max=10, min=5: 10 + 5 + 5 = 20
pub fn max_total_shares(max_size: Decimal, min_size: Decimal) -> Decimal {
    let two = Decimal::TWO;
    let mut size = max_size;
    let mut total = Decimal::ZERO;
    for _ in 0..3 {
        total += size;
        size = (size / two).max(min_size);
    }
    total
}

/// Compute dynamic order size based on per-side accumulated shares.
///
/// Halves the order size at each accumulation milestone (floored at `min_size`).
/// Each side tracks independently — heavy side reduces faster.
///
/// Example with max=15, min=5:
/// - 0..14 shares on this side → 15 (batch 1)
/// - 15..22.4 → 7.5 (batch 2)
/// - 22.5+ → 5 (batch 3)
pub fn dynamic_order_size(max_size: Decimal, min_size: Decimal, side_shares: Decimal) -> Decimal {
    let two = Decimal::TWO;
    let mut size = max_size;
    let mut threshold = Decimal::ZERO;
    for _ in 0..3 {
        threshold += size;
        if side_shares < threshold {
            return size;
        }
        size = (size / two).max(min_size);
    }
    min_size
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
        opposite_best_ask: Option<Decimal>,
    ) -> Option<QuoteAction> {
        // ── Guards ──
        if self.is_pending(side) {
            return None;
        }
        // Total shares cap: stop maker orders when either side hits the precalculated limit
        let total_cap = max_total_shares(quoting.max_order_size, quoting.min_order_size);
        let side_shares = match side {
            MarketSide::Yes => position.yes.total_shares,
            MarketSide::No => position.no.total_shares,
        };
        if side_shares >= total_cap {
            return None;
        }
        // Pair-cost feasibility guard: don't post if filling at target_price
        // would push the running pair cost above $1.00
        let other_avg = match side {
            MarketSide::Yes => position.no.avg_price(),
            MarketSide::No => position.yes.avg_price(),
        };
        if other_avg > Decimal::ZERO {
            let projected_pair_cost = other_avg + target_price;
            if projected_pair_cost >= Decimal::ONE {
                return None;
            }
        }

        // Book-aware pair-cost check: block if pairing is impossible or unprofitable
        if position.unpaired(side) > Decimal::ZERO {
            match opposite_best_ask {
                None if other_avg == Decimal::ZERO => return None, // No fills + no book = no pairing path
                Some(opp_ask) if opp_ask + target_price >= Decimal::ONE => return None,
                _ => {}
            }
        }

        // ── Size computation ──
        let dynamic_max = dynamic_order_size(
            quoting.max_order_size,
            quoting.min_order_size,
            side_shares,
        );

        if target_price <= Decimal::ZERO {
            return None;
        }
        // Cap size so we don't exceed total_cap on this side
        let remaining = total_cap - side_shares;
        let size = dynamic_max
            .min(remaining)
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
                if fv_drift_f64 >= quoting.requote_threshold {
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
                self.yes_order = Some(order);
                self.yes_pending_post = false;
            }
            MarketSide::No => {
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

    /// Generate a cancel action for a specific side, if it has a resting order
    /// and no pending cancel already in flight.
    pub fn cancel_side_action(&self, side: MarketSide) -> Option<QuoteAction> {
        let (order, pending) = match side {
            MarketSide::Yes => (&self.yes_order, self.yes_pending_cancel),
            MarketSide::No => (&self.no_order, self.no_pending_cancel),
        };
        if pending {
            return None;
        }
        order.as_ref().map(|o| QuoteAction::Cancel {
            side,
            order_id: o.order_id.clone(),
        })
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

    fn default_config() -> QuotingConfig {
        QuotingConfig::default()
    }

    // ── dynamic_order_size tests ──

    #[test]
    fn test_dynamic_size_batch1() {
        // max=10, min=5: 0..9 paired → 10
        assert_eq!(dynamic_order_size(dec("10"), dec("5"), dec("0")), dec("10"));
        assert_eq!(dynamic_order_size(dec("10"), dec("5"), dec("9")), dec("10"));
    }

    #[test]
    fn test_dynamic_size_batch2() {
        // max=10, min=5: 10..14 paired → 5 (halved, floored at min)
        assert_eq!(dynamic_order_size(dec("10"), dec("5"), dec("10")), dec("5"));
        assert_eq!(dynamic_order_size(dec("10"), dec("5"), dec("14")), dec("5"));
    }

    #[test]
    fn test_dynamic_size_batch3() {
        // max=10, min=5: 15+ paired → 5 (stays at min)
        assert_eq!(dynamic_order_size(dec("10"), dec("5"), dec("15")), dec("5"));
        assert_eq!(dynamic_order_size(dec("10"), dec("5"), dec("100")), dec("5"));
    }

    #[test]
    fn test_dynamic_size_larger_max() {
        // max=20, min=5: batch1=20 (0..19), batch2=10 (20..29), batch3=5 (30+)
        assert_eq!(dynamic_order_size(dec("20"), dec("5"), dec("0")), dec("20"));
        assert_eq!(dynamic_order_size(dec("20"), dec("5"), dec("19")), dec("20"));
        assert_eq!(dynamic_order_size(dec("20"), dec("5"), dec("20")), dec("10"));
        assert_eq!(dynamic_order_size(dec("20"), dec("5"), dec("29")), dec("10"));
        assert_eq!(dynamic_order_size(dec("20"), dec("5"), dec("30")), dec("5"));
        assert_eq!(dynamic_order_size(dec("20"), dec("5"), dec("100")), dec("5"));
    }

    #[test]
    fn test_max_total_shares() {
        // max=10, min=5: 10 + 5 + 5 = 20
        assert_eq!(max_total_shares(dec("10"), dec("5")), dec("20"));
        // max=20, min=5: 20 + 10 + 5 = 35
        assert_eq!(max_total_shares(dec("20"), dec("5")), dec("35"));
        // max=40, min=5: 40 + 20 + 10 = 70
        assert_eq!(max_total_shares(dec("40"), dec("5")), dec("70"));
    }

    #[test]
    fn test_post_when_no_resting() {
        let quoter = Quoter::new();
        let position = BilateralPosition::new();
        let qc = default_config();

        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.42"), dec("0.45"), "yes_token",
            &position, &qc,
            Some(dec("0.50")),
        );
        assert!(matches!(action, Some(QuoteAction::Post { .. })));
    }

    #[test]
    fn test_no_post_when_pending() {
        let mut quoter = Quoter::new();
        quoter.mark_post_pending(MarketSide::Yes);
        let position = BilateralPosition::new();
        let qc = default_config();

        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.42"), dec("0.45"), "yes_token",
            &position, &qc,
            Some(dec("0.50")),
        );
        assert!(action.is_none());
    }

    #[test]
    fn test_no_post_when_capital_exhausted() {
        let quoter = Quoter::new();
        let mut position = BilateralPosition::new();
        // Deploy max capital
        position.record_fill(MarketSide::Yes, dec("0.50"), dec("200"), false, dec("0"));
        let qc = default_config();

        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.42"), dec("0.45"), "yes_token",
            &position, &qc,
            Some(dec("0.50")),
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
        let qc = default_config();

        // Fair value shifted by 0.02 (> threshold 0.01), 3s elapsed (> 2s min)
        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.44"), dec("0.47"), "yes_token",
            &position, &qc,
            Some(dec("0.50")),
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
        let qc = default_config();

        // Fair value barely moved (0.005 < threshold 0.01)
        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.425"), dec("0.455"), "yes_token",
            &position, &qc,
            Some(dec("0.50")),
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
    fn test_lookup_order_for_cancel_current() {
        let mut quoter = Quoter::new();
        quoter.on_order_posted(MarketSide::No, ManagedOrder {
            order_id: "o2".into(),
            price: dec("0.55"),
            size: dec("80"),
            posted_ms: 1000,
            fair_value_at_post: dec("0.52"),
            size_filled: Decimal::ZERO,
        });

        // Finds current resting order
        let result = quoter.lookup_order_for_cancel(MarketSide::No, "o2");
        assert_eq!(result, Some((dec("0.55"), dec("0"))));

        // After partial fill, returns updated size_filled
        quoter.record_ws_fill(MarketSide::No, dec("25"));
        let result = quoter.lookup_order_for_cancel(MarketSide::No, "o2");
        assert_eq!(result, Some((dec("0.55"), dec("25"))));

        // Wrong order_id returns None
        assert!(quoter.lookup_order_for_cancel(MarketSide::No, "wrong").is_none());
    }

    #[test]
    fn test_lookup_order_for_cancel_after_full_fill() {
        let mut quoter = Quoter::new();
        quoter.on_order_posted(MarketSide::Yes, ManagedOrder {
            order_id: "o3".into(),
            price: dec("0.63"),
            size: dec("10"),
            posted_ms: 1000,
            fair_value_at_post: dec("0.60"),
            size_filled: Decimal::ZERO,
        });

        // Full WS fill clears order but saves to last_cleared
        quoter.record_ws_fill(MarketSide::Yes, dec("10"));
        quoter.on_fill(MarketSide::Yes, true);

        // Current order is gone
        assert!(quoter.order(MarketSide::Yes).is_none());

        // But lookup still finds it via last_cleared
        let result = quoter.lookup_order_for_cancel(MarketSide::Yes, "o3");
        assert_eq!(result, Some((dec("0.63"), dec("10"))));
    }

    #[test]
    fn test_pair_cost_guard_blocks_expensive_pair() {
        let quoter = Quoter::new();
        let mut position = BilateralPosition::new();
        // NO fills at avg $0.60
        position.record_fill(MarketSide::No, dec("0.60"), dec("10"), false, dec("0"));
        let qc = default_config();

        // YES at $0.45 → pair_cost = 0.60 + 0.45 = 1.05 ≥ 1.00 → blocked
        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.45"), dec("0.50"), "yes_token",
            &position, &qc,
            Some(dec("0.50")),
        );
        assert!(action.is_none());
    }

    #[test]
    fn test_pair_cost_guard_allows_cheap_pair() {
        let quoter = Quoter::new();
        let mut position = BilateralPosition::new();
        // NO fills at avg $0.60
        position.record_fill(MarketSide::No, dec("0.60"), dec("10"), false, dec("0"));
        let qc = default_config();

        // YES at $0.39 → pair_cost = 0.60 + 0.39 = 0.99 < 1.00 → allowed
        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.39"), dec("0.45"), "yes_token",
            &position, &qc,
            Some(dec("0.50")),
        );
        assert!(matches!(action, Some(QuoteAction::Post { .. })));
    }

    #[test]
    fn test_pair_cost_guard_skipped_no_opposite_fills() {
        let quoter = Quoter::new();
        let position = BilateralPosition::new();
        let qc = default_config();

        // No opposite fills → guard skipped → normal post
        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.85"), dec("0.90"), "yes_token",
            &position, &qc,
            Some(dec("0.50")),
        );
        assert!(matches!(action, Some(QuoteAction::Post { .. })));
    }

    #[test]
    fn test_book_cost_blocks_no_opposite_book() {
        let quoter = Quoter::new();
        let mut position = BilateralPosition::new();
        // 3 YES, 0 NO → has unpaired shares, opposite_best_ask = None
        position.record_fill(MarketSide::Yes, dec("0.45"), dec("3"), false, dec("0"));
        let qc = default_config();

        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.42"), dec("0.45"), "yes_token",
            &position, &qc,
            None, // No opposite book liquidity
        );
        assert!(action.is_none());
    }

    #[test]
    fn test_book_cost_blocks_expensive_opposite() {
        let quoter = Quoter::new();
        let mut position = BilateralPosition::new();
        // 3 YES, 0 NO → unpaired shares, opposite ask too expensive
        position.record_fill(MarketSide::Yes, dec("0.70"), dec("3"), false, dec("0"));
        let qc = default_config();

        // target=0.70, opposite_best_ask=0.35 → 0.70 + 0.35 = 1.05 >= 1.00 → blocked
        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.70"), dec("0.75"), "yes_token",
            &position, &qc,
            Some(dec("0.35")),
        );
        assert!(action.is_none());
    }

    #[test]
    fn test_book_cost_blocks_when_book_expensive() {
        let quoter = Quoter::new();
        let mut position = BilateralPosition::new();
        // 3 YES, 0 NO → has unpaired, book price makes next pair unprofitable
        position.record_fill(MarketSide::Yes, dec("0.60"), dec("3"), false, dec("0"));
        let qc = default_config();

        // target=0.60, opposite_best_ask=0.45 → marginal cost = 0.60 + 0.45 = 1.05 >= 1.00
        let action = quoter.evaluate_side(
            MarketSide::Yes, dec("0.60"), dec("0.65"), "yes_token",
            &position, &qc,
            Some(dec("0.45")),
        );
        assert!(action.is_none());
    }
}
