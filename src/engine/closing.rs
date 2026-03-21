//! Closing manager — end-of-market pairing logic.
//!
//! In the last ~30 seconds of a 5-minute market, cancel all resting orders
//! and aggressively pair the imbalanced side using FOK taker orders.

use rust_decimal::Decimal;

use crate::engine::position::{BilateralPosition, MarketSide};

/// Action to pair an imbalanced position in the closing phase.
#[derive(Debug, Clone)]
pub struct PairingAction {
    pub side: MarketSide, // Which side to buy (the lagging one)
    pub token_id: String,
    pub price: Decimal,   // FOK price (best ask on that side)
    pub size: Decimal,    // Shares needed to pair up
    pub expected_pair_cost: Decimal,
}

/// Manages the closing phase of a market.
pub struct ClosingManager {
    pub phase_entered: bool,
    pub resting_cancelled: bool,
    pub attempt_count: u32,
    pub pairing_sent: bool,
    max_attempts: u32,
    retry_price_increment: Decimal,
}

impl ClosingManager {
    pub fn new() -> Self {
        Self {
            phase_entered: false,
            resting_cancelled: false,
            attempt_count: 0,
            pairing_sent: false,
            max_attempts: 3,
            retry_price_increment: Decimal::new(1, 2), // 0.01
        }
    }

    pub fn with_config(max_attempts: u32, retry_price_increment: Decimal) -> Self {
        Self {
            phase_entered: false,
            resting_cancelled: false,
            attempt_count: 0,
            pairing_sent: false,
            max_attempts,
            retry_price_increment,
        }
    }

    /// Whether another FOK attempt is allowed.
    pub fn can_retry(&self) -> bool {
        self.attempt_count < self.max_attempts && !self.pairing_sent
    }

    /// Check if we should enter the closing phase.
    pub fn should_enter(time_remaining_ms: u64, closing_phase_secs: u64) -> bool {
        let closing_phase_ms = closing_phase_secs * 1000;
        time_remaining_ms <= closing_phase_ms && time_remaining_ms > 0
    }

    /// Compute a pairing action if the position is imbalanced and profitable to pair.
    ///
    /// `yes_best_ask` / `no_best_ask`: best ask price on each book.
    /// `max_pair_cost`: maximum acceptable pair cost (e.g., $0.97).
    /// `attempt`: retry number (0-indexed). Each retry widens max_pair_cost by
    /// `attempt * retry_price_increment`.
    pub fn compute_pairing(
        position: &BilateralPosition,
        yes_token_id: &str,
        no_token_id: &str,
        yes_best_ask: Option<Decimal>,
        no_best_ask: Option<Decimal>,
        max_pair_cost: Decimal,
        max_capital: Decimal,
    ) -> Option<PairingAction> {
        let yes_shares = position.yes.total_shares;
        let no_shares = position.no.total_shares;

        if yes_shares == no_shares {
            return None; // already balanced
        }

        let min_deficit = Decimal::new(5, 0);

        if yes_shares > no_shares {
            // Need more NO shares
            let deficit = yes_shares - no_shares;
            if deficit < min_deficit {
                return None;
            }
            let no_ask = no_best_ask?;
            // $1 notional minimum
            if no_ask * deficit < Decimal::new(1, 0) {
                return None;
            }
            let pair_cost = position.yes.avg_price() + no_ask;
            if pair_cost >= max_pair_cost {
                return None; // too expensive
            }
            let max_by_capital = if no_ask > Decimal::ZERO {
                (max_capital - position.total_capital_deployed()) / no_ask
            } else {
                Decimal::ZERO
            };
            let size = deficit.min(max_by_capital).max(Decimal::ZERO);
            if size < min_deficit {
                return None;
            }
            Some(PairingAction {
                side: MarketSide::No,
                token_id: no_token_id.to_string(),
                price: no_ask,
                size,
                expected_pair_cost: pair_cost,
            })
        } else {
            // Need more YES shares
            let deficit = no_shares - yes_shares;
            if deficit < min_deficit {
                return None;
            }
            let yes_ask = yes_best_ask?;
            // $1 notional minimum
            if yes_ask * deficit < Decimal::new(1, 0) {
                return None;
            }
            let pair_cost = yes_ask + position.no.avg_price();
            if pair_cost >= max_pair_cost {
                return None;
            }
            let max_by_capital = if yes_ask > Decimal::ZERO {
                (max_capital - position.total_capital_deployed()) / yes_ask
            } else {
                Decimal::ZERO
            };
            let size = deficit.min(max_by_capital).max(Decimal::ZERO);
            if size < min_deficit {
                return None;
            }
            Some(PairingAction {
                side: MarketSide::Yes,
                token_id: yes_token_id.to_string(),
                price: yes_ask,
                size,
                expected_pair_cost: pair_cost,
            })
        }
    }

    pub fn reset(&mut self) {
        self.phase_entered = false;
        self.resting_cancelled = false;
        self.attempt_count = 0;
        self.pairing_sent = false;
    }
}

impl Default for ClosingManager {
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

    #[test]
    fn test_should_enter_closing() {
        assert!(ClosingManager::should_enter(25_000, 30));
        assert!(ClosingManager::should_enter(30_000, 30));
        assert!(!ClosingManager::should_enter(31_000, 30));
        assert!(!ClosingManager::should_enter(0, 30));
    }

    #[test]
    fn test_pairing_need_no() {
        let mut pos = BilateralPosition::new();
        pos.record_fill(MarketSide::Yes, dec("0.42"), dec("30"), 1000, false, dec("0"));
        pos.record_fill(MarketSide::No, dec("0.48"), dec("20"), 2000, false, dec("0"));

        let action = ClosingManager::compute_pairing(
            &pos, "yt", "nt",
            Some(dec("0.43")), Some(dec("0.49")),
            dec("0.97"), dec("100"),
        );

        let a = action.unwrap();
        assert_eq!(a.side, MarketSide::No);
        assert_eq!(a.size, dec("10")); // 30 - 20
    }

    #[test]
    fn test_pairing_need_yes() {
        let mut pos = BilateralPosition::new();
        pos.record_fill(MarketSide::Yes, dec("0.42"), dec("10"), 1000, false, dec("0"));
        pos.record_fill(MarketSide::No, dec("0.48"), dec("20"), 2000, false, dec("0"));

        let action = ClosingManager::compute_pairing(
            &pos, "yt", "nt",
            Some(dec("0.43")), Some(dec("0.49")),
            dec("0.97"), dec("100"),
        );

        let a = action.unwrap();
        assert_eq!(a.side, MarketSide::Yes);
        assert_eq!(a.size, dec("10")); // 20 - 10
    }

    #[test]
    fn test_pairing_too_expensive() {
        let mut pos = BilateralPosition::new();
        pos.record_fill(MarketSide::Yes, dec("0.55"), dec("30"), 1000, false, dec("0"));
        pos.record_fill(MarketSide::No, dec("0.48"), dec("20"), 2000, false, dec("0"));

        // pair_cost = 0.55 + 0.49 = 1.04 > 0.97
        let action = ClosingManager::compute_pairing(
            &pos, "yt", "nt",
            Some(dec("0.56")), Some(dec("0.49")),
            dec("0.97"), dec("100"),
        );
        assert!(action.is_none());
    }

    #[test]
    fn test_pairing_balanced() {
        let mut pos = BilateralPosition::new();
        pos.record_fill(MarketSide::Yes, dec("0.42"), dec("20"), 1000, false, dec("0"));
        pos.record_fill(MarketSide::No, dec("0.48"), dec("20"), 2000, false, dec("0"));

        let action = ClosingManager::compute_pairing(
            &pos, "yt", "nt",
            Some(dec("0.43")), Some(dec("0.49")),
            dec("0.97"), dec("100"),
        );
        assert!(action.is_none());
    }

    #[test]
    fn test_pairing_no_book() {
        let mut pos = BilateralPosition::new();
        pos.record_fill(MarketSide::Yes, dec("0.42"), dec("30"), 1000, false, dec("0"));
        pos.record_fill(MarketSide::No, dec("0.48"), dec("20"), 2000, false, dec("0"));

        let action = ClosingManager::compute_pairing(
            &pos, "yt", "nt",
            Some(dec("0.43")), None, // no NO book
            dec("0.97"), dec("100"),
        );
        assert!(action.is_none());
    }

    #[test]
    fn test_pairing_deficit_below_minimum() {
        let mut pos = BilateralPosition::new();
        // 23 YES, 20 NO → deficit = 3 (below min 5)
        pos.record_fill(MarketSide::Yes, dec("0.42"), dec("23"), 1000, false, dec("0"));
        pos.record_fill(MarketSide::No, dec("0.48"), dec("20"), 2000, false, dec("0"));

        let action = ClosingManager::compute_pairing(
            &pos, "yt", "nt",
            Some(dec("0.43")), Some(dec("0.49")),
            dec("0.97"), dec("100"),
        );
        assert!(action.is_none(), "deficit of 3 should be below min 5");
    }

    #[test]
    fn test_reset() {
        let mut cm = ClosingManager::new();
        cm.phase_entered = true;
        cm.attempt_count = 2;
        cm.reset();
        assert!(!cm.phase_entered);
        assert_eq!(cm.attempt_count, 0);
    }

    #[test]
    fn test_can_retry() {
        let mut cm = ClosingManager::with_config(3, dec("0.01"));
        assert!(cm.can_retry()); // 0 < 3
        cm.attempt_count = 1;
        assert!(cm.can_retry()); // 1 < 3
        cm.attempt_count = 2;
        assert!(cm.can_retry()); // 2 < 3
        cm.attempt_count = 3;
        assert!(!cm.can_retry()); // 3 >= 3
    }

    #[test]
    fn test_can_retry_blocked_while_sent() {
        let mut cm = ClosingManager::with_config(3, dec("0.01"));
        cm.pairing_sent = true;
        assert!(!cm.can_retry()); // blocked while FOK in-flight
    }
}
