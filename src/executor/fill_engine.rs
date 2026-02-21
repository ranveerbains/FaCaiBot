// Fill simulation engine — pure fill-condition logic extracted from SimulationExecutor.
//
// `FillSimulator` owns the stateless fill-check rules: post-only rejection,
// depth checks, timing checks, emergency detection, and taker-fee computation.
// It does NOT mutate any position state — that responsibility stays in
// `SimulationExecutor::handle_leg1` / `handle_leg2`.

use rust_decimal::Decimal;

use crate::types::market::OrderBook;
use crate::types::order::{Side, TradeSignal};
use crate::types::simulation::SimFill;

// ─── Constants ────────────────────────────────────────────────────────────────

/// Minimum delay in milliseconds before a Leg 1 post-only order is considered
/// eligible for a simulated fill. Mirrors the real-world latency window before
/// a resting order can be matched.
pub(crate) const SIM_FILL_DELAY_MS: u64 = 500;

// ─── FillSimulator ────────────────────────────────────────────────────────────

/// Stateless fill-condition checker for simulation mode.
///
/// All methods are pure functions of their arguments — no mutable state is held.
/// The struct carries only configuration values that are stable for a market
/// window (tick size). Call `update_tick_size` on each market rotation.
pub(crate) struct FillSimulator {
    /// Tick size for the current market (used to compute depth windows).
    tick_size: Decimal,
}

impl FillSimulator {
    /// Create a new `FillSimulator` with the given tick size.
    pub fn new(tick_size: Decimal) -> Self {
        Self { tick_size }
    }

    /// Update the tick size on market rotation.
    pub fn update_tick_size(&mut self, tick_size: Decimal) {
        self.tick_size = tick_size;
    }

    // ─── Leg 1 fill check ─────────────────────────────────────────────────

    /// Check whether a Leg 1 post-only order would fill given the current book.
    ///
    /// Returns `Some(SimFill)` if:
    ///   1. The book has a best ask (`best_ask`).
    ///   2. Post-only check passes: `signal.price < best_ask` (does not cross spread).
    ///   3. Depth check passes: sell-side volume exists within 2 ticks of our bid.
    ///
    /// Returns `None` (and a `Leg1Reject` reason) otherwise.
    /// The reason is communicated via the returned `Option` and the caller
    /// inspects which guard failed by re-checking the preconditions.
    ///
    /// Fill is at `signal.price` (post-only maker, zero fee).
    pub fn check_leg1_fill(&self, signal: &TradeSignal, book: &OrderBook) -> Leg1Result {
        // Post-only rejection check.
        let best_ask = match book.best_ask() {
            Some(ask) => ask.price,
            None => return Leg1Result::NoAsks,
        };

        if signal.price >= best_ask {
            return Leg1Result::CrossesSpread;
        }

        // Depth check: is there any sell-side volume within 2 ticks of our bid?
        let two_ticks = self.tick_size * Decimal::TWO;
        let depth_window_top = signal.price + two_ticks;

        let near_ask_depth: Decimal = book
            .asks
            .iter()
            .filter(|lvl| lvl.price <= depth_window_top)
            .map(|lvl| lvl.size)
            .sum();

        if near_ask_depth.is_zero() {
            return Leg1Result::NoNearbyDepth;
        }

        // Simulated maker fill at our bid price (zero fee).
        let fill_size = compute_fill_size(signal.alloc_amount, signal.price);
        let now_ms = epoch_ms();

        Leg1Result::Fill(SimFill {
            side: signal.side,
            price: signal.price,
            size: fill_size,
            timestamp_ms: now_ms,
            was_partial: false,
            was_taker: false,
            taker_fee: Decimal::ZERO,
        })
    }

    // ─── Leg 2 fill check ─────────────────────────────────────────────────

    /// Check whether a Leg 2 order would fill given the current book state.
    ///
    /// `leg1_size` — size from the matching Leg 1 fill (shares).
    /// `entry_price` — Leg 1 fill price (used for break-even and emergency detection).
    /// `is_emergency` — if `true`, force a taker fill at best_ask regardless of
    ///   the signal price (the caller pre-computed the emergency condition).
    ///
    /// Returns `Leg2Result` indicating fill details or why no fill occurred.
    pub fn check_leg2_fill(
        &self,
        signal: &TradeSignal,
        book: &OrderBook,
        leg1_size: Decimal,
        entry_price: Decimal,
    ) -> Leg2Result {
        let break_even = Decimal::ONE - entry_price;
        let now_ms = epoch_ms();

        let best_ask = match book.best_ask() {
            Some(ask) => ask.price,
            None => return Leg2Result::NoAsks,
        };

        // Determine emergency condition.
        let is_emergency = signal.price >= break_even || is_near_deadline(signal, now_ms);

        if is_emergency {
            let taker_price = best_ask;
            let taker_fee = SimFill::compute_taker_fee(taker_price, leg1_size);

            let fill = SimFill {
                side: opposite_side(signal.side),
                price: taker_price,
                size: leg1_size,
                timestamp_ms: now_ms,
                was_partial: false,
                was_taker: true,
                taker_fee,
            };

            // Adverse movement: signal price is well below break-even (more than one tick).
            let is_adverse = signal.price < break_even - self.tick_size;
            return Leg2Result::EmergencyFill { fill, is_adverse };
        }

        if best_ask <= signal.price {
            // Normal maker fill: book ask has come down to or below our bid.
            let fill = SimFill {
                side: opposite_side(signal.side),
                price: signal.price,
                size: leg1_size,
                timestamp_ms: now_ms,
                was_partial: false,
                was_taker: false,
                taker_fee: Decimal::ZERO,
            };
            return Leg2Result::MakerFill(fill);
        }

        // Book ask has not yet come down to our bid.
        Leg2Result::NotFilled { best_ask }
    }
}

// ─── Result enums ─────────────────────────────────────────────────────────────

/// Result of a Leg 1 fill check.
pub(crate) enum Leg1Result {
    /// Simulated maker fill — proceed with position creation.
    Fill(SimFill),
    /// Book has no asks — cannot check post-only condition.
    NoAsks,
    /// Bid would cross the spread — post-only rejection.
    CrossesSpread,
    /// No sell-side depth within 2 ticks — insufficient liquidity.
    NoNearbyDepth,
}

/// Result of a Leg 2 fill check.
pub(crate) enum Leg2Result {
    /// Normal maker fill at `signal.price`.
    MakerFill(SimFill),
    /// Emergency taker fill at best_ask.
    /// `is_adverse` distinguishes adverse-movement (true) from deadline/break-even (false).
    EmergencyFill { fill: SimFill, is_adverse: bool },
    /// Book ask has not yet reached our bid — erosion step only.
    NotFilled { best_ask: Decimal },
    /// Book has no asks — cannot simulate.
    NoAsks,
}

// ─── Module-level helpers (pub(crate) so simulation.rs can re-use) ────────────

/// Compute fill size in shares: `alloc / price`. Rounds to 2 decimal places.
/// Returns `Decimal::ZERO` if price is zero (guard against division by zero).
pub(crate) fn compute_fill_size(alloc: Decimal, price: Decimal) -> Decimal {
    if price.is_zero() {
        return Decimal::ZERO;
    }
    let size = alloc / price;
    // Round to 2 decimal places (Polymarket minimum precision 0.01 shares).
    size.round_dp(2)
}

/// Return the opposite side (Leg 2 buys the complementary token).
pub(crate) fn opposite_side(side: Side) -> Side {
    match side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

/// Current epoch time in milliseconds.
pub(crate) fn epoch_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Returns `true` if we are within 90 seconds of market expiry, meaning we
/// must force-hedge immediately to avoid carrying a naked position past deadline.
pub(crate) fn is_near_deadline(signal: &TradeSignal, now_ms: u64) -> bool {
    let deadline_ms = signal.market_end_timestamp_ms.saturating_sub(90_000);
    now_ms >= deadline_ms
}

// ─── Unit Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::market::{Direction, PriceLevel, SpikeInfo};
    use crate::types::order::{ProfitTier, Side, TradeSignal};

    fn d(s: &str) -> Decimal {
        s.parse::<Decimal>()
            .expect("invalid decimal literal in test")
    }

    fn make_book(bid: Decimal, ask: Decimal) -> OrderBook {
        OrderBook {
            asset_id: "test_token".to_string(),
            bids: vec![PriceLevel {
                price: bid,
                size: Decimal::from(100),
            }],
            asks: vec![PriceLevel {
                price: ask,
                size: Decimal::from(100),
            }],
            timestamp_ms: 0,
        }
    }

    fn make_leg1_signal(price: Decimal) -> TradeSignal {
        TradeSignal {
            side: Side::Buy,
            token_id: "test_token".to_string(),
            price,
            size: Decimal::from(10),
            reference_price: Decimal::from(50_000),
            confidence: d("0.85"),
            profit_target_tier: ProfitTier::High,
            profit_target_pct: d("0.025"),
            alloc_amount: Decimal::from(30),
            direction: Direction::Up,
            spike_info: SpikeInfo {
                direction: Direction::Up,
                magnitude: d("0.005"),
                sustained_ms: 200,
                timestamp_ms: 1_000_000,
            },
            is_leg2: false,
            leg1_fill_price: None,
            entry_timestamp_ms: 1_000_000,
            market_end_timestamp_ms: 1_900_000,
            tick_size: d("0.01"),
            fee_rate_bps: 156,
            bot_contested: false,
            book_snapshot: None,
        }
    }

    // ─── compute_fill_size ────────────────────────────────────────────────

    #[test]
    fn test_compute_fill_size() {
        // $30 alloc at $0.45 → 66.666... → rounds to 66.67 at 2dp
        let size = compute_fill_size(Decimal::from(30), d("0.45"));
        assert!(size > Decimal::ZERO);
        // Verify it's rounded to 2 decimal places.
        assert_eq!(size, size.round_dp(2));

        // Zero price guard.
        let size_zero = compute_fill_size(Decimal::from(30), Decimal::ZERO);
        assert_eq!(size_zero, Decimal::ZERO);
    }

    // ─── opposite_side ────────────────────────────────────────────────────

    #[test]
    fn test_opposite_side() {
        assert_eq!(opposite_side(Side::Buy), Side::Sell);
        assert_eq!(opposite_side(Side::Sell), Side::Buy);
    }

    // ─── is_near_deadline ─────────────────────────────────────────────────

    #[test]
    fn test_is_near_deadline() {
        let signal = make_leg1_signal(d("0.45"));
        // market_end_timestamp_ms = 1_900_000. Deadline = 1_900_000 - 90_000 = 1_810_000 ms.

        // now_ms well before deadline — not near.
        assert!(!is_near_deadline(&signal, 1_000_000));

        // now_ms exactly at deadline — near.
        assert!(is_near_deadline(&signal, 1_810_000));

        // now_ms past deadline — near.
        assert!(is_near_deadline(&signal, 1_900_000));
    }

    // ─── Leg 1 post-only spread crossing ─────────────────────────────────

    #[test]
    fn test_leg1_rejected_crosses_spread() {
        let sim = FillSimulator::new(d("0.01"));
        // bid (0.55) >= best ask (0.54) → should be rejected.
        let signal = make_leg1_signal(d("0.55"));
        let book = make_book(d("0.50"), d("0.54"));

        assert!(matches!(sim.check_leg1_fill(&signal, &book), Leg1Result::CrossesSpread));
    }

    // ─── Leg 1 no nearby depth ────────────────────────────────────────────

    #[test]
    fn test_leg1_no_liquidity() {
        let sim = FillSimulator::new(d("0.01"));
        // Our bid = 0.45; depth window top = 0.47; ask at 0.50 → outside window.
        let signal = make_leg1_signal(d("0.45"));
        let book = make_book(d("0.40"), d("0.50"));

        assert!(matches!(sim.check_leg1_fill(&signal, &book), Leg1Result::NoNearbyDepth));
    }

    // ─── Leg 1 successful fill ────────────────────────────────────────────

    #[test]
    fn test_leg1_fill_succeeds() {
        let sim = FillSimulator::new(d("0.01"));
        // Our bid = 0.45; ask = 0.46 → within 2 ticks, price < ask → fill.
        let signal = make_leg1_signal(d("0.45"));
        let book = make_book(d("0.40"), d("0.46"));

        let result = sim.check_leg1_fill(&signal, &book);
        assert!(matches!(result, Leg1Result::Fill(_)));
        if let Leg1Result::Fill(fill) = result {
            assert_eq!(fill.price, d("0.45"));
            assert!(!fill.was_taker);
            assert_eq!(fill.taker_fee, Decimal::ZERO);
        }
    }

    // ─── Leg 1 no asks ───────────────────────────────────────────────────

    #[test]
    fn test_leg1_no_asks() {
        let sim = FillSimulator::new(d("0.01"));
        let signal = make_leg1_signal(d("0.45"));
        let book = OrderBook {
            asset_id: "tok".to_string(),
            bids: vec![PriceLevel { price: d("0.40"), size: Decimal::from(100) }],
            asks: vec![],
            timestamp_ms: 0,
        };
        assert!(matches!(sim.check_leg1_fill(&signal, &book), Leg1Result::NoAsks));
    }
}
