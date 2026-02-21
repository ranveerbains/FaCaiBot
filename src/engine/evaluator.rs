//! Signal evaluation logic for Leg 1 and Leg 2.
//!
//! This module provides [`Leg1Evaluator`] and [`Leg2Evaluator`], which encapsulate
//! the pure guard-checking and signal-building logic extracted from `StrategyEngine`.
//!
//! # Design Contract
//! - **No mutation** of shared state — evaluators only read market/erosion data.
//! - **Mutation stays in `strategy.rs`** — after `evaluate()` / `evaluate_leg2()` return
//!   `Some(signal)`, the caller is responsible for updating `leg1_state`, `leg2_state`,
//!   `spike_detected`, `cumulative_used`, and `last_erosion_signal_ms`.
//! - [`ErosionSnap`] is used to pass borrow-free snapshots of erosion state into the
//!   evaluator so it can read erosion fields without holding a mutable borrow on the engine.

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tracing::{debug, info, warn};

use crate::types::market::{Direction, MarketState, OrderBook, OrderState, SpikeInfo};
use crate::types::order::{ProfitTier, Side, TradeSignal};

use super::confidence::{compute_confidence, round_to_tick};
use super::erosion::ErosionSnap;

// ─── Leg 1 Evaluator ────────────────────────────────────────────────────────

/// Evaluates whether to emit a Leg 1 trade signal.
///
/// Holds only the config fields required for Leg 1 guard checks and signal building.
/// All reads are from borrowed `&MarketState`; no mutation occurs here.
pub(crate) struct Leg1Evaluator {
    pub spread_abort: Decimal,
    pub entry_cutoff_secs: u64,
    pub depth_min_pct: Decimal,
    pub stale_book_ms: u64,
    pub fixed_alloc: Decimal,
    pub max_alloc_pct: Decimal,
    pub sustain_window_ms: u64,
    pub depth_wall_multiplier: Decimal,
    pub high_threshold: Decimal,
    pub med_threshold: Decimal,
}

impl Leg1Evaluator {
    /// Evaluate whether to emit a Leg 1 signal given the current market state.
    ///
    /// Returns `Some(TradeSignal)` when **all** pre-entry guards pass and a confirmed
    /// spike is present. Returns `None` if any guard fails (silent in most cases;
    /// informational logs on meaningful blocks).
    ///
    /// # Important
    /// The caller **must** apply the post-signal state mutations:
    /// - `state.spike_detected = false`
    /// - `state.leg1_state = OrderState::Posted { … }`
    /// - `state.cumulative_used += alloc`
    /// - `leg1_direction = Some(spike.direction)`
    pub fn evaluate(
        &self,
        state: &MarketState,
        avg_book_depth: Option<Decimal>,
        now_ms: u64,
    ) -> Option<TradeSignal> {
        // Only generate Leg 1 signals when no active trade is in flight.
        if !matches!(state.leg1_state, OrderState::None) {
            return None;
        }

        // Must have a confirmed spike.
        if !state.spike_detected {
            return None;
        }
        let spike = state.last_spike?;

        // Require active market.
        let cond_id = match state.active_condition_id.as_ref() {
            Some(id) => id,
            None => {
                info!("spike detected but no active market — awaiting market rotation");
                return None;
            }
        };

        // ── Diagnostic: log all guard states on spike detection ──────────
        let time_remaining_secs = state.time_remaining_ms(now_ms) / 1_000;
        let has_book = state.poly_book.is_some();
        let has_binance = state.binance_price.is_some();
        let remaining_alloc = state.remaining_alloc(self.fixed_alloc);
        debug!(
            %cond_id,
            direction = ?spike.direction,
            has_book,
            has_binance,
            time_remaining_secs,
            %remaining_alloc,
            available_capital = %state.available_capital,
            "evaluate() — spike detected, checking guards"
        );

        // Direction-aware book selection: use the book for the token we're buying.
        let direction = spike.direction;
        let (book, best_bid_price, best_ask_price): (&OrderBook, Decimal, Decimal) = {
            let dir_book_opt = match direction {
                Direction::Up => state.poly_yes_book.as_ref().or(state.poly_book.as_ref()),
                Direction::Down => state.poly_no_book.as_ref(),
            };
            if let Some(b) = dir_book_opt {
                let bid = match b.best_bid() {
                    Some(x) => x.price,
                    None => {
                        debug!("evaluate() BLOCKED: no best_bid in direction book");
                        return None;
                    }
                };
                let ask = match b.best_ask() {
                    Some(x) => x.price,
                    None => {
                        debug!("evaluate() BLOCKED: no best_ask in direction book");
                        return None;
                    }
                };
                (b, bid, ask)
            } else {
                // Direction::Down with no NO book — derive NO prices from YES complement.
                let yes_b = match state.poly_yes_book.as_ref().or(state.poly_book.as_ref()) {
                    Some(b) => b,
                    None => {
                        debug!(direction = ?direction, "evaluate() BLOCKED: no book available");
                        return None;
                    }
                };
                let (yes_bid, yes_ask) = match (yes_b.best_bid(), yes_b.best_ask()) {
                    (Some(b), Some(a)) => (b.price, a.price),
                    _ => {
                        debug!("evaluate() BLOCKED: YES book missing bid/ask for complement");
                        return None;
                    }
                };
                (yes_b, Decimal::ONE - yes_ask, Decimal::ONE - yes_bid)
            }
        };
        let reference_price = match state.binance_price {
            Some(p) => p,
            None => {
                debug!("evaluate() BLOCKED: no binance_price");
                return None;
            }
        };

        // Guard: stale book
        let book_age_ms = now_ms.saturating_sub(book.timestamp_ms);
        if book_age_ms > self.stale_book_ms {
            info!(book_age_ms, "evaluate() BLOCKED: poly book stale");
            return None;
        }

        // Guard: spread too wide
        let mid = (best_bid_price + best_ask_price) / Decimal::TWO;
        if mid.is_zero() {
            return None;
        }
        let spread_pct = (best_ask_price - best_bid_price) / mid;
        if spread_pct > self.spread_abort {
            info!(%spread_pct, "evaluate() BLOCKED: spread too wide");
            return None;
        }

        // Guard: < entry_cutoff_secs remaining
        if time_remaining_secs < self.entry_cutoff_secs {
            info!(
                time_remaining_secs,
                "evaluate() BLOCKED: < 180s remaining — not enough time to trade"
            );
            return None;
        }

        // Confidence scoring
        let atr = state.atr.unwrap_or(Decimal::new(1, 3));
        let total_depth = book.total_bid_depth() + book.total_ask_depth();
        let avg_depth = avg_book_depth.unwrap_or(Decimal::ONE);
        let confidence = compute_confidence(
            spike.magnitude,
            atr,
            spike.sustained_ms,
            total_depth,
            avg_depth,
            time_remaining_secs,
            self.sustain_window_ms,
        );
        let tier = ProfitTier::from_confidence(confidence, self.high_threshold, self.med_threshold);

        // Allocation
        let alloc_for_tier = self.fixed_alloc * tier.alloc_pct();
        let remaining = state.remaining_alloc(self.fixed_alloc);
        if remaining < alloc_for_tier {
            info!(%remaining, %alloc_for_tier, "evaluate() BLOCKED: insufficient remaining allocation");
            return None;
        }
        let alloc = alloc_for_tier
            .min(self.fixed_alloc * self.max_alloc_pct)
            .min(remaining);

        // Guard: liquidity
        let required_depth = if !best_bid_price.is_zero() {
            alloc / best_bid_price
        } else {
            return None;
        };
        if book.total_bid_depth() < required_depth * self.depth_min_pct {
            info!("evaluate() BLOCKED: insufficient poly book depth");
            return None;
        }

        // Token selection
        let token_id = match direction {
            Direction::Up => state.active_yes_token_id.as_ref()?,
            Direction::Down => state.active_no_token_id.as_ref()?,
        };

        // Leg 1 bid price — post just above the current best bid of the direction book.
        let tick = state.tick_size;
        let mut bid_price = round_to_tick(best_bid_price + tick, tick);

        // Reject if bid would cross ask (post-only constraint).
        if bid_price >= best_ask_price {
            debug!(
                %bid_price,
                %best_ask_price,
                "bid would cross ask — post-only rejected, skipping"
            );
            return None;
        }

        // Smart outbidding
        let wall = detect_depth_wall(book, Side::Buy, self.depth_wall_multiplier);
        let bot_contested = wall.is_some();
        if let Some(wall_price) = wall {
            if wall_price >= bid_price {
                let outbid = round_to_tick(wall_price + tick, tick);
                let be_cap = Decimal::ONE - (Decimal::ONE - tier.target_pct() - outbid) - tick;
                if outbid <= be_cap && outbid < best_ask_price {
                    debug!(%wall_price, %outbid, "Leg 1 smart outbid");
                    bid_price = outbid;
                }
            }
        }

        // Final break-even cap
        {
            let est_leg2 = Decimal::ONE - tier.target_pct() - bid_price;
            let be_cap = Decimal::ONE - est_leg2 - tick;
            if bid_price > be_cap {
                bid_price = round_to_tick(be_cap, tick);
            }
        }

        // Entry size
        let entry_size = if !bid_price.is_zero() {
            alloc / bid_price
        } else {
            return None;
        };
        if entry_size <= Decimal::ZERO {
            return None;
        }

        info!(
            direction = ?spike.direction, %confidence, tier = tier.label(),
            %bid_price, %entry_size, %alloc, bot_contested, time_remaining_secs,
            "Leg 1 signal generated"
        );

        Some(TradeSignal {
            side: Side::Buy,
            token_id: token_id.clone(),
            price: bid_price,
            size: entry_size,
            reference_price,
            confidence,
            profit_target_tier: tier,
            profit_target_pct: tier.target_pct(),
            alloc_amount: alloc,
            direction: spike.direction,
            spike_info: spike,
            is_leg2: false,
            leg1_fill_price: None,
            entry_timestamp_ms: now_ms,
            market_end_timestamp_ms: state.market_end_timestamp_ms,
            tick_size: tick,
            fee_rate_bps: state.fee_rate_bps,
            bot_contested,
            book_snapshot: match direction {
                Direction::Up => state.poly_yes_book.clone().or(state.poly_book.clone()),
                Direction::Down => state.poly_no_book.clone().or(state.poly_book.clone()),
            },
        })
    }
}

// ─── Leg 2 Evaluator ────────────────────────────────────────────────────────

/// Evaluates whether to emit a Leg 2 hedge signal after Leg 1 fills.
///
/// Handles post-only erosion cascade, quick reversal, adverse movement,
/// and emergency FOK deadline fills.
///
/// All reads are from borrowed `&MarketState` and `&ErosionSnap`; no mutation occurs here.
pub(crate) struct Leg2Evaluator {
    pub adverse_threshold: Decimal,
    pub erosion_interval_ms: u64,
    pub emergency_deadline_secs: u64,
    pub depth_wall_multiplier: Decimal,
    pub quick_reversal_threshold: Decimal,
}

impl Leg2Evaluator {
    /// Evaluate whether to emit a Leg 2 signal.
    ///
    /// Returns `Some(TradeSignal)` when action is needed (erosion step, emergency FOK).
    ///
    /// # Important
    /// The caller **must** apply post-signal state mutations depending on signal type:
    /// - Normal erosion: `last_erosion_signal_ms = now_ms`, `leg2_state = Posted { … }`
    /// - Emergency FOK: `erosion.emergency_submitted = true`, `leg2_state = Posted { … }`
    /// - Erosion step advance: `erosion.steps_applied += 1`
    ///
    /// The `last_erosion_ms` argument is passed in by the caller (from `self.last_erosion_signal_ms`)
    /// to avoid borrow conflicts on the engine struct.
    pub fn evaluate_leg2(
        &self,
        state: &MarketState,
        snap: &ErosionSnap,
        last_erosion_ms: u64,
        now_ms: u64,
    ) -> Option<Leg2Decision> {
        // Gate: Leg 1 must be filled and Leg 2 must not be filled yet.
        let (leg1_price, leg1_size) = match &state.leg1_state {
            OrderState::Filled { price, size, .. } => (*price, *size),
            _ => return None,
        };
        if matches!(state.leg2_state, OrderState::Filled { .. }) {
            return None;
        }

        if snap.emergency_submitted {
            return None;
        }

        let reference_price = state.binance_price?;
        let tick = state.tick_size;
        let market_end_ms = state.market_end_timestamp_ms;
        let fee_rate_bps = state.fee_rate_bps;

        // Pre-compute book data.
        // Use the hedge book: the book for the token Leg 2 will buy (opposite of Leg 1).
        let hedge_book = match snap.direction {
            Direction::Up => state.poly_no_book.as_ref().or(state.poly_book.as_ref()),
            Direction::Down => state.poly_yes_book.as_ref().or(state.poly_book.as_ref()),
        };
        let hedge_book = hedge_book?;
        let hedge_book_snapshot = Some(hedge_book.clone());
        let best_ask_price = hedge_book.best_ask().map(|a| a.price);
        let ask_depth_2tick: Decimal = {
            let ba = hedge_book
                .best_ask()
                .map(|a| a.price)
                .unwrap_or(Decimal::ONE);
            hedge_book
                .asks
                .iter()
                .take_while(|l| l.price <= ba + tick * Decimal::TWO)
                .map(|l| l.size)
                .sum()
        };
        let wall_on_ask = detect_depth_wall(hedge_book, Side::Sell, self.depth_wall_multiplier);
        let hedge_token_id = match snap.direction {
            Direction::Up => state.active_no_token_id.as_ref()?.clone(),
            Direction::Down => state.active_yes_token_id.as_ref()?.clone(),
        };

        // ── Emergency deadline ────────────────────────────────────────────
        let emergency_deadline_ms =
            market_end_ms.saturating_sub(self.emergency_deadline_secs * 1_000);
        if now_ms >= emergency_deadline_ms {
            warn!(
                deadline_ms = emergency_deadline_ms,
                "emergency deadline — emitting FOK Leg 2"
            );
            let fok_size = leg1_size.min(ask_depth_2tick);
            if fok_size <= Decimal::ZERO {
                warn!("no ask depth for emergency FOK");
                return None;
            }
            let price = best_ask_price?;
            warn!(%fok_size, %price, order_type = "FOK", "emergency deadline FOK Leg 2");
            let signal = make_leg2_signal(
                &hedge_token_id,
                price,
                fok_size,
                reference_price,
                snap.confidence,
                snap.tier,
                Decimal::ZERO,
                snap.direction,
                snap.spike_info,
                leg1_price,
                now_ms,
                market_end_ms,
                tick,
                fee_rate_bps,
                false,
                hedge_book_snapshot.clone(),
            );
            return Some(Leg2Decision::Emergency {
                signal,
                price,
                size: fok_size,
            });
        }

        // ── Break-even breach (only after grace period) ──────────────────
        if now_ms >= snap.adverse_grace_expiry_ms {
            if let Some(ask_price) = best_ask_price {
                if leg1_price + ask_price >= Decimal::ONE {
                    warn!(%leg1_price, %ask_price, "hedge cost >= break-even — force FOK");
                    let fok_size = leg1_size.min(ask_depth_2tick);
                    if fok_size <= Decimal::ZERO {
                        warn!("no ask depth for break-even FOK");
                        return None;
                    }
                    warn!(%fok_size, %ask_price, order_type = "FOK", "break-even breach FOK Leg 2");
                    let signal = make_leg2_signal(
                        &hedge_token_id,
                        ask_price,
                        fok_size,
                        reference_price,
                        snap.confidence,
                        snap.tier,
                        Decimal::ZERO,
                        snap.direction,
                        snap.spike_info,
                        leg1_price,
                        now_ms,
                        market_end_ms,
                        tick,
                        fee_rate_bps,
                        false,
                        hedge_book_snapshot.clone(),
                    );
                    return Some(Leg2Decision::Emergency {
                        signal,
                        price: ask_price,
                        size: fok_size,
                    });
                }
            }
        }

        // ── Adverse movement (post-grace) ─────────────────────────────────
        if now_ms >= snap.adverse_grace_expiry_ms {
            if let (Some(cur), Some(fill_p)) = (state.binance_price, snap.binance_at_fill) {
                if !fill_p.is_zero() {
                    let change = (cur - fill_p).abs() / fill_p;
                    let adverse = match snap.direction {
                        Direction::Up => cur < fill_p,
                        Direction::Down => cur > fill_p,
                    };
                    if adverse && change >= self.adverse_threshold {
                        warn!(%change, direction = ?snap.direction, "adverse movement — emitting FOK Leg 2");
                        let fok_size = leg1_size.min(ask_depth_2tick);
                        if fok_size <= Decimal::ZERO {
                            warn!("no ask depth for adverse FOK");
                            return None;
                        }
                        let price = best_ask_price?;
                        warn!(%fok_size, %price, order_type = "FOK", "adverse movement FOK Leg 2");
                        let signal = make_leg2_signal(
                            &hedge_token_id,
                            price,
                            fok_size,
                            reference_price,
                            snap.confidence,
                            snap.tier,
                            Decimal::ZERO,
                            snap.direction,
                            snap.spike_info,
                            leg1_price,
                            now_ms,
                            market_end_ms,
                            tick,
                            fee_rate_bps,
                            false,
                            hedge_book_snapshot.clone(),
                        );
                        return Some(Leg2Decision::Emergency {
                            signal,
                            price,
                            size: fok_size,
                        });
                    }
                }
            }
        }

        // ── Quick reversal (100ms window after fill) ──────────────────────
        if now_ms.saturating_sub(snap.fill_ms) <= 100 {
            if let (Some(cur), Some(fill_p)) = (state.binance_price, snap.binance_at_fill) {
                if !fill_p.is_zero() {
                    let change = (cur - fill_p) / fill_p;
                    let reversal = match snap.direction {
                        Direction::Up => change <= -self.quick_reversal_threshold,
                        Direction::Down => change >= self.quick_reversal_threshold,
                    };
                    if reversal {
                        debug!(%change, "quick reversal within 100ms — holding off Leg 2");
                        return None;
                    }
                }
            }
        }

        // ── Erosion timing gate ───────────────────────────────────────────
        let time_since_last = now_ms.saturating_sub(last_erosion_ms.max(snap.fill_ms));
        if time_since_last < self.erosion_interval_ms && last_erosion_ms > 0 {
            return None;
        }

        // ── Determine whether to advance erosion step ─────────────────────
        let advance_step = last_erosion_ms > 0
            && now_ms.saturating_sub(last_erosion_ms) >= self.erosion_interval_ms;

        let current_profit = snap.current_profit_target;
        let steps_now = snap.steps_applied + if advance_step { 1 } else { 0 };
        // Re-compute current profit if we are advancing.
        let current_profit = if advance_step {
            // Replicate ErosionState::current_profit_target logic with incremented steps.
            // step_size * (steps_applied + 1) → we need step_size from the snap.
            // ErosionState::step_size is not in ErosionSnap, so we derive it from
            // the difference between initial and current profit divided by steps
            // (or use snap.tier.step_size()).
            let step_size = snap.tier.step_size();
            (snap.tier.target_pct() - step_size * Decimal::from(steps_now)).max(Decimal::ZERO)
        } else {
            current_profit
        };

        let target_raw = Decimal::ONE - current_profit - leg1_price;
        let mut target_price = round_to_tick(target_raw, tick);

        // Don't cross ask (post-only constraint).
        if let Some(ask_price) = best_ask_price {
            if target_price >= ask_price {
                target_price = round_to_tick(ask_price - tick, tick);
                debug!(%target_price, best_ask = %ask_price, "Leg 2 target adjusted below ask");
            }
        }

        // Break-even floor.
        let be_floor = round_to_tick(snap.break_even, tick);
        if target_price > be_floor {
            debug!(%target_price, %be_floor, "Leg 2 clamped to break-even floor");
            target_price = be_floor;
        }

        // Smart outbidding.
        let bot_contested = wall_on_ask.is_some();
        if let Some(wall_price) = wall_on_ask {
            if wall_price <= target_price {
                let outbid = round_to_tick(wall_price - tick, tick);
                if leg1_price + outbid < Decimal::ONE
                    && best_ask_price.map_or(false, |a| outbid < a)
                {
                    debug!(%wall_price, %outbid, "Leg 2 smart outbid");
                    target_price = outbid;
                }
            }
        }

        info!(
            step = steps_now,
            %target_price,
            profit_pct = %(current_profit.to_f64().unwrap_or(0.0) * 100.0),
            tier = snap.tier.label(),
            "Leg 2 erosion signal"
        );

        let signal = make_leg2_signal(
            &hedge_token_id,
            target_price,
            leg1_size,
            reference_price,
            snap.confidence,
            snap.tier,
            current_profit,
            snap.direction,
            snap.spike_info,
            leg1_price,
            now_ms,
            market_end_ms,
            tick,
            fee_rate_bps,
            bot_contested,
            hedge_book_snapshot,
        );
        Some(Leg2Decision::Erosion {
            signal,
            price: target_price,
            size: leg1_size,
            advance_step,
        })
    }
}

// ─── Decision types ───────────────────────────────────────────────────────────

/// The outcome of [`Leg2Evaluator::evaluate_leg2`].
///
/// Carries the signal plus enough metadata for the caller to apply the correct
/// state mutations without re-reading fields.
pub(crate) enum Leg2Decision {
    /// A normal post-only erosion bid.
    Erosion {
        signal: TradeSignal,
        price: Decimal,
        size: Decimal,
        /// `true` if the evaluator determined that `erosion.steps_applied` should be
        /// incremented by 1 (2s interval elapsed since last signal).
        advance_step: bool,
    },
    /// An emergency FOK fill (deadline, break-even breach, or adverse movement).
    Emergency {
        signal: TradeSignal,
        price: Decimal,
        size: Decimal,
    },
}

impl Leg2Decision {
    pub fn into_signal(self) -> TradeSignal {
        match self {
            Leg2Decision::Erosion { signal, .. } => signal,
            Leg2Decision::Emergency { signal, .. } => signal,
        }
    }

    pub fn is_emergency(&self) -> bool {
        matches!(self, Leg2Decision::Emergency { .. })
    }

    pub fn price(&self) -> Decimal {
        match self {
            Leg2Decision::Erosion { price, .. } => *price,
            Leg2Decision::Emergency { price, .. } => *price,
        }
    }

    pub fn size(&self) -> Decimal {
        match self {
            Leg2Decision::Erosion { size, .. } => *size,
            Leg2Decision::Emergency { size, .. } => *size,
        }
    }
}

// ─── Signal builder (free function to avoid borrow conflicts) ─────────────────

/// Build a Leg 2 [`TradeSignal`] from its constituent parts.
///
/// This is a free function (not a method) to avoid holding a mutable borrow on the
/// engine while reading book data.
#[allow(clippy::too_many_arguments)]
pub(crate) fn make_leg2_signal(
    token_id: &str,
    price: Decimal,
    size: Decimal,
    reference_price: Decimal,
    confidence: Decimal,
    tier: ProfitTier,
    profit_target_pct: Decimal,
    direction: Direction,
    spike_info: SpikeInfo,
    leg1_fill_price: Decimal,
    now_ms: u64,
    market_end_ms: u64,
    tick_size: Decimal,
    fee_rate_bps: u16,
    bot_contested: bool,
    book_snapshot: Option<OrderBook>,
) -> TradeSignal {
    TradeSignal {
        side: Side::Buy,
        token_id: token_id.to_string(),
        price,
        size,
        reference_price,
        confidence,
        profit_target_tier: tier,
        profit_target_pct,
        alloc_amount: leg1_fill_price * size,
        direction,
        spike_info,
        is_leg2: true,
        leg1_fill_price: Some(leg1_fill_price),
        entry_timestamp_ms: now_ms,
        market_end_timestamp_ms: market_end_ms,
        tick_size,
        fee_rate_bps,
        bot_contested,
        book_snapshot,
    }
}

// ─── Depth wall detection ─────────────────────────────────────────────────────

/// Detect a depth wall on the given side of the order book.
///
/// A wall is a single price level whose size exceeds `wall_multiplier` times the
/// average size of all OTHER levels on the same side — indicating a competitor's
/// intentional liquidity wall.
pub fn detect_depth_wall(
    book: &OrderBook,
    side: Side,
    wall_multiplier: Decimal,
) -> Option<Decimal> {
    let levels = match side {
        Side::Buy => &book.bids,
        Side::Sell => &book.asks,
    };
    if levels.len() < 2 {
        return None;
    }
    let total_size: Decimal = levels.iter().map(|l| l.size).sum();
    let n = Decimal::from(levels.len());

    for level in levels {
        let others_sum = total_size - level.size;
        let others_count = n - Decimal::ONE;
        if others_count.is_zero() {
            continue;
        }
        let avg_others = others_sum / others_count;
        if avg_others.is_zero() {
            continue;
        }
        if level.size > wall_multiplier * avg_others {
            debug!(price = %level.price, size = %level.size, avg_others = %avg_others, "depth wall detected");
            return Some(level.price);
        }
    }
    None
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::market::PriceLevel;

    fn make_book(bid: &str, ask: &str) -> OrderBook {
        OrderBook {
            asset_id: "yes".to_string(),
            bids: vec![PriceLevel {
                price: bid.parse().unwrap(),
                size: Decimal::new(500, 0),
            }],
            asks: vec![PriceLevel {
                price: ask.parse().unwrap(),
                size: Decimal::new(500, 0),
            }],
            timestamp_ms: 0,
        }
    }

    // ── detect_depth_wall ─────────────────────────────────────────────────

    #[test]
    fn test_no_wall_uniform_book() {
        let book = OrderBook {
            asset_id: "t".into(),
            bids: vec![
                PriceLevel {
                    price: Decimal::new(50, 2),
                    size: Decimal::new(100, 0),
                },
                PriceLevel {
                    price: Decimal::new(49, 2),
                    size: Decimal::new(90, 0),
                },
                PriceLevel {
                    price: Decimal::new(48, 2),
                    size: Decimal::new(110, 0),
                },
            ],
            asks: vec![],
            timestamp_ms: 0,
        };
        assert!(detect_depth_wall(&book, Side::Buy, Decimal::new(4, 0)).is_none());
    }

    #[test]
    fn test_wall_detected() {
        let book = OrderBook {
            asset_id: "t".into(),
            bids: vec![
                PriceLevel {
                    price: Decimal::new(52, 2),
                    size: Decimal::new(500, 0),
                }, // wall
                PriceLevel {
                    price: Decimal::new(51, 2),
                    size: Decimal::new(20, 0),
                },
                PriceLevel {
                    price: Decimal::new(50, 2),
                    size: Decimal::new(15, 0),
                },
                PriceLevel {
                    price: Decimal::new(49, 2),
                    size: Decimal::new(25, 0),
                },
            ],
            asks: vec![],
            timestamp_ms: 0,
        };
        assert_eq!(
            detect_depth_wall(&book, Side::Buy, Decimal::new(4, 0)),
            Some(Decimal::new(52, 2))
        );
    }

    #[test]
    fn test_single_level_no_wall() {
        let book = OrderBook {
            asset_id: "t".into(),
            bids: vec![PriceLevel {
                price: Decimal::new(50, 2),
                size: Decimal::new(100, 0),
            }],
            asks: vec![],
            timestamp_ms: 0,
        };
        assert!(detect_depth_wall(&book, Side::Buy, Decimal::new(4, 0)).is_none());
    }

    #[test]
    fn test_make_leg2_signal_fields() {
        let spike = SpikeInfo {
            direction: Direction::Up,
            magnitude: Decimal::new(5, 3),
            sustained_ms: 200,
            timestamp_ms: 0,
        };
        let book = make_book("0.30", "0.32");
        let signal = make_leg2_signal(
            "no_token",
            Decimal::new(30, 2),
            Decimal::new(100, 0),
            Decimal::new(50_000, 0),
            Decimal::new(75, 2),
            ProfitTier::Med,
            Decimal::new(15, 3),
            Direction::Up,
            spike,
            Decimal::new(70, 2),
            1_000,
            2_000,
            Decimal::new(1, 2),
            0,
            false,
            Some(book),
        );
        assert!(signal.is_leg2);
        assert_eq!(signal.token_id, "no_token");
        assert_eq!(signal.price, Decimal::new(30, 2));
        assert_eq!(signal.leg1_fill_price, Some(Decimal::new(70, 2)));
    }
}
