//! Signal evaluation logic for Leg 1 and Leg 2.
//!
//! This module provides [`Leg1Evaluator`] and [`Leg2Evaluator`], which encapsulate
//! the pure guard-checking and signal-building logic extracted from `StrategyEngine`.
//!
//! # Design Contract
//! - **No mutation** of shared state — evaluators only read market/hedge data.
//! - **Mutation stays in `strategy.rs`** — after `evaluate()` / `evaluate_leg2()` return
//!   `Some(signal)`, the caller is responsible for updating `leg1_state`, `leg2_state`,
//!   `spike_detected`, `cumulative_used`, and `last_hedge_signal_ms`.
//! - [`HedgeSnap`] is used to pass borrow-free snapshots of hedge state into the
//!   evaluator so it can read hedge fields without holding a mutable borrow on the engine.

use rust_decimal::Decimal;
use tracing::{debug, info, warn};

use crate::types::market::{Direction, MarketState, OrderBook, OrderState, SpikeInfo};
use crate::types::order::{ExitReason, ProfitTier, Side, TradeSignal};

use super::confidence::{compute_expected_repricing, round_to_tick};
use super::erosion::{HedgePhase, HedgeSnap};

// ─── Leg 1 outcome types ────────────────────────────────────────────────────

/// Reason a confirmed spike was blocked by [`Leg1Evaluator::evaluate`].
///
/// Only populated when `spike_detected = true`. Used by [`StrategyEngine`] to
/// track per-guard rejection counts in the `engine 60s` diagnostic log.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Leg1RejectReason {
    /// A Leg 1 trade is already in flight — spike discarded until it clears.
    ActiveTrade,
    /// Polymarket book missing or lacking a best bid/ask.
    NoBook,
    /// No current Binance reference price available.
    NoBinance,
    /// Polymarket book snapshot older than `stale_book_ms`.
    StaleBook,
    /// YES mid-price outside the tradeable range (too skewed toward resolution).
    PriceSkewed,
    /// Bid-ask spread exceeds `max_spread`.
    SpreadWide,
    /// Book depth insufficient relative to required trade size.
    InsufficientDepth,
    /// Model output below `min_reprice_pct` — insufficient expected repricing.
    InsufficientRepricing,
    /// Other guard failed (no active market, bid cap invalid, zero size, etc.).
    Other,
}

/// Outcome returned by [`Leg1Evaluator::evaluate`].
#[derive(Debug)]
pub(crate) enum Leg1Outcome {
    /// All guards passed — Leg 1 signal ready to emit.
    Signal(TradeSignal),
    /// A confirmed spike was present but blocked by a guard.
    /// The caller should clear `spike_detected` and record the reason.
    Rejected(Leg1RejectReason),
    /// No spike was present — nothing to count or act on.
    Skipped,
}

// ─── Leg 1 Evaluator ────────────────────────────────────────────────────────

/// Evaluates whether to emit a Leg 1 trade signal.
///
/// Holds only the config fields required for Leg 1 guard checks and signal building.
/// All reads are from borrowed `&MarketState`; no mutation occurs here.
pub(crate) struct Leg1Evaluator {
    pub max_spread: Decimal,
    pub entry_cutoff_secs: u64,
    pub depth_min_pct: Decimal,
    pub stale_book_ms: u64,
    pub max_alloc_per_trade: Decimal,
    pub depth_wall_multiplier: Decimal,
    pub leg1_timeout_ms: u64,
    #[allow(dead_code)] // kept as hard floor backstop — checked in spike detector
    pub min_magnitude_pct: Decimal,
    pub min_spike_atr_ratio: Decimal,
    pub strong_spike_atr_ratio: Decimal,
    // Repricing model fields
    pub reprice_scale: Decimal,
    pub min_reprice_pct: Decimal,
    pub min_alloc_pct: Decimal,
    pub hard_skew_cap: Decimal,
    pub time_exponent: f64,
    pub max_time_factor: f64,
}

impl Leg1Evaluator {
    /// Evaluate whether to emit a Leg 1 signal given the current market state.
    ///
    /// Returns [`Leg1Outcome::Signal`] when all pre-entry guards pass, [`Leg1Outcome::Rejected`]
    /// when a confirmed spike is present but blocked, or [`Leg1Outcome::Skipped`] when no
    /// spike is present (common case — caller should do nothing).
    ///
    /// # Important
    /// On `Signal` or `Rejected`, the caller **must** clear `spike_detected = false`.
    /// On `Signal`, the caller must also apply:
    /// - `state.leg1_state = OrderState::Posted { … }`
    /// - `state.cumulative_used += alloc`
    /// - `leg1_direction = Some(spike.direction)`
    pub fn evaluate(
        &self,
        state: &MarketState,
        now_ms: u64,
    ) -> Leg1Outcome {
        // Fast path: no spike — nothing to count or clear.
        if !state.spike_detected {
            return Leg1Outcome::Skipped;
        }
        let spike = match state.last_spike {
            Some(s) => s,
            None => return Leg1Outcome::Skipped,
        };

        // All returns from here are Rejected(reason) or Signal.

        // Require active market.
        let cond_id = match state.active_condition_id.as_ref() {
            Some(id) => id,
            None => {
                info!("spike detected but no active market — awaiting market rotation");
                return Leg1Outcome::Rejected(Leg1RejectReason::Other);
            }
        };

        // ── Diagnostic: log all guard states on spike detection ──────────
        let time_remaining_secs = state.time_remaining_ms(now_ms) / 1_000;
        let has_book = state.poly_book.is_some();
        let has_binance = state.binance_price.is_some();
        debug!(
            %cond_id,
            direction = ?spike.direction,
            has_book,
            has_binance,
            time_remaining_secs,
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
                        return Leg1Outcome::Rejected(Leg1RejectReason::NoBook);
                    }
                };
                let ask = match b.best_ask() {
                    Some(x) => x.price,
                    None => {
                        debug!("evaluate() BLOCKED: no best_ask in direction book");
                        return Leg1Outcome::Rejected(Leg1RejectReason::NoBook);
                    }
                };
                (b, bid, ask)
            } else {
                // Direction::Down with no NO book — derive NO prices from YES complement.
                let yes_b = match state.poly_yes_book.as_ref().or(state.poly_book.as_ref()) {
                    Some(b) => b,
                    None => {
                        debug!(direction = ?direction, "evaluate() BLOCKED: no book available");
                        return Leg1Outcome::Rejected(Leg1RejectReason::NoBook);
                    }
                };
                let (yes_bid, yes_ask) = match (yes_b.best_bid(), yes_b.best_ask()) {
                    (Some(b), Some(a)) => (b.price, a.price),
                    _ => {
                        debug!("evaluate() BLOCKED: YES book missing bid/ask for complement");
                        return Leg1Outcome::Rejected(Leg1RejectReason::NoBook);
                    }
                };
                (yes_b, Decimal::ONE - yes_ask, Decimal::ONE - yes_bid)
            }
        };
        let reference_price = match state.binance_price {
            Some(p) => p,
            None => {
                debug!("evaluate() BLOCKED: no binance_price");
                return Leg1Outcome::Rejected(Leg1RejectReason::NoBinance);
            }
        };

        // Guard: stale book
        let book_age_ms = now_ms.saturating_sub(book.timestamp_ms);
        if book_age_ms > self.stale_book_ms {
            debug!(book_age_ms, "evaluate() BLOCKED: poly book stale");
            return Leg1Outcome::Rejected(Leg1RejectReason::StaleBook);
        }

        // Guard: hard skew cap — reject if YES mid beyond safe range.
        let yes_mid = {
            let yes_book = state.poly_yes_book.as_ref().or(state.poly_book.as_ref());
            if let Some(yes_b) = yes_book {
                if let (Some(yes_bid_lvl), Some(yes_ask_lvl)) = (yes_b.best_bid(), yes_b.best_ask())
                {
                    let mid = (yes_bid_lvl.price + yes_ask_lvl.price) / Decimal::TWO;
                    let min_price = Decimal::ONE - self.hard_skew_cap;
                    if mid > self.hard_skew_cap || mid < min_price {
                        info!(%mid, hard_skew_cap = %self.hard_skew_cap,
                            "evaluate() BLOCKED: market price beyond hard skew cap");
                        return Leg1Outcome::Rejected(Leg1RejectReason::PriceSkewed);
                    }
                    mid
                } else {
                    Decimal::new(5, 1) // fallback 0.50 if no bid/ask
                }
            } else {
                Decimal::new(5, 1)
            }
        };

        // Guard: spread too wide (dollar-based)
        let spread = best_ask_price - best_bid_price;
        if spread > self.max_spread {
            debug!(%spread, "evaluate() BLOCKED: spread too wide");
            return Leg1Outcome::Rejected(Leg1RejectReason::SpreadWide);
        }

        // Guard: active trade — checked AFTER book/spread so rej_busy only counts spikes
        // that had a valid, tight book. Separates "genuinely good signal, executor busy"
        // from "spike on a bad book that would have been rejected anyway".
        if !matches!(state.leg1_state, OrderState::None) {
            return Leg1Outcome::Rejected(Leg1RejectReason::ActiveTrade);
        }

        // Guard: < entry_cutoff_secs remaining (defence-in-depth; normally caught upstream)
        if time_remaining_secs < self.entry_cutoff_secs {
            debug!(
                time_remaining_secs,
                "evaluate() BLOCKED: < entry_cutoff_secs remaining"
            );
            return Leg1Outcome::Rejected(Leg1RejectReason::Other);
        }

        // Repricing model — computes expected repricing percentage
        let expected_pct = compute_expected_repricing(
            spike.atr_ratio,
            self.min_spike_atr_ratio,
            self.strong_spike_atr_ratio,
            yes_mid,
            spike.direction,
            time_remaining_secs,
            self.reprice_scale,
            self.time_exponent,
            self.max_time_factor,
        );
        if expected_pct < self.min_reprice_pct {
            debug!(%expected_pct, min = %self.min_reprice_pct, "evaluate() BLOCKED: insufficient repricing");
            return Leg1Outcome::Rejected(Leg1RejectReason::InsufficientRepricing);
        }
        let alloc_fraction = (expected_pct / self.reprice_scale)
            .min(Decimal::ONE)
            .max(self.min_alloc_pct);
        let tier = ProfitTier::from_expected_reprice(expected_pct, self.reprice_scale);
        let tick = state.tick_size;
        let target_pct = round_to_tick(expected_pct, tick);

        // Allocation: dynamic fraction of max_alloc_per_trade, $0.01 floor
        let alloc = (self.max_alloc_per_trade * alloc_fraction)
            .round_dp(2)
            .max(Decimal::new(1, 2));

        // Guard: liquidity
        let required_depth = if !best_bid_price.is_zero() {
            alloc / best_bid_price
        } else {
            return Leg1Outcome::Rejected(Leg1RejectReason::NoBook);
        };
        if book.total_bid_depth() < required_depth * self.depth_min_pct {
            debug!("evaluate() BLOCKED: insufficient poly book depth");
            return Leg1Outcome::Rejected(Leg1RejectReason::InsufficientDepth);
        }

        // Token selection
        let token_id = match direction {
            Direction::Up => match state.active_yes_token_id.as_ref() {
                Some(id) => id,
                None => return Leg1Outcome::Rejected(Leg1RejectReason::Other),
            },
            Direction::Down => match state.active_no_token_id.as_ref() {
                Some(id) => id,
                None => return Leg1Outcome::Rejected(Leg1RejectReason::Other),
            },
        };

        // Leg 1 bid price — post just above the current best bid of the direction book.
        let mut bid_price = round_to_tick(best_bid_price + tick, tick);

        // Cap at one tick below ask if bid would cross (post-only constraint).
        if bid_price >= best_ask_price {
            let capped = round_to_tick(best_ask_price - tick, tick);
            if capped <= Decimal::ZERO || capped >= best_ask_price {
                debug!(%bid_price, %best_ask_price, "bid cap is zero or invalid — skipping");
                return Leg1Outcome::Rejected(Leg1RejectReason::Other);
            }
            bid_price = capped;
        }

        debug!(
            spike_direction = ?direction,
            token = %token_id,
            %best_bid_price,
            %best_ask_price,
            %bid_price,
            "Leg 1 evaluating entry"
        );

        // Smart outbidding
        let wall = detect_depth_wall(book, Side::Buy, self.depth_wall_multiplier);
        let bot_contested = wall.is_some();
        if let Some(wall_price) = wall.filter(|&wp| wp >= bid_price) {
            let outbid = round_to_tick(wall_price + tick, tick);
            if outbid < best_ask_price {
                debug!(%wall_price, %outbid, "Leg 1 smart outbid");
                bid_price = outbid;
            }
        }

        // Entry size — rounded to 2dp (Polymarket share precision).
        let entry_size = if !bid_price.is_zero() {
            (alloc / bid_price).round_dp(2)
        } else {
            return Leg1Outcome::Rejected(Leg1RejectReason::Other);
        };
        if entry_size <= Decimal::ZERO {
            return Leg1Outcome::Rejected(Leg1RejectReason::Other);
        }

        info!(
            direction = ?spike.direction, %expected_pct, %target_pct, tier = tier.label(),
            %yes_mid, %bid_price, %entry_size, %alloc, bot_contested, time_remaining_secs,
            "Leg 1 signal generated"
        );

        Leg1Outcome::Signal(TradeSignal {
            exit_reason: None,
            side: Side::Buy,
            token_id: token_id.clone(),
            condition_id: state.active_condition_id.clone().unwrap_or_default(),
            price: bid_price,
            size: entry_size,
            reference_price,
            expected_pct,
            profit_target_tier: tier,
            profit_target_pct: target_pct,
            alloc_amount: alloc,
            direction: spike.direction,
            spike_info: spike,
            is_leg2: false,
            leg1_fill_price: None,
            entry_timestamp_ms: now_ms,
            market_end_timestamp_ms: state.market_end_timestamp_ms,
            tick_size: tick,
            atr: state.atr.unwrap_or(Decimal::ZERO),
            bot_contested,
            best_ask: None,
            book_snapshot: match direction {
                Direction::Up => state.poly_yes_book.clone().or(state.poly_book.clone()),
                Direction::Down => state.poly_no_book.clone().or(state.poly_book.clone()),
            },
            sim_confirmed_fill: false,
            sim_was_taker: false,
        })
    }
}

// ─── Leg 2 Evaluator ────────────────────────────────────────────────────────

/// Evaluates whether to emit a Leg 2 hedge signal after Leg 1 fills.
///
/// Handles 2-phase hedge (profit target → break-even pursuit), Phase 1 breach,
/// and break-even breach emergency FOK fills.
///
/// All reads are from borrowed `&MarketState` and `&HedgeSnap`; no mutation occurs here.
pub(crate) struct Leg2Evaluator {
    pub phase1_timeout_ms: u64,
    pub phase1_breach_threshold: Decimal,
    pub phase2_timeout_ms: u64,
}

impl Leg2Evaluator {
    /// Evaluate whether to emit a Leg 2 signal.
    ///
    /// Returns `Some(Leg2Decision)` when action is needed (phase post, transition, emergency).
    ///
    /// # Important
    /// The caller **must** apply post-signal state mutations depending on decision type:
    /// - `Phase1Post`: `leg2_state = Posted { … }` (post once, then hold)
    /// - `Phase2Alongside`: `hedge.phase = Phase2`, `hedge.phase2_start_ms = Some(now)`
    /// - `Emergency`: `hedge.emergency_submitted = true`, `leg2_state = Posted { … }`
    ///
    pub fn evaluate_leg2(
        &self,
        state: &MarketState,
        snap: &HedgeSnap,
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

        let reference_price = state.binance_price?;
        let tick = state.tick_size;
        let market_end_ms = state.market_end_timestamp_ms;
        let atr = state.atr.unwrap_or(Decimal::ZERO);

        // Pre-compute book data BEFORE the emergency check — needed for reposts.
        // Use the hedge book: the book for the token Leg 2 will buy (opposite of Leg 1).
        let hedge_book = match snap.direction {
            Direction::Up => state.poly_no_book.as_ref().or(state.poly_book.as_ref()),
            Direction::Down => state.poly_yes_book.as_ref().or(state.poly_book.as_ref()),
        };
        let hedge_book = hedge_book?;
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
        let hedge_token_id = match snap.direction {
            Direction::Up => state.active_no_token_id.as_ref()?.clone(),
            Direction::Down => state.active_yes_token_id.as_ref()?.clone(),
        };

        // Emergency already submitted — no chase, just wait for executor.
        if snap.emergency_submitted {
            return None;
        }

        // ── Phase 1: Resting at profit target ────────────────────────────
        if snap.phase == HedgePhase::Phase1 {
            // Phase 1 breach: pair cost > threshold → immediate FOK taker at ask.
            if let Some(ask_price) = best_ask_price {
                if leg1_price + ask_price > self.phase1_breach_threshold {
                    let price = round_to_tick(ask_price, tick);
                    let fok_size = leg1_size.min(ask_depth_2tick).round_dp(2);
                    if fok_size <= Decimal::ZERO {
                        warn!(%leg1_price, %ask_price, "phase 1 breach — no ask depth for FOK");
                        return None;
                    }
                    warn!(
                        %leg1_price, %ask_price, threshold = %self.phase1_breach_threshold,
                        %fok_size, %price, "phase 1 breach — immediate FOK taker"
                    );
                    let mut signal = make_leg2_signal(
                        &hedge_token_id,
                        state.active_condition_id.as_deref().unwrap_or(""),
                        price,
                        fok_size,
                        reference_price,
                        snap.expected_pct,
                        snap.tier,
                        Decimal::ZERO,
                        snap.direction,
                        snap.spike_info,
                        leg1_price,
                        now_ms,
                        market_end_ms,
                        tick,
                        atr,
                        false,
                        best_ask_price,
                        Some(hedge_book.clone()),
                        Some(ExitReason::Phase1Breach),
                    );
                    signal.sim_was_taker = true;
                    return Some(Leg2Decision::Emergency {
                        signal,
                        price,
                        size: fok_size,
                    });
                }
            }

            // Phase 1 timeout: elapsed since Phase 1 post > phase1_timeout_ms → post Phase 2 alongside.
            // Use the actual Leg 2 post time (not Leg 1 fill time) so the CLOB round-trip
            // doesn't eat into the resting window. Falls back to fill_ms if not yet posted.
            let phase1_post_ms = match &state.leg2_state {
                OrderState::Posted { timestamp_ms, .. } => *timestamp_ms,
                _ => snap.fill_ms,
            };
            let elapsed = now_ms.saturating_sub(phase1_post_ms);
            if elapsed >= self.phase1_timeout_ms {
                if let Some(ask_price) = best_ask_price {
                    let phase2_price = round_to_tick(ask_price - tick, tick);
                    let breakeven_hedge_price = Decimal::ONE - leg1_price;
                    // Phase 2 entry guard: if even the best maker fill would give pair > $1.00,
                    // skip posting and FOK immediately.
                    if phase2_price > breakeven_hedge_price {
                        warn!(
                            %phase2_price, %breakeven_hedge_price, %leg1_price, %ask_price,
                            "phase 2 entry breach — ask-1tick > breakeven, immediate FOK"
                        );
                        let fok_price = round_to_tick(ask_price, tick);
                        let fok_size = leg1_size.min(ask_depth_2tick).round_dp(2);
                        if fok_size <= Decimal::ZERO {
                            warn!("phase 2 entry breach — no ask depth for FOK");
                            return None;
                        }
                        let mut signal = make_leg2_signal(
                            &hedge_token_id,
                            state.active_condition_id.as_deref().unwrap_or(""),
                            fok_price,
                            fok_size,
                            reference_price,
                            snap.expected_pct,
                            snap.tier,
                            Decimal::ZERO,
                            snap.direction,
                            snap.spike_info,
                            leg1_price,
                            now_ms,
                            market_end_ms,
                            tick,
                            atr,
                            false,
                            best_ask_price,
                            Some(hedge_book.clone()),
                            Some(ExitReason::BreakEvenBreach),
                        );
                        signal.sim_was_taker = true;
                        return Some(Leg2Decision::Emergency {
                            signal,
                            price: fok_price,
                            size: fok_size,
                        });
                    }
                    info!(elapsed_ms = elapsed, %phase2_price, "phase 1 timeout — posting phase 2 alongside");
                    let signal = make_leg2_signal(
                        &hedge_token_id,
                        state.active_condition_id.as_deref().unwrap_or(""),
                        phase2_price,
                        leg1_size,
                        reference_price,
                        snap.expected_pct,
                        snap.tier,
                        Decimal::ZERO,
                        snap.direction,
                        snap.spike_info,
                        leg1_price,
                        now_ms,
                        market_end_ms,
                        tick,
                        atr,
                        false,
                        best_ask_price,
                        Some(hedge_book.clone()),
                        None,
                    );
                    return Some(Leg2Decision::Phase2Alongside {
                        signal,
                        price: phase2_price,
                        size: leg1_size,
                        reason: TransitionReason::Timeout,
                    });
                }
            }

            // Phase 1: post once at profit target, then hold.
            // OrderFailed resets leg2_state to None, allowing retry.
            if matches!(state.leg2_state, OrderState::Posted { .. } | OrderState::Filled { .. }) {
                return None;
            }

            // Initial Phase 1 post at raw profit target.
            // If it crosses the book, the executor routes to favorable exit naturally.
            let target_price = snap.phase1_target_price;

            debug!(
                %target_price,
                tier = snap.tier.label(),
                "Leg 2 phase 1 post signal"
            );

            let signal = make_leg2_signal(
                &hedge_token_id,
                state.active_condition_id.as_deref().unwrap_or(""),
                target_price,
                leg1_size,
                reference_price,
                snap.expected_pct,
                snap.tier,
                snap.initial_profit_target,
                snap.direction,
                snap.spike_info,
                leg1_price,
                now_ms,
                market_end_ms,
                tick,
                atr,
                false,
                best_ask_price,
                Some(hedge_book.clone()),
                None,
            );
            return Some(Leg2Decision::Phase1Post {
                signal,
                price: target_price,
                size: leg1_size,
            });
        }

        // ── Phase 2: Break-even pursuit (ask-1tick), hold position ───────
        // Phase 2 entered via Phase2Alongside. No reposts — preserve FIFO.

        // Phase 2 timeout → FOK taker at best ask.
        if let Some(phase2_start) = snap.phase2_start_ms {
            let elapsed = now_ms.saturating_sub(phase2_start);
            if elapsed >= self.phase2_timeout_ms {
                if let Some(ask_price) = best_ask_price {
                    let price = round_to_tick(ask_price, tick);
                    warn!(elapsed_ms = elapsed, %price, "phase 2 timeout — FOK taker");
                    let mut signal = make_leg2_signal(
                        &hedge_token_id,
                        state.active_condition_id.as_deref().unwrap_or(""),
                        price,
                        leg1_size,
                        reference_price,
                        snap.expected_pct,
                        snap.tier,
                        Decimal::ZERO,
                        snap.direction,
                        snap.spike_info,
                        leg1_price,
                        now_ms,
                        market_end_ms,
                        tick,
                        atr,
                        false,
                        best_ask_price,
                        Some(hedge_book.clone()),
                        Some(ExitReason::Phase2Timeout),
                    );
                    signal.sim_was_taker = true;
                    return Some(Leg2Decision::Emergency {
                        signal,
                        price,
                        size: leg1_size,
                    });
                }
            }
        }

        // Phase 2 breach: if ask rises above the posted Phase 2 price, our order is deep
        // in the book and unlikely to fill — exit via FOK. Falls back to legacy BE breach
        // if no phase2_posted_price is available.
        if let Some(ask_price) = best_ask_price {
            let breach = snap.phase2_posted_price
                .is_some_and(|posted| ask_price > posted);
            if breach {
                let price = round_to_tick(ask_price, tick);
                let fok_size = leg1_size.min(ask_depth_2tick).round_dp(2);
                if fok_size <= Decimal::ZERO {
                    warn!(%leg1_price, %ask_price, "phase 2 breach — no ask depth for FOK");
                    return None;
                }
                warn!(
                    %leg1_price, %ask_price, phase2_posted = ?snap.phase2_posted_price,
                    %fok_size, %price, "phase 2 breach — immediate FOK taker"
                );
                let mut signal = make_leg2_signal(
                    &hedge_token_id,
                    state.active_condition_id.as_deref().unwrap_or(""),
                    price,
                    fok_size,
                    reference_price,
                    snap.expected_pct,
                    snap.tier,
                    Decimal::ZERO,
                    snap.direction,
                    snap.spike_info,
                    leg1_price,
                    now_ms,
                    market_end_ms,
                    tick,
                    atr,
                    false,
                    best_ask_price,
                    Some(hedge_book.clone()),
                    Some(ExitReason::Phase2PriceBreach),
                );
                signal.sim_was_taker = true;
                return Some(Leg2Decision::Emergency {
                    signal,
                    price,
                    size: fok_size,
                });
            }
        }

        // Hold position — preserve FIFO queue priority.
        None
    }
}

// ─── Decision types ───────────────────────────────────────────────────────────

/// The outcome of [`Leg2Evaluator::evaluate_leg2`].
///
/// Carries the signal plus enough metadata for the caller to apply the correct
/// state mutations without re-reading fields.
#[allow(dead_code)] // reason field on PhaseTransition used for diagnostic logging
pub(crate) enum Leg2Decision {
    /// Initial Phase 1 post at profit target price.
    Phase1Post {
        signal: TradeSignal,
        price: Decimal,
        size: Decimal,
    },
    /// Post Phase 2 alongside Phase 1 (dual-order): post at ask-1tick WITHOUT
    /// cancelling Phase 1. Both maker orders rest simultaneously.
    Phase2Alongside {
        signal: TradeSignal,
        price: Decimal,
        size: Decimal,
        reason: TransitionReason,
    },
    /// An emergency FOK fill (break-even breach, Phase 1 breach, Phase 2 timeout, or whipsaw).
    Emergency {
        signal: TradeSignal,
        price: Decimal,
        size: Decimal,
    },
}

/// Reason for transitioning from Phase 1 to Phase 2.
#[derive(Debug, Clone, Copy)]
pub(crate) enum TransitionReason {
    /// Phase 1 timeout elapsed without fill.
    Timeout,
}

impl Leg2Decision {
    pub fn into_signal(self) -> TradeSignal {
        match self {
            Leg2Decision::Phase1Post { signal, .. }
            | Leg2Decision::Phase2Alongside { signal, .. }
            | Leg2Decision::Emergency { signal, .. } => signal,
        }
    }

    pub fn is_emergency(&self) -> bool {
        matches!(self, Leg2Decision::Emergency { .. })
    }

    pub fn price(&self) -> Decimal {
        match self {
            Leg2Decision::Phase1Post { price, .. }
            | Leg2Decision::Phase2Alongside { price, .. }
            | Leg2Decision::Emergency { price, .. } => *price,
        }
    }

    pub fn size(&self) -> Decimal {
        match self {
            Leg2Decision::Phase1Post { size, .. }
            | Leg2Decision::Phase2Alongside { size, .. }
            | Leg2Decision::Emergency { size, .. } => *size,
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
    condition_id: &str,
    price: Decimal,
    size: Decimal,
    reference_price: Decimal,
    expected_pct: Decimal,
    tier: ProfitTier,
    profit_target_pct: Decimal,
    direction: Direction,
    spike_info: SpikeInfo,
    leg1_fill_price: Decimal,
    now_ms: u64,
    market_end_ms: u64,
    tick_size: Decimal,
    atr: Decimal,
    bot_contested: bool,
    best_ask: Option<Decimal>,
    book_snapshot: Option<OrderBook>,
    exit_reason: Option<ExitReason>,
) -> TradeSignal {
    TradeSignal {
        exit_reason,
        side: Side::Buy,
        token_id: token_id.to_string(),
        condition_id: condition_id.to_string(),
        price,
        size,
        reference_price,
        expected_pct,
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
        atr,
        bot_contested,
        best_ask,
        book_snapshot,
        sim_confirmed_fill: false,
        sim_was_taker: false,
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
            atr_ratio: Decimal::ZERO,
        };
        let book = make_book("0.30", "0.32");
        let signal = make_leg2_signal(
            "no_token",
            "test_condition",
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
            Decimal::ZERO,
            false,
            None,
            Some(book),
            None,
        );
        assert!(signal.is_leg2);
        assert_eq!(signal.token_id, "no_token");
        assert_eq!(signal.price, Decimal::new(30, 2));
        assert_eq!(signal.leg1_fill_price, Some(Decimal::new(70, 2)));
    }

    // ── Emergency repost interval + price tests ─────────────────────────

    /// Build a minimal MarketState + HedgeSnap suitable for emergency repost tests.
    /// Direction::Up → hedge token is NO → evaluator reads `poly_no_book`.
    fn make_leg2_test_setup(
        no_book_ask: &str,
        now_ms: u64,
    ) -> (MarketState, HedgeSnap, Leg2Evaluator) {
        use crate::types::market::OrderState;

        let tick = Decimal::new(1, 2); // 0.01

        let state = MarketState {
            poly_no_book: Some(OrderBook {
                asset_id: "no".to_string(),
                bids: vec![PriceLevel {
                    price: Decimal::new(40, 2),
                    size: Decimal::new(200, 0),
                }],
                asks: vec![PriceLevel {
                    price: no_book_ask.parse().unwrap(),
                    size: Decimal::new(200, 0),
                }],
                timestamp_ms: now_ms,
            }),
            poly_yes_book: Some(make_book("0.50", "0.52")),
            binance_price: Some(Decimal::new(50_000, 0)),
            active_condition_id: Some("cond".to_string()),
            active_yes_token_id: Some("yes".to_string()),
            active_no_token_id: Some("no".to_string()),
            tick_size: tick,
            market_end_timestamp_ms: now_ms + 600_000,
            leg1_state: OrderState::Filled {
                order_id: "sim-leg1".into(),
                price: Decimal::new(50, 2),
                size: Decimal::new(100, 0),
                fill_timestamp_ms: now_ms - 5_000,
            },
            leg2_state: OrderState::Posted {
                order_id: "sim-leg2".into(),
                price: Decimal::new(48, 2),
                size: Decimal::new(100, 0),
                timestamp_ms: now_ms - 1_000,
            },
            ..MarketState::default()
        };

        let snap = HedgeSnap {
            emergency_submitted: false,
            break_even: Decimal::new(50, 2),
            initial_profit_target: Decimal::new(25, 3), // 2.5%
            direction: Direction::Up,
            fill_ms: now_ms - 5_000,
            tier: ProfitTier::High,
            expected_pct: Decimal::new(7, 1),
            spike_info: SpikeInfo {
                direction: Direction::Up,
                magnitude: Decimal::new(5, 3),
                sustained_ms: 300,
                timestamp_ms: now_ms - 6_000,
                atr_ratio: Decimal::ZERO,
            },
            phase: HedgePhase::Phase1,
            phase1_target_price: Decimal::new(475, 3),
            phase2_start_ms: None,
            phase2_posted_price: None,
        };

        let evaluator = Leg2Evaluator {
            phase1_timeout_ms: 2000,
            phase1_breach_threshold: Decimal::new(105, 2),
            phase2_timeout_ms: 2000,
        };

        (state, snap, evaluator)
    }

    // ── Emergency submitted returns None (no chase) ───────────────────

    #[test]
    fn test_emergency_submitted_returns_none() {
        let now_ms = 100_000;
        let (_state, mut snap, evaluator) = make_leg2_test_setup("0.49", now_ms);
        snap.emergency_submitted = true;
        let state = {
            let (s, _, _) = make_leg2_test_setup("0.49", now_ms);
            s
        };
        let result = evaluator.evaluate_leg2(&state, &snap, now_ms);
        assert!(result.is_none(), "emergency_submitted should short-circuit to None");
    }

    // ── Phase 1 breach triggers immediate FOK ─────────────────────────

    #[test]
    fn test_phase1_breach_triggers_fok() {
        use crate::types::market::OrderState;
        let now_ms = 100_000;

        let (mut state, mut snap, evaluator) = make_leg2_test_setup("0.56", now_ms);
        snap.phase = HedgePhase::Phase1;
        state.leg2_state = OrderState::Posted {
            order_id: "sim-leg2".into(),
            price: Decimal::new(475, 3),
            size: Decimal::new(100, 0),
            timestamp_ms: now_ms - 1_000,
        };
        // leg1=0.50, ask=0.56 → pair=1.06 > 1.05 threshold → breach
        let result = evaluator.evaluate_leg2(&state, &snap, now_ms);
        assert!(result.is_some(), "phase 1 breach should trigger FOK");
        let decision = result.unwrap();
        assert!(decision.is_emergency(), "breach should be emergency (FOK)");
        // Price should be at ask: 0.56
        assert_eq!(decision.price(), Decimal::new(56, 2));
        let sig = decision.into_signal();
        assert_eq!(sig.exit_reason, Some(ExitReason::Phase1Breach));
        assert!(sig.sim_was_taker, "phase 1 breach FOK should be marked as taker");
    }

    // ── Phase 1 timeout triggers transition ───────────────────────────

    #[test]
    fn test_phase1_timeout_triggers_transition() {
        use crate::types::market::OrderState;
        let now_ms = 100_000;

        let (mut state, mut snap, evaluator) = make_leg2_test_setup("0.49", now_ms);
        snap.phase = HedgePhase::Phase1;
        // fill_ms 3s ago → past 2s timeout
        snap.fill_ms = now_ms - 3_000;
        state.leg2_state = OrderState::Posted {
            order_id: "sim-leg2".into(),
            price: Decimal::new(475, 3),
            size: Decimal::new(100, 0),
            timestamp_ms: now_ms - 2_000,
        };

        let result = evaluator.evaluate_leg2(&state, &snap, now_ms);
        assert!(result.is_some(), "timeout should trigger transition");
        let decision = result.unwrap();
        assert!(matches!(decision, Leg2Decision::Phase2Alongside { reason: TransitionReason::Timeout, .. }));
    }

    // ── Phase 2 BE breach triggers emergency ──────────────────────────

    #[test]
    fn test_phase2_be_breach_triggers_emergency() {
        use crate::types::market::OrderState;
        let now_ms = 100_000;

        let (mut state, mut snap, evaluator) = make_leg2_test_setup("0.51", now_ms);
        snap.phase = HedgePhase::Phase2;
        snap.phase2_start_ms = Some(now_ms - 500);
        snap.phase2_posted_price = Some(Decimal::new(50, 2)); // ask 0.51 > posted 0.50 → breach
        state.leg2_state = OrderState::Posted {
            order_id: "sim-leg2".into(),
            price: Decimal::new(48, 2),
            size: Decimal::new(100, 0),
            timestamp_ms: now_ms - 1_000,
        };
        // ask=0.51 > phase2_posted_price=0.50 → breach
        let result = evaluator.evaluate_leg2(&state, &snap, now_ms);
        assert!(result.is_some(), "BE breach should trigger emergency");
        let decision = result.unwrap();
        assert!(decision.is_emergency());
        // Price should be at ask (FOK): 0.51
        assert_eq!(decision.price(), Decimal::new(51, 2));
        let sig = decision.into_signal();
        assert_eq!(sig.exit_reason, Some(ExitReason::Phase2PriceBreach));
        assert!(sig.sim_was_taker, "BE breach FOK should be marked as taker");
    }

    // ── Phase 2 timeout triggers FOK ───────────────────────────────────

    #[test]
    fn test_phase2_timeout_triggers_fok() {
        use crate::types::market::OrderState;
        let now_ms = 100_000;

        let (mut state, mut snap, evaluator) = make_leg2_test_setup("0.49", now_ms);
        snap.phase = HedgePhase::Phase2;
        snap.phase2_start_ms = Some(now_ms - 3_000); // 3s ago > 2s timeout
        state.leg2_state = OrderState::Posted {
            order_id: "sim-leg2".into(),
            price: Decimal::new(48, 2),
            size: Decimal::new(100, 0),
            timestamp_ms: now_ms - 3_000,
        };
        let result = evaluator.evaluate_leg2(&state, &snap, now_ms);
        assert!(result.is_some(), "phase 2 timeout should trigger FOK");
        let decision = result.unwrap();
        assert!(decision.is_emergency());
        // Price should be at ask (FOK): 0.49
        assert_eq!(decision.price(), Decimal::new(49, 2));
        let sig = decision.into_signal();
        assert_eq!(sig.exit_reason, Some(ExitReason::Phase2Timeout));
        assert!(sig.sim_was_taker, "phase 2 timeout FOK should be marked as taker");
    }

    // ── Phase 2 holds position (no repost) ──────────────────────────────

    #[test]
    fn test_phase2_no_repost_on_price_improvement() {
        use crate::types::market::OrderState;
        let now_ms = 100_000;

        let (mut state, mut snap, evaluator) = make_leg2_test_setup("0.49", now_ms);
        snap.phase = HedgePhase::Phase2;
        snap.phase2_start_ms = Some(now_ms - 500); // within timeout
        state.leg2_state = OrderState::Posted {
            order_id: "sim-leg2".into(),
            price: Decimal::new(46, 2),
            size: Decimal::new(100, 0),
            timestamp_ms: now_ms - 1_000,
        };
        // ask=0.49 → previously would have triggered repost. Now should hold.
        let result = evaluator.evaluate_leg2(&state, &snap, now_ms);
        assert!(result.is_none(), "Phase 2 should hold position — no reposts");
    }

}
