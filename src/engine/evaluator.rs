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
use crate::executor::fill_engine::compute_maker_rebate;
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
    /// Bid-ask spread on directional book exceeds `max_entry_spread`.
    WideSpread,
    /// Model output below `min_reprice_pct` — insufficient expected repricing.
    InsufficientRepricing,
    /// Heartbeat is down — CLOB may have cancelled resting orders.
    /// Not constructed directly (heartbeat gate is in strategy.rs before evaluator),
    /// but kept for match exhaustiveness in rejection counting.
    #[allow(dead_code)]
    HeartbeatDown,
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
    pub entry_cutoff_secs: u64,
    pub stale_book_ms: u64,
    pub max_entry_spread: Decimal,
    pub max_alloc_per_trade: Decimal,
    // Repricing model fields
    pub reprice_scale: Decimal,
    pub min_reprice_pct: Decimal,
    pub min_alloc_pct: Decimal,
    pub hard_skew_cap: Decimal,
    pub time_exponent: f64,
    pub max_time_factor: f64,
    /// Phase 1 target dampening — only affects profit target, not entry gate or allocation.
    pub phase1_target_dampen: Decimal,
}

impl Leg1Evaluator {
    /// Evaluate whether to emit a Leg 1 signal given the current market state.
    ///
    /// Returns [`Leg1Outcome::Signal`] when all pre-entry guards pass, [`Leg1Outcome::Rejected`]
    /// when a confirmed buildup/spike is present but blocked, or [`Leg1Outcome::Skipped`] when no
    /// buildup is present (common case — caller should do nothing).
    ///
    /// # Important
    /// On `Signal` or `Rejected`, the caller **must** clear `buildup_detected = false`.
    /// On `Signal`, the caller must also apply:
    /// - `state.leg1_state = OrderState::Posted { … }`
    /// - `state.cumulative_used += alloc`
    /// - `leg1_direction = Some(buildup.direction)`
    pub fn evaluate(
        &self,
        state: &MarketState,
        now_ms: u64,
    ) -> Leg1Outcome {
        // Fast path: no buildup — nothing to count or clear.
        if !state.buildup_detected {
            return Leg1Outcome::Skipped;
        }
        let buildup = match &state.last_buildup {
            Some(b) => b,
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

        // ── Diagnostic: log all guard states on buildup detection ─────────
        let time_remaining_secs = state.time_remaining_ms(now_ms) / 1_000;
        let has_book = state.poly_book.is_some();
        let has_binance = state.binance_price.is_some();
        debug!(
            %cond_id,
            direction = ?buildup.direction,
            has_book,
            has_binance,
            time_remaining_secs,
            available_capital = %state.available_capital,
            "evaluate() — buildup detected, checking guards"
        );

        // Direction-aware book selection: use the book for the token we're buying.
        let direction = buildup.direction;
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

        // Guard: wide spread — reject if bid-ask spread on the directional book is too wide.
        // Prevents entries on illiquid/stale Polymarket books (e.g. off US market hours).
        let spread = best_ask_price - best_bid_price;
        if spread > self.max_entry_spread {
            debug!(%spread, max = %self.max_entry_spread, "evaluate() BLOCKED: spread too wide");
            return Leg1Outcome::Rejected(Leg1RejectReason::WideSpread);
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

        // Guard: active trade — checked AFTER book so rej_busy only counts spikes
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

        // Repricing model Phase A — uses composite score as signal strength.
        // The composite score is already [0,1]-normalized by the BuildupDetector,
        // so we pass min=0, strong=1 (identity pass-through to norm_signal).
        let expected_pct = compute_expected_repricing(
            buildup.composite_score,
            Decimal::ZERO,
            Decimal::ONE,
            yes_mid,
            buildup.direction,
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
        // Dampening: Phase 1 target uses dampened expected_pct for achievable pricing.
        // Entry gate and allocation use raw expected_pct (undampened).
        let target_pct = round_to_tick(expected_pct * self.phase1_target_dampen, tick);

        // Allocation: dynamic fraction of max_alloc_per_trade, $0.01 floor
        let alloc = (self.max_alloc_per_trade * alloc_fraction)
            .round_dp(2)
            .max(Decimal::new(1, 2));

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

        // Leg 1 maker price — one tick below the best ask.
        // Posting AT the ask crosses the book when resting sells exist there (post-only rejected).
        // Posting at ask-1-tick places us inside the spread as a resting bid: valid maker order.
        let ask_price = (best_ask_price - tick).max(tick);

        debug!(
            spike_direction = ?direction,
            token = %token_id,
            %best_bid_price,
            %best_ask_price,
            %ask_price,
            "Leg 1 evaluating entry"
        );

        let bot_contested = false;

        // Entry size — rounded to 2dp (Polymarket share precision).
        // Clamped to CLOB minimums: ≥5 shares (maker) and ≥ ceil($1/price) (FOK notional).
        let entry_size = if !ask_price.is_zero() {
            let raw = (alloc / ask_price).round_dp(2);
            let min_notional_size = (Decimal::ONE / ask_price).ceil();
            raw.max(Decimal::new(5, 0)).max(min_notional_size)
        } else {
            return Leg1Outcome::Rejected(Leg1RejectReason::Other);
        };
        if entry_size <= Decimal::ZERO {
            return Leg1Outcome::Rejected(Leg1RejectReason::Other);
        }

        // Recalculate alloc to match clamped size (in case clamp fired).
        let alloc = (entry_size * ask_price).round_dp(2);

        info!(
            direction = ?buildup.direction, %expected_pct, %target_pct, tier = tier.label(),
            %yes_mid, %ask_price, %entry_size, %alloc, bot_contested, time_remaining_secs,
            composite_score = %buildup.composite_score,
            "Leg 1 signal generated"
        );

        // Synthesize SpikeInfo from BuildupInfo for downstream backward compatibility
        // (HedgeState, Telegram, executor logging). Removed in Phase 11.
        let spike_compat = SpikeInfo {
            direction: buildup.direction,
            magnitude: buildup.atr_displacement,
            timestamp_ms: buildup.timestamp_ms,
            atr_ratio: buildup.signal_atr_ratio,
            obi: buildup.obi,
            sustained_ms: 0,
        };

        Leg1Outcome::Signal(TradeSignal {
            exit_reason: None,
            side: Side::Buy,
            token_id: token_id.clone(),
            condition_id: state.active_condition_id.clone().unwrap_or_default(),
            price: ask_price,
            size: entry_size,
            reference_price,
            expected_pct,
            profit_target_tier: tier,
            profit_target_pct: target_pct,
            alloc_amount: alloc,
            direction: buildup.direction,
            spike_info: spike_compat,
            is_leg2: false,
            leg1_fill_price: None,
            entry_timestamp_ms: now_ms,
            market_end_timestamp_ms: state.market_end_timestamp_ms,
            tick_size: tick,
            atr: state.atr.unwrap_or(Decimal::ZERO),
            bot_contested,
            leg1_fee: -compute_maker_rebate(ask_price, entry_size),
            best_ask: Some(best_ask_price),
            book_snapshot: match direction {
                Direction::Up => state.poly_yes_book.clone().or(state.poly_book.clone()),
                Direction::Down => state.poly_no_book.clone().or(state.poly_book.clone()),
            },
            buildup_info: Some(buildup.clone()),
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
    /// Buildup entry threshold (from BuildupConfig) — flow below this triggers Phase 2 tighten.
    pub entry_threshold: Decimal,
    /// Cancel threshold — flow below this triggers emergency FOK.
    pub cancel_threshold: Decimal,
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

        // Subtract any partial fills already accumulated on Leg 2.
        // This ensures subsequent Leg 2 signals post for the remainder only.
        let leg1_size = leg1_size - state.leg2_partial_filled;
        if leg1_size <= Decimal::ZERO {
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
                    let fok_size = leg1_size.round_dp(2);
                    if fok_size <= Decimal::ZERO {
                        warn!(%leg1_price, %ask_price, "phase 1 breach — zero size for FOK");
                        return None;
                    }
                    warn!(
                        %leg1_price, %ask_price, threshold = %self.phase1_breach_threshold,
                        %fok_size, %price, "phase 1 breach — immediate FOK taker"
                    );
                    let signal = make_leg2_signal(
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
                    return Some(Leg2Decision::Emergency {
                        signal,
                        price,
                        size: fok_size,
                    });
                }
            }

            // ── Flow-based graduated response (fires faster than time-based backstops) ──
            if snap.flow_monitoring_active {
                let score = snap.last_flow_score;

                // Flow reversal → immediate emergency FOK (faster whipsaw detection).
                if let Some(flow_dir) = snap.last_flow_direction {
                    if flow_dir != snap.direction && score > self.cancel_threshold {
                        if let Some(ask_price) = best_ask_price {
                            let price = round_to_tick(ask_price, tick);
                            let fok_size = leg1_size.round_dp(2);
                            if fok_size > Decimal::ZERO {
                                warn!(
                                    %score, flow_direction = ?flow_dir, hedge_direction = ?snap.direction,
                                    %price, "flow reversal — immediate FOK"
                                );
                                let signal = make_leg2_signal(
                                    &hedge_token_id,
                                    state.active_condition_id.as_deref().unwrap_or(""),
                                    price, fok_size, reference_price,
                                    snap.expected_pct, snap.tier, Decimal::ZERO,
                                    snap.direction, snap.spike_info, leg1_price,
                                    now_ms, market_end_ms, tick, atr, false,
                                    best_ask_price, Some(hedge_book.clone()),
                                    Some(ExitReason::WhipsawReversal),
                                );
                                return Some(Leg2Decision::Emergency { signal, price, size: fok_size });
                            }
                        }
                    }
                }

                // Flow collapsed → emergency FOK.
                if score < self.cancel_threshold {
                    if let Some(ask_price) = best_ask_price {
                        let price = round_to_tick(ask_price, tick);
                        let fok_size = leg1_size.round_dp(2);
                        if fok_size > Decimal::ZERO {
                            warn!(
                                %score, threshold = %self.cancel_threshold,
                                %price, "flow collapsed — immediate FOK"
                            );
                            let signal = make_leg2_signal(
                                &hedge_token_id,
                                state.active_condition_id.as_deref().unwrap_or(""),
                                price, fok_size, reference_price,
                                snap.expected_pct, snap.tier, Decimal::ZERO,
                                snap.direction, snap.spike_info, leg1_price,
                                now_ms, market_end_ms, tick, atr, false,
                                best_ask_price, Some(hedge_book.clone()),
                                Some(ExitReason::FlowCollapse),
                            );
                            return Some(Leg2Decision::Emergency { signal, price, size: fok_size });
                        }
                    }
                }

                // Flow weakening → tighten (cancel Phase 1, post Phase 2).
                if score < self.entry_threshold {
                    if let Some(ask_price) = best_ask_price {
                        let phase2_price = round_to_tick(ask_price - tick, tick);
                        let breakeven_hedge_price = Decimal::ONE - leg1_price;
                        if phase2_price > breakeven_hedge_price {
                            // Entry guard: Phase 2 price > breakeven → FOK.
                            let fok_price = round_to_tick(ask_price, tick);
                            let fok_size = leg1_size.round_dp(2);
                            if fok_size > Decimal::ZERO {
                                let signal = make_leg2_signal(
                                    &hedge_token_id,
                                    state.active_condition_id.as_deref().unwrap_or(""),
                                    fok_price, fok_size, reference_price,
                                    snap.expected_pct, snap.tier, Decimal::ZERO,
                                    snap.direction, snap.spike_info, leg1_price,
                                    now_ms, market_end_ms, tick, atr, false,
                                    best_ask_price, Some(hedge_book.clone()),
                                    Some(ExitReason::BreakEvenBreach),
                                );
                                return Some(Leg2Decision::Emergency { signal, price: fok_price, size: fok_size });
                            }
                        } else {
                            info!(%score, threshold = %self.entry_threshold, %phase2_price,
                                "flow weakening — cancelling Phase 1, posting Phase 2");
                            let signal = make_leg2_signal(
                                &hedge_token_id,
                                state.active_condition_id.as_deref().unwrap_or(""),
                                phase2_price, leg1_size, reference_price,
                                snap.expected_pct, snap.tier, Decimal::ZERO,
                                snap.direction, snap.spike_info, leg1_price,
                                now_ms, market_end_ms, tick, atr, false,
                                best_ask_price, Some(hedge_book.clone()),
                                None,
                            );
                            return Some(Leg2Decision::Phase2Alongside {
                                signal, price: phase2_price, size: leg1_size,
                                reason: TransitionReason::FlowWeakening,
                            });
                        }
                    }
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
                        let fok_size = leg1_size.round_dp(2);
                        if fok_size <= Decimal::ZERO {
                            warn!("phase 2 entry breach — zero size for FOK");
                            return None;
                        }
                        let signal = make_leg2_signal(
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

                        return Some(Leg2Decision::Emergency {
                            signal,
                            price: fok_price,
                            size: fok_size,
                        });
                    }
                    info!(elapsed_ms = elapsed, %phase2_price, "phase 1 timeout — cancelling Phase 1, posting Phase 2");
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
                    let signal = make_leg2_signal(
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
                let fok_size = leg1_size.round_dp(2);
                if fok_size <= Decimal::ZERO {
                    warn!(%leg1_price, %ask_price, "phase 2 breach — zero size for FOK");
                    return None;
                }
                warn!(
                    %leg1_price, %ask_price, phase2_posted = ?snap.phase2_posted_price,
                    %fok_size, %price, "phase 2 breach — immediate FOK taker"
                );
                let signal = make_leg2_signal(
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

                return Some(Leg2Decision::Emergency {
                    signal,
                    price,
                    size: fok_size,
                });
            }
        }

        // Phase 2 initial post: Phase 1 was cancelled, Phase 2 not yet posted (or post failed).
        // Re-post maker at ask-1tick. Reuses Phase1Post decision — engine/executor path is
        // identical; the fact that we're in Phase 2 is tracked by hedge.phase.
        if matches!(state.leg2_state, OrderState::None)
            && let Some(ask_price) = best_ask_price
        {
            let phase2_price = round_to_tick(ask_price - tick, tick);
            let breakeven_hedge_price = Decimal::ONE - leg1_price;
            if phase2_price > breakeven_hedge_price {
                // Phase 2 price above breakeven — FOK immediately.
                let fok_price = round_to_tick(ask_price, tick);
                let fok_size = leg1_size.round_dp(2);
                if fok_size > Decimal::ZERO {
                    warn!(
                        %phase2_price, %breakeven_hedge_price,
                        "Phase 2 initial post breach — immediate FOK"
                    );
                    let signal = make_leg2_signal(
                        &hedge_token_id,
                        state.active_condition_id.as_deref().unwrap_or(""),
                        fok_price, fok_size, reference_price,
                        snap.expected_pct, snap.tier, Decimal::ZERO,
                        snap.direction, snap.spike_info, leg1_price,
                        now_ms, market_end_ms, tick, atr, false,
                        best_ask_price, Some(hedge_book.clone()),
                        Some(ExitReason::BreakEvenBreach),
                    );
                    return Some(Leg2Decision::Emergency { signal, price: fok_price, size: fok_size });
                }
            }
            info!(%phase2_price, "Phase 2 initial post at ask-1tick");
            let signal = make_leg2_signal(
                &hedge_token_id,
                state.active_condition_id.as_deref().unwrap_or(""),
                phase2_price, leg1_size, reference_price,
                snap.expected_pct, snap.tier, Decimal::ZERO,
                snap.direction, snap.spike_info, leg1_price,
                now_ms, market_end_ms, tick, atr, false,
                best_ask_price, Some(hedge_book.clone()),
                None,
            );
            return Some(Leg2Decision::Phase1Post { signal, price: phase2_price, size: leg1_size });
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
    /// Transition to Phase 2: cancel Phase 1, then post at ask-1tick.
    /// Sequential — only one maker order rests at a time.
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
    /// Composite flow score dropped below entry threshold (flow weakening).
    FlowWeakening,
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
        leg1_fee: Decimal::ZERO,
        best_ask,
        book_snapshot,
        buildup_info: None,
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "tests/evaluator_tests.rs"]
mod tests;
