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
use crate::types::order::{ExitReason, ProfitTier, Side, TradeSignal};

use super::confidence::{compute_confidence, round_to_tick};
use super::erosion::{ErosionSnap, ErosionState};

/// Triangle weights for erosion steps — mirrors `erosion.rs` constants.
const EROSION_WEIGHTS: [u32; 5] = [5, 4, 3, 2, 1];
const EROSION_WEIGHT_SUM: u32 = 15;

/// Cumulative erosion after `steps` steps, given `total_margin` (initial profit target).
fn cumulative_erosion_for(total_margin: Decimal, steps: u32) -> Decimal {
    (0..steps)
        .map(|s| {
            let w = EROSION_WEIGHTS.get(s as usize).copied().unwrap_or(1);
            total_margin * Decimal::from(w) / Decimal::from(EROSION_WEIGHT_SUM)
        })
        .sum()
}

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
    /// Bid-ask spread exceeds `max_spread_ticks`.
    SpreadWide,
    /// Book depth insufficient relative to required trade size.
    InsufficientDepth,
    /// Other guard failed (no active market, bid cap invalid, zero size, etc.).
    Other,
}

/// Outcome returned by [`Leg1Evaluator::evaluate`].
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
    pub max_spread_ticks: u32,
    pub entry_cutoff_secs: u64,
    pub depth_min_pct: Decimal,
    pub stale_book_ms: u64,
    pub max_alloc_per_trade: Decimal,
    pub high_alloc_pct: Decimal,
    pub med_alloc_pct: Decimal,
    pub low_alloc_pct: Decimal,
    pub depth_wall_multiplier: Decimal,
    pub high_threshold: Decimal,
    pub med_threshold: Decimal,
    pub high_target_pct: Decimal,
    pub med_target_pct: Decimal,
    pub low_target_pct: Decimal,
    pub max_price_skew: Decimal,
    pub leg1_timeout_ms: u64,
}

impl Leg1Evaluator {
    pub fn target_pct_for_tier(&self, tier: ProfitTier) -> Decimal {
        match tier {
            ProfitTier::High => self.high_target_pct,
            ProfitTier::Med => self.med_target_pct,
            ProfitTier::Low => self.low_target_pct,
        }
    }
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
        avg_book_depth: Option<Decimal>,
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

        // Guard: market price too skewed — avoid near-certain-resolution markets.
        // YES mid > max_price_skew or < (1 - max_price_skew) → one side illiquid.
        {
            let yes_book = state.poly_yes_book.as_ref().or(state.poly_book.as_ref());
            if let Some(yes_b) = yes_book {
                if let (Some(yes_bid_lvl), Some(yes_ask_lvl)) = (yes_b.best_bid(), yes_b.best_ask())
                {
                    let yes_mid = (yes_bid_lvl.price + yes_ask_lvl.price) / Decimal::TWO;
                    let min_price = Decimal::ONE - self.max_price_skew;
                    if yes_mid > self.max_price_skew || yes_mid < min_price {
                        info!(%yes_mid, max_skew = %self.max_price_skew,
                            "evaluate() BLOCKED: market price skewed beyond threshold");
                        return Leg1Outcome::Rejected(Leg1RejectReason::PriceSkewed);
                    }
                }
            }
        }

        // Guard: spread too wide (tick-based, uniform regardless of mid price)
        let tick = state.tick_size;
        let spread_ticks = if !tick.is_zero() {
            (best_ask_price - best_bid_price) / tick
        } else {
            Decimal::ZERO
        };
        if spread_ticks > Decimal::from(self.max_spread_ticks) {
            debug!(%spread_ticks, "evaluate() BLOCKED: spread too wide");
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

        // Confidence scoring
        let atr = state.atr.unwrap_or(Decimal::new(1, 3));
        let total_depth = book.total_bid_depth() + book.total_ask_depth();
        let avg_depth = avg_book_depth.unwrap_or(Decimal::ONE);
        let confidence = compute_confidence(
            spike.magnitude,
            atr,
            total_depth,
            avg_depth,
            time_remaining_secs,
        );
        let tier = ProfitTier::from_confidence(confidence, self.high_threshold, self.med_threshold);

        // Allocation: confidence tier fraction of max_alloc_per_trade, $1 floor, nearest dollar
        let tier_pct = match tier {
            ProfitTier::High => self.high_alloc_pct,
            ProfitTier::Med => self.med_alloc_pct,
            ProfitTier::Low => self.low_alloc_pct,
        };
        let alloc = (self.max_alloc_per_trade * tier_pct)
            .round_dp(0)
            .max(Decimal::ONE);

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
        let target_pct = self.target_pct_for_tier(tier);
        let wall = detect_depth_wall(book, Side::Buy, self.depth_wall_multiplier);
        let bot_contested = wall.is_some();
        if let Some(wall_price) = wall {
            if wall_price >= bid_price {
                let outbid = round_to_tick(wall_price + tick, tick);
                let be_cap = Decimal::ONE - (Decimal::ONE - target_pct - outbid) - tick;
                if outbid <= be_cap && outbid < best_ask_price {
                    debug!(%wall_price, %outbid, "Leg 1 smart outbid");
                    bid_price = outbid;
                }
            }
        }

        // Final break-even cap
        {
            let est_leg2 = Decimal::ONE - target_pct - bid_price;
            let be_cap = Decimal::ONE - est_leg2 - tick;
            if bid_price > be_cap {
                bid_price = round_to_tick(be_cap, tick);
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
            direction = ?spike.direction, %confidence, tier = tier.label(),
            %bid_price, %entry_size, %alloc, bot_contested, time_remaining_secs,
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
            confidence,
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
/// Handles post-only erosion cascade, adverse movement,
/// and break-even breach emergency FOK fills.
///
/// All reads are from borrowed `&MarketState` and `&ErosionSnap`; no mutation occurs here.
pub(crate) struct Leg2Evaluator {
    pub adverse_threshold: Decimal,
    pub erosion_base_interval_ms: u64,
    pub erosion_interval_decay: f64,
    pub depth_wall_multiplier: Decimal,
    pub emergency_deadline_ms: u64,
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

        // ── Emergency price-improvement chase with hard deadline ────────
        // Only repost when the book offers a strictly better price (preserves
        // FIFO queue priority). After emergency_deadline_ms → FOK taker.
        if snap.emergency_submitted {
            let best_ask = hedge_book.best_ask().map(|a| a.price)?;
            let exit_reason = snap.exit_reason.unwrap_or(ExitReason::BreakEvenBreach);

            // 1. Hard deadline check — if elapsed >= deadline, FOK taker at best_ask.
            let elapsed = snap
                .emergency_first_post_ms
                .map(|first| now_ms.saturating_sub(first))
                .unwrap_or(0);

            if elapsed >= self.emergency_deadline_ms {
                let leg1_size = match &state.leg1_state {
                    OrderState::Filled { size, .. } => *size,
                    _ => return None,
                };
                warn!(%best_ask, ?exit_reason, elapsed_ms = elapsed,
                    "emergency deadline reached — FOK taker fallback");
                let mut signal = make_leg2_signal(
                    &hedge_token_id,
                    state.active_condition_id.as_deref().unwrap_or(""),
                    best_ask,
                    leg1_size,
                    reference_price,
                    snap.confidence,
                    snap.tier,
                    Decimal::ZERO,
                    snap.direction,
                    snap.spike_info,
                    snap.leg1_fill_price,
                    now_ms,
                    market_end_ms,
                    tick,
                    atr,
                    false,
                    hedge_book_snapshot.clone(),
                    Some(exit_reason),
                );
                signal.sim_was_taker = true;
                return Some(Leg2Decision::Emergency {
                    signal,
                    price: best_ask,
                    size: leg1_size,
                });
            }

            // 2. Price-improvement check — only repost if new top-of-book is strictly better.
            let best_bid_we_can_post = round_to_tick(best_ask - tick, tick);
            let current_posted = snap.emergency_posted_price.unwrap_or(Decimal::ZERO);

            if best_bid_we_can_post > current_posted {
                let leg1_size = match &state.leg1_state {
                    OrderState::Filled { size, .. } => *size,
                    _ => return None,
                };
                debug!(%best_bid_we_can_post, %current_posted, ?exit_reason,
                    "emergency price chase — reposting at improved price");
                let signal = make_leg2_signal(
                    &hedge_token_id,
                    state.active_condition_id.as_deref().unwrap_or(""),
                    best_bid_we_can_post,
                    leg1_size,
                    reference_price,
                    snap.confidence,
                    snap.tier,
                    Decimal::ZERO,
                    snap.direction,
                    snap.spike_info,
                    snap.leg1_fill_price,
                    now_ms,
                    market_end_ms,
                    tick,
                    atr,
                    false,
                    hedge_book_snapshot.clone(),
                    Some(exit_reason),
                );
                return Some(Leg2Decision::Emergency {
                    signal,
                    price: best_bid_we_can_post,
                    size: leg1_size,
                });
            }

            // 3. No improvement and within deadline — keep current order, preserve queue priority.
            return None;
        }

        // ── Adverse movement (immediate — ultimate safeguard, zero grace) ─
        // If Binance reverses > adverse_threshold at ANY point after Leg 1 fill,
        // the spike thesis is invalidated → emergency post-only at top-of-book.
        // FOK fallback after emergency_deadline_ms via the price-chase block.
        if let (Some(cur), Some(fill_p)) = (state.binance_price, snap.binance_at_fill) {
            if !fill_p.is_zero() {
                let change = (cur - fill_p).abs() / fill_p;
                let adverse = match snap.direction {
                    Direction::Up => cur < fill_p,
                    Direction::Down => cur > fill_p,
                };
                if adverse && change >= self.adverse_threshold {
                    let fok_size = leg1_size.min(ask_depth_2tick).round_dp(2);
                    if fok_size <= Decimal::ZERO {
                        warn!(%change, direction = ?snap.direction, "adverse movement — no ask depth for emergency");
                        return None;
                    }
                    let price = round_to_tick(best_ask_price? - tick, tick);
                    warn!(%change, direction = ?snap.direction, %fok_size, %price, "adverse movement — emergency post-only Leg 2");
                    let signal = make_leg2_signal(
                        &hedge_token_id,
                        state.active_condition_id.as_deref().unwrap_or(""),
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
                        atr,
                        false,
                        hedge_book_snapshot.clone(),
                        Some(ExitReason::AdverseMovement),
                    );
                    return Some(Leg2Decision::Emergency {
                        signal,
                        price,
                        size: fok_size,
                    });
                }
            }
        }

        // ── Break-even breach (after first erosion step) ─────────────────
        // Only fire after at least one erosion step has completed (~3.5s).
        // This gives the Polymarket book time to react to spike momentum.
        // Triggers when pair cost (leg1 + opposing ask) >= $1.00 — position is unprofitable.
        // Uses post-only pricing (best_ask - tick), consistent with all other emergencies.
        // FOK fallback after emergency_deadline_ms (via price-chase block).
        if snap.steps_applied >= 1 {
            if let Some(ask_price) = best_ask_price {
                if leg1_price + ask_price >= Decimal::ONE {
                    let price = round_to_tick(ask_price - tick, tick);
                    let fok_size = leg1_size.min(ask_depth_2tick).round_dp(2);
                    if fok_size <= Decimal::ZERO {
                        warn!(%leg1_price, %ask_price, "break-even breach — no ask depth for emergency");
                        return None;
                    }
                    warn!(%leg1_price, %ask_price, %fok_size, %price, "break-even breach emergency Leg 2");
                    let signal = make_leg2_signal(
                        &hedge_token_id,
                        state.active_condition_id.as_deref().unwrap_or(""),
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
                        atr,
                        false,
                        hedge_book_snapshot.clone(),
                        Some(ExitReason::BreakEvenBreach),
                    );
                    return Some(Leg2Decision::Emergency {
                        signal,
                        price,
                        size: fok_size,
                    });
                }
            }
        }

        // ── Erosion exhausted (all 5 steps applied, profit target = 0) ──
        // The cascade reached break-even without filling. Escalate to emergency
        // post-only at top of book. FOK fallback after N reposts (via repost block).
        if snap.is_exhausted() {
            if let Some(ask_price) = best_ask_price {
                let price = round_to_tick(ask_price - tick, tick);
                let fok_size = leg1_size.min(ask_depth_2tick).round_dp(2);
                if fok_size <= Decimal::ZERO {
                    warn!(
                        steps = snap.steps_applied,
                        "erosion exhausted — no ask depth for emergency"
                    );
                    return None;
                }
                warn!(
                    %price, %fok_size, steps = snap.steps_applied,
                    "erosion exhausted — escalating to BreakEvenBreach emergency"
                );
                let signal = make_leg2_signal(
                    &hedge_token_id,
                    state.active_condition_id.as_deref().unwrap_or(""),
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
                    atr,
                    false,
                    hedge_book_snapshot.clone(),
                    Some(ExitReason::BreakEvenBreach),
                );
                return Some(Leg2Decision::Emergency {
                    signal,
                    price,
                    size: fok_size,
                });
            }
        }

        // ── Erosion timing gate (exponential decay intervals) ────────────
        let current_interval = ErosionState::interval_for_step(
            snap.steps_applied,
            self.erosion_base_interval_ms,
            self.erosion_interval_decay,
        );
        let time_since_last = now_ms.saturating_sub(last_erosion_ms.max(snap.fill_ms));
        if time_since_last < current_interval && last_erosion_ms > 0 {
            return None;
        }

        // ── Determine whether to advance erosion step ─────────────────────
        let advance_step = last_erosion_ms > 0
            && now_ms.saturating_sub(last_erosion_ms) >= current_interval
            && !snap.is_exhausted();

        let current_profit = snap.current_profit_target;
        let steps_now = snap.steps_applied + if advance_step { 1 } else { 0 };
        // Re-compute current profit using triangle-weighted cumulative erosion.
        let current_profit = if advance_step {
            let total = snap.initial_profit_target;
            let eroded = cumulative_erosion_for(total, steps_now);
            (total - eroded).max(Decimal::ZERO)
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

        // ── Skip guard: don't repost if current order is already at or better ──
        if let OrderState::Posted { price: posted_price, .. } = &state.leg2_state {
            if *posted_price <= target_price {
                debug!(
                    %posted_price, %target_price,
                    "Leg 2 skip: posted price already at or better than erosion target"
                );
                return None;
            }
        }

        debug!(
            step = steps_now,
            %target_price,
            profit_pct = %(current_profit.to_f64().unwrap_or(0.0) * 100.0),
            tier = snap.tier.label(),
            "Leg 2 erosion signal"
        );

        let signal = make_leg2_signal(
            &hedge_token_id,
            state.active_condition_id.as_deref().unwrap_or(""),
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
            atr,
            bot_contested,
            hedge_book_snapshot,
            None,
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
    condition_id: &str,
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
    atr: Decimal,
    bot_contested: bool,
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
        atr,
        bot_contested,
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
            Some(book),
            None,
        );
        assert!(signal.is_leg2);
        assert_eq!(signal.token_id, "no_token");
        assert_eq!(signal.price, Decimal::new(30, 2));
        assert_eq!(signal.leg1_fill_price, Some(Decimal::new(70, 2)));
    }

    // ── Emergency repost interval + price tests ─────────────────────────

    /// Build a minimal MarketState + ErosionSnap suitable for emergency repost tests.
    /// Direction::Up → hedge token is NO → evaluator reads `poly_no_book`.
    fn make_emergency_test_setup(
        no_book_ask: &str,
        now_ms: u64,
    ) -> (MarketState, ErosionSnap, Leg2Evaluator) {
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
                order_id: "sim-leg2-emergency".into(),
                price: Decimal::new(48, 2),
                size: Decimal::new(100, 0),
                timestamp_ms: now_ms - 1_000,
            },
            ..MarketState::default()
        };

        let snap = ErosionSnap {
            emergency_submitted: true,
            break_even: Decimal::new(50, 2),
            current_profit_target: Decimal::ZERO,
            initial_profit_target: Decimal::new(25, 3), // 2.5%
            direction: Direction::Up,
            binance_at_fill: Some(Decimal::new(50_000, 0)),
            fill_ms: now_ms - 5_000,
            steps_applied: 2,
            tier: ProfitTier::High,
            confidence: Decimal::new(7, 1),
            spike_info: SpikeInfo {
                direction: Direction::Up,
                magnitude: Decimal::new(5, 3),
                sustained_ms: 300,
                timestamp_ms: now_ms - 6_000,
            },
            leg1_fill_price: Decimal::new(50, 2),
            exit_reason: Some(ExitReason::AdverseMovement),
            emergency_first_post_ms: Some(now_ms - 1_000),
            emergency_posted_price: Some(Decimal::new(48, 2)),
        };

        let evaluator = Leg2Evaluator {
            adverse_threshold: Decimal::new(3, 3),
            erosion_base_interval_ms: 4000,
            erosion_interval_decay: 0.6,
            depth_wall_multiplier: Decimal::new(4, 0),
            emergency_deadline_ms: 2500,
        };

        (state, snap, evaluator)
    }

    #[test]
    fn test_emergency_no_repost_without_price_improvement() {
        let now_ms = 100_000;
        // Posted at 0.48, ask at 0.49 → best_ask - tick = 0.48 == posted → no improvement.
        let (state, snap, evaluator) = make_emergency_test_setup("0.49", now_ms);
        let last_erosion_ms = now_ms - 200;
        let result = evaluator.evaluate_leg2(&state, &snap, last_erosion_ms, now_ms);
        assert!(
            result.is_none(),
            "should not repost when price hasn't improved (preserves queue priority)"
        );
    }

    #[test]
    fn test_emergency_price_chase_on_improvement() {
        let now_ms = 100_000;
        // Posted at 0.48, ask at 0.52 → best_ask - tick = 0.51 > 0.48 → price improved.
        let (state, snap, evaluator) = make_emergency_test_setup("0.52", now_ms);
        let last_erosion_ms = now_ms - 200;
        let decision = evaluator
            .evaluate_leg2(&state, &snap, last_erosion_ms, now_ms)
            .expect("should emit price-chase repost");
        assert!(decision.is_emergency());
        assert_eq!(
            decision.price(),
            Decimal::new(51, 2),
            "should repost at best_ask(0.52) - tick(0.01) = 0.51"
        );
        let sig = decision.into_signal();
        assert_eq!(sig.exit_reason, Some(ExitReason::AdverseMovement));
        assert!(!sig.sim_was_taker, "price-chase should be post-only (not taker)");
    }

    #[test]
    fn test_emergency_fok_at_deadline() {
        let now_ms = 100_000;
        let (state, mut snap, evaluator) = make_emergency_test_setup("0.49", now_ms);
        // Emergency started 3s ago → past 2.5s deadline.
        snap.emergency_first_post_ms = Some(now_ms - 3_000);
        let last_erosion_ms = now_ms - 200;
        let decision = evaluator
            .evaluate_leg2(&state, &snap, last_erosion_ms, now_ms)
            .expect("should emit FOK at deadline");
        assert!(decision.is_emergency());
        assert_eq!(
            decision.price(),
            Decimal::new(49, 2),
            "deadline FOK should be at best_ask (crosses spread)"
        );
        let sig = decision.into_signal();
        assert!(sig.sim_was_taker, "deadline FOK should be marked as taker");
    }

    #[test]
    fn test_emergency_no_fok_before_deadline() {
        let now_ms = 100_000;
        let (state, mut snap, evaluator) = make_emergency_test_setup("0.49", now_ms);
        // Emergency started 1s ago → within 2.5s deadline. Same price → no improvement.
        snap.emergency_first_post_ms = Some(now_ms - 1_000);
        let last_erosion_ms = now_ms - 200;
        let result = evaluator.evaluate_leg2(&state, &snap, last_erosion_ms, now_ms);
        assert!(
            result.is_none(),
            "should not FOK before deadline when no price improvement"
        );
    }

    // ── Erosion exhaustion triggers emergency ─────────────────────────

    #[test]
    fn test_erosion_exhausted_triggers_emergency() {
        use crate::types::market::OrderState;
        let now_ms = 100_000;

        let (mut state, mut snap, evaluator) = make_emergency_test_setup("0.49", now_ms);
        // Override: NOT in emergency mode, but all 5 steps applied.
        snap.emergency_submitted = false;
        snap.steps_applied = 5;
        snap.current_profit_target = Decimal::ZERO;
        snap.exit_reason = None;
        state.leg2_state = OrderState::Posted {
            order_id: "sim-leg2".into(),
            price: Decimal::new(48, 2),
            size: Decimal::new(100, 0),
            timestamp_ms: now_ms - 1_000,
        };

        let last_erosion_ms = now_ms - 5_000; // well past any interval
        let result = evaluator.evaluate_leg2(&state, &snap, last_erosion_ms, now_ms);
        assert!(
            result.is_some(),
            "should trigger emergency when erosion exhausted"
        );
        assert!(result.as_ref().unwrap().is_emergency());
        let sig = result.unwrap().into_signal();
        assert_eq!(sig.exit_reason, Some(ExitReason::BreakEvenBreach));
        // Price should be post-only: 0.49 - 0.01 = 0.48
        assert_eq!(sig.price, Decimal::new(48, 2));
    }

    // ── Adverse movement uses post-only price ─────────────────────────

    #[test]
    fn test_adverse_movement_uses_post_only_price() {
        use crate::types::market::OrderState;
        let now_ms = 100_000;

        let (mut state, mut snap, evaluator) = make_emergency_test_setup("0.49", now_ms);
        // NOT in emergency mode, normal erosion in progress.
        snap.emergency_submitted = false;
        snap.steps_applied = 1;
        snap.exit_reason = None;
        snap.current_profit_target = Decimal::new(15, 3);
        // Set Binance fill price and current — adverse movement > 0.3% reversal.
        snap.binance_at_fill = Some(Decimal::new(50_000, 0));
        state.binance_price = Some(Decimal::new(49_800, 0)); // 0.4% drop → adverse
        state.leg2_state = OrderState::Posted {
            order_id: "sim-leg2".into(),
            price: Decimal::new(48, 2),
            size: Decimal::new(100, 0),
            timestamp_ms: now_ms - 1_000,
        };

        let last_erosion_ms = now_ms - 5_000;
        let result = evaluator.evaluate_leg2(&state, &snap, last_erosion_ms, now_ms);
        assert!(result.is_some(), "adverse should trigger");
        let decision = result.unwrap();
        assert!(decision.is_emergency());
        // Price should be post-only: best_ask(0.49) - tick(0.01) = 0.48
        assert_eq!(
            decision.price(),
            Decimal::new(48, 2),
            "adverse movement should post at best_ask - tick (post-only)"
        );
    }

    // ── Steps never advance beyond MAX_EROSION_STEPS ──────────────────

    #[test]
    fn test_advance_step_capped_at_max() {
        use crate::types::market::OrderState;
        let now_ms = 100_000;

        let (mut state, mut snap, evaluator) = make_emergency_test_setup("0.49", now_ms);
        // At step 4 (one below max), advance_step should still be possible.
        snap.emergency_submitted = false;
        snap.steps_applied = 4;
        snap.exit_reason = None;
        snap.current_profit_target = Decimal::new(2, 3); // small residual
        // Posted price must be WORSE (higher) than the erosion target so the
        // skip guard doesn't suppress the signal.  Target will land at 0.48 after
        // post-only clamping, so 0.49 > 0.48 → repost is needed.
        state.leg2_state = OrderState::Posted {
            order_id: "sim-leg2".into(),
            price: Decimal::new(49, 2),
            size: Decimal::new(100, 0),
            timestamp_ms: now_ms - 1_000,
        };

        let last_erosion_ms = now_ms - 5_000;
        let result = evaluator.evaluate_leg2(&state, &snap, last_erosion_ms, now_ms);
        // At step 4, the exhaustion check (>= 5) does NOT fire, so we continue to erosion logic.
        // But we need steps_applied=4 + advance_step → steps_now=5, which is valid.
        // The erosion exhaustion block fires BEFORE the timing gate, so steps=4 won't hit it.
        // steps=4 passes the exhaustion check (4 < 5) and goes to normal erosion.
        assert!(
            result.is_some(),
            "step 4 should produce a normal erosion signal (not emergency)"
        );

        // At step 5, the exhaustion check fires and produces an emergency.
        snap.steps_applied = 5;
        snap.current_profit_target = Decimal::ZERO;
        let result2 = evaluator.evaluate_leg2(&state, &snap, last_erosion_ms, now_ms);
        assert!(result2.is_some());
        assert!(
            result2.as_ref().unwrap().is_emergency(),
            "step 5 should trigger erosion exhaustion emergency"
        );
    }
}
