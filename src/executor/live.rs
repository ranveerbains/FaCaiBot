//! Live Executor — places real orders on the Polymarket CLOB.
//!
//! Handles the full trade lifecycle:
//! - Leg 1: maker post-only order at best ask (waits for fill via User WS)
//! - Leg 1 cancel: flow-based sustain failure (composite dropped)
//! - Leg 2 hedge: cancel previous resting order, repost at new price
//! - Leg 2 emergency: cancel resting order, place FOK taker order
//! - Market rotation: cancel all open orders, reset state
//!
//! Sends `ExecutorFeedback` back to the engine so it can update `OrderState`
//! with the real CLOB order IDs (needed for User WS fill matching).

use std::str::FromStr;

use alloy::primitives::U256;
use anyhow::Result;
use crossbeam_channel::{Receiver, Sender};
use rust_decimal::Decimal;
use tracing::{error, info, warn};

use crate::gateway::polymarket::PolymarketGateway;
use crate::reporting::telegram::TelegramReporter;
use crate::storage::cold::ColdStorage;
use crate::types::market::Direction;
use crate::utils::time::epoch_ms;
use crate::types::order::{
    ExecutorCommand, ExecutorFeedback, FillMethod, OrderRequest, OrderStatus, OrderTag, TradeSignal,
};

use super::fill_engine::round_to_tick;

/// Adjusts `size` so that `price * size` has at most 2 decimal places
/// (CLOB maker amount constraint) and `price * size >= $1.00` (minimum notional).
///
/// The CLOB rejects any FOK/FAK order where `price × size` has more than 2dp.
/// This function searches DOWNWARD from the input size to find the largest valid
/// size, minimising under-hedging. Falls back to an upward search from the
/// minimum notional size if no valid size exists at or below the input.
fn clob_safe_fok_size(price: Decimal, size: Decimal) -> Decimal {
    let tick = Decimal::new(1, 2); // 0.01

    if price.is_zero() {
        return Decimal::ZERO;
    }

    // Floor input to 2dp (CLOB taker amount max 4dp; we use 2dp for cleanliness).
    let truncated = (size / tick).floor() * tick;

    // Fast path: if truncated already satisfies both constraints, return it.
    let n = price * truncated;
    if n == n.round_dp(2) && n >= Decimal::ONE {
        return truncated;
    }

    // Downward search: find the largest s ≤ truncated where price×s has ≤2dp
    // and price×s ≥ $1.00. Prefer under-hedging over over-hedging.
    let mut s = if truncated >= tick { truncated - tick } else { Decimal::ZERO };
    while s > Decimal::ZERO {
        let n = price * s;
        if n < Decimal::ONE {
            break; // further down only reduces notional
        }
        if n == n.round_dp(2) {
            return s;
        }
        s -= tick;
    }

    // Upward fallback: smallest s ≥ min_size where price×s has ≤2dp.
    let min_size = (Decimal::ONE / price).ceil();
    let mut s = min_size;
    let cap = min_size + Decimal::new(100, 0);
    while s <= cap {
        let n = price * s;
        if n == n.round_dp(2) {
            return s;
        }
        s += tick;
    }
    Decimal::ZERO
}

/// Live executor that submits real orders to the Polymarket CLOB.
pub struct LiveExecutor {
    poly: PolymarketGateway,
    feedback_tx: Sender<ExecutorFeedback>,
    reporter: TelegramReporter,
    /// QuestDB cold storage. `None` if QuestDB is unavailable — the executor
    /// still runs without analytics.
    cold: Option<ColdStorage>,

    // ── SDK cache gate ────────────────────────────────────────────────
    /// `true` once `tick_size` and `neg_risk` have been pre-warmed from the CLOB
    /// for both tokens of the current market. All signals are rejected while `false`.
    caches_warm: bool,

    // ── Position tracking ───────────────────────────────────────────
    /// Active Phase 1 Leg 2 order ID (persists into Phase 2 when dual-order).
    active_leg2_phase1_id: Option<String>,
    /// Active Phase 2 Leg 2 order ID (posted alongside Phase 1 at ask-1tick).
    active_leg2_phase2_id: Option<String>,

    /// Set `true` when a "not enough balance" / "allowance" error is detected
    /// during Leg 2 placement. All subsequent Leg 2 commands are immediately
    /// rejected with `OrderFailed` until cleared on `MarketRotation`.
    balance_exhausted: bool,

    /// Timeout (ms) for favorable maker try before FOK fallback on crosses-book.
    favorable_maker_timeout_ms: u64,

}

impl LiveExecutor {
    pub fn new(
        poly: PolymarketGateway,
        feedback_tx: Sender<ExecutorFeedback>,
        reporter: TelegramReporter,
        cold: Option<ColdStorage>,
        favorable_maker_timeout_ms: u64,
    ) -> Self {
        Self {
            poly,
            feedback_tx,
            reporter,
            cold,
            caches_warm: false,
            active_leg2_phase1_id: None,
            active_leg2_phase2_id: None,
            balance_exhausted: false,
            favorable_maker_timeout_ms,
        }
    }

    /// Main receive loop — consumes `ExecutorCommand` from the engine channel.
    pub async fn run(mut self, rx: Receiver<ExecutorCommand>) -> Result<()> {
        info!("LiveExecutor: starting receive loop");
        self.reporter.send_live_startup_message();

        loop {
            let cmd = match tokio::task::block_in_place(|| rx.recv()) {
                Ok(cmd) => cmd,
                Err(_) => break,
            };
            match cmd {
                ExecutorCommand::Signal(signal) => {
                    self.handle_signal(signal).await;
                }
                ExecutorCommand::MarketRotation {
                    condition_id,
                    yes_token_id,
                    no_token_id,
                    tick_size,
                    ..
                } => {
                    self.on_market_rotation(&condition_id, &yes_token_id, &no_token_id, tick_size)
                        .await;
                }
                ExecutorCommand::TickSizeChanged {
                    yes_token_id,
                    no_token_id,
                    new_tick_size,
                } => {
                    if let Some(sdk) = self.poly.sdk_client() {
                        use polymarket_client_sdk::clob::types::TickSize;
                        match TickSize::try_from(new_tick_size) {
                            Ok(tick) => {
                                for token_id_str in [&yes_token_id, &no_token_id] {
                                    if let Ok(id) = U256::from_str(token_id_str) {
                                        sdk.set_tick_size(id, tick);
                                    }
                                }
                                info!(%new_tick_size, "SDK tick_size cache updated for both tokens");
                            }
                            Err(e) => {
                                warn!(%new_tick_size, error = %e, "failed to convert Decimal to TickSize — SDK cache NOT updated");
                            }
                        }
                    }
                }
                ExecutorCommand::PostLeg2Phase2 { signal } => {
                    self.handle_post_leg2_phase2(&signal).await;
                }
                ExecutorCommand::CancelLeg1Order { order_id } => {
                    self.handle_cancel_leg1(&order_id).await;
                }
            }
        }

        info!("LiveExecutor: channel disconnected");
        Ok(())
    }

    // ─── Signal dispatch ────────────────────────────────────────────────

    async fn handle_signal(&mut self, signal: TradeSignal) {
        if !self.caches_warm {
            warn!(
                token = %signal.token_id,
                "signal REJECTED — SDK caches not warm (pre-warm failed on rotation)"
            );
            let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderFailed {
                is_leg2: signal.is_leg2,
            });
            return;
        }

        if !signal.is_leg2 {
            self.handle_leg1(&signal).await;
        } else if self.balance_exhausted {
            warn!("Leg 2 signal REJECTED — balance exhausted, waiting for rotation");
            let _ = self
                .feedback_tx
                .try_send(ExecutorFeedback::OrderFailed { is_leg2: true });
        } else if signal.exit_reason.is_some() {
            self.handle_leg2_emergency(&signal).await;
        } else {
            self.handle_leg2_hedge(&signal).await;
        }
    }

    // ─── Leg 1: maker post-only entry ──────────────────────────────────

    async fn handle_leg1(&mut self, signal: &TradeSignal) {
        // New trade — clear any stale Leg 2 IDs from previous trade.
        self.active_leg2_phase1_id = None;
        self.active_leg2_phase2_id = None;

        // Round price to tick size (SDK requirement: price decimals <= tick decimals).
        let rounded_price = crate::engine::confidence::round_to_tick(signal.price, signal.tick_size);

        let order = OrderRequest::post_only_gtc(
            signal.token_id.clone(),
            signal.side,
            rounded_price,
            signal.size,
        );

        info!(
            side = ?signal.side,
            token = %signal.token_id,
            price = %rounded_price,
            size = %signal.size,
            expected_pct = %signal.expected_pct,
            tier = signal.profit_target_tier.label(),
            "Leg 1: maker post-only at {}",
            rounded_price,
        );

        match self.poly.place_order(&order).await {
            Ok(resp) => {
                if resp.status == OrderStatus::Rejected {
                    // Post-only rejected — ask crossed our bid. Abort.
                    warn!("Leg 1 maker REJECTED — aborting");
                    let _ = self
                        .feedback_tx
                        .try_send(ExecutorFeedback::OrderFailed { is_leg2: false });
                    self.log_signal_to_cold(signal, "rejected");
                    return;
                }
                // Order resting on book — waiting for fill via User WS.
                // If already filled synchronously (rare for post-only), handle it.
                let already_filled = resp.status == OrderStatus::Filled;
                info!(
                    order_id = %resp.order_id,
                    status = ?resp.status,
                    %already_filled,
                    "Leg 1: maker posted"
                );
                // Use actual fill from REST response when available; fall back to posted size.
                let fill_size = if already_filled && resp.size_matched > Decimal::ZERO {
                    resp.size_matched
                } else {
                    signal.size
                };
                let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                    is_leg2: false,
                    order_id: resp.order_id,
                    price: signal.price,
                    size: fill_size,
                    fill_method: None,
                    already_filled,
                    order_tag: None,
                });
                self.log_signal_to_cold(signal, "maker_posted");
            }
            Err(e) => {
                error!("Leg 1 maker placement FAILED: {e}");
                let _ = self
                    .feedback_tx
                    .try_send(ExecutorFeedback::OrderFailed { is_leg2: false });
                self.log_signal_to_cold(signal, "failed");
            }
        }
    }

    // ─── Leg 1 cancel (flow-based sustain failure) ─────────────────────

    async fn handle_cancel_leg1(&mut self, order_id: &str) {
        let was_cancelled = match self.poly.cancel_order(order_id).await {
            Ok(c) => c,
            Err(e) => {
                warn!(%order_id, error = %e, "Leg 1 cancel failed");
                false
            }
        };

        // Query authoritative fill size — detects fills that the cancel response can't reveal.
        let size_matched = match self.poly.get_order_status(order_id).await {
            Ok((_status, matched, _original)) => Some(matched),
            Err(e) => {
                warn!(%order_id, error = %e, "get_order_status after cancel failed");
                None
            }
        };

        info!(%order_id, %was_cancelled, ?size_matched, "Leg 1 cancel result");
        let _ = self.feedback_tx.try_send(ExecutorFeedback::CancelResult {
            order_id: order_id.to_string(),
            was_cancelled,
            is_leg2: false,
            size_matched,
        });
    }

    // ─── Leg 2 hedge: post Phase 1 order (reject if one already active) ─

    async fn handle_leg2_hedge(&mut self, signal: &TradeSignal) {
        // Guard: if a Phase 1 order is already resting, reject the duplicate signal.
        // This prevents a cancel-and-replace loop when OrderPosted feedback and
        // CANCELED TradeStatusUpdate are processed in the same main-loop iteration.
        if let Some(ref existing_id) = self.active_leg2_phase1_id {
            warn!(
                existing_order = %existing_id,
                signal_price = %signal.price,
                "Leg 2 hedge: Phase 1 order already active — ignoring duplicate signal"
            );
            let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderFailed { is_leg2: true });
            return;
        }

        // Post new Leg 2 order at the hedge price.
        let order = OrderRequest::post_only_gtc(
            signal.token_id.clone(),
            signal.side,
            signal.price,
            signal.size,
        );

        match self.poly.place_order(&order).await {
            Ok(resp) => {
                if resp.status == OrderStatus::Rejected {
                    // Post-only rejected — ask is below our bid. Attempt favorable exit.
                    warn!(
                        price = %signal.price,
                        "Leg 2 hedge: post-only REJECTED — attempting favorable exit"
                    );
                    self.attempt_favorable_maker_then_fok(signal).await;
                } else {
                    info!(
                        order_id = %resp.order_id,
                        price = %signal.price,
                        "Leg 2 hedge: new order placed"
                    );
                    self.active_leg2_phase1_id = Some(resp.order_id.clone());

                    let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                        is_leg2: true,
                        order_id: resp.order_id,
                        price: signal.price,
                        size: signal.size,
                        fill_method: None,
                        already_filled: false,
                        order_tag: Some(OrderTag::Leg2Phase1),
                    });
                }
            }
            Err(e) => {
                let err_msg = e.to_string();
                if err_msg.contains("crosses book") {
                    warn!(
                        error = %e,
                        "Leg 2 hedge: 'crosses book' error — attempting favorable exit"
                    );
                    self.attempt_favorable_maker_then_fok(signal).await;
                } else {
                    error!(error = %e, "Leg 2 hedge: order placement FAILED");
                    self.active_leg2_phase1_id = None;

                    // Detect balance errors — stop all Leg 2 attempts until rotation
                    if err_msg.contains("balance") || err_msg.contains("allowance") {
                        error!("Leg 2: insufficient balance — halting all attempts until rotation");
                        self.balance_exhausted = true;
                        let _ = self
                            .feedback_tx
                            .try_send(ExecutorFeedback::BalanceExhausted);
                    }

                    let _ = self
                        .feedback_tx
                        .try_send(ExecutorFeedback::OrderFailed { is_leg2: true });
                }
            }
        }
    }

    // ─── Leg 2 favorable exit: try maker first, then FOK fallback ─────

    /// Favorable pricing detected (post-only rejected or "crosses book").
    /// 1. Post maker at best_ask - 1tick.
    /// 2. Poll for fill up to `favorable_maker_timeout_ms`.
    /// 3. If not filled → cancel → FOK taker fallback.
    async fn attempt_favorable_maker_then_fok(&mut self, signal: &TradeSignal) {
        let tick = signal.tick_size;
        let maker_price = match signal.best_ask {
            Some(ask) => round_to_tick(ask - tick, tick),
            None => {
                warn!("favorable maker: no best_ask — falling back to FOK");
                self.favorable_exit_fok(signal).await;
                return;
            }
        };

        // Post maker at ask - 1tick.
        let order = OrderRequest::post_only_gtc(
            signal.token_id.clone(),
            signal.side,
            maker_price,
            signal.size,
        );

        match self.poly.place_order(&order).await {
            Ok(resp) => {
                if resp.status == OrderStatus::Rejected {
                    // Ask dropped further — maker would cross, go straight to FOK.
                    warn!(
                        price = %maker_price,
                        "favorable maker: post-only REJECTED — immediate FOK fallback"
                    );
                    self.favorable_exit_fok(signal).await;
                    return;
                }
                if resp.status == OrderStatus::Filled {
                    // Instant fill — rare but possible.
                    let fill_size = if resp.size_matched > Decimal::ZERO {
                        resp.size_matched.min(signal.size).round_dp(2)
                    } else {
                        signal.size
                    };
                    info!(
                        order_id = %resp.order_id,
                        price = %maker_price,
                        %fill_size,
                        "favorable maker: instant fill"
                    );
                    self.active_leg2_phase1_id = None;
                    self.active_leg2_phase2_id = None;
                    let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                        is_leg2: true,
                        order_id: resp.order_id,
                        price: maker_price,
                        size: fill_size,
                        fill_method: Some(FillMethod::FavorableMaker),
                        already_filled: true,
                        order_tag: None,
                    });
                    return;
                }

                // Order resting — poll for fill.
                info!(
                    order_id = %resp.order_id,
                    price = %maker_price,
                    "favorable maker: order resting — polling for fill"
                );
                let maker_order_id = resp.order_id;
                let poll_interval_ms = 200u64;
                let max_polls = (self.favorable_maker_timeout_ms / poll_interval_ms).max(1);
                let mut filled = false;
                let mut polled_size_matched: Option<Decimal> = None;
                let breakeven = Decimal::ONE
                    - signal.leg1_fill_price.unwrap_or(Decimal::ONE);

                for _ in 0..max_polls {
                    tokio::time::sleep(std::time::Duration::from_millis(poll_interval_ms)).await;

                    // Check for fill via REST poll.
                    match self.poly.get_order_status(&maker_order_id).await {
                        Ok((status, matched, _orig)) if status == OrderStatus::Filled => {
                            info!(
                                order_id = %maker_order_id,
                                price = %maker_price,
                                %matched,
                                "favorable maker: fill detected via polling"
                            );
                            polled_size_matched = Some(matched);
                            filled = true;
                            break;
                        }
                        Ok(_) => {} // still resting, continue polling
                        Err(e) => {
                            warn!(error = %e, "favorable maker: poll error — continuing");
                        }
                    }

                    // Breakeven breach check — if ask snapped back above breakeven,
                    // cancel the maker early and FOK before the book deteriorates.
                    match self.poly.get_best_ask(&signal.token_id).await {
                        Ok(Some(current_ask)) => {
                            if current_ask - tick > breakeven {
                                warn!(
                                    %current_ask, %breakeven,
                                    "favorable maker: breakeven breach — cancelling maker, FOK fallback"
                                );
                                let _ = self.poly.cancel_order(&maker_order_id).await;
                                self.favorable_exit_fok(signal).await;
                                return;
                            }
                        }
                        Ok(None) => {} // empty ask side — continue polling
                        Err(e) => {
                            warn!(error = %e, "favorable maker: book query failed — continuing");
                        }
                    }
                }

                if filled {
                    let fill_size = if let Some(matched) = polled_size_matched {
                        if matched > Decimal::ZERO { matched.min(signal.size).round_dp(2) } else { signal.size }
                    } else {
                        signal.size
                    };
                    self.active_leg2_phase1_id = None;
                    self.active_leg2_phase2_id = None;
                    let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                        is_leg2: true,
                        order_id: maker_order_id,
                        price: maker_price,
                        size: fill_size,
                        fill_method: Some(FillMethod::FavorableMaker),
                        already_filled: true,
                        order_tag: None,
                    });
                } else {
                    // Not filled — cancel and fall back to FOK.
                    info!(order_id = %maker_order_id, "favorable maker: timeout — cancelling");
                    match self.poly.cancel_order(&maker_order_id).await {
                        Ok(true) => {
                            // Cancelled — FOK fallback.
                            self.favorable_exit_fok(signal).await;
                        }
                        Ok(false) => {
                            // Cancel NOT confirmed — order may have filled in the interim.
                            // Send OrderPosted and let User WS determine the outcome.
                            info!(
                                order_id = %maker_order_id,
                                "favorable maker: cancel NOT confirmed — may have filled"
                            );
                            self.active_leg2_phase1_id = Some(maker_order_id.clone());
                            let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                                is_leg2: true,
                                order_id: maker_order_id,
                                price: maker_price,
                                size: signal.size,
                                fill_method: Some(FillMethod::FavorableMaker),
                                already_filled: false,
                                order_tag: None,
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "favorable maker: cancel error — FOK fallback");
                            self.favorable_exit_fok(signal).await;
                        }
                    }
                }
            }
            Err(e) => {
                let err_msg = e.to_string();
                if err_msg.contains("crosses book") {
                    // Even the maker price crosses — go straight to FOK.
                    warn!(error = %e, "favorable maker: crosses book — FOK fallback");
                    self.favorable_exit_fok(signal).await;
                } else {
                    error!(error = %e, "favorable maker: placement FAILED");
                    self.active_leg2_phase1_id = None;
                    self.active_leg2_phase2_id = None;
                    let _ = self
                        .feedback_tx
                        .try_send(ExecutorFeedback::OrderFailed { is_leg2: true });
                }
            }
        }
    }

    /// FOK taker fallback for favorable exits.
    ///
    /// Uses the same retry loop as `emergency_fok_fallback()`: queries the
    /// live best ask each attempt, retries until filled or market expires.
    async fn favorable_exit_fok(&mut self, signal: &TradeSignal) {
        let deadline_ms = signal.market_end_timestamp_ms;
        let mut current_price = signal.price;
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let now_ms = epoch_ms();
            if now_ms >= deadline_ms {
                error!(attempts = attempt, "favorable FOK deadline reached (market expired) — aborting");
                break;
            }
            // Query live best ask to target actual liquidity.
            match self.poly.get_best_ask(&signal.token_id).await {
                Ok(Some(ask)) if ask > current_price => {
                    info!(old_price = %current_price, new_price = %ask, "favorable FOK: jumping to live best ask");
                    current_price = ask;
                }
                Ok(Some(_)) => {} // ask <= current_price, keep current
                Ok(None) => {
                    error!("favorable FOK: order book empty — aborting");
                    break;
                }
                Err(e) => {
                    warn!(error = %e, "favorable FOK: best ask query failed — using current price");
                }
            }
            if current_price > Decimal::ONE {
                error!("favorable FOK price exceeded $1.00 cap — aborting");
                break;
            }
            let safe_size = clob_safe_fok_size(current_price, signal.size);
            if safe_size.is_zero() {
                error!(price = %current_price, size = %signal.size, "favorable FOK size zero — aborting");
                break;
            }
            let order = OrderRequest::emergency_fok(
                signal.token_id.clone(),
                signal.side,
                current_price,
                safe_size,
            );

            match self.poly.place_order(&order).await {
                Ok(resp) => {
                    if resp.status == OrderStatus::Rejected {
                        warn!(
                            order_id = %resp.order_id,
                            price = %current_price,
                            attempt,
                            "favorable FOK rejected — escalating"
                        );
                        current_price += signal.tick_size;
                        if current_price > Decimal::ONE {
                            error!("favorable FOK price exceeded $1.00 cap — aborting");
                            break;
                        }
                        continue;
                    }

                    info!(
                        order_id = %resp.order_id,
                        status = ?resp.status,
                        price = %current_price,
                        "Leg 2 favorable exit: FOK placed"
                    );
                    if resp.status == OrderStatus::Filled {
                        self.active_leg2_phase1_id = None;
                        self.active_leg2_phase2_id = None;
                    } else {
                        self.active_leg2_phase1_id = Some(resp.order_id.clone());
                    }
                    let fill_size = if resp.size_matched > Decimal::ZERO {
                        resp.size_matched.min(safe_size).round_dp(2)
                    } else {
                        safe_size
                    };
                    let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                        is_leg2: true,
                        order_id: resp.order_id,
                        price: current_price,
                        size: fill_size,
                        fill_method: Some(FillMethod::FavorableTaker),
                        already_filled: resp.status == OrderStatus::Filled,
                        order_tag: None,
                    });
                    return;
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    if err_msg.contains("decimal places")
                        || err_msg.contains("decimals")
                        || err_msg.contains("invalid amounts")
                        || err_msg.contains("Validation")
                        || err_msg.contains("balance")
                        || err_msg.contains("allowance")
                        || err_msg.contains("too old")
                        || err_msg.contains("min size")
                    {
                        error!(error = %e, "favorable FOK non-transient error — aborting retries");
                        break;
                    }
                    warn!(error = %e, price = %current_price, attempt, "favorable FOK FAILED — escalating");
                    current_price += signal.tick_size;
                    if current_price > Decimal::ONE {
                        error!("favorable FOK price exceeded $1.00 cap — aborting");
                        break;
                    }
                    continue;
                }
            }
        }
        // Favorable FOK loop exhausted without fill.
        error!(
            token_id = %signal.token_id,
            size = %signal.size,
            attempts = attempt,
            "Leg 2 favorable FOK loop exhausted — unhedged"
        );
        self.reporter.fire_critical(format!(
            "<b>ORPHANED POSITION</b>\n\n\
            Leg 2 favorable FOK loop failed after {} attempts.\n\
            Token: <code>{}</code>\n\
            Size: {} shares\n\n\
            Manual intervention required.",
            attempt.saturating_sub(1),
            signal.token_id,
            signal.size,
        ));
        self.active_leg2_phase1_id = None;
        self.active_leg2_phase2_id = None;
        let _ = self
            .feedback_tx
            .try_send(ExecutorFeedback::OrderFailed { is_leg2: true });
    }

    // ─── Leg 2 emergency: price-chase post-only or deadline FOK ─────────

    async fn handle_leg2_emergency(&mut self, signal: &TradeSignal) {
        let exit_reason = signal.exit_reason.unwrap();

        // Cancel ALL resting Leg 2 orders (Phase 1 + Phase 2) before emergency FOK.
        for phase_id in [self.active_leg2_phase1_id.clone(), self.active_leg2_phase2_id.clone()].into_iter().flatten() {
            info!(order_id = %phase_id, "Leg 2 emergency: cancelling resting order");
            match self.poly.cancel_order(&phase_id).await {
                Ok(true) => {
                    // Cancelled successfully.
                }
                Ok(false) => {
                    warn!(
                        order_id = %phase_id,
                        "Leg 2 emergency cancel NOT confirmed — order may have filled, skipping replacement"
                    );
                    let _ =
                        self.feedback_tx.try_send(ExecutorFeedback::CancelResult {
                            order_id: phase_id,
                            was_cancelled: false,
                            is_leg2: true,
                            size_matched: None,
                        });
                    return;
                }
                Err(e) => {
                    warn!(order_id = %phase_id, error = %e, "Leg 2 emergency: cancel failed — proceeding");
                }
            }
        }
        self.active_leg2_phase1_id = None;
        self.active_leg2_phase2_id = None;

        // All emergency signals use FOK taker.
        warn!(
            reason = ?exit_reason,
            price = %signal.price,
            size = %signal.size,
            "Leg 2 EMERGENCY: direct FOK taker"
        );
        self.emergency_fok_fallback(signal, exit_reason).await;
    }

    // ─── Emergency FOK fallback (when post-only is rejected or fails) ────

    async fn emergency_fok_fallback(
        &mut self,
        signal: &TradeSignal,
        exit_reason: crate::types::order::ExitReason,
    ) {
        // Price-escalating FOK: query the live best ask each attempt to jump
        // directly to where liquidity sits, avoiding blind +1 tick walking that
        // causes catastrophic slippage (e.g. 38 attempts over 13s). Falls back
        // to +1 tick only when the best ask query fails or returns a price we
        // already tried. Aborts if the book is empty.
        //
        // After Leg 1 fills we hold a directional position — Leg 2 *must* fill.
        // The ~1.2s HTTP round-trip per attempt is the natural rate limiter.
        //
        // Time-based cutoff: retry until the market ends rather than a fixed
        // attempt count, so we maximise hedge probability without risking
        // retries on a settled market.
        let deadline_ms = signal.market_end_timestamp_ms;
        let mut current_price = signal.price;
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let now_ms = epoch_ms();
            if now_ms >= deadline_ms {
                error!(attempts = attempt, reason = ?exit_reason, "FOK deadline reached (market expired) — aborting");
                break;
            }
            let safe_size = clob_safe_fok_size(current_price, signal.size);
            if safe_size.is_zero() {
                error!(price = %current_price, size = %signal.size, "FOK size zero — aborting");
                break;
            }
            let order = OrderRequest::emergency_fok(
                signal.token_id.clone(),
                signal.side,
                current_price,
                safe_size,
            );

            match self.poly.place_order(&order).await {
                Ok(resp) => {
                    if resp.status == OrderStatus::Rejected {
                        warn!(
                            order_id = %resp.order_id,
                            price = %current_price,
                            reason = ?exit_reason,
                            attempt,
                            "Leg 2 emergency: FOK rejected — querying live best ask"
                        );
                        match self.poly.get_best_ask(&signal.token_id).await {
                            Ok(Some(ask)) if ask > current_price => {
                                info!(old_price = %current_price, new_price = %ask, "FOK: jumping to live best ask");
                                current_price = ask;
                            }
                            Ok(Some(_)) => {
                                // Best ask <= current_price: already tried, walk +1 tick
                                current_price += signal.tick_size;
                            }
                            Ok(None) => {
                                error!("FOK: order book empty — aborting");
                                break;
                            }
                            Err(e) => {
                                warn!(error = %e, "FOK: best ask query failed — walking +1 tick");
                                current_price += signal.tick_size;
                            }
                        }
                        if current_price > Decimal::ONE {
                            error!(reason = ?exit_reason, "FOK price exceeded $1.00 cap — aborting");
                            break;
                        }
                        continue;
                    }

                    info!(
                        order_id = %resp.order_id,
                        status = ?resp.status,
                        price = %current_price,
                        reason = ?exit_reason,
                        "Leg 2 emergency: FOK fallback placed"
                    );
                    // Don't track filled FOKs — prevents stale cancel by next signal
                    if resp.status == OrderStatus::Filled {
                        self.active_leg2_phase1_id = None;
                        self.active_leg2_phase2_id = None;
                    } else {
                        self.active_leg2_phase1_id = Some(resp.order_id.clone());
                    }

                    // Use actual fill from REST response; fall back to posted size if zero.
                    // Cap at order size — SDK `taking_amount` can exceed requested size.
                    let fill_size = if resp.size_matched > Decimal::ZERO {
                        resp.size_matched.min(safe_size).round_dp(2)
                    } else {
                        safe_size
                    };
                    let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                        is_leg2: true,
                        order_id: resp.order_id,
                        price: current_price,
                        size: fill_size,
                        fill_method: Some(FillMethod::EmergencyTaker),
                        already_filled: resp.status == OrderStatus::Filled,
                        order_tag: None,
                    });
                    return;
                }
                Err(e) => {
                    let err_msg = e.to_string();

                    if err_msg.contains("decimal places")
                        || err_msg.contains("decimals")
                        || err_msg.contains("invalid amounts")
                        || err_msg.contains("Validation")
                        || err_msg.contains("balance")
                        || err_msg.contains("allowance")
                        || err_msg.contains("too old")
                        || err_msg.contains("min size")
                    {
                        error!(error = %e, "FOK non-transient error — aborting retries");
                        break;
                    }
                    warn!(error = %e, price = %current_price, attempt, "Leg 2 emergency: FOK FAILED — querying live best ask");
                    match self.poly.get_best_ask(&signal.token_id).await {
                        Ok(Some(ask)) if ask > current_price => {
                            info!(old_price = %current_price, new_price = %ask, "FOK: jumping to live best ask");
                            current_price = ask;
                        }
                        Ok(Some(_)) => {
                            current_price += signal.tick_size;
                        }
                        Ok(None) => {
                            error!("FOK: order book empty — aborting");
                            break;
                        }
                        Err(e2) => {
                            warn!(error = %e2, "FOK: best ask query failed — walking +1 tick");
                            current_price += signal.tick_size;
                        }
                    }
                    if current_price > Decimal::ONE {
                        error!(reason = ?exit_reason, "FOK price exceeded $1.00 cap — aborting");
                        break;
                    }
                    continue;
                }
            }
        }
        // FOK loop exhausted without fill — orphaned Leg 1 position.
        error!(
            token_id = %signal.token_id,
            size = %signal.size,
            reason = ?exit_reason,
            attempts = attempt,
            "ORPHANED POSITION: Leg 2 FOK loop failed — Leg 1 unhedged"
        );
        self.reporter.fire_critical(format!(
            "<b>ORPHANED POSITION</b>\n\n\
            Leg 2 FOK loop failed after {} attempts (market expired).\n\
            Token: <code>{}</code>\n\
            Size: {} shares\n\
            Reason: {:?}\n\n\
            Manual intervention required.",
            attempt.saturating_sub(1),
            signal.token_id,
            signal.size,
            exit_reason,
        ));
        self.active_leg2_phase1_id = None;
        self.active_leg2_phase2_id = None;
        let _ = self
            .feedback_tx
            .try_send(ExecutorFeedback::OrderFailed { is_leg2: true });
    }

    // ─── Sequential Phase 2: cancel Phase 1, then post Phase 2 ────────

    async fn handle_post_leg2_phase2(&mut self, signal: &TradeSignal) {
        if self.balance_exhausted {
            warn!("PostLeg2Phase2 REJECTED — balance exhausted");
            let _ = self
                .feedback_tx
                .try_send(ExecutorFeedback::OrderFailed { is_leg2: true });
            return;
        }

        // Cancel Phase 1 order first (sequential, not alongside).
        // After cancel, query fill status to determine how much Phase 1 actually filled.
        let mut phase1_filled = Decimal::ZERO;
        if let Some(phase1_id) = self.active_leg2_phase1_id.clone() {
            info!(%phase1_id, "Phase 2: cancelling Phase 1 order first");
            let was_cancelled = match self.poly.cancel_order(&phase1_id).await {
                Ok(cancelled) => {
                    info!(%phase1_id, %cancelled, "Phase 2: Phase 1 cancel result");
                    cancelled
                }
                Err(e) => {
                    warn!(%phase1_id, error = %e, "Phase 2: Phase 1 cancel failed — proceeding");
                    false
                }
            };
            // Query authoritative fill status after cancel.
            let size_matched = match self.poly.get_order_status(&phase1_id).await {
                Ok((_status, matched, _original)) => Some(matched),
                Err(e) => {
                    warn!(%phase1_id, error = %e, "Phase 2: get_order_status failed — assuming zero fill");
                    None
                }
            };
            phase1_filled = size_matched.unwrap_or(Decimal::ZERO);
            self.active_leg2_phase1_id = None;

            // Compute remainder: leg1_size - phase1_filled.
            let remainder = (signal.size - phase1_filled).max(Decimal::ZERO).round_dp(2);
            // CLOB rejects orders < 5 shares or < $1 notional — treat as fully filled.
            let min_notional_size = (Decimal::ONE / signal.price).ceil();
            let remainder_too_small = remainder < Decimal::new(5, 0)
                || remainder < min_notional_size;
            if remainder <= Decimal::ZERO || remainder_too_small {
                info!(
                    phase1_filled = %phase1_filled,
                    signal_size = %signal.size,
                    %remainder,
                    "Phase 1 sufficiently filled during cancel — no Phase 2 needed"
                );
                // Phase 1 fully filled — tell the engine so it transitions leg2_state to Filled.
                // Use the Phase 1 price from the hedge.
                // NOTE: Do NOT send CancelResult here — it would store leg2_phase1_fill,
                // and the OrderPosted{already_filled} below would double-count via weighted avg.
                let phase1_price = signal.leg1_fill_price
                    .map(|l1p| Decimal::ONE - l1p - signal.profit_target_pct)
                    .unwrap_or(signal.price);
                let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                    is_leg2: true,
                    order_id: format!("phase1-filled-{}", epoch_ms()),
                    price: phase1_price,
                    size: phase1_filled.round_dp(2),
                    fill_method: None,
                    already_filled: true,
                    order_tag: None,
                });
                return;
            }

            // Actual partial fill — send CancelResult so engine stores leg2_phase1_fill
            // for weighted averaging when Phase 2 eventually fills.
            let _ = self.feedback_tx.try_send(ExecutorFeedback::CancelResult {
                order_id: phase1_id.to_string(),
                was_cancelled,
                is_leg2: true,
                size_matched,
            });
        }

        // Compute remainder for Phase 2: leg1_size - phase1_filled.
        // (If no Phase 1 order existed, phase1_filled = 0, so remainder = signal.size.)
        let remainder = (signal.size - phase1_filled).max(Decimal::ZERO).round_dp(2);
        let order = OrderRequest::post_only_gtc(
            signal.token_id.clone(),
            signal.side,
            signal.price,
            remainder,
        );

        match self.poly.place_order(&order).await {
            Ok(resp) => {
                if resp.status == OrderStatus::Rejected {
                    // Post-only rejected — ask is below our bid. Attempt favorable exit.
                    warn!(
                        price = %signal.price,
                        "Leg 2 Phase 2: post-only REJECTED — attempting favorable exit"
                    );
                    self.attempt_favorable_maker_then_fok(signal).await;
                } else {
                    info!(
                        order_id = %resp.order_id,
                        price = %signal.price,
                        "Leg 2 Phase 2: order placed (Phase 1 cancelled)"
                    );
                    self.active_leg2_phase2_id = Some(resp.order_id.clone());

                    let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                        is_leg2: true,
                        order_id: resp.order_id,
                        price: signal.price,
                        size: remainder,
                        fill_method: None,
                        already_filled: false,
                        order_tag: Some(OrderTag::Leg2Phase2),
                    });
                }
            }
            Err(e) => {
                let err_msg = e.to_string();
                if err_msg.contains("crosses book") {
                    warn!(
                        error = %e,
                        "Leg 2 Phase 2: 'crosses book' — attempting favorable exit"
                    );
                    self.attempt_favorable_maker_then_fok(signal).await;
                } else {
                    error!(error = %e, "Leg 2 Phase 2: order placement FAILED");
                    if err_msg.contains("balance") || err_msg.contains("allowance") {
                        self.balance_exhausted = true;
                        let _ = self
                            .feedback_tx
                            .try_send(ExecutorFeedback::BalanceExhausted);
                    }
                    let _ = self
                        .feedback_tx
                        .try_send(ExecutorFeedback::OrderFailed { is_leg2: true });
                }
            }
        }
    }

    // ─── Market rotation ────────────────────────────────────────────────

    async fn on_market_rotation(
        &mut self,
        condition_id: &str,
        yes_token_id: &str,
        no_token_id: &str,
        _tick_size: rust_decimal::Decimal,
    ) {
        info!(
            condition_id,
            "LiveExecutor: market rotation — cancelling all orders"
        );

        if let Err(e) = self.poly.cancel_all().await {
            warn!(error = %e, "cancel_all failed during rotation");
        }

        self.active_leg2_phase1_id = None;
        self.active_leg2_phase2_id = None;
        self.balance_exhausted = false;

        // Reset warm flag — block trading until pre-warm succeeds.
        self.caches_warm = false;

        if let Some(sdk) = self.poly.sdk_client() {
            let mut all_ok = true;
            for token_id_str in [yes_token_id, no_token_id] {
                if let Ok(id) = U256::from_str(token_id_str) {
                    if let Err(e) = sdk.tick_size(id).await {
                        warn!(token = token_id_str, error = %e, "tick_size pre-warm FAILED");
                        all_ok = false;
                    }
                    if let Err(e) = sdk.neg_risk(id).await {
                        warn!(token = token_id_str, error = %e, "neg_risk pre-warm FAILED");
                        all_ok = false;
                    }
                    if let Err(e) = sdk.fee_rate_bps(id).await {
                        warn!(token = token_id_str, error = %e, "fee_rate_bps pre-warm FAILED");
                        all_ok = false;
                    }
                } else {
                    warn!(token = token_id_str, "failed to parse token_id as U256");
                    all_ok = false;
                }
            }
            if all_ok {
                self.caches_warm = true;
                // Pre-warm CLOB connection pool (TLS session establishment).
                // The 404 response is ignored — the reqwest pool is warm regardless.
                let _ = sdk.order("0x0000000000000000000000000000000000000000000000000000000000000000").await;
                info!(
                    condition_id,
                    yes_token_id,
                    no_token_id,
                    "SDK caches pre-warmed from CLOB (tick_size, neg_risk, fee_rate, conn pool) — trading enabled"
                );
            } else {
                warn!("SDK cache pre-warm incomplete — trading BLOCKED until next rotation");
            }
        } else {
            warn!("no SDK client — trading BLOCKED");
        }
    }

    // ─── Cold storage logging ───────────────────────────────────────────

    fn log_signal_to_cold(&mut self, signal: &TradeSignal, action: &str) {
        let direction_str = match signal.direction {
            Direction::Up => "YES",
            Direction::Down => "NO",
        };
        let time_remaining_secs = signal
            .market_end_timestamp_ms
            .saturating_sub(signal.entry_timestamp_ms)
            / 1000;

        if let Some(ref mut c) = self.cold {
            // Extract normalized metrics and freshness from buildup_info if available
            let (cvd_norm, basis_norm, spot_flow_norm, obi_norm, liq_norm, atr_norm) =
                signal.buildup_info.as_ref()
                    .map(|bi| (bi.cvd_norm, bi.basis_norm, bi.spot_flow_norm, bi.obi_norm, bi.liq_norm, bi.atr_norm))
                    .unwrap_or((0.0, 0.0, 0.0, 0.0, 0.0, 0.0));

            let (cvd_age_ms, basis_age_ms, spot_flow_age_ms, obi_age_ms, liq_age_ms, atr_age_ms) =
                signal.buildup_info.as_ref()
                    .map(|bi| (bi.cvd_age_ms, bi.basis_age_ms, bi.spot_flow_age_ms, bi.obi_age_ms, bi.liq_age_ms, bi.atr_age_ms))
                    .unwrap_or((0, 0, 0, 0, 0, 0));

            let composite_score = signal.buildup_info.as_ref()
                .map(|bi| bi.composite_score)
                .unwrap_or(Decimal::ZERO);

            let dissenter_count = signal.buildup_info.as_ref()
                .map(|bi| bi.dissenter_count)
                .unwrap_or(0);

            if let Err(e) = c.record_signal(
                &signal.token_id,
                direction_str,
                signal.expected_pct,
                signal.spike_info.magnitude,
                signal.atr,
                signal
                    .book_snapshot
                    .as_ref()
                    .map(|b| b.total_bid_depth())
                    .unwrap_or(Decimal::ZERO),
                time_remaining_secs as i64,
                signal.alloc_amount,
                action,
                signal.spike_info.timestamp_ms,
                composite_score,
                cvd_norm,
                basis_norm,
                spot_flow_norm,
                obi_norm,
                liq_norm,
                atr_norm,
                cvd_age_ms,
                basis_age_ms,
                spot_flow_age_ms,
                obi_age_ms,
                liq_age_ms,
                atr_age_ms,
                dissenter_count,
            ) {
                warn!(error = %e, "failed to record signal to QuestDB");
            }
        }
    }

}


