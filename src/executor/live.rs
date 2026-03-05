//! Live Executor — places real orders on the Polymarket CLOB.
//!
//! Handles the full trade lifecycle:
//! - Leg 1: post-only GTC order on spike signal
//! - Leg 2 erosion: cancel previous resting order, repost at eroded price
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
use crate::types::order::{
    ExecutorCommand, ExecutorFeedback, FillMethod, OrderRequest, OrderStatus, TradeSignal,
};

use super::fill_engine::round_to_tick;

/// Adjusts `size` so that `price * size` has at most 2 decimal places,
/// as required by the Polymarket CLOB for Buy FOK orders (maker_amount).
fn clob_safe_fok_size(price: Decimal, size: Decimal) -> Decimal {
    let tick = Decimal::new(1, 2); // 0.01
    let mut s = (size / tick).floor() * tick; // truncate size to 2dp
    while s > Decimal::ZERO {
        let maker = price * s;
        if maker == maker.round_dp(2) {
            return s;
        }
        s -= tick;
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
    /// Current active Leg 2 order ID on the CLOB. `None` if no Leg 2 posted.
    active_leg2_order_id: Option<String>,

    /// Set `true` when a "not enough balance" / "allowance" error is detected
    /// during Leg 2 placement. All subsequent Leg 2 commands are immediately
    /// rejected with `OrderFailed` until cleared on `MarketRotation`.
    balance_exhausted: bool,

}

impl LiveExecutor {
    pub fn new(
        poly: PolymarketGateway,
        feedback_tx: Sender<ExecutorFeedback>,
        reporter: TelegramReporter,
        cold: Option<ColdStorage>,
    ) -> Self {
        Self {
            poly,
            feedback_tx,
            reporter,
            cold,
            caches_warm: false,
            active_leg2_order_id: None,
            balance_exhausted: false,
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
                ExecutorCommand::CancelLeg1 { order_id } => {
                    info!(%order_id, "cancelling stale Leg 1 order");
                    match self.poly.cancel_order(&order_id).await {
                        Ok(was_cancelled) => {
                            let _ =
                                self.feedback_tx.try_send(ExecutorFeedback::CancelResult {
                                    order_id,
                                    was_cancelled,
                                    is_leg2: false,
                                });
                        }
                        Err(e) => {
                            warn!(%order_id, error = %e, "failed to cancel stale Leg 1");
                        }
                    }
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
            self.handle_leg2_erosion(&signal).await;
        }
    }

    // ─── Leg 1: post-only GTC entry ────────────────────────────────────

    async fn handle_leg1(&mut self, signal: &TradeSignal) {
        self.active_leg2_order_id = None; // New trade — clear any stale Leg 2 ID from previous trade
        info!(
            side = ?signal.side,
            token = %signal.token_id,
            price = %signal.price,
            size = %signal.size,
            confidence = %signal.confidence,
            tier = signal.profit_target_tier.label(),
            "Leg 1: placing post-only GTC order"
        );

        let order = OrderRequest::post_only_gtc(
            signal.token_id.clone(),
            signal.side,
            signal.price,
            signal.size,
        );

        match self.poly.place_order(&order).await {
            Ok(resp) => {
                if resp.status == OrderStatus::Rejected {
                    warn!(
                        order_id = %resp.order_id,
                        "Leg 1: post-only REJECTED (would cross spread)"
                    );


                    let _ = self
                        .feedback_tx
                        .try_send(ExecutorFeedback::OrderFailed { is_leg2: false });

                    self.log_signal_to_cold(signal, "rejected");
                } else {
                    info!(
                        order_id = %resp.order_id,
                        status = ?resp.status,
                        "Leg 1: order placed"
                    );


                    let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                        is_leg2: false,
                        order_id: resp.order_id,
                        price: signal.price,
                        size: signal.size,
                        fill_method: None,
                        already_filled: false,
                    });

                    self.log_signal_to_cold(signal, "submitted");
                }
            }
            Err(e) => {
                error!(error = %e, "Leg 1: order placement FAILED");


                let _ = self
                    .feedback_tx
                    .try_send(ExecutorFeedback::OrderFailed { is_leg2: false });

                self.log_signal_to_cold(signal, "failed");
            }
        }
    }

    // ─── Leg 2 erosion: cancel previous + repost at new price ──────────

    async fn handle_leg2_erosion(&mut self, signal: &TradeSignal) {
        // Cancel existing Leg 2 order if one is resting.
        if let Some(ref prev_order_id) = self.active_leg2_order_id {
            info!(order_id = %prev_order_id, "Leg 2 erosion: cancelling previous order");
            match self.poly.cancel_order(prev_order_id).await {
                Ok(true) => {

                    self.active_leg2_order_id = None; // Cancelled — clear before posting replacement
                }
                Ok(false) => {
                    warn!(
                        order_id = %prev_order_id,
                        "Leg 2 cancel NOT confirmed — order may have filled, skipping replacement"
                    );
                    let _ =
                        self.feedback_tx.try_send(ExecutorFeedback::CancelResult {
                            order_id: prev_order_id.clone(),
                            was_cancelled: false,
                            is_leg2: true,
                        });
                    return;
                }
                Err(e) => {
                    warn!(error = %e, "Leg 2 erosion: cancel failed — posting replacement anyway");

                }
            }
        }

        // Post new Leg 2 order at the erosion-adjusted price.
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
                        "Leg 2 erosion: post-only REJECTED — attempting favorable exit"
                    );
                    self.attempt_favorable_exit(signal).await;
                } else {
                    info!(
                        order_id = %resp.order_id,
                        price = %signal.price,
                        "Leg 2 erosion: new order placed"
                    );
                    self.active_leg2_order_id = Some(resp.order_id.clone());


                    let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                        is_leg2: true,
                        order_id: resp.order_id,
                        price: signal.price,
                        size: signal.size,
                        fill_method: None,
                        already_filled: false,
                    });
                }
            }
            Err(e) => {
                let err_msg = e.to_string();
                if err_msg.contains("crosses book") {
                    // CLOB returned "crosses book" as an Err (not Ok(Rejected)).
                    // This means the ask dropped below our bid — favorable pricing.
                    warn!(
                        error = %e,
                        "Leg 2 erosion: 'crosses book' error — attempting favorable exit"
                    );
                    self.attempt_favorable_exit(signal).await;
                } else {
                    error!(error = %e, "Leg 2 erosion: order placement FAILED");

                    self.active_leg2_order_id = None;

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

    // ─── Leg 2 favorable exit: walk-down post-only, FOK fallback ──────

    /// Walk-down offsets in ticks: 1, 2, 4, 8 (exponential).
    /// Each attempt is ~100ms (CLOB HTTP round-trip). First successful
    /// placement rests as maker for the remaining erosion window (~5.8s).
    const WALKDOWN_OFFSETS: [u32; 4] = [1, 2, 4, 8];

    async fn attempt_favorable_exit(&mut self, signal: &TradeSignal) {
        // Walk down with exponential tick offsets to find a valid maker price.
        for (i, &offset) in Self::WALKDOWN_OFFSETS.iter().enumerate() {
            let tick_offset = signal.tick_size * Decimal::from(offset);
            let post_only_price = round_to_tick(signal.price - tick_offset, signal.tick_size);

            // Guard: price must be positive and meet $1 notional minimum.
            if post_only_price <= Decimal::ZERO {
                warn!(attempt = i + 1, %post_only_price, "walk-down price non-positive — skipping to FOK");
                break;
            }
            if post_only_price * signal.size < Decimal::ONE {
                warn!(attempt = i + 1, %post_only_price, size = %signal.size, "walk-down below $1 minimum — skipping to FOK");
                break;
            }

            let order = OrderRequest::aggressive_post_only(
                signal.token_id.clone(),
                signal.side,
                post_only_price,
                signal.size,
            );

            match self.poly.place_order(&order).await {
                Ok(resp) => {
                    if resp.status == OrderStatus::Rejected {
                        // Still crosses — try next offset.
                        warn!(
                            attempt = i + 1,
                            price = %post_only_price,
                            "Leg 2 favorable walk-down: post-only REJECTED — trying deeper"
                        );
                        continue;
                    }
                    // Accepted — order rests as maker.
                    info!(
                        attempt = i + 1,
                        order_id = %resp.order_id,
                        price = %post_only_price,
                        "Leg 2 favorable walk-down: post-only accepted"
                    );
                    self.active_leg2_order_id = Some(resp.order_id.clone());



                    let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                        is_leg2: true,
                        order_id: resp.order_id,
                        price: post_only_price,
                        size: signal.size,
                        fill_method: Some(FillMethod::FavorableMaker),
                        already_filled: false,
                    });
                    return;
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    if err_msg.contains("crosses book") {
                        // Same as rejection — try deeper offset.
                        warn!(
                            attempt = i + 1,
                            price = %post_only_price,
                            "Leg 2 favorable walk-down: 'crosses book' — trying deeper"
                        );
                        continue;
                    }
                    // Non-crossing error — skip remaining attempts, go to FOK.
                    error!(
                        attempt = i + 1,
                        error = %e,
                        "Leg 2 favorable walk-down: placement FAILED — skipping to FOK"
                    );
                    break;
                }
            }
        }

        // All walk-down attempts crossed or failed — FOK fallback.
        warn!("Leg 2 favorable walk-down: all post-only attempts exhausted — FOK fallback");
        self.favorable_exit_fok_fallback(signal).await;
    }

    async fn favorable_exit_fok_fallback(&mut self, signal: &TradeSignal) {
        let safe_size = clob_safe_fok_size(signal.price, signal.size);
        if safe_size.is_zero() {
            error!(price = %signal.price, size = %signal.size, "favorable FOK size zero — aborting");

            self.active_leg2_order_id = None;
            let _ = self
                .feedback_tx
                .try_send(ExecutorFeedback::OrderFailed { is_leg2: true });
            return;
        }
        if signal.price * safe_size < Decimal::ONE {
            warn!(price = %signal.price, size = %safe_size, "favorable FOK below $1 minimum — aborting");

            self.active_leg2_order_id = None;
            let _ = self
                .feedback_tx
                .try_send(ExecutorFeedback::OrderFailed { is_leg2: true });
            return;
        }
        let order = OrderRequest::emergency_fok(
            signal.token_id.clone(),
            signal.side,
            signal.price,
            safe_size,
        );

        match self.poly.place_order(&order).await {
            Ok(resp) => {
                if resp.status == OrderStatus::Rejected {
                    warn!("Leg 2 favorable exit: FOK also rejected — erosion continues");

                    self.active_leg2_order_id = None;

                    let _ = self
                        .feedback_tx
                        .try_send(ExecutorFeedback::OrderFailed { is_leg2: true });
                } else {
                    info!(
                        order_id = %resp.order_id,
                        price = %signal.price,
                        "Leg 2 favorable exit: FOK fallback filled"
                    );
                    // Don't track filled FOKs — prevents stale cancel by next signal
                    if resp.status == OrderStatus::Filled {
                        self.active_leg2_order_id = None;
                    } else {
                        self.active_leg2_order_id = Some(resp.order_id.clone());
                    }



                    let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                        is_leg2: true,
                        order_id: resp.order_id,
                        price: signal.price,
                        size: signal.size,
                        fill_method: Some(FillMethod::FavorableTaker),
                        already_filled: resp.status == OrderStatus::Filled,
                    });
                }
            }
            Err(e) => {
                error!(error = %e, "Leg 2 favorable exit: FOK FAILED");

                self.active_leg2_order_id = None;

                let _ = self
                    .feedback_tx
                    .try_send(ExecutorFeedback::OrderFailed { is_leg2: true });
            }
        }
    }

    // ─── Leg 2 emergency: price-chase post-only or deadline FOK ─────────

    async fn handle_leg2_emergency(&mut self, signal: &TradeSignal) {
        let exit_reason = signal.exit_reason.unwrap();

        // Cancel any existing Leg 2 resting order first.
        if let Some(ref prev_order_id) = self.active_leg2_order_id {
            info!(order_id = %prev_order_id, "Leg 2 emergency: cancelling previous resting order");
            match self.poly.cancel_order(prev_order_id).await {
                Ok(true) => {

                    self.active_leg2_order_id = None; // Cancelled — clear before posting replacement
                }
                Ok(false) => {
                    warn!(
                        order_id = %prev_order_id,
                        "Leg 2 emergency cancel NOT confirmed — order may have filled, skipping replacement"
                    );
                    let _ =
                        self.feedback_tx.try_send(ExecutorFeedback::CancelResult {
                            order_id: prev_order_id.clone(),
                            was_cancelled: false,
                            is_leg2: true,
                        });
                    return;
                }
                Err(e) => {
                    warn!(error = %e, "Leg 2 emergency: cancel failed — proceeding with replacement");

                }
            }
        }

        if signal.sim_was_taker {
            // Deadline expired — evaluator determined FOK taker at best_ask.
            warn!(
                reason = ?exit_reason,
                price = %signal.price,
                size = %signal.size,
                "Leg 2 EMERGENCY: deadline expired — direct FOK taker"
            );
            self.emergency_fok_fallback(signal, exit_reason).await;
        } else {
            // Price-chase — aggressive post-only at evaluator-computed price (best_ask - tick).
            warn!(
                reason = ?exit_reason,
                price = %signal.price,
                size = %signal.size,
                "Leg 2 EMERGENCY: price-chase post-only"
            );
            let order = OrderRequest::aggressive_post_only(
                signal.token_id.clone(),
                signal.side,
                signal.price,
                signal.size,
            );

            match self.poly.place_order(&order).await {
                Ok(resp) => {
                    if resp.status == OrderStatus::Rejected {
                        // Post-only would cross spread → FOK fallback at best_ask.
                        let fok_price =
                            round_to_tick(signal.price + signal.tick_size, signal.tick_size);
                        warn!(
                            price = %signal.price,
                            fok_price = %fok_price,
                            "Leg 2 emergency: post-only REJECTED — falling back to FOK"
                        );
                        self.emergency_fok_at_price(signal, exit_reason, fok_price)
                            .await;
                    } else {
                        info!(
                            order_id = %resp.order_id,
                            price = %signal.price,
                            "Leg 2 emergency: price-chase post-only accepted"
                        );
                        self.active_leg2_order_id = Some(resp.order_id.clone());
    
    

                        let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                            is_leg2: true,
                            order_id: resp.order_id,
                            price: signal.price,
                            size: signal.size,
                            fill_method: None,
                            already_filled: false,
                        });
                    }
                }
                Err(e) => {
                    error!(error = %e, "Leg 2 emergency: post-only placement FAILED — trying FOK fallback");
                    self.emergency_fok_fallback(signal, exit_reason).await;
                }
            }
        }
    }

    // ─── Emergency FOK fallback (when post-only is rejected or fails) ────

    async fn emergency_fok_fallback(
        &mut self,
        signal: &TradeSignal,
        exit_reason: crate::types::order::ExitReason,
    ) {
        // Price-escalating FOK: walk up the book +1 tick per attempt until
        // filled or $1.00 cap reached. After Leg 1 fills we hold a directional
        // position — Leg 2 *must* fill. The ~1.2s HTTP round-trip per attempt
        // is the natural rate limiter. The $1.00 price cap (~23 ticks max from
        // any starting price) prevents infinite loops.
        let mut current_price = signal.price;
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
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
                            "Leg 2 emergency: FOK rejected — escalating price"
                        );
                        current_price += signal.tick_size;
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
                        self.active_leg2_order_id = None;
                    } else {
                        self.active_leg2_order_id = Some(resp.order_id.clone());
                    }

                    let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                        is_leg2: true,
                        order_id: resp.order_id,
                        price: current_price,
                        size: safe_size,
                        fill_method: None,
                        already_filled: resp.status == OrderStatus::Filled,
                    });
                    return;
                }
                Err(e) => {
                    let err_msg = e.to_string();

                    if err_msg.contains("decimal places")
                        || err_msg.contains("Validation")
                        || err_msg.contains("balance")
                        || err_msg.contains("allowance")
                    {
                        error!(error = %e, "FOK non-transient error — aborting retries");
                        break;
                    }
                    warn!(error = %e, price = %current_price, attempt, "Leg 2 emergency: FOK FAILED — escalating price");
                    current_price += signal.tick_size;
                    if current_price > Decimal::ONE {
                        error!(reason = ?exit_reason, "FOK price exceeded $1.00 cap — aborting");
                        break;
                    }
                    continue;
                }
            }
        }
        self.active_leg2_order_id = None;
        let _ = self
            .feedback_tx
            .try_send(ExecutorFeedback::OrderFailed { is_leg2: true });
    }

    // ─── Emergency FOK at a specific price (CLOB rejection fallback) ────

    async fn emergency_fok_at_price(
        &mut self,
        signal: &TradeSignal,
        exit_reason: crate::types::order::ExitReason,
        price: Decimal,
    ) {
        // Price-escalating FOK — same rationale as emergency_fok_fallback().
        // We must exit the position. Walks up +1 tick per attempt, capped at $1.00.
        let mut current_price = price;
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let safe_size = clob_safe_fok_size(current_price, signal.size);
            if safe_size.is_zero() {
                error!(price = %current_price, size = %signal.size, "FOK at price size zero — aborting");
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
                            "Leg 2 emergency: FOK at price rejected — escalating price"
                        );
                        current_price += signal.tick_size;
                        if current_price > Decimal::ONE {
                            error!(reason = ?exit_reason, "FOK at price exceeded $1.00 cap — aborting");
                            break;
                        }
                        continue;
                    }

                    info!(
                        order_id = %resp.order_id,
                        status = ?resp.status,
                        price = %current_price,
                        reason = ?exit_reason,
                        "Leg 2 emergency: FOK at price placed"
                    );
                    // Don't track filled FOKs — prevents stale cancel by next signal
                    if resp.status == OrderStatus::Filled {
                        self.active_leg2_order_id = None;
                    } else {
                        self.active_leg2_order_id = Some(resp.order_id.clone());
                    }

                    let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                        is_leg2: true,
                        order_id: resp.order_id,
                        price: current_price,
                        size: safe_size,
                        fill_method: None,
                        already_filled: resp.status == OrderStatus::Filled,
                    });
                    return;
                }
                Err(e) => {
                    let err_msg = e.to_string();

                    if err_msg.contains("decimal places")
                        || err_msg.contains("Validation")
                        || err_msg.contains("balance")
                        || err_msg.contains("allowance")
                    {
                        error!(error = %e, "FOK at price non-transient error — aborting retries");
                        break;
                    }
                    warn!(error = %e, price = %current_price, attempt, "Leg 2 emergency: FOK at price FAILED — escalating price");
                    current_price += signal.tick_size;
                    if current_price > Decimal::ONE {
                        error!(reason = ?exit_reason, "FOK at price exceeded $1.00 cap — aborting");
                        break;
                    }
                    continue;
                }
            }
        }
        self.active_leg2_order_id = None;
        let _ = self
            .feedback_tx
            .try_send(ExecutorFeedback::OrderFailed { is_leg2: true });
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

        self.active_leg2_order_id = None;
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
                info!(
                    condition_id,
                    yes_token_id,
                    no_token_id,
                    "SDK caches pre-warmed from CLOB (tick_size, neg_risk, fee_rate) — trading enabled"
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
            if let Err(e) = c.record_signal(
                &signal.token_id,
                direction_str,
                signal.confidence,
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
            ) {
                warn!(error = %e, "failed to record signal to QuestDB");
            }
        }
    }

}


