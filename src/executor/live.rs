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

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use crossbeam_channel::{Receiver, Sender};
use rust_decimal::Decimal;
use tracing::{error, info, warn};

use crate::gateway::polymarket::PolymarketGateway;
use crate::reporting::telegram::TelegramReporter;
use crate::storage::cold::ColdStorage;
use crate::types::market::Direction;
use crate::types::order::{
    ExecutorCommand, ExecutorFeedback, OrderRequest, OrderStatus, TradeSignal,
};

use super::fill_engine::round_to_tick;

/// Live executor that submits real orders to the Polymarket CLOB.
pub struct LiveExecutor {
    poly: PolymarketGateway,
    feedback_tx: Sender<ExecutorFeedback>,
    reporter: TelegramReporter,
    cold: ColdStorage,

    // ── Position tracking ───────────────────────────────────────────
    /// Current active Leg 2 order ID on the CLOB. `None` if no Leg 2 posted.
    active_leg2_order_id: Option<String>,

    // ── Diagnostics (cumulative, logged every 60s) ──────────────────
    orders_placed: u64,
    orders_cancelled: u64,
    orders_failed: u64,
    emergency_foks: u64,
    emergency_maker_posts: u64,
    favorable_taker_fills: u64,
    last_diag_ms: u64,
}

impl LiveExecutor {
    pub fn new(
        poly: PolymarketGateway,
        feedback_tx: Sender<ExecutorFeedback>,
        reporter: TelegramReporter,
        cold: ColdStorage,
    ) -> Self {
        Self {
            poly,
            feedback_tx,
            reporter,
            cold,
            active_leg2_order_id: None,
            orders_placed: 0,
            orders_cancelled: 0,
            orders_failed: 0,
            emergency_foks: 0,
            emergency_maker_posts: 0,
            favorable_taker_fills: 0,
            last_diag_ms: 0,
        }
    }

    /// Main receive loop — consumes `ExecutorCommand` from the engine channel.
    pub async fn run(mut self, rx: Receiver<ExecutorCommand>) -> Result<()> {
        info!("LiveExecutor: starting receive loop");
        self.reporter.send_startup_message();

        while let Ok(cmd) = rx.recv() {
            match cmd {
                ExecutorCommand::Signal(signal) => {
                    self.handle_signal(signal).await;
                }
                ExecutorCommand::MarketRotation { condition_id } => {
                    self.on_market_rotation(&condition_id).await;
                }
                ExecutorCommand::MarketCutoff {
                    condition_id,
                    market_end_ms,
                } => {
                    info!(
                        condition_id,
                        market_end_ms, "LiveExecutor: market cutoff entered"
                    );
                    let short_id = if condition_id.len() > 5 {
                        &condition_id[condition_id.len() - 5..]
                    } else {
                        &condition_id
                    };
                    self.reporter.send_alert(&format!(
                        "MARKET CUTOFF: #{} — no new entries allowed. Leg 2 erosion continues for open positions.",
                        short_id,
                    ));
                }
                ExecutorCommand::CancelLeg1 { order_id } => {
                    info!(%order_id, "cancelling stale Leg 1 order");
                    if let Err(e) = self.poly.cancel_order(&order_id).await {
                        warn!(%order_id, error = %e, "failed to cancel stale Leg 1");
                    }
                }
            }
            self.check_diagnostic();
        }

        info!("LiveExecutor: channel disconnected");
        Ok(())
    }

    // ─── Signal dispatch ────────────────────────────────────────────────

    async fn handle_signal(&mut self, signal: TradeSignal) {
        if !signal.is_leg2 {
            self.handle_leg1(&signal).await;
        } else if signal.exit_reason.is_some() {
            self.handle_leg2_emergency(&signal).await;
        } else {
            self.handle_leg2_erosion(&signal).await;
        }
    }

    // ─── Leg 1: post-only GTC entry ────────────────────────────────────

    async fn handle_leg1(&mut self, signal: &TradeSignal) {
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
                    self.orders_failed += 1;

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
                    self.orders_placed += 1;

                    let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                        is_leg2: false,
                        order_id: resp.order_id,
                        price: signal.price,
                        size: signal.size,
                    });

                    self.log_signal_to_cold(signal, "submitted");
                }
            }
            Err(e) => {
                error!(error = %e, "Leg 1: order placement FAILED");
                self.orders_failed += 1;

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
            if let Err(e) = self.poly.cancel_order(prev_order_id).await {
                warn!(error = %e, "Leg 2 erosion: cancel failed (may already be filled)");
            }
            self.orders_cancelled += 1;
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
                    self.orders_placed += 1;

                    let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                        is_leg2: true,
                        order_id: resp.order_id,
                        price: signal.price,
                        size: signal.size,
                    });
                }
            }
            Err(e) => {
                error!(error = %e, "Leg 2 erosion: order placement FAILED");
                self.orders_failed += 1;
                self.active_leg2_order_id = None;

                let _ = self
                    .feedback_tx
                    .try_send(ExecutorFeedback::OrderFailed { is_leg2: true });
            }
        }
    }

    // ─── Leg 2 favorable exit: post-only first, FOK fallback ──────────

    async fn attempt_favorable_exit(&mut self, signal: &TradeSignal) {
        // Try aggressive post-only first — the ask has dropped, so posting
        // just below it should fill as maker with zero fee.
        let post_only_price = round_to_tick(signal.price - signal.tick_size, signal.tick_size);
        let order = OrderRequest::aggressive_post_only(
            signal.token_id.clone(),
            signal.side,
            post_only_price,
            signal.size,
        );

        match self.poly.place_order(&order).await {
            Ok(resp) => {
                if resp.status == OrderStatus::Rejected {
                    // Post-only rejected — ask moved, FOK fallback.
                    warn!(
                        price = %post_only_price,
                        "Leg 2 favorable exit: post-only REJECTED — FOK fallback"
                    );
                    self.favorable_exit_fok_fallback(signal).await;
                } else {
                    info!(
                        order_id = %resp.order_id,
                        price = %post_only_price,
                        "Leg 2 favorable exit: post-only accepted"
                    );
                    self.active_leg2_order_id = Some(resp.order_id.clone());
                    self.emergency_maker_posts += 1;
                    self.orders_placed += 1;

                    let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                        is_leg2: true,
                        order_id: resp.order_id,
                        price: post_only_price,
                        size: signal.size,
                    });
                }
            }
            Err(e) => {
                error!(error = %e, "Leg 2 favorable exit: post-only FAILED — FOK fallback");
                self.favorable_exit_fok_fallback(signal).await;
            }
        }
    }

    async fn favorable_exit_fok_fallback(&mut self, signal: &TradeSignal) {
        let order = OrderRequest::emergency_fok(
            signal.token_id.clone(),
            signal.side,
            signal.price,
            signal.size,
        );

        match self.poly.place_order(&order).await {
            Ok(resp) => {
                if resp.status == OrderStatus::Rejected {
                    warn!("Leg 2 favorable exit: FOK also rejected — erosion continues");
                    self.orders_failed += 1;
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
                    self.active_leg2_order_id = Some(resp.order_id.clone());
                    self.favorable_taker_fills += 1;
                    self.orders_placed += 1;

                    let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                        is_leg2: true,
                        order_id: resp.order_id,
                        price: signal.price,
                        size: signal.size,
                    });

                    let one = Decimal::ONE;
                    let inner = signal.price * (one - signal.price);
                    let fee_per_share = Decimal::new(25, 2) * inner * inner;
                    let est_fee = fee_per_share * signal.size;
                    self.reporter.send_alert(&format!(
                        "FAVORABLE TAKER FOK: at {} for {} shares (est. fee: ${:.4})",
                        signal.price, signal.size, est_fee,
                    ));
                }
            }
            Err(e) => {
                error!(error = %e, "Leg 2 favorable exit: FOK FAILED");
                self.orders_failed += 1;
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
            if let Err(e) = self.poly.cancel_order(prev_order_id).await {
                warn!(error = %e, "Leg 2 emergency: cancel failed");
            }
            self.orders_cancelled += 1;
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
                        let fok_price = round_to_tick(
                            signal.price + signal.tick_size,
                            signal.tick_size,
                        );
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
                        self.emergency_maker_posts += 1;
                        self.orders_placed += 1;

                        let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                            is_leg2: true,
                            order_id: resp.order_id,
                            price: signal.price,
                            size: signal.size,
                        });

                        self.reporter.send_alert(&format!(
                            "EMERGENCY POST-ONLY: {:?} at {} for {} shares (zero fee)",
                            exit_reason, signal.price, signal.size,
                        ));
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
        let order = OrderRequest::emergency_fok(
            signal.token_id.clone(),
            signal.side,
            signal.price,
            signal.size,
        );

        match self.poly.place_order(&order).await {
            Ok(resp) => {
                info!(
                    order_id = %resp.order_id,
                    status = ?resp.status,
                    "Leg 2 emergency: FOK fallback placed"
                );
                self.active_leg2_order_id = Some(resp.order_id.clone());
                self.emergency_foks += 1;
                self.orders_placed += 1;

                let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                    is_leg2: true,
                    order_id: resp.order_id,
                    price: signal.price,
                    size: signal.size,
                });

                self.reporter.send_alert(&format!(
                    "EMERGENCY FOK FALLBACK: {:?} at {} for {} shares",
                    exit_reason, signal.price, signal.size,
                ));
            }
            Err(e) => {
                error!(error = %e, "Leg 2 emergency: FOK FALLBACK FAILED — POSITION EXPOSED");
                self.orders_failed += 1;

                let _ = self
                    .feedback_tx
                    .try_send(ExecutorFeedback::OrderFailed { is_leg2: true });

                self.reporter.send_alert(&format!(
                    "CRITICAL: Emergency FOK FAILED: {} — position unhedged!",
                    e
                ));
            }
        }
    }

    // ─── Emergency FOK at a specific price (CLOB rejection fallback) ────

    async fn emergency_fok_at_price(
        &mut self,
        signal: &TradeSignal,
        exit_reason: crate::types::order::ExitReason,
        price: Decimal,
    ) {
        let order = OrderRequest::emergency_fok(
            signal.token_id.clone(),
            signal.side,
            price,
            signal.size,
        );

        match self.poly.place_order(&order).await {
            Ok(resp) => {
                info!(
                    order_id = %resp.order_id,
                    status = ?resp.status,
                    %price,
                    "Leg 2 emergency: FOK at price placed"
                );
                self.active_leg2_order_id = Some(resp.order_id.clone());
                self.emergency_foks += 1;
                self.orders_placed += 1;

                let _ = self.feedback_tx.try_send(ExecutorFeedback::OrderPosted {
                    is_leg2: true,
                    order_id: resp.order_id,
                    price,
                    size: signal.size,
                });

                self.reporter.send_alert(&format!(
                    "EMERGENCY FOK FALLBACK: {:?} at {} for {} shares",
                    exit_reason, price, signal.size,
                ));
            }
            Err(e) => {
                error!(error = %e, "Leg 2 emergency: FOK FALLBACK FAILED — POSITION EXPOSED");
                self.orders_failed += 1;

                let _ = self
                    .feedback_tx
                    .try_send(ExecutorFeedback::OrderFailed { is_leg2: true });

                self.reporter.send_alert(&format!(
                    "CRITICAL: Emergency FOK FAILED: {} — position unhedged!",
                    e
                ));
            }
        }
    }

    // ─── Market rotation ────────────────────────────────────────────────

    async fn on_market_rotation(&mut self, condition_id: &str) {
        info!(
            condition_id,
            "LiveExecutor: market rotation — cancelling all orders"
        );

        if let Err(e) = self.poly.cancel_all().await {
            warn!(error = %e, "cancel_all failed during rotation");
        }

        self.active_leg2_order_id = None;
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

        if let Err(e) = self.cold.record_signal(
            &signal.token_id,
            direction_str,
            signal.confidence,
            signal.spike_info.magnitude,
            signal.atr,
            signal.book_snapshot.as_ref().map(|b| b.total_bid_depth()).unwrap_or(Decimal::ZERO),
            time_remaining_secs as i64,
            signal.alloc_amount,
            action,
        ) {
            warn!(error = %e, "failed to record signal to QuestDB");
        }
    }

    // ─── Diagnostics ────────────────────────────────────────────────────

    fn check_diagnostic(&mut self) {
        let now_ms = now_epoch_ms();
        if self.last_diag_ms == 0 {
            self.last_diag_ms = now_ms;
            return;
        }
        if now_ms.saturating_sub(self.last_diag_ms) < 60_000 {
            return;
        }
        info!(
            placed = self.orders_placed,
            cancelled = self.orders_cancelled,
            failed = self.orders_failed,
            emergency_fok = self.emergency_foks,
            emergency_maker = self.emergency_maker_posts,
            favorable_taker = self.favorable_taker_fills,
            "live executor 60s"
        );
        self.last_diag_ms = now_ms;
    }
}

fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
