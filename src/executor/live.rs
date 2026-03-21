//! V2 Live Executor — simplified bilateral order management.
//!
//! Handles: post maker orders (both sides), cancel, closing FOK, market rotation.
//! No 2-phase hedge, no emergency escalation, no favorable exits.

use std::str::FromStr;

use alloy::primitives::U256;
use anyhow::Result;
use crossbeam_channel::{Receiver, Sender};
use rust_decimal::Decimal;
use tracing::{error, info, warn};

use crate::engine::position::MarketSide;
use crate::gateway::polymarket::PolymarketGateway;
use crate::reporting::telegram::TelegramReporter;
use crate::types::order::{
    OrderRequest, OrderStatus, Side, V2ExecutorCommand, V2ExecutorFeedback,
};

use super::fill_engine::round_to_tick;

/// Adjusts `size` so that `price * size` has at most 2 decimal places
/// (CLOB maker amount constraint) and `price * size >= $1.00` (minimum notional).
///
/// The CLOB rejects any FOK/FAK order where `price * size` has more than 2dp.
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

    // Downward search: find the largest s <= truncated where price*s has <=2dp
    // and price*s >= $1.00. Prefer under-hedging over over-hedging.
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

    // Upward fallback: smallest s >= min_size where price*s has <=2dp.
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

/// V2 live executor — handles bilateral maker orders, cancels, and closing FOKs.
pub struct LiveExecutor {
    poly: PolymarketGateway,
    feedback_tx: Sender<V2ExecutorFeedback>,
    reporter: TelegramReporter,

    // ── SDK cache gate ────────────────────────────────────────────────
    /// `true` once `tick_size`, `neg_risk`, and `fee_rate_bps` have been pre-warmed
    /// from the CLOB for both tokens of the current market. All orders are rejected
    /// while `false`.
    caches_warm: bool,
}

impl LiveExecutor {
    pub fn new(
        poly: PolymarketGateway,
        feedback_tx: Sender<V2ExecutorFeedback>,
        reporter: TelegramReporter,
    ) -> Self {
        Self {
            poly,
            feedback_tx,
            reporter,
            caches_warm: false,
        }
    }

    /// Main receive loop — consumes `V2ExecutorCommand` from the engine channel.
    pub async fn run(mut self, rx: Receiver<V2ExecutorCommand>) -> Result<()> {
        info!("V2 LiveExecutor: starting receive loop");
        self.reporter.send_live_startup_message();

        loop {
            let cmd = match tokio::task::block_in_place(|| rx.recv()) {
                Ok(cmd) => cmd,
                Err(_) => break,
            };
            match cmd {
                V2ExecutorCommand::PostOrder { side, token_id, price, size } => {
                    self.handle_post_order(side, &token_id, price, size).await;
                }
                V2ExecutorCommand::CancelOrder { side, order_id } => {
                    self.handle_cancel(side, &order_id).await;
                }
                V2ExecutorCommand::ClosingFok { side, token_id, price, size } => {
                    self.handle_closing_fok(side, &token_id, price, size).await;
                }
                V2ExecutorCommand::RebalanceTaker { side, token_id, price, size } => {
                    self.handle_rebalance_taker(side, &token_id, price, size).await;
                }
                V2ExecutorCommand::MarketRotation {
                    condition_id, yes_token_id, no_token_id, tick_size,
                } => {
                    self.on_market_rotation(
                        &condition_id, &yes_token_id, &no_token_id, tick_size,
                    ).await;
                }
                V2ExecutorCommand::TickSizeChanged {
                    yes_token_id, no_token_id, new_tick_size,
                } => {
                    self.handle_tick_size_change(&yes_token_id, &no_token_id, new_tick_size);
                }
            }
        }

        info!("V2 LiveExecutor: channel disconnected");
        Ok(())
    }

    // ─── Post maker order ───────────────────────────────────────────────

    async fn handle_post_order(
        &mut self,
        side: MarketSide,
        token_id: &str,
        price: Decimal,
        size: Decimal,
    ) {
        if !self.caches_warm {
            warn!(side = side.label(), "order REJECTED — SDK caches not warm");
            let _ = self.feedback_tx.try_send(V2ExecutorFeedback::OrderFailed { side });
            return;
        }

        let tick_size = Decimal::new(1, 2); // default; SDK uses cached tick size
        let rounded_price = round_to_tick(price, tick_size);
        let size = size.round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToZero);

        let order = OrderRequest::post_only_gtc(
            token_id.to_string(),
            Side::Buy,
            rounded_price,
            size,
        );

        info!(
            side = side.label(),
            token = %token_id,
            price = %rounded_price,
            size = %size,
            "posting maker order"
        );

        match self.poly.place_order(&order).await {
            Ok(resp) => {
                if resp.status == OrderStatus::Rejected {
                    warn!(side = side.label(), "maker order REJECTED (crosses book)");
                    let _ = self.feedback_tx.try_send(V2ExecutorFeedback::OrderFailed { side });
                    return;
                }
                let already_filled = resp.status == OrderStatus::Filled;
                let fill_size = if already_filled && resp.size_matched > Decimal::ZERO {
                    resp.size_matched
                } else {
                    size
                };
                info!(
                    side = side.label(),
                    order_id = %resp.order_id,
                    status = ?resp.status,
                    already_filled,
                    "maker order posted"
                );
                let _ = self.feedback_tx.try_send(V2ExecutorFeedback::OrderPosted {
                    side,
                    order_id: resp.order_id,
                    price: rounded_price,
                    size: fill_size,
                    already_filled,
                });
            }
            Err(e) => {
                error!(side = side.label(), error = %e, "maker order FAILED");
                let _ = self.feedback_tx.try_send(V2ExecutorFeedback::OrderFailed { side });
            }
        }
    }

    // ─── Cancel order ───────────────────────────────────────────────────

    async fn handle_cancel(&mut self, side: MarketSide, order_id: &str) {
        let was_cancelled = match self.poly.cancel_order(order_id).await {
            Ok(c) => c,
            Err(e) => {
                warn!(side = side.label(), %order_id, error = %e, "cancel failed");
                false
            }
        };

        // Query authoritative fill size after cancel.
        let size_matched = match self.poly.get_order_status(order_id).await {
            Ok((_status, matched, _original)) => Some(matched),
            Err(e) => {
                warn!(%order_id, error = %e, "get_order_status after cancel failed");
                None
            }
        };

        info!(side = side.label(), %order_id, %was_cancelled, ?size_matched, "cancel result");
        let _ = self.feedback_tx.try_send(V2ExecutorFeedback::CancelResult {
            side,
            order_id: order_id.to_string(),
            size_matched,
        });
    }

    // ─── Closing FOK ────────────────────────────────────────────────────

    async fn handle_closing_fok(
        &mut self,
        side: MarketSide,
        token_id: &str,
        price: Decimal,
        size: Decimal,
    ) {
        if !self.caches_warm {
            warn!(side = side.label(), "closing FOK REJECTED — caches not warm");
            let _ = self.feedback_tx.try_send(V2ExecutorFeedback::ClosingFokResult {
                side, filled: false, size_matched: Decimal::ZERO, price,
            });
            return;
        }

        let safe_size = clob_safe_fok_size(price, size);
        if safe_size.is_zero() {
            warn!(side = side.label(), %price, %size, "closing FOK: no valid CLOB-safe size");
            let _ = self.feedback_tx.try_send(V2ExecutorFeedback::ClosingFokResult {
                side, filled: false, size_matched: Decimal::ZERO, price,
            });
            return;
        }

        let order = OrderRequest::emergency_fok(
            token_id.to_string(),
            Side::Buy,
            price,
            safe_size,
        );

        info!(
            side = side.label(),
            token = %token_id,
            price = %price,
            size = %safe_size,
            "closing FOK"
        );

        match self.poly.place_order(&order).await {
            Ok(resp) => {
                let filled = resp.status == OrderStatus::Filled;
                let matched = resp.size_matched.min(safe_size);
                info!(
                    side = side.label(),
                    filled,
                    size_matched = %matched,
                    "closing FOK result"
                );
                let _ = self.feedback_tx.try_send(V2ExecutorFeedback::ClosingFokResult {
                    side,
                    filled,
                    size_matched: matched,
                    price,
                });
            }
            Err(e) => {
                error!(side = side.label(), error = %e, "closing FOK FAILED");
                let _ = self.feedback_tx.try_send(V2ExecutorFeedback::ClosingFokResult {
                    side, filled: false, size_matched: Decimal::ZERO, price,
                });
            }
        }
    }

    // ─── Rebalance taker ──────────────────────────────────────────────

    async fn handle_rebalance_taker(
        &mut self,
        side: MarketSide,
        token_id: &str,
        price: Decimal,
        size: Decimal,
    ) {
        if !self.caches_warm {
            warn!(side = side.label(), "rebalance REJECTED — caches not warm");
            let _ = self.feedback_tx.try_send(V2ExecutorFeedback::RebalanceResult {
                side, filled: false, size_matched: Decimal::ZERO, price,
            });
            return;
        }

        let safe_size = clob_safe_fok_size(price, size);
        if safe_size.is_zero() {
            warn!(side = side.label(), %price, %size, "rebalance: no valid CLOB-safe size");
            let _ = self.feedback_tx.try_send(V2ExecutorFeedback::RebalanceResult {
                side, filled: false, size_matched: Decimal::ZERO, price,
            });
            return;
        }

        let order = OrderRequest::emergency_fok(
            token_id.to_string(),
            Side::Buy,
            price,
            safe_size,
        );

        info!(
            side = side.label(),
            token = %token_id,
            price = %price,
            size = %safe_size,
            "rebalance FOK"
        );

        match self.poly.place_order(&order).await {
            Ok(resp) => {
                let filled = resp.status == OrderStatus::Filled;
                let matched = resp.size_matched.min(safe_size);
                info!(
                    side = side.label(),
                    filled,
                    size_matched = %matched,
                    "rebalance FOK result"
                );
                let _ = self.feedback_tx.try_send(V2ExecutorFeedback::RebalanceResult {
                    side,
                    filled,
                    size_matched: matched,
                    price,
                });
            }
            Err(e) => {
                error!(side = side.label(), error = %e, "rebalance FOK FAILED");
                let _ = self.feedback_tx.try_send(V2ExecutorFeedback::RebalanceResult {
                    side, filled: false, size_matched: Decimal::ZERO, price,
                });
            }
        }
    }

    // ─── Market rotation ────────────────────────────────────────────────

    async fn on_market_rotation(
        &mut self,
        condition_id: &str,
        yes_token_id: &str,
        no_token_id: &str,
        _tick_size: Decimal,
    ) {
        info!(
            condition_id,
            "V2 LiveExecutor: market rotation — cancelling all orders"
        );

        // Cancel any remaining open orders from the outgoing market.
        if let Err(e) = self.poly.cancel_all().await {
            warn!(error = %e, "cancel_all on rotation failed (may have no open orders)");
        }

        // Reset warm flag — block trading until pre-warm succeeds.
        self.caches_warm = false;

        // Pre-warm SDK caches (tick_size, neg_risk, fee_rate_bps) for both new tokens.
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
                    "SDK caches pre-warmed (tick_size, neg_risk, fee_rate, conn pool) — trading enabled"
                );
            } else {
                warn!("SDK cache pre-warm incomplete — trading BLOCKED until next rotation");
            }
        } else {
            warn!("no SDK client — trading BLOCKED");
        }
    }

    // ─── Tick size change ───────────────────────────────────────────────

    fn handle_tick_size_change(
        &mut self,
        yes_token_id: &str,
        no_token_id: &str,
        new_tick_size: Decimal,
    ) {
        if let Some(sdk) = self.poly.sdk_client() {
            use polymarket_client_sdk::clob::types::TickSize;
            match TickSize::try_from(new_tick_size) {
                Ok(tick) => {
                    for token_id_str in [yes_token_id, no_token_id] {
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
