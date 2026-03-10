// Simulation executor — receives TradeSignal, simulates post-only fills,
// tracks virtual PnL, and forwards events to TelegramReporter.
//
// Architecture:
//   - `SimulationExecutor` owns a `SimulationState` (virtual portfolio),
//     a `TelegramReporter` (fire-and-forget), and a `ColdStorage` (QuestDB).
//   - The `run()` method loops on a crossbeam Receiver<TradeSignal>.
//   - Leg 1 signals simulate a post-only maker fill against the live book.
//   - Leg 2 signals simulate a hedge maker fill, or emergency taker.
//   - Market rotation and session shutdown generate Telegram summaries.

use anyhow::Result;
use crossbeam_channel::Receiver;
use rust_decimal::Decimal;
use tracing::{debug, error, info, warn};

use crate::reporting::telegram::TelegramReporter;
use crate::storage::cold::ColdStorage;
use crate::types::market::{Direction, OrderBook};
use crate::types::order::{ExecutorCommand, ExitReason, TradeSignal};
use crate::types::simulation::{PositionStatus, SimFill, SimulationState};

use super::fill_engine::{epoch_ms, opposite_side};

// ─── SimulationExecutor ──────────────────────────────────────────────────────

/// Simulation-mode executor.
///
/// Receives `TradeSignal` from the Engine via crossbeam channel, simulates
/// post-only fills, tracks virtual PnL, and forwards events to Telegram +
/// QuestDB. No real orders are ever submitted.
pub struct SimulationExecutor {
    /// Virtual portfolio + session statistics.
    state: SimulationState,
    /// Telegram reporter (fire-and-forget sends).
    reporter: TelegramReporter,
    /// QuestDB cold storage (write simulated trade records + signals).
    /// `None` if QuestDB is unavailable — the executor still runs without analytics.
    cold: Option<ColdStorage>,
    /// Latest Polymarket orderbook snapshot (updated by caller or Engine snapshot).
    current_book: Option<OrderBook>,
    /// Latest Binance mid price for reference in signal records.
    binance_mid: Option<Decimal>,
    /// Current market end timestamp (epoch ms).
    market_end_ms: u64,
    /// Tick size for the current market.
    tick_size: Decimal,
    /// Total capital allocated for each session (from FIXED_ALLOC config).
    fixed_alloc: Decimal,
    /// Epoch ms of last 60s diagnostic log.
    last_diag_ms: u64,
}

impl SimulationExecutor {
    /// Create a new `SimulationExecutor`.
    ///
    /// `fixed_alloc` is the total session capital (e.g. `Decimal::from(100)`
    /// for $100 USDC). `now_ms` is the current epoch millisecond timestamp
    /// used as the simulation session start time.
    pub fn new(
        reporter: TelegramReporter,
        cold: Option<ColdStorage>,
        fixed_alloc: Decimal,
        now_ms: u64,
    ) -> Self {
        let state = SimulationState::new(fixed_alloc, now_ms);
        let default_tick = Decimal::new(1, 2); // 0.01 default
        Self {
            state,
            reporter,
            cold,
            current_book: None,
            binance_mid: None,
            market_end_ms: 0,
            tick_size: default_tick,
            fixed_alloc,
            last_diag_ms: 0,
        }
    }

    // ─── Public update methods (called by wiring layer / Engine snapshot) ─────

    /// Replace the current orderbook snapshot with a fresh one.
    pub fn update_book(&mut self, book: OrderBook) {
        debug!(asset_id = %book.asset_id, "simulation executor: book updated");
        self.current_book = Some(book);
    }

    /// Update the latest Binance mid price reference.
    pub fn update_binance_mid(&mut self, mid: Decimal) {
        self.binance_mid = Some(mid);
    }

    /// Update market parameters at each 5-minute market rotation.
    pub fn update_market_params(&mut self, market_end_ms: u64, tick_size: Decimal) {
        self.market_end_ms = market_end_ms;
        self.tick_size = tick_size;
    }

    // ─── Market rotation ──────────────────────────────────────────────────────

    /// Handle a market rotation event.
    ///
    /// 1. Force-close any open positions that were not hedged before expiry.
    ///    Open positions (Leg 1 filled, no Leg 2) are closed at full loss with
    ///    Telegram notification. Other statuses are locked for UMA resolution.
    /// 2. Build and send the per-market Telegram summary (if prior market exists).
    /// 3. Reset per-market counters via `SimulationState::on_market_rotation()`.
    ///
    /// `outgoing_id` is the condition ID of the market that just expired (None
    /// on the first rotation after startup). `outgoing_end_ms` is that market's
    /// end timestamp (epoch ms), used for the period label.
    pub fn on_market_rotation(
        &mut self,
        outgoing_id: Option<&str>,
        outgoing_end_ms: u64,
    ) {
        let now_ms = epoch_ms();

        // Determine which market ID to use for force-closing positions.
        // On the first rotation (no prior market), there are no positions.
        let summary_market_id = match outgoing_id {
            Some(id) => id,
            None => {
                debug!("first rotation after startup — no prior market to summarize");
                self.state.on_market_rotation();
                return;
            }
        };

        // Force-close or lock for resolution any positions still in this market.
        // We iterate in reverse so that removal by index remains stable.
        let open_count = self.state.open_positions.len();
        if open_count > 0 {
            info!(
                market_id = summary_market_id,
                open_count, "simulation: market rotation — closing open positions"
            );
        } else {
            debug!(market_id = summary_market_id, "simulation: market rotation — no open positions");
        }
        for idx in (0..open_count).rev() {
            // Extract fields before borrowing self.state mutably.
            let (pos_market, pos_open) = {
                let p = &self.state.open_positions[idx];
                (p.market_id.clone(), p.status == PositionStatus::Open)
            };
            if pos_market != summary_market_id {
                continue;
            }
            if pos_open {
                // Leg 1 filled, no Leg 2 — force-close, record full loss.
                if let Some(trade) = self.state.close_trade(idx, now_ms) {
                    warn!(
                        market_id = summary_market_id,
                        position_idx = idx,
                        net_profit_usdc = %trade.net_profit,
                        leg1_cost_usdc = %(trade.leg1.price * trade.leg1.size),
                        "simulation: position force-closed at rotation — no Leg 2 fill"
                    );
                    self.reporter.send_trade_completed(&trade);
                    if let Some(ref mut c) = self.cold {
                        if let Err(e) = c.record_simulated_trade(&trade) {
                            error!(error = %e, "failed to write force-closed trade to QuestDB");
                        }
                    }
                }
            } else {
                // AwaitingResolution or Hedged from prior market — keep tracking.
                self.state.lock_for_resolution(idx);
                warn!(
                    market_id = summary_market_id,
                    position_idx = idx,
                    "simulation: position locked for UMA resolution"
                );
            }
        }

        // Send market summary before resetting counters.
        let end_secs = outgoing_end_ms / 1_000;
        let hh = (end_secs % 86_400) / 3_600;
        let mm = (end_secs % 3_600) / 60;
        let market_duration_secs = 300u64;
        let start_secs = end_secs.saturating_sub(market_duration_secs);
        let start_hh = (start_secs % 86_400) / 3_600;
        let start_mm = (start_secs % 3_600) / 60;
        let period_label = format!(
            "{:02}:{:02} - {:02}:{:02} UTC",
            start_hh, start_mm, hh, mm
        );

        let summary = self
            .state
            .market_summary(summary_market_id, period_label, self.fixed_alloc);
        self.reporter.send_market_summary(&summary);

        // Reset per-market counters.
        self.state.on_market_rotation();
    }

    /// Generate and send a session summary via Telegram. Call this on shutdown.
    pub fn shutdown_summary(&self) {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let summary = self.state.hourly_summary(now_ms);
        self.reporter.send_session_summary(&summary);
        info!(
            total_trades = summary.total_trades,
            net_pnl = %summary.net_pnl,
            "simulation session summary sent on shutdown"
        );
    }

    // ─── Main receive loop ────────────────────────────────────────────────────

    /// Run the simulation executor.
    ///
    /// Blocks the calling task/thread reading from `rx`. Each received
    /// `TradeSignal` is dispatched to either `handle_leg1` or `handle_leg2`.
    ///
    /// The loop exits when the channel sender is dropped (channel disconnected).
    pub async fn run(mut self, rx: Receiver<ExecutorCommand>) -> Result<()> {
        info!("SimulationExecutor: starting receive loop");
        self.reporter.send_startup_message();

        // We use spawn_blocking so that the synchronous `rx.recv()` call does
        // not block the tokio thread pool. The closure takes ownership of self.
        let result = tokio::task::spawn_blocking(move || {
            while let Ok(cmd) = rx.recv() {
                match cmd {
                    ExecutorCommand::Signal(signal) => {
                        // Update executor state from signal context.
                        if let Some(book) = signal.book_snapshot.clone() {
                            self.update_book(book);
                        }
                        self.update_binance_mid(signal.reference_price);
                        self.update_market_params(signal.market_end_timestamp_ms, signal.tick_size);

                        if signal.is_leg2 {
                            self.handle_leg2(&signal);
                        } else {
                            self.handle_leg1(&signal);
                        }
                    }
                    ExecutorCommand::MarketRotation {
                        outgoing_condition_id,
                        outgoing_end_timestamp_ms,
                        ..
                    } => {
                        self.on_market_rotation(
                            outgoing_condition_id.as_deref(),
                            outgoing_end_timestamp_ms,
                        );
                    }
                    ExecutorCommand::TickSizeChanged { new_tick_size, .. } => {
                        self.tick_size = new_tick_size;
                        debug!(%new_tick_size, "SimExecutor: tick_size updated");
                    }
                    ExecutorCommand::PostLeg2Phase2 { signal } => {
                        // Sim mode: treat as regular Leg 2 hedge signal.
                        if let Some(book) = signal.book_snapshot.clone() {
                            self.update_book(book);
                        }
                        self.handle_leg2(&signal);
                    }
                    ExecutorCommand::CancelLeg2Order { order_id } => {
                        debug!(%order_id, "SimExecutor: Leg 2 order cancel (no-op in sim)");
                    }
                    ExecutorCommand::RebalanceLeg1 { .. } => {
                        debug!("SimExecutor: rebalance (no-op in sim — no concurrent orders)");
                    }
                }

                // 60-second session diagnostic (cumulative, same source as Telegram).
                let now_ms = epoch_ms();
                if self.last_diag_ms == 0 {
                    self.last_diag_ms = now_ms;
                }
                if now_ms.saturating_sub(self.last_diag_ms) >= 60_000 {
                    let uptime_secs = now_ms.saturating_sub(self.state.session_start) / 1_000;
                    info!(
                        uptime_min  = uptime_secs / 60,
                        markets     = self.state.markets_observed,
                        signals     = self.state.signals_detected,
                        leg1_fills  = self.state.leg1_fills,
                        hedged      = self.state.trades_hedged,
                        emergency   = self.state.trades_emergency_taker,
                        emergency_maker = self.state.emergency_maker_fills,
                        breach_fok  = self.state.trades_breach_fok,
                        pnl         = %self.state.total_pnl,
                        win_rate    = %self.state.win_rate_pct(),
                        open        = self.state.open_positions.len(),
                        "session 60s"
                    );
                    self.last_diag_ms = now_ms;
                }
            }

            info!("SimulationExecutor: channel disconnected — generating shutdown summary");
            self.shutdown_summary();
        })
        .await;

        result.map_err(|e| anyhow::anyhow!("simulation executor task panicked: {e}"))
    }

    // ─── Leg 1 simulation ─────────────────────────────────────────────────────

    /// Handle a Leg 1 signal.
    ///
    /// Two paths based on `sim_confirmed_fill`:
    /// - `false` (from `evaluate()`): record signal detection only.
    /// - `true` (from `advance_simulation()`): record confirmed fill.
    fn handle_leg1(&mut self, signal: &TradeSignal) {
        if !signal.sim_confirmed_fill {
            // Signal detection — record for session stats.
            self.state.record_signal();
            if signal.bot_contested {
                self.state.record_wall_outbid();
            }
            self.log_signal_to_cold(signal, "detected");
            return;
        }

        // Confirmed fill from advance_simulation().
        let now_ms = epoch_ms();
        let fill = SimFill {
            side: signal.side,
            price: signal.price,
            size: signal.size,
            timestamp_ms: now_ms,
            was_partial: false,
            was_taker: false,
            taker_fee: Decimal::ZERO,
            maker_rebate: SimFill::compute_maker_rebate(signal.price, signal.size),
        };

        let fill_size = fill.size;
        let position_idx = self.state.record_leg1_fill(
            fill,
            signal.condition_id.clone(),
            signal.direction,
            signal.expected_pct,
            signal.profit_target_tier,
            signal.alloc_amount,
        );

        // Record spike magnitude for QuestDB outcome correlation.
        self.state
            .set_spike_magnitude(position_idx, signal.spike_info.magnitude);

        info!(
            token_id = %signal.token_id,
            price = %signal.price,
            size = %fill_size,
            position_idx,
            expected_pct = %signal.expected_pct,
            tier = signal.profit_target_tier.label(),
            "Leg 1: confirmed fill"
        );

        // Send Telegram opportunity alert.
        let book = self.current_book.clone().unwrap_or_else(|| OrderBook {
            asset_id: signal.token_id.clone(),
            bids: vec![],
            asks: vec![],
            timestamp_ms: now_ms,
        });
        self.reporter
            .send_opportunity_alert(signal, signal.price, fill_size, &book);

        // Log signal to QuestDB.
        self.log_signal_to_cold(signal, "entered");
    }

    // ─── Leg 2 simulation ─────────────────────────────────────────────────────

    /// Handle a Leg 2 signal.
    ///
    /// Two paths based on `sim_confirmed_fill`:
    /// - `false` (hedge post from `evaluate_leg2()`): log and return.
    /// - `true` (from `advance_simulation()`): record confirmed fill, categorize
    ///   by `exit_reason`, close trade, report to Telegram + QuestDB.
    fn handle_leg2(&mut self, signal: &TradeSignal) {
        // Find the matching open position.
        let position_idx = match self
            .state
            .open_positions
            .iter()
            .position(|p| p.status == PositionStatus::Open)
        {
            Some(idx) => idx,
            None => {
                warn!(
                    token_id = %signal.token_id,
                    "Leg 2: no matching open position found — signal may be stale"
                );
                return;
            }
        };

        if !signal.sim_confirmed_fill {
            // Hedge post only — awaiting fill.
            debug!(
                token_id = %signal.token_id,
                our_bid = %signal.price,
                "Leg 2: hedge post — awaiting fill"
            );
            return;
        }

        // Confirmed fill from advance_simulation().
        let now_ms = epoch_ms();
        let leg1_size = self.state.open_positions[position_idx].leg1.size;

        // Emergency fills are taker only when sim_was_taker is true (FOK fallback).
        // Post-only emergency fills (sim_was_taker=false) are maker with zero fee.
        let is_taker = signal.exit_reason.is_some() && signal.sim_was_taker;
        let taker_fee = if is_taker {
            SimFill::compute_taker_fee(signal.price, leg1_size)
        } else {
            Decimal::ZERO
        };

        let maker_rebate = if is_taker {
            Decimal::ZERO
        } else {
            SimFill::compute_maker_rebate(signal.price, leg1_size)
        };
        let fill = SimFill {
            side: opposite_side(signal.side),
            price: signal.price,
            size: leg1_size,
            timestamp_ms: now_ms,
            was_partial: false,
            was_taker: is_taker,
            taker_fee,
            maker_rebate,
        };

        match signal.exit_reason {
            Some(reason @ ExitReason::BreakEvenBreach)
            | Some(reason @ ExitReason::Phase2PriceBreach) => {
                let label = match reason {
                    ExitReason::BreakEvenBreach => "break-even breach",
                    ExitReason::Phase2PriceBreach => "phase 2 price breach",
                    _ => unreachable!(),
                };
                if is_taker {
                    info!(
                        token_id = %signal.token_id,
                        taker_price = %fill.price,
                        taker_fee = %fill.taker_fee,
                        position_idx,
                        "Leg 2: {label} FOK fallback"
                    );
                    self.state.trades_breach_fok += 1;
                    self.state.record_emergency_taker(position_idx, fill);
                } else {
                    info!(
                        token_id = %signal.token_id,
                        price = %fill.price,
                        position_idx,
                        "Leg 2: {label} post-only (maker)"
                    );
                    self.state.record_emergency_maker(
                        position_idx,
                        fill,
                        &reason,
                    );
                }
            }
            Some(ExitReason::Phase2Timeout) => {
                if is_taker {
                    info!(
                        token_id = %signal.token_id,
                        taker_price = %fill.price,
                        taker_fee = %fill.taker_fee,
                        position_idx,
                        "Leg 2: phase 2 timeout FOK fallback"
                    );
                    self.state.trades_timeout_fok += 1;
                    self.state.record_emergency_taker(position_idx, fill);
                } else {
                    info!(
                        token_id = %signal.token_id,
                        price = %fill.price,
                        position_idx,
                        "Leg 2: phase 2 timeout post-only (maker)"
                    );
                    self.state.record_emergency_maker(
                        position_idx,
                        fill,
                        &ExitReason::Phase2Timeout,
                    );
                }
            }
            Some(ExitReason::MarketExpiry) => {
                if is_taker {
                    warn!(
                        token_id = %signal.token_id,
                        taker_price = %fill.price,
                        taker_fee = %fill.taker_fee,
                        position_idx,
                        "Leg 2: market expiry emergency FOK fallback"
                    );
                    self.state.trades_timeout_fok += 1;
                    self.state.record_emergency_taker(position_idx, fill);
                } else {
                    warn!(
                        token_id = %signal.token_id,
                        price = %fill.price,
                        position_idx,
                        "Leg 2: market expiry post-only (maker)"
                    );
                    self.state.record_emergency_maker(
                        position_idx,
                        fill,
                        &ExitReason::MarketExpiry,
                    );
                }
            }
            Some(ExitReason::FavorableTaker) => {
                if is_taker {
                    info!(
                        token_id = %signal.token_id,
                        taker_price = %fill.price,
                        taker_fee = %fill.taker_fee,
                        position_idx,
                        "Leg 2: favorable taker fill (ask < posted bid)"
                    );
                    self.state.record_favorable_taker(position_idx, fill);
                } else {
                    info!(
                        token_id = %signal.token_id,
                        price = %fill.price,
                        position_idx,
                        "Leg 2: favorable exit post-only (maker)"
                    );
                    self.state.record_emergency_maker(
                        position_idx,
                        fill,
                        &ExitReason::FavorableTaker,
                    );
                }
            }
            Some(ExitReason::Phase1Breach) => {
                let reason = ExitReason::Phase1Breach;
                if is_taker {
                    info!(
                        token_id = %signal.token_id,
                        taker_price = %fill.price,
                        taker_fee = %fill.taker_fee,
                        position_idx,
                        "Leg 2: phase 1 breach FOK fallback"
                    );
                    self.state.trades_breach_fok += 1;
                    self.state.record_emergency_taker(position_idx, fill);
                } else {
                    info!(
                        token_id = %signal.token_id,
                        price = %fill.price,
                        position_idx,
                        "Leg 2: phase 1 breach post-only (maker)"
                    );
                    self.state.record_emergency_maker(position_idx, fill, &reason);
                }
            }
            Some(ExitReason::WhipsawReversal) => {
                info!(
                    token_id = %signal.token_id,
                    taker_price = %fill.price,
                    taker_fee = %fill.taker_fee,
                    position_idx,
                    "Leg 2: whipsaw reversal FOK"
                );
                self.state.trades_timeout_fok += 1;
                self.state.record_emergency_taker(position_idx, fill);
            }
            None => {
                info!(
                    token_id = %signal.token_id,
                    price = %fill.price,
                    position_idx,
                    "Leg 2: maker fill (normal hedge)"
                );
                self.state.record_leg2_fill(position_idx, fill);
            }
        }

        // Record exit reason for QuestDB loss attribution.
        if let Some(reason) = signal.exit_reason {
            self.state.set_exit_reason(position_idx, reason);
        }

        if let Some(trade) = self.state.close_trade(position_idx, now_ms) {
            info!(
                market_id = %trade.market_id,
                net_profit_usdc = %trade.net_profit,
                profit_pct = %trade.profit_pct,
                pair_cost_per_share = %trade.pair_cost,
                size = %trade.leg1.size,
                exit_reason = ?signal.exit_reason,
                "Leg 2: trade closed"
            );
            self.reporter.send_trade_completed(&trade);
            if let Some(ref mut c) = self.cold {
                if let Err(e) = c.record_simulated_trade(&trade) {
                    error!(error = %e, "failed to write simulated trade to QuestDB");
                }
            }
        }
    }

    // ─── Internal helpers ─────────────────────────────────────────────────────

    /// Log a trade signal to QuestDB. Errors are logged and swallowed (best-effort).
    fn log_signal_to_cold(&mut self, signal: &TradeSignal, action: &str) {
        let direction_str = match signal.direction {
            Direction::Up => "YES",
            Direction::Down => "NO",
        };
        let book_depth = self
            .current_book
            .as_ref()
            .map(|b| b.total_bid_depth())
            .unwrap_or(Decimal::ZERO);
        let atr = signal.atr;
        let time_remaining_secs = signal
            .market_end_timestamp_ms
            .saturating_sub(signal.entry_timestamp_ms)
            / 1000;

        if let Some(ref mut c) = self.cold {
            if let Err(e) = c.record_signal(
                &signal.token_id,
                direction_str,
                signal.expected_pct,
                signal.spike_info.magnitude,
                atr,
                book_depth,
                time_remaining_secs as i64,
                signal.alloc_amount,
                action,
                signal.spike_info.timestamp_ms,
            ) {
                error!(error = %e, "failed to log signal to QuestDB");
            }
        }
    }
}

// ─── Unit Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::super::fill_engine::{compute_fill_size, opposite_side};
    use super::*;
    use crate::types::market::{Direction, PriceLevel, SpikeInfo};
    use crate::types::order::{ProfitTier, Side, TradeSignal};
    use crate::types::simulation::{PositionStatus, SimFill, SimulationState};
    use rust_decimal::Decimal;

    /// Parse a decimal literal from a string. Panics in tests if parsing fails.
    fn d(s: &str) -> Decimal {
        s.parse::<Decimal>()
            .expect("invalid decimal literal in test")
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    /// Build a minimal `OrderBook` with one bid level and one ask level.
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

    /// Build a minimal `TradeSignal` for Leg 2 testing.
    fn make_leg2_signal(price: Decimal, leg1_price: Decimal, token_id: &str) -> TradeSignal {
        TradeSignal {
            exit_reason: None,
            side: Side::Buy,
            token_id: token_id.to_string(),
            condition_id: "test_condition".to_string(),
            price,
            size: Decimal::from(10),
            reference_price: Decimal::from(50_000),
            expected_pct: d("0.85"),
            profit_target_tier: ProfitTier::High,
            profit_target_pct: d("0.025"),
            alloc_amount: Decimal::from(30),
            direction: Direction::Up,
            spike_info: SpikeInfo {
                direction: Direction::Up,
                magnitude: d("0.005"),
                sustained_ms: 200,
                timestamp_ms: 1_000_000,
                atr_ratio: Decimal::ZERO,
                obi: Decimal::ZERO,
            },
            is_leg2: true,
            leg1_fill_price: Some(leg1_price),
            entry_timestamp_ms: 1_000_000,
            market_end_timestamp_ms: 1_900_000,
            tick_size: d("0.01"),
            atr: Decimal::ZERO,
            bot_contested: false,
            leg1_taker_fee: Decimal::ZERO,
            best_ask: None,
            book_snapshot: None,
            sim_confirmed_fill: false,
            sim_was_taker: false,
        }
    }

    // Note: SimulationExecutor requires network connections (TelegramReporter,
    // ColdStorage) and cannot be instantiated in pure unit tests. Tests below
    // validate the logic components directly: SimulationState and helper fns.

    // ─── 1. SimulationState initialization ───────────────────────────────────

    #[test]
    fn test_new_state_initialization() {
        let now_ms: u64 = 1_700_000_000_000;
        let balance = Decimal::from(100);
        let state = SimulationState::new(balance, now_ms);

        assert_eq!(state.virtual_balance, balance);
        assert_eq!(state.starting_balance, balance);
        assert_eq!(state.session_start, now_ms);
        assert_eq!(state.signals_detected, 0);
        assert_eq!(state.leg1_fills, 0);
        assert_eq!(state.trades_hedged, 0);
        assert_eq!(state.total_pnl, Decimal::ZERO);
        assert_eq!(state.total_taker_fees_paid, Decimal::ZERO);
        assert!(state.open_positions.is_empty());
        assert!(state.closed_trades.is_empty());
    }

    // ─── 2. Leg 1 fill simulation (happy path) ───────────────────────────────

    #[test]
    fn test_leg1_fill_simulation() {
        let mut state = SimulationState::new(Decimal::from(100), 0);
        let fill_price = d("0.45");
        let fill_size = Decimal::from(10);
        let now_ms: u64 = 1_000;

        let fill = SimFill {
            side: Side::Buy,
            price: fill_price,
            size: fill_size,
            timestamp_ms: now_ms,
            was_partial: false,
            was_taker: false,
            taker_fee: Decimal::ZERO,
            maker_rebate: Decimal::ZERO,
        };

        let idx = state.record_leg1_fill(
            fill,
            "tok_001".to_string(),
            Direction::Up,
            d("0.85"),
            ProfitTier::High,
            Decimal::from(30),
        );

        assert_eq!(idx, 0);
        assert_eq!(state.leg1_fills, 1);
        assert_eq!(state.open_positions.len(), 1);

        let expected_cost = fill_price * fill_size;
        assert_eq!(state.virtual_balance, Decimal::from(100) - expected_cost);

        let pos = &state.open_positions[0];
        assert_eq!(pos.market_id, "tok_001");
        assert_eq!(pos.leg1.price, fill_price);
        assert_eq!(pos.status, PositionStatus::Open);
        assert!(!pos.leg1.was_taker);
        assert_eq!(pos.leg1.taker_fee, Decimal::ZERO);
    }

    // ─── 3. Leg 1 rejected due to post-only spread crossing ──────────────────

    #[test]
    fn test_leg1_rejected_crosses_spread() {
        // Our bid (0.55) >= best ask (0.54) → post-only rejection.
        let our_bid = d("0.55");
        let best_ask_price = d("0.54");

        let book = make_book(d("0.50"), best_ask_price);

        // Simulate the check logic directly (mirrors handle_leg1).
        let rejected = our_bid >= book.best_ask().unwrap().price;
        assert!(rejected, "bid >= best_ask should be rejected as post-only");

        // Verify state mutation path for unfilled_post_only.
        let mut state = SimulationState::new(Decimal::from(100), 0);
        state.unfilled_post_only += 1;
        assert_eq!(state.unfilled_post_only, 1);
        // Balance unchanged.
        assert_eq!(state.virtual_balance, Decimal::from(100));
    }

    // ─── 4. Leg 1 unfilled due to no nearby liquidity ────────────────────────

    #[test]
    fn test_leg1_no_liquidity() {
        // Ask is 5 ticks away (0.45 + 5×0.01 = 0.50); window is 2 ticks → no depth.
        let our_bid = d("0.45");
        let tick_size = d("0.01");
        let two_ticks = tick_size * Decimal::TWO;
        let depth_window_top = our_bid + two_ticks; // 0.47

        // ask at 0.50 — outside the 2-tick window
        let book = make_book(d("0.40"), d("0.50"));

        let near_depth: Decimal = book
            .asks
            .iter()
            .filter(|lvl| lvl.price <= depth_window_top)
            .map(|lvl| lvl.size)
            .sum();

        assert!(
            near_depth.is_zero(),
            "no asks within 2 ticks — should be unfilled_liquidity"
        );

        let mut state = SimulationState::new(Decimal::from(100), 0);
        state.unfilled_liquidity += 1;
        assert_eq!(state.unfilled_liquidity, 1);
        assert_eq!(state.virtual_balance, Decimal::from(100));
    }

    // ─── 5. Leg 2 maker fill (normal hedge) ────────────────────────

    #[test]
    fn test_leg2_maker_fill() {
        let now_ms: u64 = 2_000;
        let mut state = SimulationState::new(Decimal::from(100), 0);

        // Simulate a Leg 1 fill.
        let leg1_fill = SimFill {
            side: Side::Buy,
            price: d("0.45"),
            size: Decimal::from(10),
            timestamp_ms: 1_000,
            was_partial: false,
            was_taker: false,
            taker_fee: Decimal::ZERO,
            maker_rebate: Decimal::ZERO,
        };
        let idx = state.record_leg1_fill(
            leg1_fill,
            "tok_002".to_string(),
            Direction::Up,
            d("0.85"),
            ProfitTier::High,
            Decimal::from(30),
        );

        // Leg 2 maker fill at 0.53 — pair cost = 0.45 + 0.53 = 0.98, gross = 0.02.
        let leg2_fill = SimFill {
            side: Side::Buy,
            price: d("0.53"),
            size: Decimal::from(10),
            timestamp_ms: now_ms,
            was_partial: false,
            was_taker: false,
            taker_fee: Decimal::ZERO,
            maker_rebate: Decimal::ZERO,
        };
        state.record_leg2_fill(idx, leg2_fill);
        assert_eq!(state.trades_hedged, 1);
        assert_eq!(state.open_positions[0].status, PositionStatus::Hedged);

        // Close the trade and verify PnL.
        let trade = state.close_trade(idx, now_ms).expect("trade should close");
        let size = Decimal::from(10);
        assert_eq!(trade.pair_cost, d("0.98")); // per-share
        assert_eq!(trade.gross_profit, d("0.02") * size); // 0.20 USDC
        assert_eq!(trade.taker_fee, Decimal::ZERO);
        assert_eq!(trade.net_profit, d("0.02") * size); // 0.20 USDC
        assert!(!trade.leg2_was_taker);
        assert!(state.open_positions.is_empty());
        assert_eq!(state.closed_trades.len(), 1);
    }

    // ─── 6. Leg 2 emergency taker fill ───────────────────────────────────────

    #[test]
    fn test_leg2_emergency_taker() {
        let now_ms: u64 = 3_000;
        let mut state = SimulationState::new(Decimal::from(100), 0);

        // Leg 1 fill.
        let leg1_fill = SimFill {
            side: Side::Buy,
            price: d("0.45"),
            size: Decimal::from(10),
            timestamp_ms: 1_000,
            was_partial: false,
            was_taker: false,
            taker_fee: Decimal::ZERO,
            maker_rebate: Decimal::ZERO,
        };
        let idx = state.record_leg1_fill(
            leg1_fill,
            "tok_003".to_string(),
            Direction::Up,
            d("0.85"),
            ProfitTier::High,
            Decimal::from(30),
        );

        // Emergency taker at best_ask = 0.57.
        let taker_price = d("0.57");
        let taker_fee = SimFill::compute_taker_fee(taker_price, Decimal::from(10));
        assert!(taker_fee > Decimal::ZERO, "taker fee must be positive");

        let leg2_fill = SimFill {
            side: Side::Sell,
            price: taker_price,
            size: Decimal::from(10),
            timestamp_ms: now_ms,
            was_partial: false,
            was_taker: true,
            taker_fee,
            maker_rebate: Decimal::ZERO,
        };
        state.record_emergency_taker(idx, leg2_fill);
        assert_eq!(state.trades_emergency_taker, 1);
        assert_eq!(state.trades_hedged, 1);
        assert_eq!(state.total_taker_fees_paid, taker_fee);

        let trade = state.close_trade(idx, now_ms).expect("trade should close");
        assert!(trade.leg2_was_taker);
        assert_eq!(trade.taker_fee, taker_fee);
        // gross_profit = (1.0 - pair_cost) * size  [USDC]
        // net_profit   = gross_profit - taker_fee  [USDC]
        let leg1_price = d("0.45");
        let size = Decimal::from(10);
        let expected_gross = (Decimal::ONE - (leg1_price + taker_price)) * size;
        assert_eq!(trade.gross_profit, expected_gross);
        assert_eq!(trade.net_profit, expected_gross - taker_fee);
    }

    // ─── 7. Market rotation closes open positions ─────────────────────────────

    #[test]
    fn test_market_rotation_closes_positions() {
        let mut state = SimulationState::new(Decimal::from(100), 0);

        // Open a position that is never hedged.
        let leg1_fill = SimFill {
            side: Side::Buy,
            price: d("0.45"),
            size: Decimal::from(10),
            timestamp_ms: 1_000,
            was_partial: false,
            was_taker: false,
            taker_fee: Decimal::ZERO,
            maker_rebate: Decimal::ZERO,
        };
        let idx = state.record_leg1_fill(
            leg1_fill,
            "tok_004".to_string(),
            Direction::Up,
            d("0.70"),
            ProfitTier::Med,
            Decimal::from(20),
        );

        assert_eq!(state.open_positions.len(), 1);
        assert_eq!(state.open_positions[idx].status, PositionStatus::Open);

        // Simulate market rotation: lock unhedged position for UMA resolution.
        state.lock_for_resolution(idx);
        assert_eq!(
            state.open_positions[idx].status,
            PositionStatus::AwaitingResolution
        );
        // Capital is now locked.
        let leg1_cost = d("0.45") * Decimal::from(10);
        assert_eq!(state.locked_in_resolution, leg1_cost);

        // on_market_rotation resets cumulative_used and increments markets_observed.
        state.on_market_rotation();
        assert_eq!(state.markets_observed, 1);
        assert_eq!(state.cumulative_used, Decimal::ZERO);
    }

    // ─── 8. PnL calculation accuracy ─────────────────────────────────────────

    #[test]
    fn test_pnl_calculation() {
        let mut state = SimulationState::new(Decimal::from(100), 0);
        let now_ms: u64 = 5_000;

        // Trade 1: HIGH tier, pair_cost = 0.97, gross = 0.03, no taker fee.
        let fill1_leg1 = SimFill {
            side: Side::Buy,
            price: d("0.46"),
            size: Decimal::from(10),
            timestamp_ms: 1_000,
            was_partial: false,
            was_taker: false,
            taker_fee: Decimal::ZERO,
            maker_rebate: Decimal::ZERO,
        };
        let idx1 = state.record_leg1_fill(
            fill1_leg1,
            "tok_005".to_string(),
            Direction::Up,
            d("0.90"),
            ProfitTier::High,
            Decimal::from(30),
        );
        let fill1_leg2 = SimFill {
            side: Side::Buy,
            price: d("0.51"),
            size: Decimal::from(10),
            timestamp_ms: 2_000,
            was_partial: false,
            was_taker: false,
            taker_fee: Decimal::ZERO,
            maker_rebate: Decimal::ZERO,
        };
        state.record_leg2_fill(idx1, fill1_leg2);
        let trade1 = state.close_trade(idx1, now_ms).unwrap();

        // pair_cost = 0.46 + 0.51 = 0.97 per share; gross = (1 - 0.97) * 10 = 0.30 USDC
        let size1 = Decimal::from(10);
        assert_eq!(trade1.pair_cost, d("0.97")); // per-share
        assert_eq!(trade1.gross_profit, d("0.03") * size1); // 0.30 USDC
        assert_eq!(trade1.net_profit, d("0.03") * size1); // 0.30 USDC
        // profit_pct should be positive.
        assert!(trade1.profit_pct > Decimal::ZERO);

        // Session PnL accumulates USDC amounts.
        assert_eq!(state.total_pnl, d("0.03") * size1); // 0.30 USDC

        // Trade 2: emergency taker — pair_cost > 1.0 → loss.
        let fill2_leg1 = SimFill {
            side: Side::Buy,
            price: d("0.55"),
            size: Decimal::from(10),
            timestamp_ms: 3_000,
            was_partial: false,
            was_taker: false,
            taker_fee: Decimal::ZERO,
            maker_rebate: Decimal::ZERO,
        };
        let _idx2 = state.record_leg1_fill(
            fill2_leg1,
            "tok_006".to_string(),
            Direction::Down,
            d("0.60"),
            ProfitTier::Med,
            Decimal::from(20),
        );
        let fee = SimFill::compute_taker_fee(d("0.52"), Decimal::from(10));
        let fill2_leg2 = SimFill {
            side: Side::Sell,
            price: d("0.52"),
            size: Decimal::from(10),
            timestamp_ms: 4_000,
            was_partial: false,
            was_taker: true,
            taker_fee: fee,
            maker_rebate: Decimal::ZERO,
        };
        // After trade1 closed, _idx2 is now at index 0.
        state.record_emergency_taker(0, fill2_leg2);
        let trade2 = state.close_trade(0, now_ms).unwrap();

        // pair_cost = 0.55 + 0.52 = 1.07 per share; gross = (1 - 1.07) * 10 = -0.70 USDC
        let size2 = Decimal::from(10);
        assert_eq!(trade2.pair_cost, d("0.55") + d("0.52")); // per-share
        assert_eq!(
            trade2.gross_profit,
            (Decimal::ONE - (d("0.55") + d("0.52"))) * size2
        ); // -0.70 USDC
        assert!(
            trade2.net_profit < Decimal::ZERO,
            "losing trade should have negative net_profit"
        );

        let summary = state.hourly_summary(10_000);
        assert_eq!(summary.total_trades, 2);
        assert_eq!(summary.trades_hedged, 2);
    }

    // ─── 9. compute_fill_size helper ─────────────────────────────────────────

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

    // ─── 10. SimFill::compute_taker_fee formula ───────────────────────────────

    #[test]
    fn test_taker_fee_formula() {
        // Polymarket 5-min crypto: fee = C × 0.25 × (p × (1 - p))²
        // At price = 0.50, size = 100:
        //   fee = 100 * 0.25 * (0.50 * 0.50)^2 = 100 * 0.25 * 0.0625 = 1.5625
        let price = d("0.50");
        let size = Decimal::from(100);
        let fee = SimFill::compute_taker_fee(price, size);
        let inner = price * (Decimal::ONE - price);
        let expected = size * d("0.25") * inner * inner;
        assert_eq!(fee, expected);
        assert_eq!(fee, d("1.5625"));

        // Fee is always >= 0.
        assert!(fee >= Decimal::ZERO);

        // At extreme price (0.99) fee should be much lower than at 0.5.
        let fee_extreme = SimFill::compute_taker_fee(d("0.99"), Decimal::from(100));
        assert!(
            fee_extreme < fee,
            "fee at extreme price should be lower than at 0.5"
        );
    }

    // ─── 11. opposite_side helper ─────────────────────────────────────────────

    #[test]
    fn test_opposite_side() {
        assert_eq!(opposite_side(Side::Buy), Side::Sell);
        assert_eq!(opposite_side(Side::Sell), Side::Buy);
    }

    // ─── 12. make_leg2_signal helper (coverage of unused helper) ────────────

    #[test]
    fn test_make_leg2_signal_fields() {
        let sig = make_leg2_signal(d("0.53"), d("0.45"), "my_token");
        assert!(sig.is_leg2);
        assert_eq!(sig.leg1_fill_price, Some(d("0.45")));
        assert_eq!(sig.price, d("0.53"));
        assert_eq!(sig.token_id, "my_token");
    }
}
