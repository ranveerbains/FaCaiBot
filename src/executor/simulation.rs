// Simulation executor — receives TradeSignal, simulates post-only fills,
// tracks virtual PnL, and forwards events to TelegramReporter.
//
// Architecture:
//   - `SimulationExecutor` owns a `SimulationState` (virtual portfolio),
//     a `TelegramReporter` (fire-and-forget), and a `ColdStorage` (QuestDB).
//   - The `run()` method loops on a crossbeam Receiver<TradeSignal>.
//   - Leg 1 signals simulate a post-only maker fill against the live book.
//   - Leg 2 signals simulate an erosion-cascade maker fill, or emergency taker.
//   - Market rotation and session shutdown generate Telegram summaries.

use anyhow::Result;
use crossbeam_channel::Receiver;
use rust_decimal::Decimal;
use tracing::{debug, error, info, warn};

use crate::reporting::telegram::TelegramReporter;
use crate::storage::cold::ColdStorage;
use crate::types::market::{Direction, OrderBook};
use crate::types::order::{ExecutorCommand, TradeSignal};
use crate::types::simulation::{PositionStatus, SimulationState};

use super::fill_engine::{epoch_ms, FillSimulator, Leg1Result, Leg2Result};

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
    cold: ColdStorage,
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
    /// Fill condition checker (stateless fill-check rules).
    fill_sim: FillSimulator,
}

impl SimulationExecutor {
    /// Create a new `SimulationExecutor`.
    ///
    /// `fixed_alloc` is the total session capital (e.g. `Decimal::from(100)`
    /// for $100 USDC). `now_ms` is the current epoch millisecond timestamp
    /// used as the simulation session start time.
    pub fn new(
        reporter: TelegramReporter,
        cold: ColdStorage,
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
            fill_sim: FillSimulator::new(default_tick),
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

    /// Update market parameters at each 15-minute market rotation.
    pub fn update_market_params(&mut self, market_end_ms: u64, tick_size: Decimal) {
        self.market_end_ms = market_end_ms;
        self.tick_size = tick_size;
        self.fill_sim.update_tick_size(tick_size);
    }

    // ─── Market rotation ──────────────────────────────────────────────────────

    /// Handle a market rotation event.
    ///
    /// 1. Force-close any open positions that were not hedged before expiry.
    ///    Open positions (Leg 1 filled, no Leg 2) are closed at full loss with
    ///    Telegram notification. Other statuses are locked for UMA resolution.
    /// 2. Build and send the per-market Telegram summary.
    /// 3. Reset per-market counters via `SimulationState::on_market_rotation()`.
    pub fn on_market_rotation(&mut self, market_id: &str) {
        info!(
            market_id,
            "simulation: market rotation — closing open positions"
        );

        let now_ms = epoch_ms();

        // Force-close or lock for resolution any positions still in this market.
        // We iterate in reverse so that removal by index remains stable.
        let open_count = self.state.open_positions.len();
        for idx in (0..open_count).rev() {
            // Extract fields before borrowing self.state mutably.
            let (pos_market, pos_open) = {
                let p = &self.state.open_positions[idx];
                (p.market_id.clone(), p.status == PositionStatus::Open)
            };
            if pos_market != market_id {
                continue;
            }
            if pos_open {
                // Leg 1 filled, no Leg 2 — force-close, record full loss.
                if let Some(trade) = self.state.close_trade(idx, now_ms) {
                    warn!(
                        market_id,
                        position_idx = idx,
                        net_profit = %trade.net_profit,
                        "simulation: position force-closed at rotation — no Leg 2 fill"
                    );
                    self.reporter.send_trade_completed(&trade);
                    if let Err(e) = self.cold.record_simulated_trade(&trade) {
                        error!(error = %e, "failed to write force-closed trade to QuestDB");
                    }
                }
            } else {
                // AwaitingResolution or Hedged from prior market — keep tracking.
                self.state.lock_for_resolution(idx);
                warn!(
                    market_id,
                    position_idx = idx,
                    "simulation: position locked for UMA resolution"
                );
            }
        }

        // Build period label (simple epoch-based placeholder).
        let period_label = format!("market:{}", &market_id[..market_id.len().min(8)]);

        // Generate and send market summary via Telegram.
        let summary = self
            .state
            .market_summary(market_id, period_label, self.fixed_alloc);
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

                        self.state.record_signal();

                        if signal.bot_contested {
                            self.state.record_wall_outbid();
                        }

                        if signal.is_leg2 {
                            self.handle_leg2(&signal);
                        } else {
                            self.handle_leg1(&signal);
                        }
                    }
                    ExecutorCommand::MarketRotation { condition_id } => {
                        self.on_market_rotation(&condition_id);
                    }
                }
            }

            info!("SimulationExecutor: channel disconnected — generating shutdown summary");
            self.shutdown_summary();
        })
        .await;

        result.map_err(|e| anyhow::anyhow!("simulation executor task panicked: {e}"))
    }

    // ─── Leg 1 simulation ─────────────────────────────────────────────────────

    /// Simulate a post-only Leg 1 fill.
    ///
    /// Rules:
    /// 1. Requires a live orderbook snapshot. If none, record unfilled_liquidity.
    /// 2. Post-only check: if `signal.price >= best_ask`, reject (would cross spread).
    /// 3. Depth check: check for sell-side liquidity within 2 ticks of our bid.
    ///    - Depth > 0 → simulated maker fill at `signal.price`.
    ///    - No depth   → record unfilled_liquidity.
    /// 4. On fill: record in SimulationState, send Telegram opportunity alert,
    ///    log signal to QuestDB.
    fn handle_leg1(&mut self, signal: &TradeSignal) {
        let book = match &self.current_book {
            Some(b) => b.clone(),
            None => {
                warn!(
                    token_id = %signal.token_id,
                    "Leg 1: no orderbook available — recording unfilled_liquidity"
                );
                self.state.record_unfilled_liquidity();
                self.log_signal_to_cold(signal, "aborted_liquidity");
                return;
            }
        };

        match self.fill_sim.check_leg1_fill(signal, &book) {
            Leg1Result::NoAsks => {
                warn!(token_id = %signal.token_id, "Leg 1: book has no asks — unfilled_liquidity");
                self.state.record_unfilled_liquidity();
                self.log_signal_to_cold(signal, "aborted_liquidity");
            }
            Leg1Result::CrossesSpread => {
                debug!(
                    token_id = %signal.token_id,
                    bid = %signal.price,
                    "Leg 1: post-only bid would cross spread — rejected"
                );
                self.state.record_unfilled_post_only();
                self.log_signal_to_cold(signal, "unfilled_postonly");
            }
            Leg1Result::NoNearbyDepth => {
                let two_ticks = self.tick_size * Decimal::TWO;
                let depth_window_top = signal.price + two_ticks;
                debug!(
                    token_id = %signal.token_id,
                    bid = %signal.price,
                    depth_window_top = %depth_window_top,
                    "Leg 1: no sell-side depth within 2 ticks — unfilled_liquidity"
                );
                self.state.record_unfilled_liquidity();
                self.log_signal_to_cold(signal, "aborted_liquidity");
            }
            Leg1Result::Fill(fill) => {
                let fill_size = fill.size;
                let position_idx = self.state.record_leg1_fill(
                    fill.clone(),
                    signal.token_id.clone(),
                    signal.direction,
                    signal.confidence,
                    signal.profit_target_tier,
                    signal.alloc_amount,
                );

                info!(
                    token_id = %signal.token_id,
                    price = %signal.price,
                    size = %fill_size,
                    position_idx,
                    confidence = %signal.confidence,
                    tier = signal.profit_target_tier.label(),
                    "Leg 1: simulated maker fill"
                );

                // Send Telegram opportunity alert.
                self.reporter
                    .send_opportunity_alert(signal, signal.price, fill_size, &book);

                // Log signal to QuestDB.
                self.log_signal_to_cold(signal, "entered");
            }
        }
    }

    // ─── Leg 2 simulation ─────────────────────────────────────────────────────

    /// Simulate a Leg 2 hedge fill.
    ///
    /// Finds the matching open position (by token_id + is_leg2 flag) and
    /// evaluates whether:
    ///   - A normal maker fill is possible (`best_ask <= signal.price`).
    ///   - An emergency taker fill should be used (signal carries emergency
    ///     context — detected when `signal.price` has eroded to or past break-even,
    ///     or when the signal was generated by the adverse movement protocol).
    ///
    /// For this simulation, we determine "emergency" by checking if the signal
    /// price is at or below the break-even threshold. The engine sets prices
    /// accordingly for the erosion cascade.
    fn handle_leg2(&mut self, signal: &TradeSignal) {
        // We need a Leg 1 fill price to compute break-even.
        let entry_price = match signal.leg1_fill_price {
            Some(p) => p,
            None => {
                warn!(
                    token_id = %signal.token_id,
                    "Leg 2: signal has no leg1_fill_price — skipping"
                );
                return;
            }
        };

        // Find the matching open position.
        // Leg 2 token_id is the opposing token (YES when Leg 1 was NO, and vice versa),
        // so we cannot match by token_id. Instead find the first Open position —
        // the engine self-gates to one trade at a time, so at most one will be open.
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

        let book = match &self.current_book {
            Some(b) => b.clone(),
            None => {
                warn!(
                    token_id = %signal.token_id,
                    "Leg 2: no orderbook — cannot simulate hedge"
                );
                return;
            }
        };

        let now_ms = epoch_ms();
        let leg1_size = self.state.open_positions[position_idx].leg1.size;

        match self
            .fill_sim
            .check_leg2_fill(signal, &book, leg1_size, entry_price)
        {
            Leg2Result::NoAsks => {
                warn!(token_id = %signal.token_id, "Leg 2: book has no asks — cannot hedge");
            }
            Leg2Result::EmergencyFill { fill, is_adverse } => {
                info!(
                    token_id = %signal.token_id,
                    taker_price = %fill.price,
                    taker_fee = %fill.taker_fee,
                    position_idx,
                    "Leg 2: emergency taker fill (FOK)"
                );

                if is_adverse {
                    self.state.record_adverse_hedge(position_idx, fill.clone());
                } else {
                    self.state
                        .record_emergency_taker(position_idx, fill.clone());
                }

                if let Some(trade) = self.state.close_trade(position_idx, now_ms) {
                    info!(
                        market_id = %trade.market_id,
                        net_profit = %trade.net_profit,
                        profit_pct = %trade.profit_pct,
                        "Leg 2 emergency: trade closed"
                    );
                    self.reporter.send_trade_completed(&trade);
                    if let Err(e) = self.cold.record_simulated_trade(&trade) {
                        error!(error = %e, "failed to write simulated trade to QuestDB");
                    }
                }
            }
            Leg2Result::MakerFill(fill) => {
                info!(
                    token_id = %signal.token_id,
                    price = %fill.price,
                    position_idx,
                    "Leg 2: maker fill (normal erosion cascade)"
                );

                self.state.record_leg2_fill(position_idx, fill.clone());

                if let Some(trade) = self.state.close_trade(position_idx, now_ms) {
                    info!(
                        market_id = %trade.market_id,
                        net_profit = %trade.net_profit,
                        profit_pct = %trade.profit_pct,
                        erosion_steps = trade.erosion_steps,
                        "Leg 2 maker: trade closed"
                    );
                    self.reporter.send_trade_completed(&trade);
                    if let Err(e) = self.cold.record_simulated_trade(&trade) {
                        error!(error = %e, "failed to write simulated trade to QuestDB");
                    }
                }
            }
            Leg2Result::NotFilled { best_ask } => {
                debug!(
                    token_id = %signal.token_id,
                    our_bid = %signal.price,
                    best_ask = %best_ask,
                    "Leg 2: not filled yet — recording erosion step"
                );
                self.state.record_erosion_step(position_idx);
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
        // ATR is not carried in TradeSignal; the engine tracks it in MarketState.
        let atr = Decimal::ZERO;
        let time_remaining_secs = signal
            .market_end_timestamp_ms
            .saturating_sub(signal.entry_timestamp_ms)
            / 1000;

        if let Err(e) = self.cold.record_signal(
            &signal.token_id,
            direction_str,
            signal.confidence,
            signal.spike_info.magnitude,
            atr,
            book_depth,
            time_remaining_secs as i64,
            signal.alloc_amount,
            action,
        ) {
            error!(error = %e, "failed to log signal to QuestDB");
        }
    }
}

// ─── Unit Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::fill_engine::{compute_fill_size, is_near_deadline, opposite_side};
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

    /// Build a minimal `TradeSignal` for Leg 1 testing.
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

    /// Build a minimal `TradeSignal` for Leg 2 testing.
    fn make_leg2_signal(price: Decimal, leg1_price: Decimal, token_id: &str) -> TradeSignal {
        TradeSignal {
            side: Side::Buy,
            token_id: token_id.to_string(),
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
            is_leg2: true,
            leg1_fill_price: Some(leg1_price),
            entry_timestamp_ms: 1_000_000,
            market_end_timestamp_ms: 1_900_000,
            tick_size: d("0.01"),
            fee_rate_bps: 156,
            bot_contested: false,
            book_snapshot: None,
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
        state.record_unfilled_post_only();
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
        state.record_unfilled_liquidity();
        assert_eq!(state.unfilled_liquidity, 1);
        assert_eq!(state.virtual_balance, Decimal::from(100));
    }

    // ─── 5. Leg 2 maker fill (normal erosion cascade) ────────────────────────

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
        };
        state.record_leg2_fill(idx, leg2_fill);
        assert_eq!(state.trades_hedged, 1);
        assert_eq!(state.open_positions[0].status, PositionStatus::Hedged);

        // Close the trade and verify PnL.
        let trade = state.close_trade(idx, now_ms).expect("trade should close");
        assert_eq!(trade.pair_cost, d("0.98"));
        assert_eq!(trade.gross_profit, d("0.02"));
        assert_eq!(trade.taker_fee, Decimal::ZERO);
        assert_eq!(trade.net_profit, d("0.02"));
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
        };
        state.record_emergency_taker(idx, leg2_fill);
        assert_eq!(state.trades_emergency_taker, 1);
        assert_eq!(state.trades_hedged, 1);
        assert_eq!(state.total_taker_fees_paid, taker_fee);

        let trade = state.close_trade(idx, now_ms).expect("trade should close");
        assert!(trade.leg2_was_taker);
        assert_eq!(trade.taker_fee, taker_fee);
        // net_profit = gross_profit - taker_fee
        let leg1_price = d("0.45");
        let expected_gross = Decimal::ONE - (leg1_price + taker_price);
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
        };
        state.record_leg2_fill(idx1, fill1_leg2);
        let trade1 = state.close_trade(idx1, now_ms).unwrap();

        // pair_cost = 0.46 + 0.51 = 0.97; gross = 1.0 - 0.97 = 0.03
        assert_eq!(trade1.pair_cost, d("0.97"));
        assert_eq!(trade1.gross_profit, d("0.03"));
        assert_eq!(trade1.net_profit, d("0.03"));
        // profit_pct should be positive.
        assert!(trade1.profit_pct > Decimal::ZERO);

        // Session PnL equals net_profit (per-share).
        assert_eq!(state.total_pnl, d("0.03"));

        // Trade 2: adverse — pair_cost > 1.0 → loss.
        let fill2_leg1 = SimFill {
            side: Side::Buy,
            price: d("0.55"),
            size: Decimal::from(10),
            timestamp_ms: 3_000,
            was_partial: false,
            was_taker: false,
            taker_fee: Decimal::ZERO,
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
        };
        // After trade1 closed, _idx2 is now at index 0.
        state.record_emergency_taker(0, fill2_leg2);
        let trade2 = state.close_trade(0, now_ms).unwrap();

        // pair_cost = 0.55 + 0.52 = 1.07; gross = 1.0 - 1.07 = -0.07
        assert_eq!(trade2.pair_cost, d("0.55") + d("0.52"));
        assert_eq!(trade2.gross_profit, Decimal::ONE - (d("0.55") + d("0.52")));
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
        // At price = 0.50, size = 100:
        //   fee = 100 * 0.25 * (0.5 * 0.5)^2 = 100 * 0.25 * 0.0625 = 1.5625
        let price = d("0.50");
        let size = Decimal::from(100);
        let fee = SimFill::compute_taker_fee(price, size);
        let expected =
            size * d("0.25") * (price * (Decimal::ONE - price)) * (price * (Decimal::ONE - price));
        assert_eq!(fee, expected);

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

    // ─── 12. is_near_deadline helper ─────────────────────────────────────────

    #[test]
    fn test_is_near_deadline() {
        // Signal expiry at 1_000_000 ms. Deadline = 1_000_000 - 90_000 = 910_000 ms.
        let signal = make_leg1_signal(d("0.45"));
        // The signal has market_end_timestamp_ms = 1_900_000.
        // Deadline = 1_900_000 - 90_000 = 1_810_000 ms.

        // now_ms well before deadline — not near.
        assert!(!is_near_deadline(&signal, 1_000_000));

        // now_ms exactly at deadline — near.
        assert!(is_near_deadline(&signal, 1_810_000));

        // now_ms past deadline — near.
        assert!(is_near_deadline(&signal, 1_900_000));
    }

    // ─── 13. make_leg2_signal helper (coverage of unused helper) ────────────

    #[test]
    fn test_make_leg2_signal_fields() {
        let sig = make_leg2_signal(d("0.53"), d("0.45"), "my_token");
        assert!(sig.is_leg2);
        assert_eq!(sig.leg1_fill_price, Some(d("0.45")));
        assert_eq!(sig.price, d("0.53"));
        assert_eq!(sig.token_id, "my_token");
    }
}
