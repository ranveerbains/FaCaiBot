//! V2 Strategy Engine — bilateral accumulation.
//!
//! State machine: IDLE → QUIET → QUOTING → CLOSING
//! Accumulates YES and NO shares independently, pairs at resolution.

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tracing::{debug, info};

use crate::config::Config;
use crate::engine::closing::ClosingManager;
use crate::engine::fair_value::{FairValueConfig, FairValueEstimator};
use crate::engine::position::{BilateralPosition, MarketSide};
use crate::engine::quoter::{ManagedOrder, QuoteAction, Quoter, QuotingConfig, RiskV2Config};
use crate::executor::fill_engine::{compute_taker_fee, round_to_tick};
use crate::reporting::telegram::TelegramReporter;
use crate::storage::cold::{FillRecord, MarketSummaryRecord};
use crate::types::market::{IngestorEvent, MarketState, TradeStatus};
use crate::types::order::{V2ExecutorCommand, V2ExecutorFeedback};
use crate::utils::time::epoch_ms;

// ─── Market Phase ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketPhase {
    Idle,
    Quiet,
    Quoting,
    Closing,
}

// ─── V2 Strategy Engine ─────────────────────────────────────────────────────

pub struct V2StrategyEngine {
    // ── Config ──
    quoting_config: QuotingConfig,
    risk_config: RiskV2Config,
    fair_value_config: FairValueConfig,
    min_edge: f64,

    // ── Market state ──
    state: MarketState,
    phase: MarketPhase,
    strike_price: Option<Decimal>,
    quiet_until_ms: u64,

    // ── Core modules ──
    position: BilateralPosition,
    fair_value: FairValueEstimator,
    quoter: Quoter,
    closing: ClosingManager,

    // ── Reporting ──
    reporter: Option<TelegramReporter>,

    // ── Control ──
    draining: bool,
    paused: bool,

    // ── Heartbeat ──
    heartbeat_healthy: bool,
    heartbeat_failures: u32,
    heartbeat_latency_ms: u64,

    // ── Diagnostics (60s log) ──
    session_start_ms: u64,
    diag_last_ms: u64,
    diag_yes_fills: u32,
    diag_no_fills: u32,
    diag_requotes: u32,
    diag_closing_foks: u32,
    diag_markets_traded: u32,

    // ── Rebalance state ──
    last_rebalance_ms: u64,
    pending_rebalance: bool,

    // ── Pending commands ──
    pending_commands: Vec<V2ExecutorCommand>,

    // ── Pending tick size change ──
    pending_tick_change: Option<V2ExecutorCommand>,

    // ── Pending Telegram diagnostics ──
    pending_telegram_diag: Option<String>,

    // ── Fill notifications (Task 2) ──
    pending_fill_messages: Vec<String>,

    // ── Market report / session summary gated by summary_enabled (Task 3) ──
    pending_market_report: Option<String>,
    pending_session_summary: Option<String>,

    // ── QuestDB records (Task 5 & 6) ──
    pending_fill_records: Vec<FillRecord>,
    pending_market_summary: Option<MarketSummaryRecord>,
}

impl V2StrategyEngine {
    pub fn new(config: &Config) -> Self {
        let fv_toml = &config.bot.fair_value;
        let fair_value_config = FairValueConfig {
            momentum_weight_basis: fv_toml.momentum_weight_basis,
            momentum_weight_cvd: fv_toml.momentum_weight_cvd,
            momentum_weight_obi: fv_toml.momentum_weight_obi,
            max_momentum_adj: fv_toml.max_momentum_adj,
            vol_edge_scale: fv_toml.vol_edge_scale,
            time_edge_scale: fv_toml.time_edge_scale,
            baseline_vol: fv_toml.baseline_vol,
            vol_ring_capacity: fv_toml.vol_ring_capacity,
            vol_session_capacity: fv_toml.vol_session_capacity,
            vol_freshness_ms: fv_toml.vol_freshness_ms,
            vol_min_warmup: fv_toml.vol_min_warmup,
            vol_default: fv_toml.vol_default,
            vol_ticks_per_sec: fv_toml.vol_ticks_per_sec,
            tail_compression_factor: fv_toml.tail_compression_factor,
            stale_data_edge_penalty: fv_toml.stale_data_edge_penalty,
            regime_spike_threshold: fv_toml.regime_spike_threshold,
            regime_spike_penalty: fv_toml.regime_spike_penalty,
            strike_warmup_count: fv_toml.strike_warmup_count,
            basis_halflife_ms: fv_toml.basis_halflife_ms,
            basis_freshness_ms: fv_toml.basis_freshness_ms,
            basis_min: fv_toml.basis_min,
            basis_saturation: fv_toml.basis_saturation,
            cvd_fast_halflife_ms: fv_toml.cvd_fast_halflife_ms,
            cvd_slow_halflife_ms: fv_toml.cvd_slow_halflife_ms,
            cvd_freshness_ms: fv_toml.cvd_freshness_ms,
            cvd_min: fv_toml.cvd_min,
            cvd_saturation: fv_toml.cvd_saturation,
            obi_halflife_ms: fv_toml.obi_halflife_ms,
            obi_freshness_ms: fv_toml.obi_freshness_ms,
            obi_min: fv_toml.obi_min,
            obi_saturation: fv_toml.obi_saturation,
        };

        let q_toml = &config.bot.quoting;
        let quoting_config = QuotingConfig {
            min_edge: q_toml.min_edge,
            requote_threshold: q_toml.requote_threshold,
            min_requote_interval_ms: q_toml.min_requote_interval_ms,
            max_order_size: Decimal::try_from(q_toml.max_order_size).unwrap_or(Decimal::new(100, 0)),
            min_order_size: Decimal::try_from(q_toml.min_order_size).unwrap_or(Decimal::new(5, 0)),
            emergency_requote_threshold: q_toml.emergency_requote_threshold,
            max_imbalance_skew: q_toml.max_imbalance_skew,
        };

        let r_toml = &config.bot.risk_v2;
        let risk_config = RiskV2Config {
            max_unpaired_shares: Decimal::try_from(r_toml.max_unpaired_shares).unwrap_or(Decimal::new(30, 0)),
            max_unpaired_usdc: Decimal::try_from(r_toml.max_unpaired_usdc).unwrap_or(Decimal::new(25, 0)),
            max_capital_per_market: Decimal::try_from(r_toml.max_capital_per_market).unwrap_or(Decimal::new(100, 0)),
            closing_phase_secs: r_toml.closing_phase_secs,
            rotation_quiet_ms: r_toml.rotation_quiet_ms,
            max_closing_pair_cost: r_toml.max_closing_pair_cost,
            closing_retry_price_increment: r_toml.closing_retry_price_increment,
            rebalance_threshold: Decimal::try_from(r_toml.rebalance_threshold).unwrap_or(Decimal::new(20, 0)),
            rebalance_size: Decimal::try_from(r_toml.rebalance_size).unwrap_or(Decimal::new(10, 0)),
            rebalance_max_pair_cost: r_toml.rebalance_max_pair_cost,
            min_rebalance_interval_ms: r_toml.min_rebalance_interval_ms,
            stale_book_ms: r_toml.stale_book_ms,
            max_entry_spread: r_toml.max_entry_spread,
            heartbeat_dead_threshold: r_toml.heartbeat_dead_threshold,
        };

        let now = epoch_ms();
        Self {
            min_edge: q_toml.min_edge,
            quoting_config,
            risk_config,
            fair_value_config: fair_value_config.clone(),
            state: MarketState::new(),
            phase: MarketPhase::Idle,
            strike_price: None,
            quiet_until_ms: 0,
            position: BilateralPosition::new(),
            fair_value: FairValueEstimator::new(&fair_value_config),
            quoter: Quoter::new(),
            closing: ClosingManager::with_config(r_toml.max_closing_attempts),
            reporter: None,
            draining: false,
            paused: false,
            heartbeat_healthy: true,
            heartbeat_failures: 0,
            heartbeat_latency_ms: 0,
            session_start_ms: now,
            diag_last_ms: now,
            diag_yes_fills: 0,
            diag_no_fills: 0,
            diag_requotes: 0,
            diag_closing_foks: 0,
            diag_markets_traded: 0,
            last_rebalance_ms: 0,
            pending_rebalance: false,
            pending_commands: Vec::new(),
            pending_tick_change: None,
            pending_telegram_diag: None,
            pending_fill_messages: Vec::new(),
            pending_market_report: None,
            pending_session_summary: None,
            pending_fill_records: Vec::new(),
            pending_market_summary: None,
        }
    }

    pub fn set_reporter(&mut self, reporter: TelegramReporter) {
        self.reporter = Some(reporter);
    }

    pub fn reporter(&self) -> &Option<TelegramReporter> {
        &self.reporter
    }

    // ─── Event handling ─────────────────────────────────────────────────

    pub fn on_event(&mut self, event: IngestorEvent) {
        let now = epoch_ms();

        match event {
            IngestorEvent::MarketRotation {
                condition_id,
                yes_token_id,
                no_token_id,
                end_timestamp_ms,
                tick_size,
            } => {
                self.on_market_rotation(
                    condition_id, yes_token_id, no_token_id,
                    end_timestamp_ms, tick_size, now,
                );
            }

            IngestorEvent::BinanceTick(tick) => {
                let mid = tick.mid_price();
                let mid_f64 = mid.to_f64().unwrap_or(0.0);
                self.state.binance_price = Some(mid);
                self.fair_value.update_btc_price(mid_f64, now);
                self.fair_value.update_vol(mid_f64, now);
                self.fair_value.update_spot_mid(mid_f64, now);
                self.state.last_update_ms = now;

                // Set strike via warmup (median of first N ticks)
                if self.strike_price.is_none()
                    && self.state.active_condition_id.is_some()
                    && self.fair_value.try_set_strike(mid_f64)
                {
                    self.strike_price = Some(self.fair_value.strike_price());
                    info!(strike = %self.fair_value.strike_price(), "strike price set from warmup median");
                }
            }

            IngestorEvent::BinanceDepth(depth) => {
                if let Some(obi) = depth.obi() {
                    let obi_f64 = obi.to_f64().unwrap_or(0.0);
                    self.fair_value.update_obi(obi_f64, now);
                }
                if let Some(mid) = depth.mid_price() {
                    let mid_f64 = mid.to_f64().unwrap_or(0.0);
                    self.fair_value.update_vol(mid_f64, now);
                    self.fair_value.update_spot_mid(mid_f64, now);
                }
                self.state.last_update_ms = now;
            }

            IngestorEvent::FuturesBookTicker(ticker) => {
                let bid_f64 = ticker.bid_price.to_f64().unwrap_or(0.0);
                let ask_f64 = ticker.ask_price.to_f64().unwrap_or(0.0);
                self.fair_value.update_futures_mid(bid_f64, ask_f64, now);
            }

            IngestorEvent::FuturesAggTrade(trade) => {
                let qty_f64 = trade.quantity.to_f64().unwrap_or(0.0);
                self.fair_value.update_cvd(qty_f64, trade.is_buyer_maker, now);
            }

            IngestorEvent::PolymarketBook(book) => {
                let asset_id = book.asset_id.clone();
                if Some(&asset_id) == self.state.active_yes_token_id.as_ref() {
                    self.state.poly_yes_book = Some(book.clone());
                } else if Some(&asset_id) == self.state.active_no_token_id.as_ref() {
                    self.state.poly_no_book = Some(book.clone());
                }
                self.state.poly_book = Some(book);
                self.state.last_update_ms = now;
            }

            IngestorEvent::PolymarketPriceChange { asset_id, .. } |
            IngestorEvent::PolymarketBestBidAsk { asset_id, .. } => {
                let _ = asset_id;
                self.state.last_update_ms = now;
            }

            IngestorEvent::PolymarketTickSizeChange {
                asset_id: _,
                old_tick_size: _,
                new_tick_size,
            } => {
                self.state.tick_size = new_tick_size;
                if let (Some(yes_id), Some(no_id)) = (
                    self.state.active_yes_token_id.clone(),
                    self.state.active_no_token_id.clone(),
                ) {
                    self.pending_tick_change = Some(V2ExecutorCommand::TickSizeChanged {
                        yes_token_id: yes_id,
                        no_token_id: no_id,
                        new_tick_size,
                    });
                }
            }

            IngestorEvent::TradeStatusUpdate {
                order_id,
                status,
                size_matched,
                ..
            } => {
                self.on_trade_status_update(&order_id, status, size_matched, now);
            }

            IngestorEvent::HeartbeatStatus { success, latency_ms } => {
                if success {
                    self.heartbeat_failures = 0;
                    self.heartbeat_healthy = true;
                    self.heartbeat_latency_ms = latency_ms;
                } else {
                    self.heartbeat_failures += 1;
                    if self.heartbeat_failures >= self.risk_config.heartbeat_dead_threshold
                        && self.heartbeat_healthy
                    {
                        // Healthy → unhealthy transition: emergency cancel all resting orders
                        self.heartbeat_healthy = false;
                        let cancel_actions = self.quoter.cancel_all_actions();
                        for action in cancel_actions {
                            if let QuoteAction::Cancel { side, order_id } = action {
                                self.quoter.on_cancel_sent(side);
                                self.pending_commands.push(V2ExecutorCommand::CancelOrder {
                                    side,
                                    order_id,
                                });
                            }
                        }
                        info!("heartbeat DEAD — cancelled all resting orders");
                        if let Some(ref reporter) = self.reporter {
                            reporter.fire_critical(
                                "<b>⚠ HEARTBEAT DEAD</b>\nAll resting orders cancelled. \
                                 Quoting paused until heartbeat recovers."
                                    .to_string(),
                            );
                        }
                    }
                }
            }

            IngestorEvent::WsStatus { .. } => {}
            IngestorEvent::SpotTrade(_) => {}
            IngestorEvent::FuturesForceOrder(_) => {}
            IngestorEvent::PolymarketMarketResolved { .. } => {}
            IngestorEvent::Shutdown | IngestorEvent::DrainAndRestart
            | IngestorEvent::PauseTrading | IngestorEvent::ResumeTrading => {}
        }

        // Recompute fair value after event processing
        if self.fair_value.is_warm() && self.state.market_end_timestamp_ms > 0 {
            self.fair_value.recompute(
                self.state.market_end_timestamp_ms,
                self.min_edge,
                now,
            );
        }

        // Phase transition: QUIET → QUOTING
        if self.phase == MarketPhase::Quiet && now >= self.quiet_until_ms && !self.paused {
            self.phase = MarketPhase::Quoting;
            info!("phase transition: QUIET → QUOTING");
        }

        // Phase transition: QUOTING → CLOSING
        if self.phase == MarketPhase::Quoting && !self.paused {
            let remaining = self.state.time_remaining_ms(now);
            if ClosingManager::should_enter(remaining, self.risk_config.closing_phase_secs) {
                self.phase = MarketPhase::Closing;
                self.closing.phase_entered = true;
                info!(remaining_ms = remaining, "phase transition: QUOTING → CLOSING");
            }
        }
    }

    // ─── Market rotation ────────────────────────────────────────────────

    fn on_market_rotation(
        &mut self,
        condition_id: String,
        yes_token_id: String,
        no_token_id: String,
        end_timestamp_ms: u64,
        tick_size: Decimal,
        now: u64,
    ) {
        // Report on outgoing market
        if self.state.active_condition_id.is_some() && self.position.total_fills() > 0 {
            self.send_market_report(now);
        }

        // Reset state for new market
        self.state.active_condition_id = Some(condition_id.clone());
        self.state.active_yes_token_id = Some(yes_token_id.clone());
        self.state.active_no_token_id = Some(no_token_id.clone());
        self.state.market_end_timestamp_ms = end_timestamp_ms;
        self.state.tick_size = tick_size;
        self.state.poly_yes_book = None;
        self.state.poly_no_book = None;
        self.state.poly_book = None;

        self.strike_price = None;
        self.position.reset();
        self.quoter.reset();
        self.closing.reset();
        self.fair_value.reset(&self.fair_value_config);

        self.last_rebalance_ms = 0;
        self.pending_rebalance = false;
        self.pending_fill_messages.clear();
        self.pending_fill_records.clear();

        self.phase = MarketPhase::Quiet;
        self.quiet_until_ms = now + self.risk_config.rotation_quiet_ms;
        self.diag_markets_traded += 1;

        // Forward rotation to executor
        self.pending_commands.push(V2ExecutorCommand::MarketRotation {
            condition_id,
            yes_token_id,
            no_token_id,
            tick_size,
        });

        info!(
            phase = ?self.phase,
            quiet_until = self.quiet_until_ms,
            "market rotation → QUIET"
        );
    }

    // ─── Trade status updates (User WS fills) ──────────────────────────

    fn on_trade_status_update(
        &mut self,
        order_id: &str,
        status: TradeStatus,
        size_matched: Option<Decimal>,
        now: u64,
    ) {
        if !matches!(status, TradeStatus::Matched) {
            return;
        }
        let cumulative = match size_matched {
            Some(s) if s > Decimal::ZERO => s,
            _ => return,
        };

        // Match order_id to a resting order
        let matched_side = if let Some(ref o) = self.quoter.yes_order {
            if o.order_id == order_id { Some(MarketSide::Yes) } else { None }
        } else {
            None
        }.or_else(|| {
            if let Some(ref o) = self.quoter.no_order {
                if o.order_id == order_id { Some(MarketSide::No) } else { None }
            } else {
                None
            }
        });

        let Some(side) = matched_side else {
            debug!(order_id, "TradeStatusUpdate: no matching resting order");
            return;
        };

        // Dedup: compute delta against previously recorded fills for this order
        let delta = self.quoter.record_ws_fill(side, cumulative);
        if delta <= Decimal::ZERO {
            debug!(order_id, cumulative = %cumulative, "TradeStatusUpdate: duplicate, no new shares");
            return;
        }

        let fill_price = self.quoter.order(side).map(|o| o.price).unwrap_or(Decimal::ZERO);
        let order_size = self.quoter.order(side).map(|o| o.size).unwrap_or(Decimal::ZERO);
        let was_full = cumulative >= order_size;

        // Record only the new delta (no maker rebate — too variable to estimate)
        self.position.record_fill(side, fill_price, delta, false, Decimal::ZERO);
        self.quoter.on_fill(side, was_full);
        self.push_fill_message(side, fill_price, delta, false);
        self.push_fill_record(side, fill_price, delta, false, Decimal::ZERO, now);

        match side {
            MarketSide::Yes => self.diag_yes_fills += 1,
            MarketSide::No => self.diag_no_fills += 1,
        }

        info!(
            side = side.label(),
            price = %fill_price,
            delta = %delta,
            cumulative = %cumulative,
            yes_total = %self.position.yes.total_shares,
            no_total = %self.position.no.total_shares,
            paired = %self.position.paired_shares(),
            "fill recorded (deduped)"
        );
    }

    // ─── Feedback handling ──────────────────────────────────────────────

    pub fn on_feedback(&mut self, fb: V2ExecutorFeedback) {
        let now = epoch_ms();
        match fb {
            V2ExecutorFeedback::OrderPosted {
                side, order_id, price, size, already_filled,
            } => {
                if already_filled {
                    // Rare for post-only, but handle it
                    self.position.record_fill(side, price, size, false, Decimal::ZERO);
                    self.quoter.on_order_failed(side); // clear pending state
                    self.push_fill_message(side, price, size, false);
                    self.push_fill_record(side, price, size, false, Decimal::ZERO, now);
                    match side {
                        MarketSide::Yes => self.diag_yes_fills += 1,
                        MarketSide::No => self.diag_no_fills += 1,
                    }
                } else {
                    let fv = match side {
                        MarketSide::Yes => self.fair_value.yes_fair_value(),
                        MarketSide::No => self.fair_value.no_fair_value(),
                    };
                    self.quoter.on_order_posted(side, ManagedOrder {
                        order_id,
                        price,
                        size,
                        posted_ms: now,
                        fair_value_at_post: fv,
                        size_filled: Decimal::ZERO,
                    });
                }
            }

            V2ExecutorFeedback::OrderFailed { side } => {
                self.quoter.on_order_failed(side);
            }

            V2ExecutorFeedback::CancelResult {
                side, size_matched, ..
            } => {
                // Dedup: only record the delta not already seen via User WS
                if let Some(matched) = size_matched {
                    if matched > Decimal::ZERO {
                        let already = self.quoter.filled_so_far(side);
                        let delta = matched - already;
                        if delta > Decimal::ZERO {
                            let fill_price = self.quoter.order(side)
                                .map(|o| o.price)
                                .unwrap_or(Decimal::ZERO);
                            self.position.record_fill(side, fill_price, delta, false, Decimal::ZERO);
                            self.push_fill_message(side, fill_price, delta, false);
                            self.push_fill_record(side, fill_price, delta, false, Decimal::ZERO, now);
                            match side {
                                MarketSide::Yes => self.diag_yes_fills += 1,
                                MarketSide::No => self.diag_no_fills += 1,
                            }
                            info!(
                                side = side.label(),
                                size_matched = %matched,
                                already_recorded = %already,
                                delta = %delta,
                                "cancel revealed fill — recorded delta"
                            );
                        }
                    }
                }
                self.quoter.on_cancel_result(side);
            }

            V2ExecutorFeedback::ClosingFokResult {
                side, filled, size_matched, price,
            } => {
                if filled && size_matched > Decimal::ZERO {
                    let fee = compute_taker_fee(price, size_matched);
                    self.position.record_fill(side, price, size_matched, true, fee);
                    self.push_fill_message(side, price, size_matched, true);
                    self.push_fill_record(side, price, size_matched, true, fee, now);
                    self.diag_closing_foks += 1;
                    info!(
                        side = side.label(),
                        price = %price,
                        size = %size_matched,
                        attempt = self.closing.attempt_count,
                        "closing FOK filled"
                    );
                }
                // Allow retry: clear pairing_sent so can_retry() can fire again
                self.closing.pairing_sent = false;
            }

            V2ExecutorFeedback::RebalanceResult {
                side, filled, size_matched, price,
            } => {
                if filled && size_matched > Decimal::ZERO {
                    let fee = compute_taker_fee(price, size_matched);
                    self.position.record_fill(side, price, size_matched, true, fee);
                    self.push_fill_message(side, price, size_matched, true);
                    self.push_fill_record(side, price, size_matched, true, fee, now);
                    info!(
                        side = side.label(),
                        price = %price,
                        size = %size_matched,
                        "taker rebalance filled"
                    );
                }
                self.pending_rebalance = false;
            }
        }
    }

    // ─── Quote tick (called each iteration) ─────────────────────────────

    pub fn quote_tick(&mut self) -> Vec<V2ExecutorCommand> {
        if self.phase != MarketPhase::Quoting || self.draining || self.paused {
            return Vec::new();
        }

        if !self.fair_value.is_warm() {
            return Vec::new();
        }

        let now = epoch_ms();

        // Stale vol data → don't quote
        if self.fair_value.is_stale(now) {
            return Vec::new();
        }

        // Vol tracker not warmed up → don't quote
        if !self.fair_value.is_vol_warm() {
            return Vec::new();
        }

        // Unhealthy heartbeat → don't quote
        if !self.heartbeat_healthy {
            return Vec::new();
        }

        // Polymarket book spread too wide → don't quote either side
        let spread_ok = |book: &Option<crate::types::market::OrderBook>| -> bool {
            book.as_ref().map_or(false, |b| {
                match (b.best_bid(), b.best_ask()) {
                    (Some(bid), Some(ask)) => {
                        (ask.price - bid.price).to_f64().unwrap_or(1.0) <= self.risk_config.max_entry_spread
                    }
                    _ => false,
                }
            })
        };
        if !spread_ok(&self.state.poly_yes_book) || !spread_ok(&self.state.poly_no_book) {
            return Vec::new();
        }

        // Polymarket book data stale → don't quote
        let book_fresh = |book: &Option<crate::types::market::OrderBook>| -> bool {
            book.as_ref().map_or(false, |b| {
                now.saturating_sub(b.timestamp_ms) <= self.risk_config.stale_book_ms
            })
        };
        if !book_fresh(&self.state.poly_yes_book) || !book_fresh(&self.state.poly_no_book) {
            return Vec::new();
        }

        let (yes_token, no_token) = match (
            self.state.active_yes_token_id.clone(),
            self.state.active_no_token_id.clone(),
        ) {
            (Some(y), Some(n)) => (y, n),
            _ => return Vec::new(),
        };

        // ── Bilateral emergency cancel ──
        // If EITHER side has FV drift >= emergency threshold, cancel BOTH sides
        let emergency_threshold = self.quoting_config.emergency_requote_threshold;
        let yes_drift = self.quoter.order(MarketSide::Yes).map(|o| {
            let fv = self.fair_value.yes_fair_value();
            (fv - o.fair_value_at_post).abs().to_f64().unwrap_or(0.0)
        }).unwrap_or(0.0);
        let no_drift = self.quoter.order(MarketSide::No).map(|o| {
            let fv = self.fair_value.no_fair_value();
            (fv - o.fair_value_at_post).abs().to_f64().unwrap_or(0.0)
        }).unwrap_or(0.0);

        if yes_drift >= emergency_threshold || no_drift >= emergency_threshold {
            let mut commands = Vec::new();
            let cancel_actions = self.quoter.cancel_all_actions();
            for action in cancel_actions {
                if let QuoteAction::Cancel { side, order_id } = action {
                    self.quoter.on_cancel_sent(side);
                    self.diag_requotes += 1;
                    commands.push(V2ExecutorCommand::CancelOrder { side, order_id });
                }
            }
            if !commands.is_empty() {
                debug!(yes_drift, no_drift, "bilateral emergency cancel triggered");
                return commands;
            }
        }

        // ── Compute edge with scaled inventory skewing ──
        let base_edge = self.fair_value.edge();
        let imbalance = self.position.yes.total_shares - self.position.no.total_shares;
        let abs_imbalance = imbalance.abs();
        let max_unpaired = self.risk_config.max_unpaired_shares;

        // Scale skew linearly: skew = max_imbalance_skew × (abs(imbalance) / max_unpaired)
        let skew_ratio = if max_unpaired > Decimal::ZERO {
            abs_imbalance.to_f64().unwrap_or(0.0) / max_unpaired.to_f64().unwrap_or(1.0)
        } else {
            0.0
        };
        let skew = Decimal::try_from(
            self.quoting_config.max_imbalance_skew * skew_ratio.min(1.0)
        ).unwrap_or(Decimal::ZERO);

        let (yes_edge, no_edge) = if imbalance > Decimal::ZERO {
            // Long YES → tighten NO edge (attract fills), widen YES edge (discourage more)
            (base_edge + skew, (base_edge - skew).max(Decimal::new(1, 2)))
        } else if imbalance < Decimal::ZERO {
            // Long NO → tighten YES edge, widen NO edge
            ((base_edge - skew).max(Decimal::new(1, 2)), base_edge + skew)
        } else {
            (base_edge, base_edge)
        };

        let tick = self.state.tick_size;
        let yes_target = round_to_tick(self.fair_value.yes_target_price(yes_edge), tick);
        let no_target = round_to_tick(self.fair_value.no_target_price(no_edge), tick);

        let yes_fv = self.fair_value.yes_fair_value();
        let no_fv = self.fair_value.no_fair_value();

        let actions = self.quoter.evaluate_both(
            yes_target, no_target, yes_fv, no_fv,
            &yes_token, &no_token,
            &self.position, &self.quoting_config, &self.risk_config,
            now,
        );

        let mut commands = Vec::new();
        for action in actions {
            match action {
                QuoteAction::Post { side, token_id, price, size } => {
                    self.quoter.mark_post_pending(side);
                    commands.push(V2ExecutorCommand::PostOrder {
                        side,
                        token_id,
                        price,
                        size,
                    });
                }
                QuoteAction::Cancel { side, order_id } => {
                    self.quoter.on_cancel_sent(side);
                    self.diag_requotes += 1;
                    commands.push(V2ExecutorCommand::CancelOrder { side, order_id });
                }
            }
        }

        // ── Taker rebalance ──
        // If imbalance exceeds threshold, actively rebalance by taking the opposite side
        if abs_imbalance >= self.risk_config.rebalance_threshold
            && !self.pending_rebalance
            && now.saturating_sub(self.last_rebalance_ms) >= self.risk_config.min_rebalance_interval_ms
        {
            // Determine which side to buy (the lagging one)
            let (rebal_side, rebal_token, rebal_book) = if imbalance > Decimal::ZERO {
                // Long YES → buy NO
                (MarketSide::No, no_token.clone(), &self.state.poly_no_book)
            } else {
                // Long NO → buy YES
                (MarketSide::Yes, yes_token.clone(), &self.state.poly_yes_book)
            };

            if let Some(best_ask) = rebal_book.as_ref().and_then(|b| b.best_ask()) {
                // Check pair cost feasibility
                let other_avg = match rebal_side {
                    MarketSide::No => self.position.yes.avg_price(),
                    MarketSide::Yes => self.position.no.avg_price(),
                };
                let pair_cost = other_avg + best_ask.price;
                let max_rebal_cost = Decimal::try_from(self.risk_config.rebalance_max_pair_cost)
                    .unwrap_or(Decimal::new(96, 2));

                if pair_cost < max_rebal_cost {
                    let size = self.risk_config.rebalance_size.min(abs_imbalance);
                    if size >= Decimal::new(5, 0) {
                        self.pending_rebalance = true;
                        self.last_rebalance_ms = now;
                        commands.push(V2ExecutorCommand::RebalanceTaker {
                            side: rebal_side,
                            token_id: rebal_token,
                            price: best_ask.price,
                            size,
                        });
                        info!(
                            side = rebal_side.label(),
                            price = %best_ask.price,
                            size = %size,
                            pair_cost = %pair_cost,
                            "taker rebalance triggered"
                        );
                    }
                }
            }
        }

        commands
    }

    // ─── Closing tick (called each iteration) ───────────────────────────

    pub fn closing_tick(&mut self) -> Vec<V2ExecutorCommand> {
        if self.phase != MarketPhase::Closing || self.paused {
            return Vec::new();
        }

        let mut commands = Vec::new();

        // Step 1: Cancel all resting orders
        if !self.closing.resting_cancelled {
            let cancel_actions = self.quoter.cancel_all_actions();
            for action in cancel_actions {
                if let QuoteAction::Cancel { side, order_id } = action {
                    self.quoter.on_cancel_sent(side);
                    commands.push(V2ExecutorCommand::CancelOrder { side, order_id });
                }
            }
            self.closing.resting_cancelled = true;
            if !commands.is_empty() {
                return commands; // wait for cancel confirmations before pairing
            }
        }

        // Step 2: Wait for all pending operations to resolve
        if self.quoter.is_pending(MarketSide::Yes) || self.quoter.is_pending(MarketSide::No) {
            return commands;
        }
        if self.closing.pairing_sent {
            return commands;
        }

        // Step 3: Attempt pairing (multi-attempt with progressive price widening)
        if self.closing.can_retry() {
            self.closing.attempt_count += 1;

            let yes_ask = self.state.poly_yes_book.as_ref()
                .and_then(|b| b.best_ask())
                .map(|l| l.price);
            let no_ask = self.state.poly_no_book.as_ref()
                .and_then(|b| b.best_ask())
                .map(|l| l.price);

            let (yes_token, no_token) = match (
                self.state.active_yes_token_id.clone(),
                self.state.active_no_token_id.clone(),
            ) {
                (Some(y), Some(n)) => (y, n),
                _ => return commands,
            };

            // Widen max_pair_cost by attempt × retry_price_increment
            let base_max = Decimal::try_from(self.risk_config.max_closing_pair_cost)
                .unwrap_or(Decimal::new(97, 2));
            let widening = Decimal::try_from(
                self.risk_config.closing_retry_price_increment
            ).unwrap_or(Decimal::ZERO)
                * Decimal::from(self.closing.attempt_count.saturating_sub(1));
            let max_pair_cost = base_max + widening;

            if let Some(pairing) = ClosingManager::compute_pairing(
                &self.position,
                &yes_token,
                &no_token,
                yes_ask,
                no_ask,
                max_pair_cost,
                self.risk_config.max_capital_per_market,
            ) {
                info!(
                    side = pairing.side.label(),
                    price = %pairing.price,
                    size = %pairing.size,
                    pair_cost = %pairing.expected_pair_cost,
                    attempt = self.closing.attempt_count,
                    max_pair_cost = %max_pair_cost,
                    "closing: FOK pairing"
                );
                self.closing.pairing_sent = true;
                commands.push(V2ExecutorCommand::ClosingFok {
                    side: pairing.side,
                    token_id: pairing.token_id,
                    price: pairing.price,
                    size: pairing.size,
                });
            }
        }

        commands
    }

    // ─── Drain pending commands ─────────────────────────────────────────

    pub fn take_pending_commands(&mut self) -> Vec<V2ExecutorCommand> {
        std::mem::take(&mut self.pending_commands)
    }

    pub fn take_tick_size_change(&mut self) -> Option<V2ExecutorCommand> {
        self.pending_tick_change.take()
    }

    // ─── Control ────────────────────────────────────────────────────────

    pub fn set_draining(&mut self) {
        self.draining = true;
    }

    pub fn set_paused(&mut self, paused: bool) -> Vec<V2ExecutorCommand> {
        self.paused = paused;
        if paused {
            // Cancel all resting orders immediately
            let mut commands = Vec::new();
            let cancel_actions = self.quoter.cancel_all_actions();
            for action in cancel_actions {
                if let QuoteAction::Cancel { side, order_id } = action {
                    self.quoter.on_cancel_sent(side);
                    commands.push(V2ExecutorCommand::CancelOrder { side, order_id });
                }
            }
            if !commands.is_empty() {
                info!("paused: cancelling {} resting order(s)", commands.len());
            }
            commands
        } else {
            Vec::new()
        }
    }

    pub fn has_no_open_position(&self) -> bool {
        !self.quoter.has_resting_orders()
            && !self.quoter.is_pending(MarketSide::Yes)
            && !self.quoter.is_pending(MarketSide::No)
    }

    // ─── Diagnostics ────────────────────────────────────────────────────

    pub fn check_diagnostic(&mut self) {
        let now = epoch_ms();
        if now.saturating_sub(self.diag_last_ms) < 60_000 {
            return;
        }
        self.diag_last_ms = now;

        let paired = self.position.paired_shares();
        let locked = self.position.locked_profit();
        let deployed = self.position.total_capital_deployed();

        let msg = format!(
            "v2 60s | phase={:?} | yes_fills={} no_fills={} requotes={} closing_foks={} | \
             yes={:.0} no={:.0} paired={:.0} locked=${:.2} deployed=${:.2} | \
             fv_yes={:.3} fv_no={:.3} edge={:.3} | markets={}",
            self.phase,
            self.diag_yes_fills,
            self.diag_no_fills,
            self.diag_requotes,
            self.diag_closing_foks,
            self.position.yes.total_shares,
            self.position.no.total_shares,
            paired,
            locked,
            deployed,
            self.fair_value.yes_fair_value(),
            self.fair_value.no_fair_value(),
            self.fair_value.edge(),
            self.diag_markets_traded,
        );
        info!("{msg}");
        self.pending_telegram_diag = Some(msg);

        // Reset per-interval counters
        self.diag_yes_fills = 0;
        self.diag_no_fills = 0;
        self.diag_requotes = 0;
        self.diag_closing_foks = 0;
    }

    pub fn take_pending_telegram_diag(&mut self) -> Option<String> {
        self.pending_telegram_diag.take()
    }

    // ─── Fill notifications ─────────────────────────────────────────────

    fn push_fill_message(&mut self, side: MarketSide, price: Decimal, size: Decimal, was_taker: bool) {
        let tag = if was_taker { "TAKER" } else { "MAKER" };
        let msg = format!(
            "{} {} {:.0}@${:.3} | YES:{:.0} NO:{:.0} | paired:{:.0} locked:${:.2}",
            tag, side.label(), size, price,
            self.position.yes.total_shares, self.position.no.total_shares,
            self.position.paired_shares(), self.position.locked_profit(),
        );
        self.pending_fill_messages.push(msg);
    }

    pub fn take_pending_fill_messages(&mut self) -> Vec<String> {
        std::mem::take(&mut self.pending_fill_messages)
    }

    fn push_fill_record(&mut self, side: MarketSide, price: Decimal, size: Decimal, was_taker: bool, fee: Decimal, now: u64) {
        let condition_id = self.state.active_condition_id.clone().unwrap_or_default();
        self.pending_fill_records.push(FillRecord {
            condition_id,
            side: side.label().to_string(),
            price: price.to_f64().unwrap_or(0.0),
            size: size.to_f64().unwrap_or(0.0),
            was_taker,
            fee: fee.to_f64().unwrap_or(0.0),
            pair_cost: self.position.avg_pair_cost().to_f64().unwrap_or(0.0),
            paired: self.position.paired_shares().to_f64().unwrap_or(0.0),
            locked_profit: self.position.locked_profit().to_f64().unwrap_or(0.0),
            timestamp_ms: now,
        });
    }

    pub fn take_pending_fill_records(&mut self) -> Vec<FillRecord> {
        std::mem::take(&mut self.pending_fill_records)
    }

    pub fn take_pending_market_summary(&mut self) -> Option<MarketSummaryRecord> {
        self.pending_market_summary.take()
    }

    // ─── Market report / session summary (gated) ────────────────────────

    pub fn take_pending_market_report(&mut self) -> Option<String> {
        self.pending_market_report.take()
    }

    pub fn take_pending_session_summary(&mut self) -> Option<String> {
        self.pending_session_summary.take()
    }

    // ─── Status ─────────────────────────────────────────────────────────

    pub fn build_status(&self, mode: &str) -> crate::control::types::BotStatus {
        let now = epoch_ms();
        let uptime = now.saturating_sub(self.session_start_ms) / 1000;

        crate::control::types::BotStatus {
            uptime_secs: uptime,
            mode: mode.to_string(),
            current_market: self.state.active_condition_id.clone(),
            phase: format!("{:?}", self.phase),
            position_summary: format!("YES:{:.0} NO:{:.0}",
                self.position.yes.total_shares,
                self.position.no.total_shares),
            pairing_summary: format!("paired:{:.0} locked:${:.2}",
                self.position.paired_shares(),
                self.position.locked_profit()),
            unpaired_yes: format!("{:.0}", self.position.unpaired_yes()),
            unpaired_no: format!("{:.0}", self.position.unpaired_no()),
            markets_traded: self.diag_markets_traded as u64,
            total_fills: self.position.total_fills() as u64,
            trades_enabled: !self.paused,
            summary_enabled: true,
            draining: self.draining,
            paused: self.paused,
            heartbeat_healthy: self.heartbeat_healthy,
            heartbeat_failures: self.heartbeat_failures,
            heartbeat_latency_ms: self.heartbeat_latency_ms,
        }
    }

    // ─── Market report ──────────────────────────────────────────────────

    fn send_market_report(&mut self, _now: u64) {
        let paired = self.position.paired_shares();
        let locked = self.position.locked_profit();
        let deployed = self.position.total_capital_deployed();
        let yes_avg = self.position.yes.avg_price();
        let no_avg = self.position.no.avg_price();

        let market_id = self.state.active_condition_id.as_deref().unwrap_or("unknown");
        let short_id = if market_id.len() > 8 {
            &market_id[market_id.len() - 8..]
        } else {
            market_id
        };

        let text = format!(
            "<b>Market Complete</b> ...{short_id}\n\n\
             YES: {yes_shares:.0} shares @ ${yes_avg:.3} avg\n\
             NO: {no_shares:.0} shares @ ${no_avg:.3} avg\n\n\
             Paired: {paired:.0} @ ${pair_cost:.3} = ${locked:.2} locked profit\n\
             Unpaired YES: {up_yes:.0} | Unpaired NO: {up_no:.0}\n\
             Capital deployed: ${deployed:.2}\n\
             Taker fees: ${taker:.4}",
            yes_shares = self.position.yes.total_shares,
            yes_avg = yes_avg,
            no_shares = self.position.no.total_shares,
            no_avg = no_avg,
            paired = paired,
            pair_cost = self.position.avg_pair_cost(),
            locked = locked,
            up_yes = self.position.unpaired_yes(),
            up_no = self.position.unpaired_no(),
            deployed = deployed,
            taker = self.position.total_taker_fees(),
        );
        info!("{text}");
        self.pending_market_report = Some(text);

        // QuestDB market summary record
        let condition_id = self.state.active_condition_id.clone().unwrap_or_default();
        self.pending_market_summary = Some(MarketSummaryRecord {
            condition_id,
            yes_shares: self.position.yes.total_shares.to_f64().unwrap_or(0.0),
            no_shares: self.position.no.total_shares.to_f64().unwrap_or(0.0),
            yes_avg: yes_avg.to_f64().unwrap_or(0.0),
            no_avg: no_avg.to_f64().unwrap_or(0.0),
            paired: paired.to_f64().unwrap_or(0.0),
            pair_cost: self.position.avg_pair_cost().to_f64().unwrap_or(0.0),
            locked_profit: locked.to_f64().unwrap_or(0.0),
            taker_fees: self.position.total_taker_fees().to_f64().unwrap_or(0.0),
            fill_count: self.position.total_fills() as i64,
            timestamp_ms: epoch_ms(),
        });
    }

    pub fn send_session_summary(&mut self) {
        let now = epoch_ms();
        let uptime = now.saturating_sub(self.session_start_ms);
        let uptime_h = uptime / 3_600_000;
        let uptime_m = (uptime % 3_600_000) / 60_000;

        let text = format!(
            "<b>v2 Session Summary</b>\n\n\
             Uptime: {}h {:02}m\n\
             Markets traded: {}\n\
             Total fills: {} (YES: {} | NO: {})\n\
             Current position — YES: {:.0} NO: {:.0}\n\
             Paired: {:.0} | Locked profit: ${:.2}",
            uptime_h, uptime_m,
            self.diag_markets_traded,
            self.position.total_fills(),
            self.position.yes.fill_count,
            self.position.no.fill_count,
            self.position.yes.total_shares,
            self.position.no.total_shares,
            self.position.paired_shares(),
            self.position.locked_profit(),
        );
        info!("{text}");
        self.pending_session_summary = Some(text);
    }
}
