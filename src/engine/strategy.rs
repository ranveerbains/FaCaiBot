//! Strategy Engine — Layer 2 ("The Brain").
//!
//! Pulls [`IngestorEvent`]s from the ingestor crossbeam channel, maintains
//! [`MarketState`], evaluates arbitrage conditions, and emits [`TradeSignal`]s
//! to the Executor layer.
//!
//! # Architecture
//! - Runs as a tokio task on the main multi-threaded runtime.
//! - All state updates are O(1) — no sorting or searching in the critical path.
//! - All pricing arithmetic uses `rust_decimal::Decimal` — zero f32/f64.
//! - No I/O in the hot path (no Redis reads, no HTTP calls).
//! - Latency target: <2ms from event pull to signal emit.
//!
//! # Spike detection (speculative Leg 1 posting)
//! The Binance gateway emits `SpikeCandidate` immediately when ATR + magnitude pass.
//! The engine speculatively posts a Leg 1 order, then waits for `SpikeConfirmed`
//! (sustain passed → sim fill gate opens) or `SpikeFailed` (cancel speculative order).

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::types::market::{
    DataSource, Direction, IngestorEvent, MarketState, OrderBook, OrderState, PriceLevel,
    TradeStatus,
};
use crate::types::order::{ExecutorCommand, ExitReason, ProfitTier, Side, TradeSignal};
use crate::utils::time::epoch_ms as now_epoch_ms;

use super::confidence::compute_confidence;
use super::erosion::{ConnectivityState, ErosionSnap, ErosionState};
use super::evaluator::{
    Leg1Evaluator, Leg1Outcome, Leg1RejectReason, Leg2Decision, Leg2Evaluator, make_leg2_signal,
};

// ─── Spike diagnostic snapshot (stored from SpikeDiagnostic event) ───────────

/// Lightweight copy of spike detector diagnostics for Telegram forwarding.
#[derive(Debug, Clone)]
struct SpikeDiagData {
    atr: f64,
    threshold: f64,
    mid: f64,
    candidates: u64,
    rej_momentum: u64,
    rej_magnitude: u64,
    confirmed: u64,
    stale: u64,
}

// ─── Precomputed Decimal constants ───────────────────────────────────────────

/// EMA alpha for avg_book_depth smoothing (0.1).
const BOOK_DEPTH_ALPHA: Decimal = Decimal::from_parts(1, 0, 0, false, 1);
/// 1.0 - BOOK_DEPTH_ALPHA = 0.9.
const BOOK_DEPTH_ONE_MINUS: Decimal = Decimal::from_parts(9, 0, 0, false, 1);

// ─── Strategy Engine ─────────────────────────────────────────────────────────

/// Evaluates ingestor events against the current market state and produces trade signals.
///
/// Implements the full three-stage arbitrage strategy:
/// 1. **Leg 1**: Detect Binance spike → enter directional position (post-only maker)
/// 2. **Leg 2**: After Leg 1 fills → hedge with opposite side (post-only maker)
/// 3. **Emergency**: FOK taker fills when deadlines or adverse movement is detected
///
/// Signal evaluation logic (guard checks + signal building) lives in
/// [`Leg1Evaluator`] and [`Leg2Evaluator`]. This struct owns shared mutable
/// state and applies post-signal mutations after evaluator calls.
pub struct StrategyEngine {
    state: MarketState,
    erosion: Option<ErosionState>,
    connectivity: ConnectivityState,
    /// EMA of total poly book depth (alpha=0.1) for depth wall detection.
    avg_book_depth: Option<Decimal>,
    /// Epoch ms of the last Leg 2 erosion signal emitted.
    last_erosion_signal_ms: u64,
    /// Direction of the active Leg 1 trade. Set by evaluate(), cleared on completion/rotation.
    leg1_direction: Option<Direction>,

    // ── Cutoff window tracking ────────────────────────────────────────────
    /// `true` once the entry_cutoff window is entered for the current market.
    /// Reset to `false` on `MarketRotation`.
    in_cutoff_window: bool,
    /// Set to `true` when first entering the cutoff window; cleared by `take_cutoff_trigger()`.
    cutoff_trigger_pending: bool,
    /// The `market_end_ms` captured when the cutoff was first detected.
    cutoff_market_end_ms: u64,

    /// Stored Leg 1 signal for building confirmed fill signals in advance_simulation().
    pending_leg1_signal: Option<TradeSignal>,

    /// Emergency Leg 2 signals generated during MarketRotation when a Leg 1 is
    /// still open. Drained by main loop before sending the rotation command.
    rotation_emergency_buffer: Vec<TradeSignal>,

    /// Cancel command for a speculative Leg 1 order when SpikeFailed arrives.
    /// Drained by main loop after on_event().
    pending_spike_cancel: Option<ExecutorCommand>,

    /// Gates sim Leg 1 fills until `SpikeConfirmed` clears it.
    /// Set `true` on `SpikeCandidate`, cleared on `SpikeConfirmed` or `SpikeFailed`.
    speculative_awaiting_sustain: bool,

    /// Set `true` when any Polymarket book event updates the hedge book.
    /// During emergency mode, `evaluate_leg2()` skips evaluation if this is `false`
    /// (Binance ticks can't change the Polymarket book, so re-evaluation is pointless).
    hedge_book_changed: bool,

    /// Latest spike detector diagnostics (received via `SpikeDiagnostic` event).
    last_spike_diag: Option<SpikeDiagData>,

    // ── Diagnostic counters (cumulative from app start, logged every 60s) ──
    diag_markets_rotated: u64,
    diag_spikes_received: u64,
    diag_spikes_dropped_cutoff: u64,
    // Leg 1 rejection distribution — only incremented when spike_detected = true
    diag_rej_busy: u64,    // ActiveTrade: trade already in flight
    diag_rej_no_book: u64, // NoBook / NoBinance: data unavailable
    diag_rej_stale: u64,   // StaleBook
    diag_rej_skew: u64,    // PriceSkewed
    diag_rej_spread: u64,  // SpreadWide
    diag_rej_depth: u64,   // InsufficientDepth
    diag_rej_hedge: u64,   // HedgeInfeasible
    diag_rej_other: u64,   // Other (no market, bid cap, zero size, etc.)
    diag_leg1_signals: u64,
    diag_leg1_fills: u64,
    diag_leg2_erosion_steps: u64,
    diag_leg2_fills: u64,
    diag_emg_adverse: u64,
    diag_emg_breakeven: u64,
    diag_emg_expiry: u64,
    diag_emg_favorable: u64,
    diag_leg1_timeouts: u64,
    diag_spike_failures: u64,
    last_diag_ms: u64,

    // ── Drain mode ────────────────────────────────────────────────────────
    /// When `true`, `evaluate()` blocks new Leg 1 entries. Set by `/stop` or `/set`.
    pub draining: bool,

    /// Epoch ms when the engine was created (for uptime calculation).
    start_ms: u64,

    // ── Sub-evaluators ────────────────────────────────────────────────────
    leg1: Leg1Evaluator,
    leg2: Leg2Evaluator,
}

impl StrategyEngine {
    pub fn new(config: &Config) -> Self {
        let state = MarketState::new();

        let max_spread_ticks = config.bot.entry_guards.max_spread_ticks;
        let depth_min_pct =
            Decimal::try_from(config.bot.entry_guards.depth_min_pct).unwrap_or(Decimal::new(15, 2));
        let depth_wall_multiplier =
            Decimal::try_from(config.bot.risk.depth_wall_multiplier).unwrap_or(Decimal::new(4, 0));
        let high_threshold =
            Decimal::try_from(config.bot.confidence.high_threshold).unwrap_or(Decimal::new(8, 1));
        let med_threshold =
            Decimal::try_from(config.bot.confidence.med_threshold).unwrap_or(Decimal::new(5, 1));
        Self {
            state,
            erosion: None,
            connectivity: ConnectivityState::default(),
            avg_book_depth: None,
            last_erosion_signal_ms: 0,
            leg1_direction: None,
            in_cutoff_window: false,
            cutoff_trigger_pending: false,
            cutoff_market_end_ms: 0,
            pending_leg1_signal: None,
            rotation_emergency_buffer: Vec::new(),
            pending_spike_cancel: None,
            speculative_awaiting_sustain: false,
            hedge_book_changed: false,
            last_spike_diag: None,
            diag_markets_rotated: 0,
            diag_spikes_received: 0,
            diag_spikes_dropped_cutoff: 0,
            diag_rej_busy: 0,
            diag_rej_no_book: 0,
            diag_rej_stale: 0,
            diag_rej_skew: 0,
            diag_rej_spread: 0,
            diag_rej_depth: 0,
            diag_rej_hedge: 0,
            diag_rej_other: 0,
            diag_leg1_signals: 0,
            diag_leg1_fills: 0,
            diag_leg2_erosion_steps: 0,
            diag_leg2_fills: 0,
            diag_emg_adverse: 0,
            diag_emg_breakeven: 0,
            diag_emg_expiry: 0,
            diag_emg_favorable: 0,
            diag_leg1_timeouts: 0,
            diag_spike_failures: 0,
            last_diag_ms: 0,
            draining: false,
            start_ms: now_epoch_ms(),
            leg1: Leg1Evaluator {
                max_spread_ticks,
                entry_cutoff_secs: config.bot.entry_guards.entry_cutoff_secs,
                depth_min_pct,
                stale_book_ms: config.bot.entry_guards.stale_book_ms,
                max_alloc_per_trade: config.max_alloc_per_trade,
                high_alloc_pct: config.high_alloc_pct,
                med_alloc_pct: config.med_alloc_pct,
                low_alloc_pct: config.low_alloc_pct,
                depth_wall_multiplier,
                high_threshold,
                med_threshold,
                high_target_pct: config.high_target_pct,
                med_target_pct: config.med_target_pct,
                low_target_pct: config.low_target_pct,
                max_price_skew: Decimal::try_from(config.bot.entry_guards.max_price_skew)
                    .unwrap_or(Decimal::new(9, 1)),
                leg1_timeout_ms: config.bot.entry_guards.leg1_timeout_ms,
            },
            leg2: Leg2Evaluator {
                adverse_threshold: config.adverse_threshold,
                erosion_base_interval_ms: config.bot.risk.erosion_base_interval_ms,
                erosion_interval_decay: config.bot.risk.erosion_interval_decay,
                depth_wall_multiplier,
                emergency_deadline_ms: config.bot.risk.emergency_deadline_ms,
            },
        }
    }

    // ─── Public Interface ─────────────────────────────────────────────────

    /// Process an inbound event and update internal state.
    /// All 11 `IngestorEvent` variants are handled. All updates are O(1).
    pub fn on_event(&mut self, event: IngestorEvent) {
        let now_ms = now_epoch_ms();
        match event {
            // ── Full Polymarket book snapshot ─────────────────────────────
            IngestorEvent::PolymarketBook(book) => {
                let ts = book.timestamp_ms;
                let total_depth = book.total_bid_depth() + book.total_ask_depth();
                self.avg_book_depth = Some(match self.avg_book_depth {
                    None => total_depth,
                    Some(prev) => BOOK_DEPTH_ALPHA * total_depth + BOOK_DEPTH_ONE_MINUS * prev,
                });
                let yes_id = self.state.active_yes_token_id.as_deref().unwrap_or("");
                if book.asset_id == yes_id {
                    self.state.poly_yes_book = Some(book.clone());
                } else {
                    self.state.poly_no_book = Some(book.clone());
                }
                self.state.poly_book = Some(book);
                self.state.last_update_ms = ts;
                self.hedge_book_changed = true;
            }

            // ── Incremental price level update ────────────────────────────
            IngestorEvent::PolymarketPriceChange {
                asset_id,
                price,
                size,
                side,
                best_bid,
                best_ask,
            } => {
                let _ = (best_bid, best_ask);

                // Helper: apply a single level change to an order book.
                macro_rules! apply_level {
                    ($book:expr) => {
                        if $book.asset_id == asset_id {
                            let levels = match side {
                                Side::Buy => &mut $book.bids,
                                Side::Sell => &mut $book.asks,
                            };
                            if let Some(idx) = levels.iter().position(|l| l.price == price) {
                                if size.is_zero() {
                                    levels.remove(idx);
                                } else {
                                    levels[idx].size = size;
                                }
                            } else if !size.is_zero() {
                                let insert_pos = match side {
                                    Side::Buy => levels
                                        .iter()
                                        .position(|l| l.price < price)
                                        .unwrap_or(levels.len()),
                                    Side::Sell => levels
                                        .iter()
                                        .position(|l| l.price > price)
                                        .unwrap_or(levels.len()),
                                };
                                levels.insert(insert_pos, PriceLevel { price, size });
                            }
                            $book.timestamp_ms = now_ms;
                        }
                    };
                }

                if let Some(ref mut book) = self.state.poly_book {
                    apply_level!(book);
                }

                // Also keep directional books in sync so their timestamps stay
                // fresh and the stale-book guard doesn't reject evaluations
                // during periods when BestBidAsk events are sparse (e.g., at
                // market open).
                let yes_id = self.state.active_yes_token_id.as_deref().unwrap_or("").to_owned();
                let is_yes = asset_id == yes_id;
                if is_yes {
                    if let Some(ref mut book) = self.state.poly_yes_book {
                        apply_level!(book);
                    }
                } else {
                    if let Some(ref mut book) = self.state.poly_no_book {
                        apply_level!(book);
                    }
                }

                self.state.last_update_ms = now_ms;
                self.hedge_book_changed = true;
            }

            // ── Fast-path top-of-book update ──────────────────────────────
            IngestorEvent::PolymarketBestBidAsk {
                asset_id,
                best_bid,
                best_ask,
            } => {
                let yes_id = self.state.active_yes_token_id.as_deref().unwrap_or("");
                let is_yes = asset_id == yes_id;
                let synthetic_depth = Decimal::new(500, 0);

                // Update poly_book (backward compat).
                if let Some(ref mut book) = self.state.poly_book {
                    if book.asset_id == asset_id {
                        if let Some(top) = book.bids.first_mut() {
                            top.price = best_bid;
                        }
                        if let Some(top) = book.asks.first_mut() {
                            top.price = best_ask;
                        }
                        book.timestamp_ms = now_ms;
                    }
                } else if !best_bid.is_zero() && !best_ask.is_zero() {
                    debug!(
                        %asset_id, %best_bid, %best_ask,
                        "bootstrapping poly_book from BestBidAsk (no full book yet)"
                    );
                    self.state.poly_book = Some(OrderBook {
                        asset_id: asset_id.clone(),
                        bids: vec![PriceLevel {
                            price: best_bid,
                            size: synthetic_depth,
                        }],
                        asks: vec![PriceLevel {
                            price: best_ask,
                            size: synthetic_depth,
                        }],
                        timestamp_ms: now_ms,
                    });
                }

                // Update directional book.
                if is_yes {
                    if let Some(ref mut book) = self.state.poly_yes_book {
                        if book.asset_id == asset_id {
                            if let Some(top) = book.bids.first_mut() {
                                top.price = best_bid;
                            }
                            if let Some(top) = book.asks.first_mut() {
                                top.price = best_ask;
                            }
                            book.timestamp_ms = now_ms;
                        }
                    } else if !best_bid.is_zero() && !best_ask.is_zero() {
                        self.state.poly_yes_book = Some(OrderBook {
                            asset_id: asset_id.clone(),
                            bids: vec![PriceLevel {
                                price: best_bid,
                                size: synthetic_depth,
                            }],
                            asks: vec![PriceLevel {
                                price: best_ask,
                                size: synthetic_depth,
                            }],
                            timestamp_ms: now_ms,
                        });
                    }
                } else {
                    if let Some(ref mut book) = self.state.poly_no_book {
                        if book.asset_id == asset_id {
                            if let Some(top) = book.bids.first_mut() {
                                top.price = best_bid;
                            }
                            if let Some(top) = book.asks.first_mut() {
                                top.price = best_ask;
                            }
                            book.timestamp_ms = now_ms;
                        }
                    } else if !best_bid.is_zero() && !best_ask.is_zero() {
                        self.state.poly_no_book = Some(OrderBook {
                            asset_id: asset_id.clone(),
                            bids: vec![PriceLevel {
                                price: best_bid,
                                size: synthetic_depth,
                            }],
                            asks: vec![PriceLevel {
                                price: best_ask,
                                size: synthetic_depth,
                            }],
                            timestamp_ms: now_ms,
                        });
                    }
                }

                self.state.last_update_ms = now_ms;
                self.hedge_book_changed = true;
            }

            // ── Tick size change (rare, at price extremes) ────────────────
            IngestorEvent::PolymarketTickSizeChange {
                asset_id,
                old_tick_size,
                new_tick_size,
            } => {
                info!(%asset_id, %old_tick_size, %new_tick_size, "tick size changed");
                self.state.tick_size = new_tick_size;
                self.state.last_update_ms = now_ms;
            }

            // ── Market resolved ───────────────────────────────────────────
            IngestorEvent::PolymarketMarketResolved {
                market,
                winning_asset_id,
            } => {
                info!(%market, %winning_asset_id, "market resolved — suspending trading");
                if self.state.active_condition_id.as_deref() == Some(&market) {
                    self.state.active_condition_id = None;
                }
                self.state.last_update_ms = now_ms;
            }

            // ── Binance ticker update ─────────────────────────────────────
            IngestorEvent::BinanceTick(tick) => {
                self.state.binance_price = Some(tick.mid_price());
                self.state.last_update_ms = tick.timestamp_ms;
            }

            // ── Binance depth snapshot ────────────────────────────────────
            IngestorEvent::BinanceDepth(depth) => {
                if let Some(mid) = depth.mid_price() {
                    self.state.binance_price = Some(mid);
                }
                self.state.last_update_ms = depth.timestamp_ms;
            }

            // ── Spike candidate — speculative Leg 1 posting ──────────────
            IngestorEvent::SpikeCandidate(spike) => {
                self.diag_spikes_received += 1;
                // Drop spike if within entry_cutoff window — no new trades allowed.
                if self.in_cutoff_window {
                    self.diag_spikes_dropped_cutoff += 1;
                    debug!("spike candidate ignored — within cutoff window");
                    return;
                }

                info!(
                    direction = ?spike.direction,
                    magnitude_pct = %(spike.magnitude.to_f64().unwrap_or(0.0) * 100.0),
                    spike_detected_ms = spike.timestamp_ms,
                    "spike candidate received — speculative Leg 1"
                );
                self.state.spike_detected = true;
                self.state.last_spike = Some(spike);
                self.speculative_awaiting_sustain = true;
            }

            // ── Spike confirmed — sustain passed, sim fill gate opens ────
            IngestorEvent::SpikeConfirmed(spike) => {
                self.speculative_awaiting_sustain = false;
                // Update spike info with the confirmed (sustain-time) magnitude.
                self.state.last_spike = Some(spike);
                info!(
                    direction = ?spike.direction,
                    magnitude_pct = %(spike.magnitude.to_f64().unwrap_or(0.0) * 100.0),
                    sustained_ms = spike.sustained_ms,
                    spike_detected_ms = spike.timestamp_ms,
                    "spike sustained — sim fill gate open"
                );
            }

            // ── Spike failed — cancel speculative Leg 1 ─────────────────
            IngestorEvent::SpikeFailed { timestamp_ms } => {
                self.speculative_awaiting_sustain = false;
                self.diag_spike_failures += 1;

                match &self.state.leg1_state {
                    OrderState::Posted { order_id, .. } => {
                        info!(%order_id, timestamp_ms, "spike FAILED — cancelling speculative Leg 1");
                        self.pending_spike_cancel = Some(ExecutorCommand::CancelLeg1 {
                            order_id: order_id.clone(),
                        });
                        self.state.leg1_state = OrderState::None;
                        self.pending_leg1_signal = None;
                        self.leg1_direction = None;
                        self.state.spike_detected = false;
                        self.state.last_spike = None;
                    }
                    OrderState::Filled { .. } => {
                        // Already filled — proceed with Leg 2 normally.
                        debug!(
                            timestamp_ms,
                            "spike failed but Leg 1 already filled — no-op"
                        );
                    }
                    OrderState::None => {
                        // Evaluator rejected the candidate — no order was posted.
                        debug!(timestamp_ms, "spike failed but no Leg 1 posted — no-op");
                        self.state.spike_detected = false;
                        self.state.last_spike = None;
                    }
                }
            }

            // ── Market rotation ───────────────────────────────────────────
            IngestorEvent::MarketRotation {
                condition_id,
                yes_token_id,
                no_token_id,
                end_timestamp_ms,
            } => {
                info!(%condition_id, %yes_token_id, %no_token_id, end_timestamp_ms, "market rotated");

                // ── Rotation emergency: protect open Leg 1 positions ─────
                // If Leg 1 is filled but Leg 2 hasn't completed, build an
                // emergency FOK signal BEFORE resetting state. The main loop
                // drains this buffer and sends it to the executor before the
                // MarketRotation command, ensuring the position is hedged
                // (or best-effort attempted) instead of force-closed.
                let leg1_filled = matches!(self.state.leg1_state, OrderState::Filled { .. });
                let leg2_filled = matches!(self.state.leg2_state, OrderState::Filled { .. });
                if leg1_filled && !leg2_filled {
                    let fill_size = match &self.state.leg1_state {
                        OrderState::Filled { size, .. } => *size,
                        _ => unreachable!(),
                    };
                    // Opposing ask = the price we'd pay as taker on the hedge side.
                    let opposing_ask = match self.leg1_direction {
                        Some(Direction::Up) => self
                            .state
                            .poly_no_book
                            .as_ref()
                            .or(self.state.poly_book.as_ref()),
                        Some(Direction::Down) => self
                            .state
                            .poly_yes_book
                            .as_ref()
                            .or(self.state.poly_book.as_ref()),
                        None => None,
                    }
                    .and_then(|b| b.best_ask())
                    .map(|a| a.price);

                    if let Some(ask_price) = opposing_ask {
                        if let Some(mut signal) =
                            self.build_sim_leg2_fill_signal(ask_price, fill_size, now_ms)
                        {
                            signal.exit_reason = Some(ExitReason::MarketExpiry);
                            warn!(
                                %ask_price, %fill_size,
                                "rotation emergency: Leg 1 filled, Leg 2 incomplete \
                                 — emitting emergency FOK before state reset"
                            );
                            self.diag_emg_expiry += 1;
                            self.rotation_emergency_buffer.push(signal);
                        } else {
                            warn!(
                                "rotation emergency: could not build signal \
                                 (missing erosion/token) — position will be force-closed"
                            );
                        }
                    } else {
                        warn!(
                            "rotation emergency: no opposing ask available \
                             — position will be force-closed"
                        );
                    }
                }

                // ── Reset all state for the new market ───────────────────
                self.state.active_condition_id = Some(condition_id);
                self.state.active_yes_token_id = Some(yes_token_id);
                self.state.active_no_token_id = Some(no_token_id);
                self.state.market_end_timestamp_ms = end_timestamp_ms;
                self.state.poly_book = None;
                self.state.poly_yes_book = None;
                self.state.poly_no_book = None;
                self.state.spike_detected = false;
                self.state.last_spike = None;
                self.state.leg1_state = OrderState::None;
                self.state.leg2_state = OrderState::None;
                self.state.cumulative_used = Decimal::ZERO;
                self.state.last_update_ms = now_ms;
                self.erosion = None;
                self.last_erosion_signal_ms = 0;
                self.avg_book_depth = None;
                self.leg1_direction = None;
                self.pending_leg1_signal = None;
                self.pending_spike_cancel = None;
                self.speculative_awaiting_sustain = false;
                self.in_cutoff_window = false;
                self.diag_markets_rotated += 1;
            }

            // ── Trade status update (fill tracking via User WS) ───────────
            IngestorEvent::TradeStatusUpdate { order_id, status } => {
                let is_leg1 = match &self.state.leg1_state {
                    OrderState::Posted { order_id: oid, .. } => *oid == order_id,
                    _ => false,
                };
                if is_leg1 {
                    let (price, size) = match &self.state.leg1_state {
                        OrderState::Posted { price, size, .. } => (*price, *size),
                        _ => unreachable!(),
                    };
                    match status {
                        TradeStatus::Matched | TradeStatus::Mined | TradeStatus::Confirmed => {
                            info!(%order_id, %price, %size, status = ?status, "Leg 1 fill confirmed");
                            self.state.leg1_state = OrderState::Filled {
                                order_id: order_id.clone(),
                                price,
                                size,
                                fill_timestamp_ms: now_ms,
                            };
                            self.init_erosion(price, size, now_ms);
                        }
                        TradeStatus::Failed => {
                            warn!(%order_id, "Leg 1 FAILED — resetting to None");
                            self.state.leg1_state = OrderState::None;
                        }
                        TradeStatus::Retrying => {
                            debug!(%order_id, "Leg 1 RETRYING");
                        }
                    }
                }

                let is_leg2 = match &self.state.leg2_state {
                    OrderState::Posted { order_id: oid, .. } => *oid == order_id,
                    _ => false,
                };
                if is_leg2 {
                    let (price, size) = match &self.state.leg2_state {
                        OrderState::Posted { price, size, .. } => (*price, *size),
                        _ => unreachable!(),
                    };
                    match status {
                        TradeStatus::Matched | TradeStatus::Mined | TradeStatus::Confirmed => {
                            info!(%order_id, %price, %size, status = ?status, "Leg 2 fill — pair complete");
                            self.state.leg2_state = OrderState::Filled {
                                order_id: order_id.clone(),
                                price,
                                size,
                                fill_timestamp_ms: now_ms,
                            };
                            self.erosion = None;
                            self.last_erosion_signal_ms = 0;
                        }
                        TradeStatus::Failed => {
                            warn!(%order_id, "Leg 2 FAILED — re-entry via evaluate_leg2");
                            self.state.leg2_state = OrderState::None;
                        }
                        TradeStatus::Retrying => {
                            debug!(%order_id, "Leg 2 RETRYING");
                        }
                    }
                }
                self.state.last_update_ms = now_ms;
            }

            // ── Heartbeat status ──────────────────────────────────────────
            IngestorEvent::HeartbeatStatus {
                success,
                latency_ms,
            } => {
                if success {
                    self.connectivity.heartbeat_healthy = true;
                    self.connectivity.consecutive_heartbeat_failures = 0;
                    debug!(latency_ms, "heartbeat OK");
                } else {
                    self.connectivity.consecutive_heartbeat_failures += 1;
                    self.connectivity.heartbeat_healthy = false;
                    warn!(
                        failures = self.connectivity.consecutive_heartbeat_failures,
                        "heartbeat FAILED — orders may be auto-cancelled by CLOB"
                    );
                }
            }

            // ── WebSocket connectivity ────────────────────────────────────
            IngestorEvent::WsStatus { source, connected } => match source {
                DataSource::Binance => {
                    self.connectivity.binance_connected = connected;
                    if !connected {
                        warn!("Binance WS disconnected — spike detection paused");
                        self.state.spike_detected = false;
                    } else {
                        info!("Binance WS reconnected");
                    }
                }
                DataSource::PolymarketMarket => {
                    self.connectivity.polymarket_market_connected = connected;
                    if !connected {
                        warn!("Polymarket Market WS disconnected");
                    }
                }
                DataSource::PolymarketUser => {
                    self.connectivity.polymarket_user_connected = connected;
                    if !connected {
                        warn!("Polymarket User WS disconnected");
                    }
                }
            },

            // ── Spike diagnostic snapshot (for Telegram forwarding) ────
            IngestorEvent::SpikeDiagnostic {
                atr,
                threshold,
                mid,
                candidates,
                rej_momentum,
                rej_magnitude,
                confirmed,
                stale,
            } => {
                self.last_spike_diag = Some(SpikeDiagData {
                    atr,
                    threshold,
                    mid,
                    candidates,
                    rej_momentum,
                    rej_magnitude,
                    confirmed,
                    stale,
                });
            }

            // Control events are handled in main.rs before on_event() is called.
            IngestorEvent::Shutdown | IngestorEvent::DrainAndRestart => {}
        }

        // ── Cutoff window detection (runs on every event) ──────────────
        // Checks time remaining on every event so the market summary is
        // sent even if no spike arrives during the cutoff window.
        if !self.in_cutoff_window && self.state.active_condition_id.is_some() {
            let time_remaining_secs = self.state.time_remaining_ms(now_ms) / 1_000;
            if time_remaining_secs < self.leg1.entry_cutoff_secs {
                self.in_cutoff_window = true;
                self.cutoff_trigger_pending = true;
                self.cutoff_market_end_ms = self.state.market_end_timestamp_ms;
                info!(
                    time_remaining_secs,
                    "entering cutoff window — trading suspended, sending market summary"
                );
                if matches!(
                    self.state.leg1_state,
                    OrderState::Filled { .. } | OrderState::Posted { .. }
                ) {
                    info!(
                        "open position detected at cutoff — \
                         Leg 2 will continue until rotation"
                    );
                }
            }
        }
    }

    /// Evaluate current state and optionally emit a **Leg 1** trade signal.
    ///
    /// Delegates guard checking and signal building to [`Leg1Evaluator::evaluate`].
    /// Applies post-signal state mutations here after a successful evaluation:
    /// - Clears `spike_detected`
    /// - Sets `leg1_state` to `Posted`
    /// - Increments `cumulative_used`
    /// - Records `leg1_direction`
    pub fn evaluate(&mut self) -> Option<TradeSignal> {
        // Drain mode: block new Leg 1 entries.
        if self.draining {
            self.state.spike_detected = false;
            return None;
        }

        let now_ms = now_epoch_ms();
        let outcome = self.leg1.evaluate(&self.state, self.avg_book_depth, now_ms);

        match outcome {
            Leg1Outcome::Signal(signal) => {
                let direction = signal.direction;
                let alloc = signal.alloc_amount;
                let price = signal.price;
                let size = signal.size;

                self.state.spike_detected = false;
                self.state.leg1_state = OrderState::Posted {
                    order_id: format!("sim-leg1-{}", now_ms),
                    price,
                    size,
                    timestamp_ms: now_ms,
                };
                self.state.cumulative_used += alloc;
                self.leg1_direction = Some(direction);
                self.pending_leg1_signal = Some(signal.clone());
                self.diag_leg1_signals += 1;

                Some(signal)
            }
            Leg1Outcome::Rejected(reason) => {
                // Spike consumed — record why it was blocked.
                self.state.spike_detected = false;
                match reason {
                    Leg1RejectReason::ActiveTrade => self.diag_rej_busy += 1,
                    Leg1RejectReason::NoBook | Leg1RejectReason::NoBinance => {
                        self.diag_rej_no_book += 1
                    }
                    Leg1RejectReason::StaleBook => self.diag_rej_stale += 1,
                    Leg1RejectReason::PriceSkewed => self.diag_rej_skew += 1,
                    Leg1RejectReason::SpreadWide => self.diag_rej_spread += 1,
                    Leg1RejectReason::InsufficientDepth => self.diag_rej_depth += 1,
                    Leg1RejectReason::HedgeInfeasible => self.diag_rej_hedge += 1,
                    Leg1RejectReason::Other => self.diag_rej_other += 1,
                }
                None
            }
            Leg1Outcome::Skipped => {
                // No spike — nothing to count or clear.
                None
            }
        }
    }

    /// Evaluate Leg 2 hedge signal after Leg 1 fills.
    ///
    /// Delegates guard checking and signal building to [`Leg2Evaluator::evaluate_leg2`].
    /// Applies post-signal state mutations here after a successful evaluation:
    /// - Sets `leg2_state` to `Posted`
    /// - On emergency: sets `erosion.emergency_submitted = true`
    /// - On erosion: advances `erosion.steps_applied` if interval elapsed,
    ///   updates `last_erosion_signal_ms`
    pub fn evaluate_leg2(&mut self) -> Option<TradeSignal> {
        let now_ms = now_epoch_ms();

        // During emergency exit, only Polymarket book changes matter for
        // price-improvement checks. Skip evaluation if the hedge book hasn't
        // changed — UNLESS the hard deadline may have expired (time-based).
        let in_emergency = self.erosion.as_ref().is_some_and(|e| e.emergency_submitted);
        if in_emergency {
            let deadline_may_have_expired = self.erosion.as_ref().is_some_and(|e| {
                e.emergency_first_post_ms
                    .map(|first| now_ms.saturating_sub(first) >= self.leg2.emergency_deadline_ms)
                    .unwrap_or(false)
            });
            if !self.hedge_book_changed && !deadline_may_have_expired {
                return None;
            }
            self.hedge_book_changed = false;
        }

        // Build borrow-free snapshot of erosion state to pass to evaluator.
        let snap = match self.erosion.as_ref() {
            None => return None,
            Some(e) => ErosionSnap {
                emergency_submitted: e.emergency_submitted,
                break_even: e.break_even(),
                current_profit_target: e.current_profit_target(),
                initial_profit_target: e.initial_profit_target,
                direction: e.direction,
                binance_at_fill: e.binance_at_fill,
                fill_ms: e.leg1_fill_ms,
                steps_applied: e.steps_applied,
                tier: e.tier,
                confidence: e.confidence,
                spike_info: e.spike_info,
                leg1_fill_price: e.leg1_fill_price,
                exit_reason: e.exit_reason,
                emergency_first_post_ms: e.emergency_first_post_ms,
                emergency_posted_price: e.emergency_posted_price,
            },
        };

        let last_erosion_ms = self.last_erosion_signal_ms;
        let decision = match self
            .leg2
            .evaluate_leg2(&self.state, &snap, last_erosion_ms, now_ms)
        {
            Some(d) => d,
            None => {
                // Evaluator returned None — could be timing gate, skip guard, or missing data.
                // If the erosion timing gate has passed, silently advance the step to prevent
                // the cascade from stalling when per-step target increments are smaller than
                // tick size (e.g. MED tier's 2% margin over 5 steps < $0.01 tick).
                // Without this, steps_applied never reaches MAX_EROSION_STEPS and the
                // erosion-exhausted emergency never fires.
                if last_erosion_ms > 0 && !snap.is_exhausted() {
                    let interval = ErosionState::interval_for_step(
                        snap.steps_applied,
                        self.leg2.erosion_base_interval_ms,
                        self.leg2.erosion_interval_decay,
                    );
                    if now_ms.saturating_sub(last_erosion_ms) >= interval {
                        if let Some(e) = self.erosion.as_mut() {
                            e.steps_applied += 1;
                        }
                        self.last_erosion_signal_ms = now_ms;
                    }
                }
                return None;
            }
        };

        // ── Apply mutations based on decision type ────────────────────────
        let (price, size) = (decision.price(), decision.size());
        let is_emergency = decision.is_emergency();

        if is_emergency {
            let reason = match &decision {
                Leg2Decision::Emergency { signal, .. } => signal.exit_reason,
                _ => None,
            };
            match reason {
                Some(ExitReason::AdverseMovement) => self.diag_emg_adverse += 1,
                Some(ExitReason::BreakEvenBreach) => self.diag_emg_breakeven += 1,
                Some(ExitReason::MarketExpiry) => self.diag_emg_expiry += 1,
                Some(ExitReason::FavorableTaker) => self.diag_emg_favorable += 1,
                None => self.diag_emg_adverse += 1, // fallback
            }
            if let Some(e) = self.erosion.as_mut() {
                if !e.emergency_submitted {
                    // First emergency post — record the timestamp for deadline tracking.
                    e.emergency_first_post_ms = Some(now_ms);
                }
                e.emergency_submitted = true;
                e.emergency_posted_price = Some(price);
                e.exit_reason = match &decision {
                    Leg2Decision::Emergency { signal, .. } => signal.exit_reason,
                    _ => None,
                };
            }
            self.last_erosion_signal_ms = now_ms;
            self.state.leg2_state = OrderState::Posted {
                order_id: format!("sim-leg2-emergency-{}", now_ms),
                price,
                size,
                timestamp_ms: now_ms,
            };
        } else {
            // Normal erosion path.
            self.diag_leg2_erosion_steps += 1;
            if let Leg2Decision::Erosion { advance_step, .. } = &decision {
                if *advance_step {
                    if let Some(e) = self.erosion.as_mut() {
                        if !e.is_exhausted() {
                            e.steps_applied += 1;
                        }
                    }
                }
            }
            self.last_erosion_signal_ms = now_ms;
            self.state.leg2_state = OrderState::Posted {
                order_id: format!("sim-leg2-erosion-{}", now_ms),
                price,
                size,
                timestamp_ms: now_ms,
            };
        }

        Some(decision.into_signal())
    }

    // ─── Cutoff trigger ───────────────────────────────────────────────────

    /// Returns `Some((condition_id, market_end_ms))` exactly once when the bot
    /// first enters the cutoff window for the current market.
    /// Returns `None` on subsequent calls until the next `MarketRotation`.
    pub fn take_cutoff_trigger(&mut self) -> Option<(String, u64)> {
        if self.cutoff_trigger_pending {
            self.cutoff_trigger_pending = false;
            let cond_id = self.state.active_condition_id.clone()?;
            Some((cond_id, self.cutoff_market_end_ms))
        } else {
            None
        }
    }

    // ─── Spike cancel drain ─────────────────────────────────────────────

    /// Take the pending spike cancel command (if any).
    /// Called by the main loop after `on_event()` to send `CancelLeg1` to the executor.
    pub fn take_spike_cancel(&mut self) -> Option<ExecutorCommand> {
        self.pending_spike_cancel.take()
    }

    // ─── Rotation emergency drain ─────────────────────────────────────

    /// Drain any emergency signals buffered during the last `MarketRotation`.
    ///
    /// Called by the main loop immediately after `on_event()` and BEFORE sending
    /// `ExecutorCommand::MarketRotation`, so the executor processes the emergency
    /// hedge before cleaning up the old market's state.
    pub fn take_rotation_emergencies(&mut self) -> Vec<TradeSignal> {
        std::mem::take(&mut self.rotation_emergency_buffer)
    }

    // ─── Diagnostic ─────────────────────────────────────────────────────

    /// Log cumulative engine diagnostics every 60 seconds.
    /// Returns a formatted diagnostic message when the 60s gate fires (for Telegram
    /// forwarding), or `None` otherwise.
    /// Call this once per engine loop iteration (cheap — checks timestamp first).
    pub fn check_diagnostic(&mut self) -> Option<String> {
        let now_ms = now_epoch_ms();
        if self.last_diag_ms == 0 {
            self.last_diag_ms = now_ms;
            return None;
        }
        if now_ms.saturating_sub(self.last_diag_ms) < 60_000 {
            return None;
        }
        info!(
            markets = self.diag_markets_rotated,
            spikes = self.diag_spikes_received,
            spikes_cut = self.diag_spikes_dropped_cutoff,
            rej_busy = self.diag_rej_busy,
            rej_no_book = self.diag_rej_no_book,
            rej_stale = self.diag_rej_stale,
            rej_skew = self.diag_rej_skew,
            rej_spread = self.diag_rej_spread,
            rej_depth = self.diag_rej_depth,
            rej_hedge = self.diag_rej_hedge,
            rej_other = self.diag_rej_other,
            leg1_sig = self.diag_leg1_signals,
            leg1_fill = self.diag_leg1_fills,
            erosion_stp = self.diag_leg2_erosion_steps,
            leg2_fill = self.diag_leg2_fills,
            emg_adverse = self.diag_emg_adverse,
            emg_breakeven = self.diag_emg_breakeven,
            emg_expiry = self.diag_emg_expiry,
            emg_favorable = self.diag_emg_favorable,
            leg1_timeout = self.diag_leg1_timeouts,
            spike_fail = self.diag_spike_failures,
            "engine 60s"
        );
        self.last_diag_ms = now_ms;

        // Build combined Telegram message (spike + engine).
        let spike_section = if let Some(ref s) = self.last_spike_diag {
            format!(
                "<b>Spike Detector</b>\n\
                 ATR: <code>{atr:.4}</code>  Threshold: <code>{thr:.4}</code>  Mid: <code>${mid:.2}</code>\n\
                 Candidates: {cand}  Confirmed: {conf}  Stale: {stale}\n\
                 Rejected — momentum: {rej_mom}  magnitude: {rej_mag}",
                atr = s.atr,
                thr = s.threshold,
                mid = s.mid,
                cand = s.candidates,
                conf = s.confirmed,
                stale = s.stale,
                rej_mom = s.rej_momentum,
                rej_mag = s.rej_magnitude,
            )
        } else {
            "<b>Spike Detector</b>\n(no data yet)".to_string()
        };

        let msg = format!(
            "<b>--- DIAGNOSTICS (60s) ---</b>\n\
             \n\
             {spike}\n\
             \n\
             <b>Engine</b>\n\
             Markets rotated: {mkts}  Spikes: {spikes}  Spike fails: {spike_fail}  Cutoff drops: {spikes_cut}\n\
             \n\
             <b>Leg 1 Rejections</b>\n\
             Busy: {busy}  No book: {no_book}  Stale: {stale}  Skewed: {skew}\n\
             Spread: {spread}  Depth: {depth}  Hedge: {hedge}  Other: {other}\n\
             \n\
             <b>Leg 1</b>\n\
             Signals: {sig}  Fills: {fill}  Timeouts: {timeout}\n\
             \n\
             <b>Leg 2</b>\n\
             Erosion steps: {erosion}  Fills: {l2fill}\n\
             Emergencies — adverse: {emg_adv}  break-even: {emg_be}  expiry: {emg_exp}  favorable: {emg_fav}",
            spike = spike_section,
            mkts = self.diag_markets_rotated,
            spikes = self.diag_spikes_received,
            spike_fail = self.diag_spike_failures,
            spikes_cut = self.diag_spikes_dropped_cutoff,
            busy = self.diag_rej_busy,
            no_book = self.diag_rej_no_book,
            stale = self.diag_rej_stale,
            skew = self.diag_rej_skew,
            spread = self.diag_rej_spread,
            depth = self.diag_rej_depth,
            hedge = self.diag_rej_hedge,
            other = self.diag_rej_other,
            sig = self.diag_leg1_signals,
            fill = self.diag_leg1_fills,
            timeout = self.diag_leg1_timeouts,
            erosion = self.diag_leg2_erosion_steps,
            l2fill = self.diag_leg2_fills,
            emg_adv = self.diag_emg_adverse,
            emg_be = self.diag_emg_breakeven,
            emg_exp = self.diag_emg_expiry,
            emg_fav = self.diag_emg_favorable,
        );

        Some(msg)
    }

    // ─── Accessors and Executor callbacks ────────────────────────────────

    pub fn state(&self) -> &MarketState {
        &self.state
    }

    /// Check if Leg 1 has been posted too long without filling.
    /// Returns a `CancelLeg1` command if timed out, or `None`.
    pub fn check_leg1_staleness(&mut self) -> Option<ExecutorCommand> {
        let now_ms = now_epoch_ms();
        if let OrderState::Posted {
            order_id,
            timestamp_ms,
            ..
        } = &self.state.leg1_state
        {
            if now_ms.saturating_sub(*timestamp_ms) > self.leg1.leg1_timeout_ms {
                let cmd = ExecutorCommand::CancelLeg1 {
                    order_id: order_id.clone(),
                };
                info!(
                    elapsed_ms = now_ms.saturating_sub(*timestamp_ms),
                    timeout_ms = self.leg1.leg1_timeout_ms,
                    "Leg 1 stale — cancelling unfilled order"
                );
                self.state.leg1_state = OrderState::None;
                self.pending_leg1_signal = None;
                self.leg1_direction = None;
                self.pending_spike_cancel = None;
                self.speculative_awaiting_sustain = false;
                self.diag_leg1_timeouts += 1;
                return Some(cmd);
            }
        }
        None
    }

    /// Called by the Executor when an order is successfully posted to the CLOB.
    pub fn on_order_posted(
        &mut self,
        is_leg2: bool,
        order_id: String,
        price: Decimal,
        size: Decimal,
    ) {
        let now_ms = now_epoch_ms();
        let leg = if is_leg2 {
            &mut self.state.leg2_state
        } else {
            &mut self.state.leg1_state
        };
        *leg = OrderState::Posted {
            order_id,
            price,
            size,
            timestamp_ms: now_ms,
        };
    }

    /// Called by the live executor (via feedback channel) when order placement fails.
    /// Resets the affected leg state to `None` so the engine can re-evaluate.
    pub fn on_order_failed(&mut self, is_leg2: bool) {
        if is_leg2 {
            self.state.leg2_state = OrderState::None;
        } else {
            self.state.leg1_state = OrderState::None;
        }
        warn!(is_leg2, "order placement failed — leg state reset to None");
    }

    /// Record a completed live trade to QuestDB's `executed_trades` table.
    ///
    /// Call this when both legs are `Filled` — right before `on_trade_complete()`
    /// resets state. All required fields are extracted from engine state:
    /// `leg1_state`, `leg2_state`, `erosion`, `pending_leg1_signal`, `leg1_direction`.
    pub fn record_live_trade(
        &self,
        cold: &mut crate::storage::cold::ColdStorage,
    ) -> anyhow::Result<()> {
        let (l1_order_id, l1_price, l1_size, l1_fill_ts) = match &self.state.leg1_state {
            OrderState::Filled {
                order_id,
                price,
                size,
                fill_timestamp_ms,
            } => (order_id.as_str(), *price, *size, *fill_timestamp_ms),
            _ => return Ok(()), // not filled — nothing to record
        };
        let (l2_order_id, l2_price, l2_size) = match &self.state.leg2_state {
            OrderState::Filled {
                order_id,
                price,
                size,
                ..
            } => (order_id.as_str(), *price, *size),
            _ => return Ok(()), // not filled — nothing to record
        };

        let direction_str = match self.leg1_direction {
            Some(Direction::Up) => "YES",
            Some(Direction::Down) => "NO",
            None => "YES", // fallback — should not happen if both legs filled
        };

        let pair_cost = l1_price + l2_price;
        let gross_profit = (Decimal::ONE - pair_cost) * l1_size;

        // Extract erosion metadata (if available).
        let (confidence, profit_tier, erosion_steps, exit_reason, alloc_amount, bot_contested) =
            match (&self.erosion, &self.pending_leg1_signal) {
                (Some(ero), Some(sig)) => (
                    ero.confidence,
                    ero.tier.label(),
                    ero.steps_applied,
                    ero.exit_reason,
                    sig.alloc_amount,
                    sig.bot_contested,
                ),
                (Some(ero), None) => (
                    ero.confidence,
                    ero.tier.label(),
                    ero.steps_applied,
                    ero.exit_reason,
                    Decimal::ZERO,
                    false,
                ),
                _ => (Decimal::ZERO, "LOW", 0, None, Decimal::ZERO, false),
            };

        let leg2_was_taker = exit_reason.is_some();
        let adverse_movement = exit_reason == Some(ExitReason::AdverseMovement);

        // Taker fee: CLOB deducts fees automatically; the REST response and User WS
        // do not return the actual amount charged. Recorded as zero in QuestDB.
        let taker_fee = Decimal::ZERO;

        let net_profit = gross_profit - taker_fee;
        let profit_pct = if pair_cost > Decimal::ZERO && l1_size > Decimal::ZERO {
            net_profit / (pair_cost * l1_size) * Decimal::ONE_HUNDRED
        } else {
            Decimal::ZERO
        };

        cold.record_trade(
            self.state.active_condition_id.as_deref().unwrap_or(""),
            direction_str,
            l1_price,
            Some(l2_price),
            l1_size,
            Some(l2_size),
            pair_cost,
            gross_profit,
            taker_fee,
            net_profit,
            profit_pct,
            confidence,
            profit_tier,
            alloc_amount,
            erosion_steps,
            leg2_was_taker,
            adverse_movement,
            bot_contested,
            l1_order_id,
            Some(l2_order_id),
            l1_fill_ts,
        )
    }

    /// Called in live mode when both legs are filled (detected in main engine loop).
    /// Replicates the trade completion logic from `advance_simulation()`.
    pub fn on_trade_complete(&mut self) {
        if let (
            OrderState::Filled {
                price: l1_price, ..
            },
            OrderState::Filled {
                price: l2_price, ..
            },
        ) = (&self.state.leg1_state, &self.state.leg2_state)
        {
            let pair_cost = *l1_price + *l2_price;
            let net_profit = Decimal::ONE - pair_cost;
            info!(
                l1_price = %l1_price, l2_price = %l2_price,
                %pair_cost, %net_profit,
                "live trade pair complete — resetting for next trade"
            );
        }
        self.state.leg1_state = OrderState::None;
        self.state.leg2_state = OrderState::None;
        self.erosion = None;
        self.last_erosion_signal_ms = 0;
        self.leg1_direction = None;
        self.pending_leg1_signal = None;
        self.pending_spike_cancel = None;
        self.speculative_awaiting_sustain = false;
        // cumulative_used is NOT reset — capital stays allocated within this market.
    }

    // ─── Simulation helpers ────────────────────────────────────────────────

    /// Initialize erosion state after a Leg 1 fill.
    ///
    /// Reused by both `TradeStatusUpdate` handler (live mode) and
    /// `advance_simulation()` (simulation mode).
    fn init_erosion(&mut self, fill_price: Decimal, fill_size: Decimal, now_ms: u64) {
        if let Some(spike) = self.state.last_spike {
            let atr = self.state.atr.unwrap_or(Decimal::new(1, 3));
            let t_secs = self.state.time_remaining_ms(now_ms) / 1_000;
            let depth = self
                .state
                .poly_book
                .as_ref()
                .map(|b| b.total_bid_depth() + b.total_ask_depth())
                .unwrap_or(Decimal::ONE);
            let conf = compute_confidence(
                spike.magnitude,
                atr,
                depth,
                self.avg_book_depth.unwrap_or(Decimal::ONE),
                t_secs,
            );
            let tier = ProfitTier::from_confidence(
                conf,
                self.leg1.high_threshold,
                self.leg1.med_threshold,
            );
            let initial_profit_target = self.leg1.target_pct_for_tier(tier);
            self.erosion = Some(ErosionState::new(
                now_ms,
                fill_price,
                fill_size,
                tier,
                initial_profit_target,
                spike.direction,
                spike,
                conf,
                self.state.binance_price,
            ));
            info!(tier = tier.label(), %fill_price, %fill_size, "erosion initialised");
        } else {
            warn!("Leg 1 filled but no spike info — erosion not initialised");
        }
    }

    /// Advance simulation state: simulate Leg 1 and Leg 2 fills based on
    /// current orderbook conditions. Called once per engine loop iteration
    /// in simulation mode only.
    ///
    /// Returns confirmed fill signals (`sim_confirmed_fill = true`) for the
    /// executor to record directly. Usually 0 or 1 items per call.
    pub fn advance_simulation(&mut self) -> Vec<TradeSignal> {
        let now_ms = now_epoch_ms();
        let mut signals: Vec<TradeSignal> = Vec::new();

        // ── Leg 1: Posted → Filled ─────────────────────────────────────
        if let OrderState::Posted {
            price,
            size,
            timestamp_ms,
            ..
        } = &self.state.leg1_state
        {
            let fill_price = *price;
            let fill_size = *size;
            let posted_ts = *timestamp_ms;

            // ── Speculative fill gate: wait for SpikeConfirmed ───────────
            // Don't simulate fills until the spike has been confirmed.
            // This prevents fills during the sustain window (T=0 to T=~300ms).
            if self.speculative_awaiting_sustain {
                return signals;
            }

            // ── Leg 1 staleness: cancel if resting too long ──────────────
            if now_ms.saturating_sub(posted_ts) > self.leg1.leg1_timeout_ms {
                info!(
                    elapsed_ms = now_ms.saturating_sub(posted_ts),
                    timeout_ms = self.leg1.leg1_timeout_ms,
                    "Leg 1 stale — cancelling unfilled order"
                );
                self.state.leg1_state = OrderState::None;
                self.pending_leg1_signal = None;
                self.leg1_direction = None;
                self.diag_leg1_timeouts += 1;
                return signals;
            }

            let tick = self.state.tick_size;
            let two_ticks = tick * Decimal::TWO;

            // Leg 1 fill check: post-only valid + depth exists. No timing gate —
            // we simulate instant fill (execution speed is the priority).
            let should_fill = match self.leg1_direction {
                Some(Direction::Up) => {
                    let book_opt = self
                        .state
                        .poly_yes_book
                        .as_ref()
                        .or(self.state.poly_book.as_ref());
                    match book_opt {
                        Some(book) => {
                            let ask = book.best_ask().map(|a| a.price);
                            let post_only_valid = ask.is_some_and(|a| fill_price < a);
                            let near_ask_depth: Decimal = book
                                .asks
                                .iter()
                                .filter(|lvl| lvl.price <= fill_price + two_ticks)
                                .map(|lvl| lvl.size)
                                .sum();
                            post_only_valid && near_ask_depth > Decimal::ZERO
                        }
                        None => false,
                    }
                }
                Some(Direction::Down) => match self.state.poly_no_book.as_ref() {
                    Some(book) => {
                        let ask = book.best_ask().map(|a| a.price);
                        let post_only_valid = ask.is_some_and(|a| fill_price < a);
                        let near_ask_depth: Decimal = book
                            .asks
                            .iter()
                            .filter(|lvl| lvl.price <= fill_price + two_ticks)
                            .map(|lvl| lvl.size)
                            .sum();
                        post_only_valid && near_ask_depth > Decimal::ZERO
                    }
                    None => false,
                },
                None => false,
            };

            if should_fill {
                self.diag_leg1_fills += 1;
                info!(
                    %fill_price, %fill_size,
                    "advance_simulation: Leg 1 simulated fill (counterparty sold into bid)"
                );
                if let Some(mut sig) = self.pending_leg1_signal.take() {
                    sig.sim_confirmed_fill = true;
                    signals.push(sig);
                }
                self.state.leg1_state = OrderState::Filled {
                    order_id: format!("sim-leg1-{}", posted_ts),
                    price: fill_price,
                    size: fill_size,
                    fill_timestamp_ms: now_ms,
                };
                self.init_erosion(fill_price, fill_size, now_ms);
            }
        }

        // ── Leg 2: Posted → Filled ─────────────────────────────────────
        if let OrderState::Posted { price, size, .. } = &self.state.leg2_state {
            let posted_price = *price;
            let posted_size = *size;

            let is_emergency = self.erosion.as_ref().is_some_and(|e| e.emergency_submitted);

            // Determine fill outcome: (should_fill, is_favorable_taker, fill_price, sim_was_taker).
            // Emergency fills use deadline-aware model: wait for market to come to our
            // posted price (maker fill), or FOK taker after emergency_deadline_ms.
            // Normal fills check the opposing book's best ask:
            //   ask < posted_price → favorable taker fill at ask_price
            //   ask == posted_price → normal maker fill at posted_price
            //   ask > posted_price or no ask → no fill (order rests)
            let (should_fill, is_favorable_taker, fill_price, sim_was_taker) = if is_emergency {
                let best_ask = match self.leg1_direction {
                    Some(Direction::Up) => self
                        .state
                        .poly_no_book
                        .as_ref()
                        .or(self.state.poly_book.as_ref())
                        .and_then(|b| b.best_ask())
                        .map(|a| a.price),
                    Some(Direction::Down) => self
                        .state
                        .poly_yes_book
                        .as_ref()
                        .or(self.state.poly_book.as_ref())
                        .and_then(|b| b.best_ask())
                        .map(|a| a.price),
                    None => self
                        .state
                        .poly_book
                        .as_ref()
                        .and_then(|b| b.best_ask())
                        .map(|a| a.price),
                };

                // Market moved to our price → maker fill (our bid gets hit).
                let maker_fillable = best_ask.is_some_and(|ask| ask <= posted_price);

                if maker_fillable {
                    (true, false, posted_price, false)
                } else {
                    // Check hard deadline.
                    let deadline_passed = self.erosion.as_ref().is_some_and(|e| {
                        e.emergency_first_post_ms
                            .map(|first| {
                                now_ms.saturating_sub(first) >= self.leg2.emergency_deadline_ms
                            })
                            .unwrap_or(false)
                    });

                    if deadline_passed {
                        match best_ask {
                            Some(ask) => (true, false, ask, true), // taker FOK
                            None => (false, false, posted_price, false),
                        }
                    } else {
                        (false, false, posted_price, false) // wait
                    }
                }
            } else {
                let best_ask = match self.leg1_direction {
                    Some(Direction::Up) => self
                        .state
                        .poly_no_book
                        .as_ref()
                        .or(self.state.poly_book.as_ref())
                        .and_then(|b| b.best_ask())
                        .map(|a| a.price),
                    Some(Direction::Down) => self
                        .state
                        .poly_yes_book
                        .as_ref()
                        .or(self.state.poly_book.as_ref())
                        .and_then(|b| b.best_ask())
                        .map(|a| a.price),
                    None => self
                        .state
                        .poly_book
                        .as_ref()
                        .and_then(|b| b.best_ask())
                        .map(|a| a.price),
                };
                match best_ask {
                    Some(ask) if ask < posted_price => (true, true, ask, false),
                    Some(ask) if ask <= posted_price => (true, false, posted_price, false),
                    _ => (false, false, posted_price, false),
                }
            };

            if should_fill {
                self.diag_leg2_fills += 1;
                info!(
                    %posted_price, %posted_size, %fill_price, is_emergency, is_favorable_taker, sim_was_taker,
                    "advance_simulation: Leg 2 simulated fill"
                );
                // Build signal BEFORE setting Filled state — erosion is still valid.
                if let Some(mut sig) =
                    self.build_sim_leg2_fill_signal(fill_price, posted_size, now_ms)
                {
                    if is_favorable_taker {
                        sig.exit_reason = Some(ExitReason::FavorableTaker);
                    }
                    sig.sim_was_taker = sim_was_taker;
                    signals.push(sig);
                }
                self.state.leg2_state = OrderState::Filled {
                    order_id: format!("sim-leg2-{}", now_ms),
                    price: fill_price,
                    size: posted_size,
                    fill_timestamp_ms: now_ms,
                };
            }
        }

        // ── Trade completion: both legs Filled → reset for next trade ───
        let leg1_filled = matches!(self.state.leg1_state, OrderState::Filled { .. });
        let leg2_filled = matches!(self.state.leg2_state, OrderState::Filled { .. });

        if leg1_filled && leg2_filled {
            if let (
                OrderState::Filled {
                    price: l1_price, ..
                },
                OrderState::Filled {
                    price: l2_price, ..
                },
            ) = (&self.state.leg1_state, &self.state.leg2_state)
            {
                let pair_cost = *l1_price + *l2_price;
                let net_profit = Decimal::ONE - pair_cost;
                info!(
                    l1_price = %l1_price, l2_price = %l2_price,
                    %pair_cost, %net_profit,
                    "advance_simulation: trade pair complete — resetting for next trade"
                );
            }

            self.state.leg1_state = OrderState::None;
            self.state.leg2_state = OrderState::None;
            self.erosion = None;
            self.last_erosion_signal_ms = 0;
            self.leg1_direction = None;
            // cumulative_used is NOT reset — capital stays allocated within this market.
        }

        signals
    }

    /// Build a Leg 2 fill [`TradeSignal`] for routing to the `SimulationExecutor`.
    ///
    /// Uses the current erosion state and hedge book to construct a confirmed fill
    /// signal (`sim_confirmed_fill = true`). Reads `exit_reason` from erosion state
    /// (set on emergency) so the executor can categorize the exit correctly.
    /// Returns `None` if erosion state or hedge token ID is unavailable.
    fn build_sim_leg2_fill_signal(
        &self,
        price: Decimal,
        size: Decimal,
        now_ms: u64,
    ) -> Option<TradeSignal> {
        let erosion = self.erosion.as_ref()?;
        let exit_reason = if erosion.emergency_submitted {
            erosion.exit_reason
        } else {
            None
        };
        let (hedge_token_id, hedge_book) = match self.leg1_direction? {
            Direction::Up => (
                self.state.active_no_token_id.as_deref()?.to_string(),
                self.state
                    .poly_no_book
                    .clone()
                    .or_else(|| self.state.poly_book.clone()),
            ),
            Direction::Down => (
                self.state.active_yes_token_id.as_deref()?.to_string(),
                self.state
                    .poly_yes_book
                    .clone()
                    .or_else(|| self.state.poly_book.clone()),
            ),
        };
        let mut signal = make_leg2_signal(
            &hedge_token_id,
            self.state.active_condition_id.as_deref().unwrap_or(""),
            price,
            size,
            self.state.binance_price.unwrap_or(Decimal::ZERO),
            erosion.confidence,
            erosion.tier,
            erosion.initial_profit_target,
            erosion.direction,
            erosion.spike_info,
            erosion.leg1_fill_price,
            now_ms,
            self.state.market_end_timestamp_ms,
            self.state.tick_size,
            self.state.atr.unwrap_or(Decimal::ZERO),
            false,
            hedge_book,
            exit_reason,
        );
        signal.sim_confirmed_fill = true;
        Some(signal)
    }

    // ─── Drain & Status ───────────────────────────────────────────────────

    /// Enter drain mode: block new Leg 1 entries, let Leg 2 continue.
    pub fn set_draining(&mut self) {
        self.draining = true;
    }

    /// Returns `true` if no position is open (safe to exit immediately).
    pub fn has_no_open_position(&self) -> bool {
        matches!(self.state.leg1_state, OrderState::None)
            && matches!(self.state.leg2_state, OrderState::None)
    }

    /// Build a status snapshot for the `/status` command.
    pub fn build_status(&self, mode_str: &str) -> crate::control::types::BotStatus {
        use crate::control::types::BotStatus;

        let now_ms = now_epoch_ms();
        let uptime_secs = now_ms.saturating_sub(self.start_ms) / 1_000;

        let leg1_str = match &self.state.leg1_state {
            OrderState::None => "None".into(),
            OrderState::Posted { price, size, .. } => format!("Posted @ ${price} x {size}"),
            OrderState::Filled { price, size, .. } => format!("Filled @ ${price} x {size}"),
        };
        let leg2_str = match &self.state.leg2_state {
            OrderState::None => "None".into(),
            OrderState::Posted { price, size, .. } => format!("Posted @ ${price} x {size}"),
            OrderState::Filled { price, size, .. } => format!("Filled @ ${price} x {size}"),
        };

        BotStatus {
            uptime_secs,
            mode: mode_str.to_string(),
            current_market: self.state.active_condition_id.clone(),
            market_end_ms: self.state.market_end_timestamp_ms,
            leg1_state: leg1_str,
            leg2_state: leg2_str,
            spikes_received: self.diag_spikes_received,
            signals_emitted: self.diag_leg1_signals,
            trades_completed: self.diag_leg2_fills,
            trades_enabled: true,  // updated by main loop from NotifyFlags
            summary_enabled: true, // updated by main loop from NotifyFlags
            draining: self.draining,
        }
    }
}

#[cfg(test)]
impl Default for StrategyEngine {
    fn default() -> Self {
        Self::new(&Config::test_defaults())
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::erosion::MAX_EROSION_STEPS;
    use crate::types::market::{BinanceTick, OrderBook, PriceLevel, SpikeInfo};

    const TEST_FIXED_ALLOC: Decimal = Decimal::from_parts(100, 0, 0, false, 0);
    fn make_engine_with_market(secs_remaining: u64) -> StrategyEngine {
        let mut engine = StrategyEngine::new(&Config::test_defaults());
        engine.on_event(IngestorEvent::MarketRotation {
            condition_id: "cond".to_string(),
            yes_token_id: "yes".to_string(),
            no_token_id: "no".to_string(),
            end_timestamp_ms: now_epoch_ms() + secs_remaining * 1_000,
        });
        engine.state.available_capital = TEST_FIXED_ALLOC;
        engine
    }

    fn set_book(engine: &mut StrategyEngine, bid: &str, ask: &str) {
        engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
            asset_id: "yes".to_string(),
            bids: vec![PriceLevel {
                price: bid.parse().unwrap(),
                size: Decimal::new(500, 0),
            }],
            asks: vec![PriceLevel {
                price: ask.parse().unwrap(),
                size: Decimal::new(500, 0),
            }],
            timestamp_ms: now_epoch_ms(),
        }));
    }

    fn inject_spike(engine: &mut StrategyEngine, direction: Direction) {
        engine.state.spike_detected = true;
        engine.state.last_spike = Some(SpikeInfo {
            direction,
            magnitude: Decimal::new(5, 3),
            sustained_ms: 300,
            timestamp_ms: now_epoch_ms() - 200,
        });
        engine.state.atr = Some(Decimal::new(2, 3));
        engine.state.binance_price = Some(Decimal::new(50_000, 0));
    }

    // ── on_event: MarketRotation ──────────────────────────────────────────

    #[test]
    fn test_market_rotation_resets_state() {
        let mut engine = make_engine_with_market(600);
        engine.state.cumulative_used = Decimal::new(50, 0);
        engine.state.spike_detected = true;
        engine.state.leg1_state = OrderState::Posted {
            order_id: "old".to_string(),
            price: Decimal::new(48, 2),
            size: Decimal::new(100, 0),
            timestamp_ms: 0,
        };
        engine.on_event(IngestorEvent::MarketRotation {
            condition_id: "new_cond".to_string(),
            yes_token_id: "new_yes".to_string(),
            no_token_id: "new_no".to_string(),
            end_timestamp_ms: now_epoch_ms() + 900_000,
        });
        assert_eq!(engine.state.cumulative_used, Decimal::ZERO);
        assert!(!engine.state.spike_detected);
        assert!(matches!(engine.state.leg1_state, OrderState::None));
        assert_eq!(
            engine.state.active_condition_id.as_deref(),
            Some("new_cond")
        );
    }

    // ── on_event: TickSizeChange ──────────────────────────────────────────

    #[test]
    fn test_tick_size_change_updates() {
        let mut engine = StrategyEngine::new(&Config::test_defaults());
        let new_tick = Decimal::new(1, 3);
        engine.on_event(IngestorEvent::PolymarketTickSizeChange {
            asset_id: "tok".to_string(),
            old_tick_size: Decimal::new(1, 2),
            new_tick_size: new_tick,
        });
        assert_eq!(engine.state.tick_size, new_tick);
    }

    // ── on_event: BinanceTick (normal) ────────────────────────────────────

    #[test]
    fn test_binance_tick_updates_price() {
        let mut engine = StrategyEngine::new(&Config::test_defaults());
        engine.on_event(IngestorEvent::BinanceTick(BinanceTick {
            symbol: "BTCUSDT",
            bid_price: Decimal::new(52_000_00, 2),
            bid_qty: Decimal::new(10, 0), // > 1.0 → NOT spike-encoded
            ask_price: Decimal::new(52_001_00, 2),
            ask_qty: Decimal::new(5, 0),
            timestamp_ms: now_epoch_ms(),
        }));
        let expected = (Decimal::new(52_000_00, 2) + Decimal::new(52_001_00, 2)) / Decimal::TWO;
        assert_eq!(engine.state.binance_price, Some(expected));
    }

    // ── evaluate ─────────────────────────────────────────────────────────

    #[test]
    fn test_evaluate_no_signal_without_spike() {
        let mut engine = make_engine_with_market(600);
        set_book(&mut engine, "0.48", "0.52");
        engine.state.binance_price = Some(Decimal::new(50_000, 0));
        assert!(engine.evaluate().is_none());
    }

    #[test]
    fn test_evaluate_aborts_on_wide_spread() {
        let mut engine = make_engine_with_market(600);
        set_book(&mut engine, "0.30", "0.70");
        inject_spike(&mut engine, Direction::Up);
        assert!(engine.evaluate().is_none());
    }

    #[test]
    fn test_evaluate_aborts_near_expiry() {
        let mut engine = make_engine_with_market(60);
        set_book(&mut engine, "0.48", "0.52");
        inject_spike(&mut engine, Direction::Up);
        assert!(engine.evaluate().is_none());
    }

    #[test]
    fn test_evaluate_generates_leg1_signal() {
        let mut engine = make_engine_with_market(600);
        set_book(&mut engine, "0.495", "0.505");
        inject_spike(&mut engine, Direction::Up);
        let s = engine.evaluate().expect("should generate signal");
        assert!(!s.is_leg2);
        assert_eq!(s.side, Side::Buy);
        assert_eq!(s.token_id, "yes"); // UP → YES
        assert_eq!(s.price, Decimal::new(50, 2));
    }

    #[test]
    fn test_evaluate_no_signal_with_active_leg1() {
        let mut engine = make_engine_with_market(600);
        set_book(&mut engine, "0.48", "0.52");
        inject_spike(&mut engine, Direction::Up);
        engine.state.leg1_state = OrderState::Posted {
            order_id: "ord1".to_string(),
            price: Decimal::new(49, 2),
            size: Decimal::new(100, 0),
            timestamp_ms: now_epoch_ms(),
        };
        assert!(engine.evaluate().is_none());
    }

    // ── ProfitTier ────────────────────────────────────────────────────────

    #[test]
    fn test_profit_tier_thresholds() {
        let high = Decimal::new(8, 1);
        let med = Decimal::new(5, 1);
        assert_eq!(
            ProfitTier::from_confidence(Decimal::new(9, 1), high, med),
            ProfitTier::High
        );
        assert_eq!(
            ProfitTier::from_confidence(Decimal::new(8, 1), high, med),
            ProfitTier::High
        );
        assert_eq!(
            ProfitTier::from_confidence(Decimal::new(75, 2), high, med),
            ProfitTier::Med
        );
        assert_eq!(
            ProfitTier::from_confidence(Decimal::new(5, 1), high, med),
            ProfitTier::Med
        );
        assert_eq!(
            ProfitTier::from_confidence(Decimal::new(3, 1), high, med),
            ProfitTier::Low
        );
        assert_eq!(
            ProfitTier::from_confidence(Decimal::ZERO, high, med),
            ProfitTier::Low
        );
    }

    // ── ErosionState ──────────────────────────────────────────────────────

    #[test]
    fn test_erosion_high_tier_5_steps_reach_break_even() {
        let now_ms = now_epoch_ms();
        let spike = SpikeInfo {
            direction: Direction::Up,
            magnitude: Decimal::new(5, 3),
            sustained_ms: 250,
            timestamp_ms: now_ms,
        };
        let leg1_price = Decimal::new(48, 2); // 0.48
        let mut e = ErosionState::new(
            now_ms,
            leg1_price,
            Decimal::new(100, 0),
            ProfitTier::High,
            ProfitTier::High.target_pct(),
            Direction::Up,
            spike,
            Decimal::new(85, 2),
            None,
        );

        let be = e.break_even();
        assert_eq!(be, Decimal::new(52, 2)); // 1.0 - 0.48

        let t0 = e.current_leg2_target(); // 1.0 - 0.025 - 0.48 = 0.495
        assert_eq!(t0, Decimal::new(495, 3));

        e.steps_applied = 1;
        let t1 = e.current_leg2_target(); // 1.0 - 0.020 - 0.48 = 0.500
        assert!(t1 > t0, "erosion raises bid");

        e.steps_applied = 5;
        assert_eq!(e.current_profit_target(), Decimal::ZERO);
        assert_eq!(e.current_leg2_target(), be);
    }

    #[test]
    fn test_erosion_profit_floors_at_zero() {
        let now_ms = now_epoch_ms();
        let spike = SpikeInfo {
            direction: Direction::Up,
            magnitude: Decimal::new(5, 3),
            sustained_ms: 250,
            timestamp_ms: now_ms,
        };
        let mut e = ErosionState::new(
            now_ms,
            Decimal::new(48, 2),
            Decimal::new(100, 0),
            ProfitTier::High,
            ProfitTier::High.target_pct(),
            Direction::Up,
            spike,
            Decimal::ZERO,
            None,
        );
        e.steps_applied = 20;
        assert_eq!(e.current_profit_target(), Decimal::ZERO);
    }

    // ── TradeStatusUpdate fills Leg 1 ─────────────────────────────────────

    #[test]
    fn test_leg1_fill_initialises_erosion() {
        let mut engine = make_engine_with_market(600);
        let now_ms = now_epoch_ms();
        engine.state.leg1_state = OrderState::Posted {
            order_id: "ord1".to_string(),
            price: Decimal::new(48, 2),
            size: Decimal::new(100, 0),
            timestamp_ms: now_ms - 500,
        };
        engine.state.last_spike = Some(SpikeInfo {
            direction: Direction::Up,
            magnitude: Decimal::new(5, 3),
            sustained_ms: 250,
            timestamp_ms: now_ms - 300,
        });
        engine.state.atr = Some(Decimal::new(2, 3));

        engine.on_event(IngestorEvent::TradeStatusUpdate {
            order_id: "ord1".to_string(),
            status: TradeStatus::Matched,
        });

        assert!(matches!(engine.state.leg1_state, OrderState::Filled { .. }));
        assert!(engine.erosion.is_some());
    }

    // ── HeartbeatStatus ───────────────────────────────────────────────────

    #[test]
    fn test_heartbeat_failure_tracking() {
        let mut engine = StrategyEngine::new(&Config::test_defaults());
        engine.on_event(IngestorEvent::HeartbeatStatus {
            success: false,
            latency_ms: 0,
        });
        engine.on_event(IngestorEvent::HeartbeatStatus {
            success: false,
            latency_ms: 0,
        });
        assert_eq!(engine.connectivity.consecutive_heartbeat_failures, 2);
        engine.on_event(IngestorEvent::HeartbeatStatus {
            success: true,
            latency_ms: 3,
        });
        assert_eq!(engine.connectivity.consecutive_heartbeat_failures, 0);
        assert!(engine.connectivity.heartbeat_healthy);
    }

    // ── WsStatus ─────────────────────────────────────────────────────────

    #[test]
    fn test_ws_disconnect_clears_spike_detected() {
        let mut engine = StrategyEngine::new(&Config::test_defaults());
        engine.state.spike_detected = true;
        engine.on_event(IngestorEvent::WsStatus {
            source: DataSource::Binance,
            connected: false,
        });
        assert!(!engine.state.spike_detected);
        assert!(!engine.connectivity.binance_connected);
    }

    // ── Self-gating & advance_simulation ────────────────────────────────

    #[test]
    fn test_evaluate_self_gates_after_signal() {
        let mut engine = make_engine_with_market(600);
        set_book(&mut engine, "0.495", "0.505");
        inject_spike(&mut engine, Direction::Up);

        let s1 = engine.evaluate();
        assert!(s1.is_some(), "first evaluate should produce signal");

        let s2 = engine.evaluate();
        assert!(
            s2.is_none(),
            "second evaluate should be blocked by self-gating"
        );

        assert!(!engine.state.spike_detected, "spike should be cleared");
        assert!(
            matches!(engine.state.leg1_state, OrderState::Posted { .. }),
            "leg1_state should be Posted"
        );
        assert!(
            engine.state.cumulative_used > Decimal::ZERO,
            "allocation should be recorded"
        );
    }

    #[test]
    fn test_evaluate_allows_new_signal_after_reset() {
        let mut engine = make_engine_with_market(600);
        set_book(&mut engine, "0.495", "0.505");
        inject_spike(&mut engine, Direction::Up);

        let s1 = engine.evaluate();
        assert!(s1.is_some());

        // Simulate trade completion reset.
        engine.state.leg1_state = OrderState::None;
        engine.state.leg2_state = OrderState::None;
        engine.erosion = None;
        engine.last_erosion_signal_ms = 0;

        inject_spike(&mut engine, Direction::Up);
        let s2 = engine.evaluate();
        assert!(
            s2.is_some(),
            "new signal should be generated after state reset"
        );
    }

    /// Backdate the Leg 1 Posted timestamp (used by tests that need a non-fresh post).
    fn backdate_leg1(_engine: &mut StrategyEngine) {
        // No-op: fill delay is 0 (instant fills). Kept for test call-site compatibility.
    }

    #[test]
    fn test_advance_simulation_leg1_fill() {
        let mut engine = make_engine_with_market(600);
        set_book(&mut engine, "0.495", "0.505");
        inject_spike(&mut engine, Direction::Up);

        let _signal = engine.evaluate().expect("should generate signal");
        assert!(matches!(engine.state.leg1_state, OrderState::Posted { .. }));

        backdate_leg1(&mut engine);

        engine.advance_simulation();
        assert!(
            matches!(engine.state.leg1_state, OrderState::Filled { .. }),
            "Leg 1 should be Filled when depth exists near bid and delay elapsed"
        );
        assert!(engine.erosion.is_some(), "erosion should be initialized");
    }

    // test_advance_simulation_leg1_no_fill_before_delay removed:
    // fill delay is now 0 (instant fills) — no timing gate to test.

    #[test]
    fn test_advance_simulation_leg1_no_fill_no_depth() {
        let mut engine = make_engine_with_market(600);
        set_book(&mut engine, "0.495", "0.505");
        inject_spike(&mut engine, Direction::Up);

        let _signal = engine.evaluate().expect("should generate signal");
        backdate_leg1(&mut engine);

        engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
            asset_id: "yes".to_string(),
            bids: vec![PriceLevel {
                price: "0.495".parse().unwrap(),
                size: Decimal::new(500, 0),
            }],
            asks: vec![PriceLevel {
                price: "0.60".parse().unwrap(),
                size: Decimal::new(500, 0),
            }],
            timestamp_ms: now_epoch_ms(),
        }));

        engine.advance_simulation();
        assert!(
            matches!(engine.state.leg1_state, OrderState::Posted { .. }),
            "should remain Posted when no depth within 2 ticks"
        );
    }

    #[test]
    fn test_advance_simulation_full_trade_cycle() {
        let mut engine = make_engine_with_market(600);
        set_book(&mut engine, "0.495", "0.505");
        inject_spike(&mut engine, Direction::Up);

        let _s1 = engine.evaluate().expect("Leg 1 signal");
        assert!(matches!(engine.state.leg1_state, OrderState::Posted { .. }));

        backdate_leg1(&mut engine);
        engine.advance_simulation();
        assert!(matches!(engine.state.leg1_state, OrderState::Filled { .. }));
        assert!(engine.erosion.is_some());

        let s2 = engine.evaluate_leg2();
        assert!(s2.is_some(), "Leg 2 erosion signal should be generated");
        assert!(matches!(engine.state.leg2_state, OrderState::Posted { .. }));

        set_book(&mut engine, "0.40", "0.45");
        engine.advance_simulation();

        assert!(
            matches!(engine.state.leg1_state, OrderState::None),
            "leg1 should be reset after trade completion"
        );
        assert!(
            matches!(engine.state.leg2_state, OrderState::None),
            "leg2 should be reset after trade completion"
        );
        assert!(engine.erosion.is_none());
        assert!(
            engine.state.cumulative_used > Decimal::ZERO,
            "cumulative_used should persist"
        );
    }

    #[test]
    fn test_advance_simulation_noop_when_idle() {
        let mut engine = make_engine_with_market(600);
        engine.advance_simulation();
        assert!(matches!(engine.state.leg1_state, OrderState::None));
        assert!(matches!(engine.state.leg2_state, OrderState::None));
    }

    #[test]
    fn test_multiple_trades_per_market() {
        let mut engine = make_engine_with_market(600);

        set_book(&mut engine, "0.495", "0.505");
        inject_spike(&mut engine, Direction::Up);
        let _s1 = engine.evaluate().expect("Trade 1 Leg 1");

        backdate_leg1(&mut engine);
        engine.advance_simulation();
        assert!(matches!(engine.state.leg1_state, OrderState::Filled { .. }));

        let _s2 = engine.evaluate_leg2().expect("Trade 1 Leg 2");
        set_book(&mut engine, "0.40", "0.45");
        engine.advance_simulation();

        assert!(matches!(engine.state.leg1_state, OrderState::None));
        let used_after_trade1 = engine.state.cumulative_used;
        assert!(used_after_trade1 > Decimal::ZERO);

        set_book(&mut engine, "0.495", "0.505");
        inject_spike(&mut engine, Direction::Up);
        let s3 = engine.evaluate();
        if engine.state.remaining_alloc(TEST_FIXED_ALLOC) >= TEST_FIXED_ALLOC * Decimal::new(10, 2)
        {
            assert!(s3.is_some(), "Trade 2 should generate if capital remains");
            assert!(engine.state.cumulative_used > used_after_trade1);
        }
    }

    #[test]
    fn test_init_erosion_helper() {
        let mut engine = make_engine_with_market(600);
        set_book(&mut engine, "0.495", "0.505");
        inject_spike(&mut engine, Direction::Up);
        engine.state.atr = Some(Decimal::new(2, 3));

        let now_ms = now_epoch_ms();
        engine.init_erosion(Decimal::new(50, 2), Decimal::new(100, 0), now_ms);

        let erosion = engine
            .erosion
            .as_ref()
            .expect("erosion should be initialized");
        assert_eq!(erosion.leg1_fill_price, Decimal::new(50, 2));
        assert_eq!(erosion.leg1_fill_size, Decimal::new(100, 0));
        assert_eq!(erosion.leg1_fill_ms, now_ms);
        assert!(erosion.confidence > Decimal::ZERO);
    }

    // ── Rotation emergency protection ──────────────────────────────────

    /// Helper: set up an engine with a Leg 1 filled position + erosion state.
    /// Returns the engine in a state where Leg 1 is Filled and erosion is initialized.
    fn engine_with_filled_leg1() -> StrategyEngine {
        let mut engine = make_engine_with_market(600);
        set_book(&mut engine, "0.495", "0.505");
        inject_spike(&mut engine, Direction::Up);

        let _signal = engine.evaluate().expect("should generate Leg 1 signal");
        assert!(matches!(engine.state.leg1_state, OrderState::Posted { .. }));

        // Simulate Leg 1 fill via advance_simulation.
        engine.advance_simulation();
        assert!(
            matches!(engine.state.leg1_state, OrderState::Filled { .. }),
            "Leg 1 should be filled"
        );
        assert!(engine.erosion.is_some(), "erosion should be initialized");
        engine
    }

    #[test]
    fn test_rotation_with_filled_leg1_generates_emergency() {
        let mut engine = engine_with_filled_leg1();

        // Set up the NO book (opposing side for an Up spike) so the emergency
        // signal can find an opposing ask price.
        engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
            asset_id: "no".to_string(),
            bids: vec![PriceLevel {
                price: Decimal::new(45, 2),
                size: Decimal::new(100, 0),
            }],
            asks: vec![PriceLevel {
                price: Decimal::new(50, 2),
                size: Decimal::new(100, 0),
            }],
            timestamp_ms: now_epoch_ms(),
        }));

        // Rotate to a new market.
        engine.on_event(IngestorEvent::MarketRotation {
            condition_id: "new_cond".to_string(),
            yes_token_id: "new_yes".to_string(),
            no_token_id: "new_no".to_string(),
            end_timestamp_ms: now_epoch_ms() + 900_000,
        });

        let emergencies = engine.take_rotation_emergencies();
        assert_eq!(
            emergencies.len(),
            1,
            "should generate exactly 1 emergency signal"
        );

        let sig = &emergencies[0];
        assert!(sig.is_leg2, "emergency should be a Leg 2 signal");
        assert_eq!(
            sig.exit_reason,
            Some(ExitReason::MarketExpiry),
            "exit reason should be MarketExpiry"
        );
        assert!(
            sig.sim_confirmed_fill,
            "should be a confirmed fill for sim mode"
        );
        // Signal should reference the OLD market's token, not the new one.
        assert_eq!(sig.token_id, "no", "should use old market's NO token");
        assert_eq!(
            sig.condition_id, "cond",
            "should use old market's condition ID"
        );
    }

    #[test]
    fn test_rotation_without_position_no_emergency() {
        let mut engine = make_engine_with_market(600);

        engine.on_event(IngestorEvent::MarketRotation {
            condition_id: "new_cond".to_string(),
            yes_token_id: "new_yes".to_string(),
            no_token_id: "new_no".to_string(),
            end_timestamp_ms: now_epoch_ms() + 900_000,
        });

        let emergencies = engine.take_rotation_emergencies();
        assert!(
            emergencies.is_empty(),
            "no emergency when no position is open"
        );
    }

    #[test]
    fn test_rotation_both_legs_filled_no_emergency() {
        let mut engine = engine_with_filled_leg1();

        // Set up NO book and generate a Leg 2 erosion signal.
        engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
            asset_id: "no".to_string(),
            bids: vec![PriceLevel {
                price: Decimal::new(45, 2),
                size: Decimal::new(100, 0),
            }],
            asks: vec![PriceLevel {
                price: Decimal::new(50, 2),
                size: Decimal::new(100, 0),
            }],
            timestamp_ms: now_epoch_ms(),
        }));
        let _leg2 = engine.evaluate_leg2();

        // Force-fill Leg 2 via advance_simulation (set ask <= posted price).
        engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
            asset_id: "no".to_string(),
            bids: vec![PriceLevel {
                price: Decimal::new(40, 2),
                size: Decimal::new(100, 0),
            }],
            asks: vec![PriceLevel {
                price: Decimal::new(42, 2),
                size: Decimal::new(100, 0),
            }],
            timestamp_ms: now_epoch_ms(),
        }));
        engine.advance_simulation();

        // Both legs should now be filled (or reset after trade completion).
        // Either way, no emergency should be generated.
        engine.on_event(IngestorEvent::MarketRotation {
            condition_id: "new_cond".to_string(),
            yes_token_id: "new_yes".to_string(),
            no_token_id: "new_no".to_string(),
            end_timestamp_ms: now_epoch_ms() + 900_000,
        });

        let emergencies = engine.take_rotation_emergencies();
        assert!(
            emergencies.is_empty(),
            "no emergency when both legs are filled/completed"
        );
    }

    #[test]
    fn test_rotation_posted_leg1_no_emergency() {
        let mut engine = make_engine_with_market(600);
        set_book(&mut engine, "0.495", "0.505");
        inject_spike(&mut engine, Direction::Up);

        let _signal = engine.evaluate().expect("should generate signal");
        assert!(matches!(engine.state.leg1_state, OrderState::Posted { .. }));

        // Leg 1 is Posted but not Filled — no position to protect.
        engine.on_event(IngestorEvent::MarketRotation {
            condition_id: "new_cond".to_string(),
            yes_token_id: "new_yes".to_string(),
            no_token_id: "new_no".to_string(),
            end_timestamp_ms: now_epoch_ms() + 900_000,
        });

        let emergencies = engine.take_rotation_emergencies();
        assert!(
            emergencies.is_empty(),
            "no emergency when Leg 1 is only Posted (not Filled)"
        );
    }

    // ── Emergency post-only vs FOK taker in advance_simulation ──────────

    /// Helper: set up an engine with Leg 1 filled, erosion initialized, Leg 2 posted,
    /// and emergency_submitted=true. Returns the engine ready for advance_simulation tests.
    fn engine_with_emergency_leg2() -> StrategyEngine {
        let mut engine = engine_with_filled_leg1();

        // Set up the NO book (hedge for Up direction) so evaluate_leg2 can emit.
        engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
            asset_id: "no".to_string(),
            bids: vec![PriceLevel {
                price: Decimal::new(45, 2),
                size: Decimal::new(200, 0),
            }],
            asks: vec![PriceLevel {
                price: Decimal::new(50, 2),
                size: Decimal::new(200, 0),
            }],
            timestamp_ms: now_epoch_ms(),
        }));

        // Generate a Leg 2 erosion signal to set leg2_state = Posted.
        let _leg2 = engine.evaluate_leg2();
        assert!(
            matches!(engine.state.leg2_state, OrderState::Posted { .. }),
            "Leg 2 should be Posted after evaluate_leg2"
        );

        // Simulate emergency: set emergency_submitted = true on the erosion state.
        if let Some(e) = engine.erosion.as_mut() {
            e.emergency_submitted = true;
            e.exit_reason = Some(ExitReason::AdverseMovement);
        }

        engine
    }

    #[test]
    fn test_sim_emergency_maker_when_ask_drops_to_posted() {
        let mut engine = engine_with_emergency_leg2();

        // Set emergency_first_post_ms so deadline hasn't passed yet.
        if let Some(e) = engine.erosion.as_mut() {
            e.emergency_first_post_ms = Some(now_epoch_ms());
        }

        // Get the posted Leg 2 price.
        let posted_price = match &engine.state.leg2_state {
            OrderState::Posted { price, .. } => *price,
            _ => panic!("expected Posted"),
        };

        // Set NO book ask AT posted_price → market moved to our bid → maker fill.
        engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
            asset_id: "no".to_string(),
            bids: vec![PriceLevel {
                price: Decimal::new(40, 2),
                size: Decimal::new(200, 0),
            }],
            asks: vec![PriceLevel {
                price: posted_price,
                size: Decimal::new(200, 0),
            }],
            timestamp_ms: now_epoch_ms(),
        }));

        let signals = engine.advance_simulation();
        let leg2_fills: Vec<_> = signals
            .iter()
            .filter(|s| s.is_leg2 && s.sim_confirmed_fill)
            .collect();
        assert_eq!(
            leg2_fills.len(),
            1,
            "should produce exactly 1 confirmed Leg 2 fill"
        );
        assert!(
            !leg2_fills[0].sim_was_taker,
            "when ask <= posted_price, fill should be maker (sim_was_taker=false)"
        );
        assert_eq!(
            leg2_fills[0].price, posted_price,
            "maker fill should be at posted_price"
        );
    }

    #[test]
    fn test_sim_emergency_waits_within_deadline() {
        let mut engine = engine_with_emergency_leg2();

        // Set emergency_first_post_ms to now — deadline not yet reached.
        if let Some(e) = engine.erosion.as_mut() {
            e.emergency_first_post_ms = Some(now_epoch_ms());
        }

        // Get the posted Leg 2 price.
        let posted_price = match &engine.state.leg2_state {
            OrderState::Posted { price, .. } => *price,
            _ => panic!("expected Posted"),
        };
        let tick = engine.state.tick_size;

        // Set NO book ask ABOVE posted_price → market hasn't reached our bid.
        let ask_above = posted_price + tick * Decimal::TWO;
        engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
            asset_id: "no".to_string(),
            bids: vec![PriceLevel {
                price: Decimal::new(40, 2),
                size: Decimal::new(200, 0),
            }],
            asks: vec![PriceLevel {
                price: ask_above,
                size: Decimal::new(200, 0),
            }],
            timestamp_ms: now_epoch_ms(),
        }));

        let signals = engine.advance_simulation();
        let leg2_fills: Vec<_> = signals
            .iter()
            .filter(|s| s.is_leg2 && s.sim_confirmed_fill)
            .collect();
        assert_eq!(
            leg2_fills.len(),
            0,
            "should NOT fill when ask > posted_price and deadline not reached"
        );
    }

    #[test]
    fn test_sim_emergency_taker_at_deadline() {
        let mut engine = engine_with_emergency_leg2();

        // Set emergency_first_post_ms far in the past → deadline expired.
        if let Some(e) = engine.erosion.as_mut() {
            e.emergency_first_post_ms = Some(0); // epoch 0 — well past any deadline
        }

        // Get the posted Leg 2 price.
        let posted_price = match &engine.state.leg2_state {
            OrderState::Posted { price, .. } => *price,
            _ => panic!("expected Posted"),
        };
        let tick = engine.state.tick_size;

        // Set NO book ask ABOVE posted_price — market hasn't reached our bid,
        // but deadline has passed → FOK taker at best_ask.
        let ask_above = posted_price + tick * Decimal::TWO;
        engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
            asset_id: "no".to_string(),
            bids: vec![PriceLevel {
                price: Decimal::new(40, 2),
                size: Decimal::new(200, 0),
            }],
            asks: vec![PriceLevel {
                price: ask_above,
                size: Decimal::new(200, 0),
            }],
            timestamp_ms: now_epoch_ms(),
        }));

        let signals = engine.advance_simulation();
        let leg2_fills: Vec<_> = signals
            .iter()
            .filter(|s| s.is_leg2 && s.sim_confirmed_fill)
            .collect();
        assert_eq!(
            leg2_fills.len(),
            1,
            "should produce FOK taker fill after deadline"
        );
        assert!(
            leg2_fills[0].sim_was_taker,
            "deadline-expired fill should be taker (sim_was_taker=true)"
        );
        assert_eq!(
            leg2_fills[0].price, ask_above,
            "taker fill should be at the ask price"
        );
    }

    // ── Speculative Leg 1: SpikeFailed cancels Posted order ──────────

    #[test]
    fn test_spike_failed_cancels_posted_leg1() {
        let mut engine = make_engine_with_market(600);
        set_book(&mut engine, "0.495", "0.505");
        inject_spike(&mut engine, Direction::Up);

        // evaluate() should generate a Leg 1 signal.
        let signal = engine.evaluate().expect("should generate Leg 1 signal");
        assert!(matches!(engine.state.leg1_state, OrderState::Posted { .. }));
        assert!(signal.token_id == "yes");

        // Simulate speculative_awaiting_sustain = true (would be set by SpikeCandidate handler).
        engine.speculative_awaiting_sustain = true;

        // SpikeFailed → should cancel the speculative Leg 1 order.
        engine.on_event(IngestorEvent::SpikeFailed {
            timestamp_ms: now_epoch_ms(),
        });

        assert!(
            matches!(engine.state.leg1_state, OrderState::None),
            "Leg 1 should be reset to None after spike failed"
        );
        assert!(
            !engine.state.spike_detected,
            "spike_detected should be cleared"
        );
        assert!(
            engine.state.last_spike.is_none(),
            "last_spike should be cleared"
        );
        assert!(
            engine.pending_leg1_signal.is_none(),
            "pending signal should be cleared"
        );
        assert!(
            engine.leg1_direction.is_none(),
            "leg1_direction should be cleared"
        );
        assert!(
            !engine.speculative_awaiting_sustain,
            "speculative gate should be cleared"
        );

        // The cancel command should be pending.
        let cancel = engine.take_spike_cancel();
        assert!(cancel.is_some(), "should have a pending CancelLeg1 command");
        match cancel.unwrap() {
            ExecutorCommand::CancelLeg1 { .. } => {}
            other => panic!("expected CancelLeg1, got {other:?}"),
        }
    }

    #[test]
    fn test_spike_failed_noop_when_filled() {
        let mut engine = engine_with_filled_leg1();

        // Set up NO book for the Leg 2 side.
        engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
            asset_id: "no".to_string(),
            bids: vec![PriceLevel {
                price: Decimal::new(45, 2),
                size: Decimal::new(100, 0),
            }],
            asks: vec![PriceLevel {
                price: Decimal::new(50, 2),
                size: Decimal::new(100, 0),
            }],
            timestamp_ms: now_epoch_ms(),
        }));

        // SpikeFailed after Leg 1 is already filled → should be no-op.
        engine.on_event(IngestorEvent::SpikeFailed {
            timestamp_ms: now_epoch_ms(),
        });

        assert!(
            matches!(engine.state.leg1_state, OrderState::Filled { .. }),
            "Leg 1 should remain Filled"
        );
        assert!(
            engine.take_spike_cancel().is_none(),
            "no cancel command when already filled"
        );
    }

    #[test]
    fn test_speculative_gate_blocks_sim_fills() {
        let mut engine = make_engine_with_market(600);
        set_book(&mut engine, "0.495", "0.505");
        inject_spike(&mut engine, Direction::Up);

        let _signal = engine.evaluate().expect("should generate signal");
        assert!(matches!(engine.state.leg1_state, OrderState::Posted { .. }));

        // Simulate speculative awaiting sustain.
        engine.speculative_awaiting_sustain = true;

        // advance_simulation() should NOT fill while gate is closed.
        engine.advance_simulation();
        assert!(
            matches!(engine.state.leg1_state, OrderState::Posted { .. }),
            "Leg 1 should still be Posted — gate is closed"
        );

        // Open the gate (SpikeConfirmed).
        engine.speculative_awaiting_sustain = false;

        // Now advance_simulation() should fill.
        engine.advance_simulation();
        assert!(
            matches!(engine.state.leg1_state, OrderState::Filled { .. }),
            "Leg 1 should be Filled after gate opens"
        );
    }

    // ── Silent step advancement when skip guard fires ─────────────────

    #[test]
    fn test_erosion_cascade_advances_when_target_rounds_to_same_tick() {
        // Regression test: when per-step erosion increments are smaller than
        // tick size, the skip guard fires (posted price is already optimal).
        // The step counter must still advance so that erosion-exhausted emergency
        // eventually fires. Without the fix, the cascade stalls at step 0 forever.
        //
        // Setup: leg1=0.20 (YES entry), NO ask=0.79, pair_cost=0.99 < $1.00.
        // This avoids break-even breach but keeps ask above all erosion targets.
        let mut engine = make_engine_with_market(600);
        set_book(&mut engine, "0.19", "0.21"); // YES: bid=0.19, ask=0.21
        inject_spike(&mut engine, Direction::Up);

        // Leg 1: evaluate → posted at 0.20 (bid+tick).
        let _s = engine.evaluate().expect("Leg 1 signal");
        assert!(matches!(engine.state.leg1_state, OrderState::Posted { .. }));

        // Sim fill Leg 1.
        engine.advance_simulation();
        assert!(matches!(engine.state.leg1_state, OrderState::Filled { .. }));
        assert!(engine.erosion.is_some());

        // Set up the NO book: ask=0.79 (pair_cost = 0.20 + 0.79 = 0.99 < 1.0).
        engine.on_event(IngestorEvent::PolymarketBook(OrderBook {
            asset_id: "no".to_string(),
            bids: vec![PriceLevel {
                price: Decimal::new(70, 2),
                size: Decimal::new(200, 0),
            }],
            asks: vec![PriceLevel {
                price: Decimal::new(79, 2),
                size: Decimal::new(200, 0),
            }],
            timestamp_ms: now_epoch_ms(),
        }));

        // Step 0: initial post (no erosion applied).
        let initial = engine.evaluate_leg2();
        assert!(
            initial.is_some(),
            "step 0 should produce initial Leg 2 signal"
        );
        assert!(matches!(engine.state.leg2_state, OrderState::Posted { .. }));
        let initial_posted = match &engine.state.leg2_state {
            OrderState::Posted { price, .. } => *price,
            _ => unreachable!(),
        };
        assert_eq!(engine.erosion.as_ref().unwrap().steps_applied, 0);

        // Advance through all 5 erosion steps by backdating the timing gate.
        let base = engine.leg2.erosion_base_interval_ms;
        let decay = engine.leg2.erosion_interval_decay;

        for expected_step in 1..=MAX_EROSION_STEPS {
            let interval = ErosionState::interval_for_step(expected_step - 1, base, decay);
            engine.last_erosion_signal_ms = now_epoch_ms() - interval - 1;

            // evaluate_leg2 may return None (skip guard) or Some (target changed a tick).
            // Either way, the step counter must advance.
            let result = engine.evaluate_leg2();

            let steps = engine.erosion.as_ref().unwrap().steps_applied;
            assert!(
                steps >= expected_step,
                "step should have advanced to at least {expected_step}, got {steps} (signal={:?})",
                result.is_some()
            );

            // When skip guard fires, the posted price stays at the initial optimal price.
            if result.is_none() {
                let still_posted = match &engine.state.leg2_state {
                    OrderState::Posted { price, .. } => *price,
                    _ => panic!("leg2 should still be Posted"),
                };
                assert_eq!(
                    still_posted, initial_posted,
                    "posted price should stay optimal"
                );
            }
        }

        // After all 5 steps, the cascade is exhausted.
        assert_eq!(
            engine.erosion.as_ref().unwrap().steps_applied,
            MAX_EROSION_STEPS,
            "all 5 erosion steps should have advanced"
        );

        // The next evaluate_leg2 should trigger the erosion-exhausted emergency.
        engine.hedge_book_changed = true;
        let emergency = engine.evaluate_leg2();
        assert!(
            emergency.is_some(),
            "erosion exhausted should trigger emergency signal"
        );
        let sig = emergency.unwrap();
        assert_eq!(
            sig.exit_reason,
            Some(ExitReason::BreakEvenBreach),
            "exhaustion emergency should have BreakEvenBreach exit reason"
        );
        assert!(
            engine.erosion.as_ref().unwrap().emergency_submitted,
            "emergency_submitted should be true"
        );
    }

    // ── Drain mode ────────────────────────────────────────────────────

    #[test]
    fn test_draining_blocks_new_leg1() {
        let mut engine = make_engine_with_market(600);
        set_book(&mut engine, "0.495", "0.505");
        inject_spike(&mut engine, Direction::Up);

        // Without draining, evaluate would produce a signal.
        engine.set_draining();
        let signal = engine.evaluate();
        assert!(signal.is_none(), "draining should block new Leg 1 entries");
        assert!(
            !engine.state.spike_detected,
            "spike_detected should be cleared"
        );
    }

    #[test]
    fn test_has_no_open_position_when_idle() {
        let engine = StrategyEngine::default();
        assert!(engine.has_no_open_position());
    }

    #[test]
    fn test_has_open_position_when_leg1_posted() {
        let mut engine = StrategyEngine::default();
        engine.state.leg1_state = OrderState::Posted {
            order_id: "test".into(),
            price: Decimal::new(50, 2),
            size: Decimal::new(10, 0),
            timestamp_ms: 1000,
        };
        assert!(!engine.has_no_open_position());
    }

    #[test]
    fn test_build_status_snapshot() {
        let engine = StrategyEngine::default();
        let status = engine.build_status("simulation");
        assert_eq!(status.mode, "simulation");
        assert_eq!(status.leg1_state, "None");
        assert_eq!(status.leg2_state, "None");
        assert!(!status.draining);
    }

    #[test]
    fn test_on_event_ignores_control_variants() {
        let mut engine = StrategyEngine::default();
        // Should not panic or modify state.
        engine.on_event(IngestorEvent::Shutdown);
        engine.on_event(IngestorEvent::DrainAndRestart);
        assert!(engine.has_no_open_position());
    }
}
