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
//! # Buildup detection
//! The internal BuildupDetector is fed raw Binance events and evaluated after each dirty tick.
//! The engine posts a Leg 1 maker order on confirmed buildups only (no speculative posting).

use rust_decimal::Decimal;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::engine::buildup::detector::{BuildupConfig, BuildupDetector};
use crate::engine::confidence::round_to_tick;
use crate::reporting::telegram::TelegramReporter;
use crate::types::market::{
    BuildupInfo, DataSource, Direction, IngestorEvent, MarketState, OrderBook, OrderState,
    PriceLevel, SpikeInfo, TradeStatus,
};
use crate::types::order::{
    ExecutorCommand, ExitReason, FillInfo, FillMethod, LiveTradeReport, MarketSummary, OrderTag,
    ProfitTier, SessionSummary, Side, TradeSignal,
};
use crate::utils::time::epoch_ms as now_epoch_ms;

use super::confidence::compute_expected_repricing;
use super::erosion::{ConnectivityState, HedgePhase, HedgeSnap, HedgeState};
use super::evaluator::{
    Leg1Evaluator, Leg1Outcome, Leg1RejectReason, Leg2Decision, Leg2Evaluator, make_leg2_signal,
};

// ─── Deferred partial fill tracking ──────────────────────────────────────────

/// Tracks orders with partial `size_matched` at MATCHED time.
/// Resolved when MINED/CONFIRMED arrives with the final cumulative size.
/// Prevents false "PARTIAL FILL" alerts when the CLOB splits a fill across
/// multiple rapid MATCHED events (~3ms apart).
struct PendingPartialFill {
    leg: &'static str,
    size_matched: Decimal,
    original_size: Decimal,
}

// ─── Per-trade Leg 2 metadata for live mode Telegram reporting ───────────────

/// Tracks Leg 2 exit metadata in live mode so `on_trade_complete()` can build
/// the correct tags for the trade-completed Telegram message.
/// Reset to default at the start of each trade (on Leg 1 fill).
#[derive(Debug, Default, Clone)]
struct LiveTradeMeta {
    leg2_was_taker: bool,
    emergency_maker: bool,
    favorable_taker: bool,
    /// `true` if Leg 2 filled via the favorable maker try-first path (no taker fee).
    favorable_maker: bool,
    phase1_breach: bool,
    /// `true` if Leg 2 filled from the Phase 1 order while Phase 2 was also active (dual-order).
    phase1_dual_fill: bool,
    exit_reason: Option<ExitReason>,
}

// ─── Orphan state for dual-order double-fill recovery ───────────────────────

/// State for the "other" order after one of the dual Phase 1/Phase 2 orders fills.
/// Persists across `on_trade_complete()` — the orphan may fill after the trade resets.
/// Cleared on: confirmed cancel, rebalance complete, or market rotation.
#[derive(Debug, Clone)]
struct OrphanState {
    order_id: String,
    size: Decimal,
    leg1_direction: Direction,
    leg1_price: Decimal,
    condition_id: String,
    tick_size: Decimal,
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
/// 3. **Emergency**: FOK taker fills when breach, timeout, or flow reversal is detected
///
/// Signal evaluation logic (guard checks + signal building) lives in
/// [`Leg1Evaluator`] and [`Leg2Evaluator`]. This struct owns shared mutable
/// state and applies post-signal mutations after evaluator calls.
pub struct StrategyEngine {
    state: MarketState,
    hedge: Option<HedgeState>,
    connectivity: ConnectivityState,
    /// EMA of total poly book depth (alpha=0.1) for depth wall detection.
    avg_book_depth: Option<Decimal>,
    /// Direction of the active Leg 1 trade. Set by evaluate(), cleared on completion/rotation.
    leg1_direction: Option<Direction>,

    // ── Cutoff window tracking ────────────────────────────────────────────
    /// `true` once the entry_cutoff window is entered for the current market.
    /// Reset to `false` on `MarketRotation`.
    in_cutoff_window: bool,

    /// `true` during the post-rotation quiet period. Set on `MarketRotation`,
    /// cleared when `rotation_quiet_ms` elapses.
    in_quiet_period: bool,
    /// Epoch ms when the current market rotation occurred.
    rotation_ms: u64,
    /// Config: quiet period duration after rotation.
    rotation_quiet_ms: u64,

    /// `true` during post-trade cooldown. Set on trade completion,
    /// cleared when `trade_cooldown_ms` elapses.
    in_trade_cooldown: bool,
    /// Epoch ms when the last trade completed.
    last_trade_complete_ms: u64,
    /// Config: cooldown duration after trade completion.
    trade_cooldown_ms: u64,

    /// Stored Leg 1 signal for building confirmed fill signals in advance_simulation().
    pending_leg1_signal: Option<TradeSignal>,

    /// Emergency Leg 2 signals generated during MarketRotation when a Leg 1 is
    /// still open. Drained by main loop before sending the rotation command.
    rotation_emergency_buffer: Vec<TradeSignal>,

    /// Cancel window (ms) for unfilled Leg 1 maker.
    cancel_window_ms: u64,
    /// Set when a Leg 1 cancel command is queued; cleared on cancel result.
    /// Gates sustain monitor to prevent retry loops when `was_cancelled=false`.
    leg1_cancel_inflight: bool,

    /// Tick size change command to forward to executor (SDK cache update).
    /// Drained by main loop after on_event().
    pending_tick_size_cmd: Option<ExecutorCommand>,

    /// Pending Leg 1 cancel command (flow-based sustain failure or timeout).
    /// Drained by main loop via `take_leg1_cancel()`.
    pending_leg1_cancel: Option<ExecutorCommand>,

    /// Set `true` when an emergency Leg 2 signal is dispatched to the executor.
    /// Cleared on feedback (OrderPosted, OrderFailed, CancelResult for leg2, trade complete).
    /// Gates `evaluate_leg2()` to prevent signal stacking in the executor channel.
    emergency_signal_in_flight: bool,

    /// Set `true` when ANY Leg 2 command is dispatched to the executor (hedge or emergency).
    /// Cleared on ANY Leg 2 feedback (OrderPosted, OrderFailed, CancelResult for leg2).
    /// Prevents stale hedge commands from queuing while the executor is still processing
    /// a previous Leg 2 command (e.g., favorable exit takes ~3.6s for 3 HTTP calls).
    leg2_command_pending: bool,

    /// Pending cancel command from heartbeat dead proactive reset.
    /// Drained by main loop via `take_heartbeat_cancel()`.
    pending_heartbeat_cancel: Option<ExecutorCommand>,
    /// Consecutive heartbeat failure threshold for proactive state reset.
    heartbeat_dead_threshold: u32,

    /// Set `true` when the engine's 60s terminal log fires. Cleared after the
    /// Telegram diagnostic is sent.
    engine_diag_ready: bool,
    /// Pending Telegram diagnostic message (engine counters).
    /// Drained by main loop via `take_pending_telegram_diag()`.
    pending_telegram_diag: Option<String>,

    // ── Live mode Telegram reporting ───────────────────────────────────────
    /// Telegram reporter for live mode. `None` in simulation mode.
    reporter: Option<TelegramReporter>,
    /// Leg 2 metadata for the current live trade (reset on Leg 1 fill).
    live_trade_meta: LiveTradeMeta,
    /// Completed trades for the current market window (cleared on rotation).
    live_market_trades: Vec<LiveTradeReport>,
    /// Completed trades for the entire session.
    live_session_trades: Vec<LiveTradeReport>,
    /// Leg 1 signals sent for the current market window (cleared on rotation).
    live_market_signals: u32,
    /// Depth walls outbid for the current market window (cleared on rotation).
    live_market_walls: u32,
    /// Epoch ms when set_reporter() was called (used for session uptime).
    live_session_start_ms: u64,

    // ── Diagnostic counters (cumulative from app start, logged every 60s) ──
    diag_markets_rotated: u64,
    diag_buildups_received: u64,
    diag_buildups_dropped_cutoff: u64,
    diag_buildups_dropped_quiet: u64,
    diag_buildups_dropped_cooldown: u64,
    diag_emg_phase1_breach: u64,
    // Leg 1 rejection distribution — only incremented when buildup_detected = true
    diag_rej_busy: u64,    // ActiveTrade: trade already in flight
    diag_rej_no_book: u64,    // NoBook: Polymarket book missing
    diag_rej_no_binance: u64, // NoBinance: no Binance reference price
    diag_rej_stale: u64,   // StaleBook
    diag_rej_skew: u64,    // PriceSkewed
    diag_rej_reprice: u64, // InsufficientRepricing: model output below min_reprice_pct
    diag_rej_paused: u64,  // Paused/Draining: spike dropped while paused or draining
    diag_rej_other: u64,   // Other (no market, bid cap, zero size, etc.)
    diag_leg1_signals: u64,
    diag_leg1_fills: u64,
    diag_leg2_phase1_posts: u64,
    diag_phase_transitions: u64,
    diag_leg2_fills_maker: u64,
    diag_leg2_fills_taker: u64,
    diag_emg_be_breach: u64,
    diag_emg_phase2_timeout: u64,
    diag_emg_phase2_price_breach: u64,
    diag_emg_expiry: u64,
    diag_favorable_exits: u64,
    diag_emg_maker: u64,  // emergency exits filled as post-only maker
    diag_emg_taker: u64,  // emergency exits filled as FOK taker
    diag_favorable_maker_fills: u64,
    diag_favorable_maker_timeouts: u64,
    diag_double_fills: u64,
    diag_rebalance_attempts: u64,
    diag_rebalance_successes: u64,
    diag_phase2_entry_breach: u64,
    diag_buildup_failures: u64,
    diag_order_failures: u64,
    diag_sustain_cancels: u64,    // Leg 1 cancelled: composite < cancel_threshold
    diag_sustain_timeouts: u64,   // Leg 1 cancelled: cancel_window_ms timeout
    diag_opposite_dir_cancels: u64, // Leg 1 cancelled: opposite-direction buildup confirmed
    diag_rej_heartbeat: u64,       // Leg 1 rejected: heartbeat down
    diag_heartbeat_resets: u64,    // Proactive state resets triggered by heartbeat death
    last_diag_ms: u64,

    // ── Dual-order tracking (Phase 1 + Phase 2) ────────────────────────
    /// Phase 1 Leg 2 order ID (persists into Phase 2 when dual-order active).
    leg2_phase1_order_id: Option<String>,
    /// Phase 2 Leg 2 order ID (posted alongside Phase 1 at ask-1tick).
    leg2_phase2_order_id: Option<String>,
    /// Orphan state: survives `on_trade_complete()` — tracks the "other" order
    /// after one of the dual orders fills. Cleared on confirmed cancel, rebalance
    /// complete, or market rotation.
    post_trade_orphan: Option<OrphanState>,
    /// `true` after `take_orphan_cancel()` dispatches a cancel — prevents re-dispatch
    /// on every main-loop iteration while waiting for the CLOB response.
    orphan_cancel_sent: bool,
    /// `true` when a rebalance FOK is in progress (double-fill recovery).
    rebalance_in_progress: bool,
    /// Set `true` when the last `evaluate_leg2()` returned a Phase2Alongside signal.
    /// Main loop checks this to send `PostLeg2Phase2` instead of `Signal`.
    /// Cleared after being consumed.
    last_leg2_was_phase2_alongside: bool,

    // ── Drain / pause mode ─────────────────────────────────────────────
    /// When `true`, `evaluate()` blocks new Leg 1 entries. Set by `/shutdown` or `/set`.
    pub draining: bool,
    /// When `true`, `evaluate()` blocks new Leg 1 entries but the bot stays alive.
    /// Set by `/stop`, cleared by `/resume`. Unlike `draining`, does not exit.
    paused: bool,

    /// Saved previous Leg 2 order info before evaluate_leg2() overwrites
    /// with a provisional ID. Restored if CancelResult reports not confirmed.
    prev_leg2_order: Option<(String, Decimal, Decimal)>,

    /// Buffer for TradeStatusUpdate events that arrived before OrderPosted
    /// feedback. Replayed after on_order_posted() or on_cancel_result().
    pending_fills: std::collections::VecDeque<(String, TradeStatus, Option<Decimal>, Option<Decimal>)>,

    /// Deferred partial fill checks — keyed by order_id.
    /// Inserted on first MATCHED with `size_matched < original_size`,
    /// resolved on subsequent MATCHED (if now fully filled) or MINED/CONFIRMED.
    pending_partial_fills: std::collections::HashMap<String, PendingPartialFill>,

    /// Epoch ms when the engine was created (for uptime calculation).
    start_ms: u64,

    // ── Sub-evaluators ────────────────────────────────────────────────────
    leg1: Leg1Evaluator,
    leg2: Leg2Evaluator,

    // ── BuildupDetector (internal — fed from raw Binance events) ─────────
    detector: BuildupDetector,
}

impl StrategyEngine {
    pub fn new(config: &Config) -> Self {
        let state = MarketState::new();
        let buildup_cfg = BuildupConfig::from_toml(&config.bot.buildup);

        Self {
            state,
            hedge: None,
            connectivity: ConnectivityState::default(),
            avg_book_depth: None,
            leg1_direction: None,
            in_cutoff_window: false,
            in_quiet_period: false,
            rotation_ms: 0,
            rotation_quiet_ms: config.bot.entry_guards.rotation_quiet_ms,
            in_trade_cooldown: false,
            last_trade_complete_ms: 0,
            trade_cooldown_ms: config.bot.entry_guards.trade_cooldown_ms,
            pending_leg1_signal: None,
            rotation_emergency_buffer: Vec::new(),
            cancel_window_ms: config.bot.buildup.cancel_window_ms,
            leg1_cancel_inflight: false,
            pending_tick_size_cmd: None,
            pending_heartbeat_cancel: None,
            heartbeat_dead_threshold: config.bot.entry_guards.heartbeat_dead_threshold,
            emergency_signal_in_flight: false,
            leg2_command_pending: false,
            pending_leg1_cancel: None,
            engine_diag_ready: false,
            pending_telegram_diag: None,
            reporter: None,
            live_trade_meta: LiveTradeMeta::default(),
            live_market_trades: Vec::new(),
            live_session_trades: Vec::new(),
            live_market_signals: 0,
            live_market_walls: 0,
            live_session_start_ms: 0,
            diag_markets_rotated: 0,
            diag_buildups_received: 0,
            diag_buildups_dropped_cutoff: 0,
            diag_buildups_dropped_quiet: 0,
            diag_buildups_dropped_cooldown: 0,
            diag_emg_phase1_breach: 0,
            diag_rej_busy: 0,
            diag_rej_no_book: 0,
            diag_rej_no_binance: 0,
            diag_rej_stale: 0,
            diag_rej_skew: 0,
            diag_rej_reprice: 0,
            diag_rej_paused: 0,
            diag_rej_other: 0,
            diag_leg1_signals: 0,
            diag_leg1_fills: 0,
            diag_leg2_phase1_posts: 0,
            diag_phase_transitions: 0,
            diag_leg2_fills_maker: 0,
            diag_leg2_fills_taker: 0,
            diag_emg_be_breach: 0,
            diag_emg_phase2_timeout: 0,
            diag_emg_phase2_price_breach: 0,
            diag_emg_expiry: 0,
            diag_favorable_exits: 0,
            diag_emg_maker: 0,
            diag_emg_taker: 0,
            diag_favorable_maker_fills: 0,
            diag_favorable_maker_timeouts: 0,
            diag_double_fills: 0,
            diag_rebalance_attempts: 0,
            diag_rebalance_successes: 0,
            diag_phase2_entry_breach: 0,
            diag_buildup_failures: 0,
            diag_order_failures: 0,
            diag_sustain_cancels: 0,
            diag_sustain_timeouts: 0,
            diag_opposite_dir_cancels: 0,
            diag_rej_heartbeat: 0,
            diag_heartbeat_resets: 0,
            last_diag_ms: 0,
            leg2_phase1_order_id: None,
            leg2_phase2_order_id: None,
            post_trade_orphan: None,
            orphan_cancel_sent: false,
            rebalance_in_progress: false,
            last_leg2_was_phase2_alongside: false,
            draining: false,
            paused: false,
            prev_leg2_order: None,
            pending_fills: std::collections::VecDeque::with_capacity(4),
            pending_partial_fills: std::collections::HashMap::new(),
            start_ms: now_epoch_ms(),
            leg1: Leg1Evaluator {
                entry_cutoff_secs: config.bot.entry_guards.entry_cutoff_secs,
                stale_book_ms: config.bot.entry_guards.stale_book_ms,
                max_alloc_per_trade: config.max_alloc_per_trade,
                reprice_scale: config.reprice_scale,
                min_reprice_pct: config.min_reprice_pct,
                min_alloc_pct: config.min_alloc_pct,
                hard_skew_cap: config.hard_skew_cap,
                time_exponent: config.time_exponent,
                max_time_factor: config.max_time_factor,
                phase1_target_dampen: config.phase1_target_dampen,
            },
            leg2: Leg2Evaluator {
                phase1_timeout_ms: config.bot.risk.phase1_timeout_ms,
                phase1_breach_threshold: config.phase1_breach_threshold,
                phase2_timeout_ms: config.bot.risk.phase2_timeout_ms,
                entry_threshold: Decimal::try_from(config.bot.buildup.entry_threshold).unwrap_or(Decimal::new(40, 2)),
                cancel_threshold: Decimal::try_from(config.bot.buildup.cancel_threshold).unwrap_or(Decimal::new(25, 2)),
            },
            detector: BuildupDetector::new(&buildup_cfg),
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
            }

            // ── Tick size change (rare, at price extremes) ────────────────
            IngestorEvent::PolymarketTickSizeChange {
                asset_id,
                old_tick_size,
                new_tick_size,
            } => {
                let matches_active = self.state.active_yes_token_id.as_deref() == Some(&asset_id)
                    || self.state.active_no_token_id.as_deref() == Some(&asset_id);
                if matches_active {
                    info!(%asset_id, %old_tick_size, %new_tick_size, "tick size changed — updating engine + SDK cache");
                    self.state.tick_size = new_tick_size;
                    if let (Some(yes_id), Some(no_id)) = (
                        self.state.active_yes_token_id.clone(),
                        self.state.active_no_token_id.clone(),
                    ) {
                        self.pending_tick_size_cmd = Some(ExecutorCommand::TickSizeChanged {
                            yes_token_id: yes_id,
                            no_token_id: no_id,
                            new_tick_size,
                        });
                    }
                } else {
                    info!(%asset_id, %old_tick_size, %new_tick_size, "tick size changed for non-active asset — ignored");
                }
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
                let mid = tick.mid_price();
                self.state.binance_price = Some(mid);
                self.state.last_update_ms = tick.timestamp_ms;
                // Feed BuildupDetector spot BBA
                let mid_f64 = mid.to_string().parse::<f64>().unwrap_or(0.0);
                self.detector.on_spot_bba(mid_f64, now_ms);
            }

            // ── Binance depth snapshot ────────────────────────────────────
            IngestorEvent::BinanceDepth(depth) => {
                if let Some(mid) = depth.mid_price() {
                    self.state.binance_price = Some(mid);
                    // Feed BuildupDetector with spot depth (OBI + ATR + basis spot side)
                    let mid_f64 = mid.to_string().parse::<f64>().unwrap_or(0.0);
                    let obi_f64 = depth.obi()
                        .map(|d| d.to_string().parse::<f64>().unwrap_or(0.0))
                        .unwrap_or(0.0);
                    self.detector.on_spot_depth(mid_f64, obi_f64, now_ms);
                }
                self.state.last_update_ms = depth.timestamp_ms;
            }

            // BuildupConfirmed removed — detector is now internal; see handle_buildup_confirmed()

            // ── Market rotation ───────────────────────────────────────────
            IngestorEvent::MarketRotation {
                condition_id,
                yes_token_id,
                no_token_id,
                end_timestamp_ms,
                tick_size,
            } => {
                info!(%condition_id, %yes_token_id, %no_token_id, end_timestamp_ms, %tick_size, "market rotated");

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
                                 (missing hedge/token) — position will be force-closed"
                            );
                        }
                    } else {
                        warn!(
                            "rotation emergency: no opposing ask available \
                             — position will be force-closed"
                        );
                    }
                }

                // ── Telegram alert for abandoned positions ────────────────
                if leg1_filled && !leg2_filled
                    && let Some(ref reporter) = self.reporter
                {
                    let (l1_price, l1_size) = match &self.state.leg1_state {
                        OrderState::Filled { price, size, .. } => (*price, *size),
                        _ => (Decimal::ZERO, Decimal::ZERO),
                    };
                    let dir = match self.leg1_direction {
                        Some(Direction::Up) => "YES",
                        Some(Direction::Down) => "NO",
                        None => "?",
                    };
                    let has_fok = !self.rotation_emergency_buffer.is_empty();
                    reporter.fire_critical(format!(
                        "ROTATION EMERGENCY\nOpen: {} @ {} ({})\nLeg 2: NOT FILLED\nFOK: {}",
                        dir,
                        l1_price,
                        l1_size,
                        if has_fok { "SUBMITTED" } else { "COULD NOT BUILD" },
                    ));
                }

                // Persist condition ID for redemption if we traded in this market.
                if !self.live_market_trades.is_empty()
                    && let Some(ref cid) = self.state.active_condition_id
                {
                    crate::control::wallet::append_condition_id_sync(cid);
                }

                // ── Reset all state for the new market ───────────────────
                self.state.active_condition_id = Some(condition_id);
                self.state.active_yes_token_id = Some(yes_token_id);
                self.state.active_no_token_id = Some(no_token_id);
                self.state.market_end_timestamp_ms = end_timestamp_ms;
                self.state.tick_size = tick_size;
                self.state.poly_book = None;
                self.state.poly_yes_book = None;
                self.state.poly_no_book = None;
                self.state.buildup_detected = false;
                self.state.last_buildup = None;
                self.state.leg1_state = OrderState::None;
                self.state.leg2_state = OrderState::None;
                self.state.cumulative_used = Decimal::ZERO;
                self.state.last_update_ms = now_ms;
                self.hedge = None;
                self.avg_book_depth = None;
                self.leg1_direction = None;
                self.pending_leg1_signal = None;
                self.pending_tick_size_cmd = None;
                self.pending_leg1_cancel = None;
                self.leg1_cancel_inflight = false;
                self.emergency_signal_in_flight = false;
                self.leg2_command_pending = false;
                self.prev_leg2_order = None;
                self.pending_fills.clear();
                self.pending_partial_fills.clear();
                self.state.leg1_posted_ask = None;
                // Dual-order: clear ALL state on rotation (new market).
                self.leg2_phase1_order_id = None;
                self.leg2_phase2_order_id = None;
                self.post_trade_orphan = None;
                self.orphan_cancel_sent = false;
                self.rebalance_in_progress = false;
                self.in_cutoff_window = false;
                self.in_quiet_period = true;
                self.in_trade_cooldown = false;
                self.rotation_ms = now_ms;
                self.diag_markets_rotated += 1;
                // Reset per-market live reporting state.
                self.live_market_trades.clear();
                self.live_market_signals = 0;
                self.live_market_walls = 0;
            }

            // ── Trade status update (fill tracking via User WS) ───────────
            IngestorEvent::TradeStatusUpdate { order_id, status, size_matched, original_size } => {
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
                            // Partial fill detection — defer alert to MINED/CONFIRMED.
                            if let (Some(matched), Some(original)) = (size_matched, original_size) {
                                if matched < original {
                                    warn!(%order_id, %matched, %original, "partial fill on Leg 1 — deferring alert to MINED");
                                    self.pending_partial_fills.insert(order_id.clone(), PendingPartialFill {
                                        leg: "Leg 1", size_matched: matched, original_size: original,
                                    });
                                }
                            }
                            // Use actual fill size when partial fill detected.
                            // Truncate to 2dp — CLOB max lot precision is 2 decimal places.
                            let actual_size = if let (Some(matched), Some(_orig)) = (size_matched, original_size) {
                                if matched > Decimal::ZERO && matched < size { matched.round_dp(2) } else { size.round_dp(2) }
                            } else {
                                size.round_dp(2)
                            };
                            self.state.leg1_state = OrderState::Filled {
                                order_id: order_id.clone(),
                                price,
                                size: actual_size,
                                fill_timestamp_ms: now_ms,
                            };
                            self.init_leg2(price, actual_size, now_ms);

                            // Cancel remaining shares on the CLOB to prevent unhedged fills.
                            if actual_size < size {
                                info!(%order_id, filled = %actual_size, posted = %size,
                                    "partial fill — cancelling remaining order on CLOB");
                                self.pending_leg1_cancel = Some(ExecutorCommand::CancelLeg1Order {
                                    order_id: order_id.clone(),
                                });
                                self.leg1_cancel_inflight = true;
                            }

                            // Reset live trade meta for the new trade.
                            self.live_trade_meta = LiveTradeMeta::default();

                            // Send opportunity alert via Telegram in live mode.
                            if let (Some(reporter), Some(signal)) = (
                                self.reporter.clone(),
                                self.pending_leg1_signal.clone(),
                            ) {
                                let book = signal
                                    .book_snapshot
                                    .clone()
                                    .or_else(|| self.state.poly_book.clone())
                                    .unwrap_or_else(|| OrderBook {
                                        asset_id: signal.token_id.clone(),
                                        bids: vec![],
                                        asks: vec![],
                                        timestamp_ms: now_ms,
                                    });
                                reporter.send_opportunity_alert(&signal, price, size, &book);
                                self.live_market_signals += 1;
                                if signal.bot_contested {
                                    self.live_market_walls += 1;
                                }
                            }
                        }
                        TradeStatus::Failed => {
                            warn!(%order_id, "Leg 1 FAILED — resetting to None");
                            self.state.leg1_state = OrderState::None;
                        }
                        TradeStatus::Canceled => {
                            if self.leg1_cancel_inflight {
                                // Our cancel is in flight — cancel result will handle cleanup.
                                // Don't reset leg1_state here: if the order actually filled,
                                // the MATCHED event arriving after this still needs to find
                                // leg1_state=Posted to recognise it as a Leg 1 fill.
                                debug!(%order_id, "Leg 1 CANCELED by CLOB (cancel in flight) — deferring to cancel result");
                            } else {
                                // Unexpected CLOB cancellation with no cancel in flight from our side.
                                warn!(%order_id, "Leg 1 CANCELED by CLOB unexpectedly — full reset");
                                self.state.leg1_state = OrderState::None;
                                self.state.leg1_posted_ask = None;
                                self.state.last_buildup = None;
                                self.leg1_direction = None;
                                self.pending_leg1_signal = None;
                            }
                        }
                        TradeStatus::Retrying => {
                            debug!(%order_id, "Leg 1 RETRYING");
                        }
                    }
                }

                // Leg 2 matching: check leg2_state AND both phase order IDs.
                let is_leg2 = match &self.state.leg2_state {
                    OrderState::Posted { order_id: oid, .. } => *oid == order_id,
                    _ => false,
                };
                // Also check if order matches phase1 or phase2 tracked IDs
                // (dual-order: the non-primary order may fill).
                let is_leg2_phase1 = self.leg2_phase1_order_id.as_deref() == Some(&order_id);
                let is_leg2_phase2 = self.leg2_phase2_order_id.as_deref() == Some(&order_id);
                let is_any_leg2 = is_leg2 || is_leg2_phase1 || is_leg2_phase2;

                if is_any_leg2 {
                    // For dual-order fills, use the price/size from the matching order.
                    // If the order matches leg2_state, use that. Otherwise find it
                    // from the tracked phase IDs.
                    let (fill_price, fill_size) = if is_leg2 {
                        match &self.state.leg2_state {
                            OrderState::Posted { price, size, .. } => (*price, *size),
                            _ => unreachable!(),
                        }
                    } else if let Some(hedge) = &self.hedge {
                        // Order matched a tracked phase ID but not leg2_state.
                        // Use hedge state to determine price.
                        if is_leg2_phase1 {
                            let size_from_state = match &self.state.leg2_state {
                                OrderState::Posted { size, .. } => *size,
                                _ => match &self.state.leg1_state {
                                    OrderState::Filled { size, .. } => *size,
                                    _ => Decimal::ZERO,
                                },
                            };
                            (hedge.phase1_target_price, size_from_state)
                        } else {
                            // Phase 2 order filled
                            let p2_price = hedge.phase2_posted_price.unwrap_or(Decimal::ZERO);
                            let size_from_state = match &self.state.leg2_state {
                                OrderState::Posted { size, .. } => *size,
                                _ => match &self.state.leg1_state {
                                    OrderState::Filled { size, .. } => *size,
                                    _ => Decimal::ZERO,
                                },
                            };
                            (p2_price, size_from_state)
                        }
                    } else {
                        // No hedge state — shouldn't happen, but fallback.
                        (Decimal::ZERO, Decimal::ZERO)
                    };

                    match status {
                        TradeStatus::Matched | TradeStatus::Mined | TradeStatus::Confirmed => {
                            info!(%order_id, %fill_price, %fill_size, status = ?status, "Leg 2 fill — pair complete");
                            // Partial fill detection — defer alert to MINED/CONFIRMED.
                            if let (Some(matched), Some(original)) = (size_matched, original_size) {
                                if matched < original {
                                    warn!(%order_id, %matched, %original, "partial fill on Leg 2 — deferring alert to MINED");
                                    self.pending_partial_fills.insert(order_id.clone(), PendingPartialFill {
                                        leg: "Leg 2", size_matched: matched, original_size: original,
                                    });
                                }
                            }
                            self.state.leg2_state = OrderState::Filled {
                                order_id: order_id.clone(),
                                price: fill_price,
                                size: fill_size,
                                fill_timestamp_ms: now_ms,
                            };

                            // Dual-order: identify the "other" order and store as orphan.
                            let phase1_filled = is_leg2_phase1 || (!is_leg2_phase2 && is_leg2);
                            let other_id = if phase1_filled {
                                // Phase 1 filled (or single-order leg2) — orphan is Phase 2.
                                self.leg2_phase2_order_id.take()
                            } else {
                                // Phase 2 filled — orphan is Phase 1.
                                self.leg2_phase1_order_id.take()
                            };
                            // Mark phase1_dual_fill if Phase 1 order filled while Phase 2 was also active.
                            if phase1_filled && self.hedge.as_ref().is_some_and(|e| matches!(e.phase, HedgePhase::Phase2)) {
                                self.live_trade_meta.phase1_dual_fill = true;
                            }
                            if let Some(orphan_id) = other_id {
                                if let (Some(hedge), Some(dir), Some(l1_price)) = (
                                    &self.hedge,
                                    self.leg1_direction,
                                    match &self.state.leg1_state {
                                        OrderState::Filled { price, .. } => Some(*price),
                                        _ => None,
                                    },
                                ) {
                                    let orphan_price = if is_leg2_phase2 || (!is_leg2_phase1 && !is_leg2) {
                                        // The filled one was Phase 2, orphan is Phase 1.
                                        hedge.phase1_target_price
                                    } else {
                                        // The filled one was Phase 1, orphan is Phase 2.
                                        hedge.phase2_posted_price.unwrap_or(Decimal::ZERO)
                                    };
                                    self.post_trade_orphan = Some(OrphanState {
                                        order_id: orphan_id.clone(),
                                        size: fill_size,
                                        leg1_direction: dir,
                                        leg1_price: l1_price,
                                        condition_id: self.state.active_condition_id.clone().unwrap_or_default(),
                                        tick_size: self.state.tick_size,
                                    });
                                    self.orphan_cancel_sent = false;
                                    info!(%orphan_id, %orphan_price, "dual-order: storing orphan, emitting cancel");
                                }
                            }
                            // hedge cleared by on_trade_complete() after Telegram + recording
                        }
                        TradeStatus::Failed => {
                            warn!(%order_id, "Leg 2 FAILED — re-entry via evaluate_leg2");
                            self.state.leg2_state = OrderState::None;
                        }
                        TradeStatus::Canceled => {
                            warn!(%order_id, "Leg 2 CANCELED by CLOB — resetting for re-evaluation");
                            self.state.leg2_state = OrderState::None;
                            self.prev_leg2_order = None;
                        }
                        TradeStatus::Retrying => {
                            debug!(%order_id, "Leg 2 RETRYING");
                        }
                    }
                }

                // Check orphan fill (dual-order double-fill recovery).
                let is_orphan = self.post_trade_orphan.as_ref().is_some_and(|o| o.order_id == order_id);
                if is_orphan && !is_any_leg2 && !is_leg1 {
                    match status {
                        TradeStatus::Matched | TradeStatus::Mined | TradeStatus::Confirmed => {
                            warn!(%order_id, ?status, "DOUBLE-FILL: orphan order filled — triggering rebalance");
                            // The orphan filled after the trade completed. Need rebalance.
                            self.diag_double_fills += 1;
                            self.rebalance_in_progress = true;
                            // Rebalance signal built in take_rebalance_signal().
                        }
                        TradeStatus::Canceled => {
                            info!(%order_id, "orphan order cancelled — clearing orphan state");
                            self.post_trade_orphan = None;
                            self.orphan_cancel_sent = false;
                        }
                        _ => {}
                    }
                }

                // Buffer unmatched TradeStatusUpdate events (may arrive before
                // OrderPosted feedback or after a cancel cleared state).
                if !is_leg1 && !is_any_leg2 && !is_orphan {
                    // Check if this event resolves a deferred partial fill check.
                    if let Some(mut pending) = self.pending_partial_fills.remove(&order_id) {
                        match status {
                            TradeStatus::Matched => {
                                // Another MATCHED — update cumulative size.
                                if let Some(matched) = size_matched {
                                    pending.size_matched = matched;
                                }
                                if pending.size_matched >= pending.original_size {
                                    // Fully filled now — no alert needed.
                                    info!(%order_id, "deferred partial fill resolved — fully filled");
                                } else {
                                    // Still partial — re-insert and wait for MINED.
                                    self.pending_partial_fills.insert(order_id, pending);
                                }
                            }
                            TradeStatus::Mined | TradeStatus::Confirmed => {
                                // Terminal status — final size_matched is authoritative.
                                let final_matched = size_matched.unwrap_or(pending.size_matched);
                                if final_matched < pending.original_size {
                                    warn!(
                                        %order_id, matched = %final_matched, original = %pending.original_size,
                                        "PARTIAL FILL confirmed at {}", if status == TradeStatus::Mined { "MINED" } else { "CONFIRMED" }
                                    );
                                    if let Some(reporter) = self.reporter.as_ref() {
                                        reporter.send_partial_fill_alert(
                                            pending.leg, &order_id, final_matched, pending.original_size,
                                        );
                                    }
                                } else {
                                    info!(%order_id, "deferred partial fill resolved at MINED — fully filled");
                                }
                            }
                            _ => {
                                // FAILED/CANCELED/RETRYING — discard, other handlers deal with these.
                            }
                        }
                        self.state.last_update_ms = now_ms;
                    } else if self.pending_fills.len() < 8 {
                        warn!(%order_id, ?status, "TradeStatusUpdate unmatched — buffering");
                        self.pending_fills.push_back((order_id, status, size_matched, original_size));
                    } else {
                        warn!(%order_id, ?status, "TradeStatusUpdate unmatched AND buffer full — DROPPED");
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
                    let was_unhealthy = !self.connectivity.heartbeat_healthy;
                    self.connectivity.heartbeat_healthy = true;
                    self.connectivity.consecutive_heartbeat_failures = 0;
                    self.connectivity.last_heartbeat_latency_ms = latency_ms;
                    if was_unhealthy {
                        info!(latency_ms, "heartbeat RECOVERED");
                    } else {
                        debug!(latency_ms, "heartbeat OK");
                    }
                } else {
                    self.connectivity.consecutive_heartbeat_failures += 1;
                    self.connectivity.heartbeat_healthy = false;
                    let failures = self.connectivity.consecutive_heartbeat_failures;
                    warn!(
                        failures,
                        "heartbeat FAILED — orders may be auto-cancelled by CLOB"
                    );
                    if failures == self.heartbeat_dead_threshold {
                        self.handle_heartbeat_dead();
                    }
                }
            }

            // ── WebSocket connectivity ────────────────────────────────────
            IngestorEvent::WsStatus { source, connected } => match source {
                DataSource::Binance => {
                    self.connectivity.binance_connected = connected;
                    if !connected {
                        warn!("Binance WS disconnected — buildup detection paused");
                        self.state.buildup_detected = false;
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
                DataSource::BinanceFutures => {
                    if !connected {
                        warn!("Binance Futures WS disconnected");
                    }
                }
            },

            // ── Futures / Spot events → feed BuildupDetector ──────────────
            IngestorEvent::FuturesAggTrade(ref trade) => {
                self.detector.on_futures_agg_trade(trade, now_ms);
            }
            IngestorEvent::FuturesBookTicker(ref ticker) => {
                self.detector.on_futures_book_ticker(ticker, now_ms);
            }
            IngestorEvent::FuturesForceOrder(ref order) => {
                self.detector.on_futures_force_order(order, now_ms);
            }
            IngestorEvent::SpotTrade(ref trade) => {
                self.detector.on_spot_trade(trade, now_ms);
            }

            // Control events are handled in main.rs before on_event() is called.
            IngestorEvent::Shutdown
            | IngestorEvent::DrainAndRestart
            | IngestorEvent::PauseTrading
            | IngestorEvent::ResumeTrading => {}
        }

        // ── BuildupDetector: evaluate after feeding ───────────────────
        if self.detector.is_dirty() {
            let (score, dir, entry) = self.detector.tick(now_ms);
            if let Some(buildup) = entry {
                self.handle_buildup_confirmed(buildup, now_ms);
            }
            if let Some(d) = dir {
                self.handle_flow_update(score, d, now_ms);
            }
        }

        // ── Cutoff window detection (runs on every event) ──────────────
        // Checks time remaining on every event so the market summary is
        // sent even if no spike arrives during the cutoff window.
        if !self.in_cutoff_window && self.state.active_condition_id.is_some() {
            let time_remaining_secs = self.state.time_remaining_ms(now_ms) / 1_000;
            if time_remaining_secs < self.leg1.entry_cutoff_secs {
                self.in_cutoff_window = true;
                info!(
                    time_remaining_secs,
                    "entering cutoff window — trading suspended"
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

        // ── Rotation quiet period detection (runs on every event) ──────
        if self.in_quiet_period {
            let elapsed = now_ms.saturating_sub(self.rotation_ms);
            if elapsed >= self.rotation_quiet_ms {
                self.in_quiet_period = false;
                info!(elapsed_ms = elapsed, "rotation quiet period ended — trading enabled");
            }
        }

        // ── Post-trade cooldown timer (runs on every event) ─────────
        if self.in_trade_cooldown {
            let elapsed = now_ms.saturating_sub(self.last_trade_complete_ms);
            if elapsed >= self.trade_cooldown_ms {
                self.in_trade_cooldown = false;
                info!(elapsed_ms = elapsed, "trade cooldown ended — trading enabled");
            }
        }

        // ── Leg 1 sustain timeout (runs on every event) ─────────
        if self.pending_leg1_cancel.is_none() && !self.leg1_cancel_inflight {
            if let OrderState::Posted { ref order_id, timestamp_ms: post_ms, .. } = self.state.leg1_state {
                if !Self::is_provisional_order(order_id) {
                    let elapsed = now_ms.saturating_sub(post_ms);
                    if elapsed >= self.cancel_window_ms {
                        info!(elapsed_ms = elapsed, "Leg 1 sustain — cancel_window_ms timeout");
                        self.pending_leg1_cancel = Some(ExecutorCommand::CancelLeg1Order {
                            order_id: order_id.clone(),
                        });
                        self.leg1_cancel_inflight = true;
                        self.diag_sustain_timeouts += 1;
                    }
                }
            }
        }
    }

    // ─── BuildupDetector integration ────────────────────────────────────

    /// Handle a buildup entry signal from the internal BuildupDetector.
    /// Extracted from the former `IngestorEvent::BuildupConfirmed` match arm.
    fn handle_buildup_confirmed(&mut self, buildup: BuildupInfo, now_ms: u64) {
        let _ = now_ms; // available for future use
        self.diag_buildups_received += 1;
        // Drop buildup if within post-rotation quiet period.
        if self.in_quiet_period {
            self.diag_buildups_dropped_quiet += 1;
            debug!("buildup ignored — within rotation quiet period");
            return;
        }
        // Drop buildup if within entry_cutoff window — no new trades allowed.
        if self.in_cutoff_window {
            self.diag_buildups_dropped_cutoff += 1;
            debug!("buildup ignored — within cutoff window");
            return;
        }

        // If Leg 1 is already active, don't process new buildups — but cancel on
        // opposite-direction buildup to avoid filling into a reversed market.
        if !matches!(self.state.leg1_state, OrderState::None) {
            if let OrderState::Posted { ref order_id, .. } = self.state.leg1_state {
                if !Self::is_provisional_order(order_id)
                    && self.pending_leg1_cancel.is_none()
                    && !self.leg1_cancel_inflight
                {
                    if let Some(leg1_dir) = self.leg1_direction {
                        if leg1_dir != buildup.direction {
                            info!(
                                leg1_dir = ?leg1_dir,
                                new_dir = ?buildup.direction,
                                "opposite direction buildup — cancelling Leg 1"
                            );
                            self.pending_leg1_cancel = Some(ExecutorCommand::CancelLeg1Order {
                                order_id: order_id.clone(),
                            });
                            self.leg1_cancel_inflight = true;
                            self.diag_opposite_dir_cancels += 1;
                            self.diag_sustain_cancels += 1;
                        }
                    }
                }
            }
            return;
        }

        // Gate: if buildup already active with same direction, skip repeated firing.
        if self.state.buildup_detected {
            if let Some(ref last) = self.state.last_buildup {
                if last.direction == buildup.direction {
                    return;
                }
            }
        }

        self.state.buildup_detected = true;
        self.state.last_buildup = Some(buildup.clone());

        info!(
            direction = ?buildup.direction,
            composite_score = %buildup.composite_score,
            buildup_detected_ms = buildup.timestamp_ms,
            "buildup CONFIRMED"
        );
    }

    /// Handle a flow update from the internal BuildupDetector (every dirty tick).
    /// Updates composite score state and performs Leg 1 sustain monitoring.
    /// Extracted from the former `IngestorEvent::BuildupUpdate` match arm.
    fn handle_flow_update(&mut self, score: f64, direction: Direction, now_ms: u64) {
        let composite_score = Decimal::try_from(score).unwrap_or(Decimal::ZERO);
        self.state.current_composite_score = composite_score;
        self.state.current_composite_direction = Some(direction);
        self.state.composite_update_ms = now_ms;
        // Update hedge flow monitoring if active.
        if let Some(ref mut hedge) = self.hedge {
            hedge.last_flow_score = composite_score;
            hedge.last_flow_direction = Some(direction);
            hedge.last_flow_update_ms = now_ms;
        }

        // ── Leg 1 sustain monitoring ──
        // If Leg 1 is Posted and no cancel is already pending, check composite fade.
        // Skip if order ID is provisional (real CLOB ID not yet received).
        if self.pending_leg1_cancel.is_none() && !self.leg1_cancel_inflight {
            if let OrderState::Posted { ref order_id, .. } = self.state.leg1_state {
                if !Self::is_provisional_order(order_id) {
                    // Composite fade: composite fell below cancel_threshold → cancel
                    if composite_score < self.leg2.cancel_threshold {
                        info!(
                            %composite_score, cancel_threshold = %self.leg2.cancel_threshold,
                            "Leg 1 sustain — composite faded below cancel threshold"
                        );
                        self.pending_leg1_cancel = Some(ExecutorCommand::CancelLeg1Order {
                            order_id: order_id.clone(),
                        });
                        self.leg1_cancel_inflight = true;
                        self.diag_sustain_cancels += 1;
                    }
                }
            }
        }
    }

    /// Evaluate current state and optionally emit a **Leg 1** trade signal.
    ///
    /// Delegates guard checking and signal building to [`Leg1Evaluator::evaluate`].
    /// Applies post-signal state mutations here after a successful evaluation:
    /// - Clears `buildup_detected`
    /// - Sets `leg1_state` to `Posted`
    /// - Increments `cumulative_used`
    /// - Records `leg1_direction`
    pub fn evaluate(&mut self) -> Option<TradeSignal> {
        // Drain/pause mode: block new Leg 1 entries.
        if self.draining || self.paused {
            if self.state.buildup_detected {
                self.diag_rej_paused += 1;
            }
            self.state.buildup_detected = false;
            return None;
        }

        // Heartbeat down: block new Leg 1 entries.
        if !self.connectivity.heartbeat_healthy {
            if self.state.buildup_detected {
                self.diag_rej_heartbeat += 1;
            }
            self.state.buildup_detected = false;
            return None;
        }

        // Post-trade cooldown: block new Leg 1 entries.
        if self.in_trade_cooldown {
            if self.state.buildup_detected {
                self.diag_buildups_dropped_cooldown += 1;
            }
            self.state.buildup_detected = false;
            return None;
        }

        let now_ms = now_epoch_ms();
        let outcome = self.leg1.evaluate(&self.state, now_ms);

        match outcome {
            Leg1Outcome::Signal(signal) => {
                let direction = signal.direction;
                let alloc = signal.alloc_amount;
                let price = signal.price;
                let size = signal.size;

                // Consume the buildup: clear detected flag. last_buildup is preserved so
                // init_leg2() can use it when Leg 1 fills. It will be cleared in
                // on_trade_complete() and the MarketRotation handler.
                self.state.buildup_detected = false;
                self.state.leg1_state = OrderState::Posted {
                    order_id: format!("sim-leg1-{}", now_ms),
                    price,
                    size,
                    timestamp_ms: now_ms,
                };
                // Record best_ask at time of post for sustain monitoring.
                self.state.leg1_posted_ask = Some(price);
                self.state.cumulative_used += alloc;
                self.leg1_direction = Some(direction);
                self.pending_leg1_signal = Some(signal.clone());
                self.diag_leg1_signals += 1;

                Some(signal)
            }
            Leg1Outcome::Rejected(reason) => {
                // Buildup consumed (rejected) — clear both flags.
                self.state.buildup_detected = false;
                self.state.last_buildup = None;
                match reason {
                    Leg1RejectReason::ActiveTrade => self.diag_rej_busy += 1,
                    Leg1RejectReason::NoBook => self.diag_rej_no_book += 1,
                    Leg1RejectReason::NoBinance => self.diag_rej_no_binance += 1,
                    Leg1RejectReason::StaleBook => self.diag_rej_stale += 1,
                    Leg1RejectReason::PriceSkewed => self.diag_rej_skew += 1,
                    Leg1RejectReason::InsufficientRepricing => self.diag_rej_reprice += 1,
                    Leg1RejectReason::HeartbeatDown => self.diag_rej_heartbeat += 1,
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
    /// - On emergency: sets `hedge.emergency_submitted = true`
    /// - On phase transition: sets `hedge.phase` to `Phase2`
    pub fn evaluate_leg2(&mut self) -> Option<TradeSignal> {
        if self.emergency_signal_in_flight {
            return None;
        }
        if self.leg2_command_pending {
            return None;
        }
        let now_ms = now_epoch_ms();

        // Build borrow-free snapshot of hedge state to pass to evaluator.
        let snap = match self.hedge.as_ref() {
            None => return None,
            Some(e) => HedgeSnap {
                emergency_submitted: e.emergency_submitted,
                break_even: e.break_even(),
                leg1_fee: e.leg1_fee,
                initial_profit_target: e.initial_profit_target,
                direction: e.direction,
                fill_ms: e.leg1_fill_ms,
                tier: e.tier,
                expected_pct: e.expected_pct,
                spike_info: e.spike_info,
                phase: e.phase,
                phase1_target_price: e.phase1_target_price,
                phase2_start_ms: e.phase2_start_ms,
                phase2_posted_price: e.phase2_posted_price,
                flow_monitoring_active: e.flow_monitoring_active,
                last_flow_score: e.last_flow_score,
                last_flow_direction: e.last_flow_direction,
            },
        };

        let decision = match self
            .leg2
            .evaluate_leg2(&self.state, &snap, now_ms)
        {
            Some(d) => d,
            None => {
                // Evaluator returned None — skip guard, no improvement, or missing data.
                return None;
            }
        };

        // ── Apply mutations based on decision type ────────────────────────
        let (price, size) = (decision.price(), decision.size());
        let is_emergency = decision.is_emergency();

        // Heartbeat down: block non-emergency Leg 2 posts (emergency FOK must always fire).
        if !self.connectivity.heartbeat_healthy && !is_emergency {
            debug!("heartbeat down — blocking non-emergency Leg 2 post");
            return None;
        }

        // Save current Leg 2 order before overwriting with provisional ID.
        if let OrderState::Posted { ref order_id, price: p, size: s, .. } = self.state.leg2_state
            && !order_id.starts_with("sim-")
        {
            self.prev_leg2_order = Some((order_id.clone(), p, s));
        }

        if is_emergency {
            let (reason, was_taker) = match &decision {
                Leg2Decision::Emergency { signal, .. } => {
                    (signal.exit_reason, true) // Emergency exits are always FOK taker
                }
                _ => (None, false),
            };
            match reason {
                Some(ExitReason::BreakEvenBreach) => self.diag_emg_be_breach += 1,
                Some(ExitReason::Phase2Timeout) => self.diag_emg_phase2_timeout += 1,
                Some(ExitReason::Phase2PriceBreach) => self.diag_emg_phase2_price_breach += 1,
                Some(ExitReason::MarketExpiry) => self.diag_emg_expiry += 1,
                Some(ExitReason::FavorableTaker) => self.diag_favorable_exits += 1,
                Some(ExitReason::Phase1Breach) => self.diag_emg_phase1_breach += 1,
                Some(ExitReason::WhipsawReversal) => {} // flow reversal FOK — counted via flow monitoring
                Some(ExitReason::FlowCollapse) => {} // composite flow collapse — counted via flow monitoring
                None => {}
            }
            // Track for live mode trade-completed Telegram message.
            self.live_trade_meta = LiveTradeMeta {
                leg2_was_taker: was_taker,
                exit_reason: reason,
                emergency_maker: reason.is_some() && !was_taker,
                favorable_taker: matches!(reason, Some(ExitReason::FavorableTaker)),
                favorable_maker: false,
                phase1_breach: matches!(reason, Some(ExitReason::Phase1Breach)),
                phase1_dual_fill: false,
            };
            if let Some(e) = self.hedge.as_mut() {
                e.emergency_submitted = true;
                e.exit_reason = match &decision {
                    Leg2Decision::Emergency { signal, .. } => signal.exit_reason,
                    _ => None,
                };
                if was_taker {
                    e.fok_emitted = true;
                }
            }
            self.emergency_signal_in_flight = true;
            if self.reporter.is_some() {
                self.leg2_command_pending = true;
            }
            self.state.leg2_state = OrderState::Posted {
                order_id: format!("sim-leg2-emergency-{}", now_ms),
                price,
                size,
                timestamp_ms: now_ms,
            };
        } else {
            // Non-emergency hedge path: Phase1Post or Phase2Alongside.
            match &decision {
                Leg2Decision::Phase1Post { .. } => {
                    // Initial post at profit target — no phase change needed.
                    self.diag_leg2_phase1_posts += 1;
                    self.last_leg2_was_phase2_alongside = false;
                    if self.reporter.is_some() {
                        self.leg2_command_pending = true;
                    }
                    self.state.leg2_state = OrderState::Posted {
                        order_id: format!("sim-leg2-hedge-{}", now_ms),
                        price,
                        size,
                        timestamp_ms: now_ms,
                    };
                }
                Leg2Decision::Phase2Alongside { .. } => {
                    // Dual-order: keep Phase 1 alive, post Phase 2 alongside.
                    if let Some(e) = self.hedge.as_mut() {
                        e.phase = HedgePhase::Phase2;
                        e.phase2_posted_price = Some(price);
                        e.phase2_start_ms = Some(now_ms);
                    }
                    self.diag_phase_transitions += 1;
                    self.last_leg2_was_phase2_alongside = true;
                    if self.reporter.is_some() {
                        self.leg2_command_pending = true;
                    }
                    // In live mode, Phase 1 order stays alive — we track Phase 2
                    // separately via leg2_phase2_order_id.
                    // For sim mode, update leg2_state to Phase 2 price (sim only
                    // tracks one order; advance_simulation handles dual-order).
                    self.leg2_phase2_order_id = Some(format!("sim-leg2-p2-{}", now_ms));
                    if self.reporter.is_none() {
                        // Sim mode: overwrite leg2_state (Phase 1 order doesn't
                        // exist in CLOB, sim tracks it via phase1_target_price).
                        self.state.leg2_state = OrderState::Posted {
                            order_id: format!("sim-leg2-hedge-{}", now_ms),
                            price,
                            size,
                            timestamp_ms: now_ms,
                        };
                    }
                }
                _ => {}
            }
        }

        Some(decision.into_signal())
    }

    // ─── Spike cancel drain ─────────────────────────────────────────────

    /// Returns `true` if the last `evaluate_leg2()` returned a Phase2Alongside signal.
    /// Consumed (cleared) on read. Main loop uses this to send `PostLeg2Phase2`.
    pub fn take_phase2_alongside_flag(&mut self) -> bool {
        let v = self.last_leg2_was_phase2_alongside;
        self.last_leg2_was_phase2_alongside = false;
        v
    }

    /// Returns `true` if a rebalance FOK is in progress.
    pub fn rebalance_in_progress(&self) -> bool {
        self.rebalance_in_progress
    }

    /// Returns `true` if there's a pending orphan cancel to send.
    pub fn has_orphan_cancel(&self) -> bool {
        self.post_trade_orphan.is_some() && !self.orphan_cancel_sent
    }

    /// Take the pending tick size change command (if any).
    /// Called by the main loop after `on_event()` to forward to the executor.
    pub fn take_tick_size_change(&mut self) -> Option<ExecutorCommand> {
        self.pending_tick_size_cmd.take()
    }

    /// Take the pending Leg 1 cancel command (if any).
    /// Called by the main loop after `on_event()` to forward to the executor.
    pub fn take_leg1_cancel(&mut self) -> Option<ExecutorCommand> {
        self.pending_leg1_cancel.take()
    }

    /// Take the pending heartbeat-dead cancel command (if any).
    /// Called by the main loop after `on_event()` to forward to the executor.
    pub fn take_heartbeat_cancel(&mut self) -> Option<ExecutorCommand> {
        self.pending_heartbeat_cancel.take()
    }

    /// Proactive state reset when heartbeat failure count reaches the dead threshold.
    /// CLOB cancels resting orders ~10-15s after heartbeat death. This resets
    /// engine state to match, preventing stale `OrderState::Posted` entries.
    fn handle_heartbeat_dead(&mut self) {
        warn!("HEARTBEAT DEAD — assuming CLOB cancelled all resting orders");
        self.diag_heartbeat_resets += 1;

        // Telegram alert
        if let Some(ref reporter) = self.reporter {
            reporter.fire_critical(format!(
                "⚠️ <b>HEARTBEAT DEAD</b> — {} consecutive failures\n\
                 Assuming CLOB cancelled all resting orders.\n\
                 Leg 1: {:?}  Leg 2: {:?}",
                self.connectivity.consecutive_heartbeat_failures,
                std::mem::discriminant(&self.state.leg1_state),
                std::mem::discriminant(&self.state.leg2_state),
            ));
        }

        // Reset Leg 1 if posted (unfilled) — CLOB likely already cancelled it.
        if let OrderState::Posted { ref order_id, .. } = self.state.leg1_state {
            self.pending_heartbeat_cancel = Some(ExecutorCommand::CancelLeg1Order {
                order_id: order_id.clone(),
            });
            self.state.leg1_state = OrderState::None;
            self.state.leg1_posted_ask = None;
            self.leg1_direction = None;
            self.pending_leg1_signal = None;
            self.leg1_cancel_inflight = false;
            self.state.buildup_detected = false;
            self.state.last_buildup = None;
        }

        // Reset Leg 2 if posted (unfilled) — naked position until heartbeat recovers.
        if matches!(self.state.leg2_state, OrderState::Posted { .. }) {
            self.state.leg2_state = OrderState::None;
            self.leg2_phase1_order_id = None;
            self.leg2_phase2_order_id = None;
            self.last_leg2_was_phase2_alongside = false;
            self.emergency_signal_in_flight = false;
            self.leg2_command_pending = false;
            self.prev_leg2_order = None;
        }
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

    /// Log cumulative engine diagnostics every 60 seconds (terminal only).
    ///
    /// Sets `engine_diag_ready` and calls `try_build_telegram_diag()`.
    ///
    /// Call this once per engine loop iteration (cheap — checks timestamp first).
    pub fn check_diagnostic(&mut self) {
        let now_ms = now_epoch_ms();
        if self.last_diag_ms == 0 {
            self.last_diag_ms = now_ms;
            return;
        }
        if now_ms.saturating_sub(self.last_diag_ms) < 60_000 {
            return;
        }
        info!(
            markets = self.diag_markets_rotated,
            buildups = self.diag_buildups_received,
            buildups_cut = self.diag_buildups_dropped_cutoff,
            buildups_quiet = self.diag_buildups_dropped_quiet,
            buildups_cooldown = self.diag_buildups_dropped_cooldown,
            emg_phase1_breach = self.diag_emg_phase1_breach,
            rej_paused = self.diag_rej_paused,
            rej_busy = self.diag_rej_busy,
            rej_no_book = self.diag_rej_no_book,
            rej_no_bnc = self.diag_rej_no_binance,
            rej_stale = self.diag_rej_stale,
            rej_skew = self.diag_rej_skew,
            rej_reprice = self.diag_rej_reprice,
            rej_other = self.diag_rej_other,
            leg1_sig = self.diag_leg1_signals,
            leg1_fill = self.diag_leg1_fills,
            l2_p1_posts = self.diag_leg2_phase1_posts,
            phase_transitions = self.diag_phase_transitions,
            l2_maker = self.diag_leg2_fills_maker,
            l2_taker = self.diag_leg2_fills_taker,
            emg_be_breach = self.diag_emg_be_breach,
            emg_p2_timeout = self.diag_emg_phase2_timeout,
            emg_p2_price_breach = self.diag_emg_phase2_price_breach,
            emg_expiry = self.diag_emg_expiry,
            fav_exits = self.diag_favorable_exits,
            fav_maker = self.diag_favorable_maker_fills,
            fav_maker_timeout = self.diag_favorable_maker_timeouts,
            emg_maker = self.diag_emg_maker,
            emg_taker = self.diag_emg_taker,
            double_fills = self.diag_double_fills,
            rebal_attempts = self.diag_rebalance_attempts,
            rebal_success = self.diag_rebalance_successes,
            p2_entry_breach = self.diag_phase2_entry_breach,
            buildup_fail = self.diag_buildup_failures,
            order_fail = self.diag_order_failures,
            sustain_cancels = self.diag_sustain_cancels,
            sustain_timeouts = self.diag_sustain_timeouts,
            opposite_dir_cancels = self.diag_opposite_dir_cancels,
            hb_rej = self.diag_rej_heartbeat,
            hb_resets = self.diag_heartbeat_resets,
            hb_failures = self.connectivity.consecutive_heartbeat_failures,
            det_signals = self.detector.diag_signals_emitted(),
            det_dir_veto = self.detector.diag_direction_vetoes(),
            det_causal_veto = self.detector.diag_causal_vetoes(),
            det_composite = %self.state.current_composite_score,
            "engine 60s"
        );
        self.last_diag_ms = now_ms;
        self.engine_diag_ready = true;
        self.try_build_telegram_diag();
    }

    /// Build and store the Telegram diagnostic message when engine data is ready.
    fn try_build_telegram_diag(&mut self) {
        if !self.engine_diag_ready {
            return;
        }
        self.engine_diag_ready = false;

        self.pending_telegram_diag = Some(format!(
            "<b>--- DIAGNOSTICS (60s) ---</b>\n\
             \n\
             <b>Engine</b>\n\
             Markets rotated: {mkts}  Buildups: {buildups}  Buildup fails: {buildup_fail}  Cutoff drops: {buildups_cut}  Quiet drops: {buildups_quiet}  Cooldown drops: {buildups_cooldown}\n\
             \n\
             <b>Leg 1 Rejections</b>\n\
             Paused: {paused}  Busy: {busy}  No book: {no_book}  No Binance: {no_bnc}  Stale: {stale}  Skewed: {skew}\n\
             Reprice: {reprice}  Other: {other}\n\
             \n\
             <b>Leg 1</b>\n\
             Signals: {sig}  Fills: {fill}  Failed: {failed}\n\
             Sustain — cancels: {sustain_cancels}  timeouts: {sustain_timeouts}  opposite-dir: {opposite_dir_cancels}\n\
             \n\
             <b>Leg 2</b>\n\
             P1 Posts: {l2_p1_posts}  Transitions: {transitions}  Fills: {l2_maker} maker / {l2_taker} taker\n\
             Favorable: exits={fav_exits}  maker={fav_maker}  maker-timeout={fav_maker_timeout}\n\
             Emergency — p1-breach: {emg_p1b}  be-breach: {emg_beb}  p2-timeout: {emg_p2t}  p2-price-breach: {emg_p2pb}  p2-entry-breach: {p2_entry_breach}  expiry: {emg_exp}\n\
             Emergency fills — {emg_mkr} maker / {emg_tkr} taker\n\
             Double-fill: detected={double_fills}  rebalance={rebal_attempts}  success={rebal_success}\n\
             \n\
             <b>Heartbeat</b>\n\
             {hb_status}  Failures: {hb_failures}  Latency: {hb_latency}ms  Rej: {hb_rej}  Resets: {hb_resets}\n\
             \n\
             <b>BuildupDetector</b>\n\
             Signals: {det_signals}  Dir vetoes: {det_dir_veto}  Causal vetoes: {det_causal_veto}\n\
             Composite: {composite_score}",
            mkts = self.diag_markets_rotated,
            buildups = self.diag_buildups_received,
            buildup_fail = self.diag_buildup_failures,
            buildups_cut = self.diag_buildups_dropped_cutoff,
            buildups_quiet = self.diag_buildups_dropped_quiet,
            buildups_cooldown = self.diag_buildups_dropped_cooldown,
            paused = self.diag_rej_paused,
            busy = self.diag_rej_busy,
            no_book = self.diag_rej_no_book,
            no_bnc = self.diag_rej_no_binance,
            stale = self.diag_rej_stale,
            skew = self.diag_rej_skew,
            reprice = self.diag_rej_reprice,
            other = self.diag_rej_other,
            sig = self.diag_leg1_signals,
            fill = self.diag_leg1_fills,
            failed = self.diag_order_failures,
            sustain_cancels = self.diag_sustain_cancels,
            sustain_timeouts = self.diag_sustain_timeouts,
            opposite_dir_cancels = self.diag_opposite_dir_cancels,
            l2_p1_posts = self.diag_leg2_phase1_posts,
            transitions = self.diag_phase_transitions,
            l2_maker = self.diag_leg2_fills_maker,
            l2_taker = self.diag_leg2_fills_taker,
            fav_exits = self.diag_favorable_exits,
            fav_maker = self.diag_favorable_maker_fills,
            fav_maker_timeout = self.diag_favorable_maker_timeouts,
            emg_beb = self.diag_emg_be_breach,
            emg_p2t = self.diag_emg_phase2_timeout,
            emg_p2pb = self.diag_emg_phase2_price_breach,
            p2_entry_breach = self.diag_phase2_entry_breach,
            emg_exp = self.diag_emg_expiry,
            emg_p1b = self.diag_emg_phase1_breach,
            emg_mkr = self.diag_emg_maker,
            emg_tkr = self.diag_emg_taker,
            double_fills = self.diag_double_fills,
            rebal_attempts = self.diag_rebalance_attempts,
            rebal_success = self.diag_rebalance_successes,
            hb_status = if self.connectivity.heartbeat_healthy { "OK" } else { "DOWN" },
            hb_failures = self.connectivity.consecutive_heartbeat_failures,
            hb_latency = self.connectivity.last_heartbeat_latency_ms,
            hb_rej = self.diag_rej_heartbeat,
            hb_resets = self.diag_heartbeat_resets,
            det_signals = self.detector.diag_signals_emitted(),
            det_dir_veto = self.detector.diag_direction_vetoes(),
            det_causal_veto = self.detector.diag_causal_vetoes(),
            composite_score = self.state.current_composite_score,
        ));
    }

    /// Drain the pending Telegram diagnostic message (if any).
    /// Called by the main loop to send to Telegram after engine 60s diagnostics.
    pub fn take_pending_telegram_diag(&mut self) -> Option<String> {
        self.pending_telegram_diag.take()
    }

    // ─── Accessors and Executor callbacks ────────────────────────────────

    pub fn state(&self) -> &MarketState {
        &self.state
    }

    /// Get the current best ask from the direction-appropriate order book.
    /// Returns `true` if the order ID is a provisional placeholder (real CLOB ID pending).
    fn is_provisional_order(order_id: &str) -> bool {
        order_id.starts_with("sim-leg1-")
    }

    /// Called by the Executor when an order is successfully posted to the CLOB.
    pub fn on_order_posted(
        &mut self,
        is_leg2: bool,
        order_id: String,
        price: Decimal,
        size: Decimal,
        fill_method: Option<FillMethod>,
        already_filled: bool,
        order_tag: Option<OrderTag>,
    ) {
        // Stale feedback guard: ignore Leg 2 feedback that arrives after the trade
        // has been reset (e.g., stale hedge command processed after trade completed).
        if is_leg2 && !matches!(self.state.leg1_state, OrderState::Filled { .. }) {
            warn!(%order_id, %price, %size,
                "ignoring stale Leg 2 OrderPosted — no active Leg 1 fill");
            self.emergency_signal_in_flight = false;
            self.leg2_command_pending = false;
            return;
        }

        let now_ms = now_epoch_ms();
        if is_leg2 {
            self.emergency_signal_in_flight = false;
            self.leg2_command_pending = false;

            // Bug 1 fix: executor-initiated favorable exits set live_trade_meta
            // so Telegram shows the correct tag.
            match fill_method {
                Some(FillMethod::FavorableTaker) => {
                    self.live_trade_meta.favorable_taker = true;
                    self.live_trade_meta.leg2_was_taker = true;
                }
                Some(FillMethod::FavorableMaker) => {
                    // Favorable maker try succeeded — no taker fee, maker rebate.
                    self.live_trade_meta.favorable_taker = false;
                    self.live_trade_meta.favorable_maker = true;
                    self.live_trade_meta.leg2_was_taker = false;
                    self.diag_favorable_maker_fills += 1;
                    self.diag_favorable_exits += 1;
                }
                Some(FillMethod::EmergencyTaker) => {
                    self.live_trade_meta.leg2_was_taker = true;
                    self.live_trade_meta.emergency_maker = false;
                }
                None => {}
            }

            // Track dual-order IDs: update Phase 1 or Phase 2 based on OrderTag.
            match order_tag {
                Some(OrderTag::Leg2Phase1) => {
                    self.leg2_phase1_order_id = Some(order_id.clone());
                }
                Some(OrderTag::Leg2Phase2) => {
                    self.leg2_phase2_order_id = Some(order_id.clone());
                }
                None => {
                    // Backward compat: no tag → treat as Phase 1 (initial Leg 2 post).
                    self.leg2_phase1_order_id = Some(order_id.clone());
                }
            }

            // Bug 3 fix: FOK returned Filled synchronously from REST API.
            // Transition directly to Filled state — don't wait for User WS MATCHED.
            if already_filled {
                info!(%order_id, %price, %size, "FOK already filled — direct transition to Filled");
                self.state.leg2_state = OrderState::Filled {
                    order_id,
                    price,
                    size,
                    fill_timestamp_ms: now_ms,
                };
                // Re-evaluate favorable_taker based on actual fill price
                if let OrderState::Filled { price: l1_price, .. } = &self.state.leg1_state {
                    if *l1_price + price >= Decimal::ONE {
                        self.live_trade_meta.favorable_taker = false;
                    }
                }
                // Don't replay pending fills — go straight to trade completion
                // (checked by main loop after feedback drain).
                return;
            }
        }

        // Leg 1 already_filled: batch FAK fills are confirmed synchronously.
        // Transition directly to Filled, init Leg 2 hedge, and send opportunity alert.
        if !is_leg2 && already_filled {
            info!(%order_id, %price, %size, "Leg 1 FAK batch filled — direct transition to Filled");
            self.state.leg1_state = OrderState::Filled {
                order_id,
                price,
                size,
                fill_timestamp_ms: now_ms,
            };
            self.init_leg2(price, size, now_ms);
            self.live_trade_meta = LiveTradeMeta::default();

            // Send opportunity alert via Telegram in live mode.
            if let (Some(reporter), Some(signal)) = (
                self.reporter.clone(),
                self.pending_leg1_signal.clone(),
            ) {
                let book = signal
                    .book_snapshot
                    .clone()
                    .or_else(|| self.state.poly_book.clone())
                    .unwrap_or_else(|| OrderBook {
                        asset_id: signal.token_id.clone(),
                        bids: vec![],
                        asks: vec![],
                        timestamp_ms: now_ms,
                    });
                reporter.send_opportunity_alert(&signal, price, size, &book);
                self.live_market_signals += 1;
                if signal.bot_contested {
                    self.live_market_walls += 1;
                }
            }
            return;
        }

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

        // Leg 1: record posted ask for sustain monitoring.
        if !is_leg2 {
            self.state.leg1_posted_ask = Some(price);
        }

        self.replay_pending_fills(now_ms);
    }


    /// Called by the live executor (via feedback channel) when order placement fails.
    /// Resets the affected leg state to `None` so the engine can re-evaluate.
    pub fn on_order_failed(&mut self, is_leg2: bool) {
        // Stale feedback guard: ignore Leg 2 feedback that arrives after the trade
        // has been reset (e.g., stale hedge command processed after trade completed).
        if is_leg2 && !matches!(self.state.leg1_state, OrderState::Filled { .. }) {
            warn!(is_leg2, "ignoring stale Leg 2 OrderFailed — no active Leg 1 fill");
            self.emergency_signal_in_flight = false;
            self.leg2_command_pending = false;
            return;
        }
        self.diag_order_failures += 1;
        if is_leg2 {
            self.emergency_signal_in_flight = false;
            self.leg2_command_pending = false;
            self.state.leg2_state = OrderState::None;
        } else {
            self.state.leg1_state = OrderState::None;
        }
        warn!(is_leg2, "order placement failed — leg state reset to None");
    }

    /// Called when the executor detects "not enough balance / allowance" on a Leg 2
    /// placement. Sends a critical Telegram alert with position details so the
    /// operator knows the position is stuck.
    pub fn on_balance_exhausted(&mut self) {
        if let Some(ref reporter) = self.reporter {
            let dir = match self.leg1_direction {
                Some(Direction::Up) => "YES",
                Some(Direction::Down) => "NO",
                None => "?",
            };
            let (price, size) = match &self.state.leg1_state {
                OrderState::Filled { price, size, .. } => (price.to_string(), size.to_string()),
                _ => ("?".into(), "?".into()),
            };
            reporter.fire_critical(format!(
                "BALANCE EXHAUSTED\nLeg 1: {} @ {} ({})\nLeg 2: HALTED — insufficient balance/allowance\nWaiting for rotation",
                dir, price, size,
            ));
        }
    }

    /// Called when a `CancelResult` feedback arrives from the executor.
    /// If the cancel was confirmed, clears saved info. If NOT confirmed (order
    /// may have filled), restores the Posted state so User WS events can match.
    pub fn on_cancel_result(&mut self, order_id: String, was_cancelled: bool, is_leg2: bool) {
        if is_leg2 {
            self.emergency_signal_in_flight = false;
            self.leg2_command_pending = false;
        }

        if is_leg2 {
            if was_cancelled {
                self.prev_leg2_order = None;
                debug!(%order_id, "Leg 2 cancel confirmed by CLOB");
            } else {
                // NOT cancelled — order may have filled. Restore state so User WS events match.
                let now_ms = now_epoch_ms();
                if let Some((saved_id, price, size)) = self.prev_leg2_order.take()
                    && saved_id == order_id
                {
                    let is_provisional = matches!(
                        &self.state.leg2_state,
                        OrderState::Posted { order_id: oid, .. } if oid.starts_with("sim-")
                    );
                    if is_provisional || matches!(self.state.leg2_state, OrderState::None) {
                        warn!(%order_id, "Leg 2 cancel NOT confirmed — restoring Posted state");
                        self.state.leg2_state = OrderState::Posted {
                            order_id: saved_id,
                            price,
                            size,
                            timestamp_ms: now_ms,
                        };
                        self.live_trade_meta = LiveTradeMeta::default();
                        if let Some(e) = self.hedge.as_mut() {
                            e.emergency_submitted = false;
                            e.exit_reason = None;
                        }
                    }
                } else {
                    debug!(%order_id, "Leg 2 cancel NOT confirmed — no saved state to restore (trade may have completed)");
                }
                self.replay_pending_fills(now_ms);
            }
        } else {
            // Leg 1 cancel result
            if was_cancelled {
                self.leg1_cancel_inflight = false;
                if matches!(self.state.leg1_state, OrderState::Filled { .. }) {
                    // Partial-fill cleanup — trade is actively hedging, don't reset.
                    debug!(%order_id, "Leg 1 partial-fill remainder cancelled — trade continues");
                } else {
                    // Normal sustain-fade cancel — full reset.
                    debug!(%order_id, "Leg 1 cancel confirmed — resetting Leg 1 state");
                    self.state.leg1_state = OrderState::None;
                    self.state.leg1_posted_ask = None;
                    self.leg1_direction = None;
                    self.state.buildup_detected = false;
                    self.state.last_buildup = None;
                    self.pending_leg1_signal = None;
                }
            } else {
                if matches!(self.state.leg1_state, OrderState::Filled { .. }) {
                    // Partial-fill cancel failed — remaining shares may have also filled unhedged.
                    self.leg1_cancel_inflight = false;
                    warn!(%order_id, "partial-fill remainder cancel failed — additional fills may be unhedged");
                } else {
                    // Normal sustain cancel not confirmed — order likely filled.
                    // Leave leg1_cancel_inflight=true so sustain monitor does NOT retry.
                    debug!(%order_id, "Leg 1 cancel NOT confirmed — order filled or gone, no retry");
                }
            }
            let now_ms = now_epoch_ms();
            self.replay_pending_fills(now_ms);
        }
    }

    /// Called when the executor confirms a specific Leg 2 order cancel (dual-order).
    pub fn on_leg2_order_cancel_result(&mut self, order_id: String, was_cancelled: bool) {
        if let Some(ref orphan) = self.post_trade_orphan {
            if orphan.order_id == order_id {
                if was_cancelled {
                    info!(%order_id, "orphan cancel confirmed — clearing orphan state");
                    self.post_trade_orphan = None;
                    self.orphan_cancel_sent = false;
                } else {
                    warn!(%order_id, "orphan cancel NOT confirmed — watching for fill via User WS");
                    // Keep orphan state — may fill via User WS TradeStatusUpdate.
                }
                return;
            }
        }
        // Not an orphan cancel — treat as standard Leg 2 cancel result.
        self.on_cancel_result(order_id, was_cancelled, true);
    }

    /// Called when the executor completes a rebalance FOK (double-fill recovery).
    pub fn on_rebalance_result(
        &mut self,
        success: bool,
        price: Decimal,
        size: Decimal,
        _order_id: Option<String>,
    ) {
        self.rebalance_in_progress = false;
        self.post_trade_orphan = None;
        self.orphan_cancel_sent = false;
        self.diag_rebalance_attempts += 1;

        if success {
            self.diag_rebalance_successes += 1;
            info!(%price, %size, "rebalance FOK succeeded — double-fill recovered");
            if let Some(ref reporter) = self.reporter {
                reporter.fire_critical(format!(
                    "REBALANCE COMPLETED\nDouble-fill recovery: bought {} @ {}\nPair cost (rebalance): {}",
                    size, price, Decimal::ONE - price,
                ));
            }
        } else {
            warn!(%price, %size, "REBALANCE FOK FAILED — naked directional exposure");
            if let Some(ref reporter) = self.reporter {
                reporter.fire_critical(format!(
                    "REBALANCE FAILED\nDouble-fill detected\nRebalance FOK FAILED — naked directional exposure\nManual intervention required",
                ));
            }
        }
    }

    /// Take the pending orphan cancel command (if any).
    /// Called by the main loop after `on_trade_complete()` when `post_trade_orphan` is set.
    pub fn take_orphan_cancel(&mut self) -> Option<ExecutorCommand> {
        let orphan = self.post_trade_orphan.as_ref()?;
        self.orphan_cancel_sent = true;
        Some(ExecutorCommand::CancelLeg2Order {
            order_id: orphan.order_id.clone(),
        })
    }

    /// Take the pending rebalance signal (if any).
    /// Called by the main loop when `rebalance_in_progress` is set.
    pub fn take_rebalance_signal(&mut self) -> Option<ExecutorCommand> {
        if !self.rebalance_in_progress {
            return None;
        }
        let orphan = self.post_trade_orphan.as_ref()?;

        // Build a rebalance signal: buy Leg 1 side (same direction as original entry).
        let (token_id, book) = match orphan.leg1_direction {
            Direction::Up => (
                self.state.active_yes_token_id.clone().unwrap_or_default(),
                self.state
                    .poly_yes_book
                    .clone()
                    .or_else(|| self.state.poly_book.clone()),
            ),
            Direction::Down => (
                self.state.active_no_token_id.clone().unwrap_or_default(),
                self.state
                    .poly_no_book
                    .clone()
                    .or_else(|| self.state.poly_book.clone()),
            ),
        };

        let breakeven_price = Decimal::ONE - orphan.leg1_price;
        let signal = TradeSignal {
            side: Side::Buy,
            token_id,
            condition_id: orphan.condition_id.clone(),
            price: breakeven_price,
            size: orphan.size,
            reference_price: Decimal::ZERO,
            expected_pct: Decimal::ZERO,
            profit_target_tier: ProfitTier::Low,
            profit_target_pct: Decimal::ZERO,
            alloc_amount: Decimal::ZERO,
            direction: orphan.leg1_direction,
            spike_info: self.state.last_buildup.as_ref().map(|b| SpikeInfo {
                direction: b.direction,
                magnitude: b.composite_score,
                sustained_ms: 0,
                timestamp_ms: b.timestamp_ms,
                atr_ratio: b.signal_atr_ratio,
                obi: b.obi,
            }).unwrap_or(SpikeInfo {
                direction: orphan.leg1_direction,
                magnitude: Decimal::ZERO,
                sustained_ms: 0,
                timestamp_ms: 0,
                atr_ratio: Decimal::ZERO,
                obi: Decimal::ZERO,
            }),
            is_leg2: true,
            leg1_fill_price: Some(orphan.leg1_price),
            entry_timestamp_ms: now_epoch_ms(),
            market_end_timestamp_ms: self.state.market_end_timestamp_ms,
            tick_size: orphan.tick_size,
            exit_reason: None,
            atr: Decimal::ZERO,
            bot_contested: false,
            leg1_fee: Decimal::ZERO,
            best_ask: None,
            book_snapshot: book,
            buildup_info: None,
        };

        Some(ExecutorCommand::RebalanceLeg1 { signal })
    }

    /// Replay buffered TradeStatusUpdate events that didn't match any leg when
    /// they first arrived. Called after state changes (OrderPosted, CancelResult)
    /// that may make previously-unmatched events matchable.
    fn replay_pending_fills(&mut self, now_ms: u64) {
        if self.pending_fills.is_empty() {
            return;
        }

        let fills: Vec<(String, TradeStatus, Option<Decimal>, Option<Decimal>)> = self.pending_fills.drain(..).collect();
        for (order_id, status, size_matched, original_size) in fills {
            let is_leg1 = matches!(
                &self.state.leg1_state,
                OrderState::Posted { order_id: oid, .. } if *oid == order_id
            );
            let is_leg2 = matches!(
                &self.state.leg2_state,
                OrderState::Posted { order_id: oid, .. } if *oid == order_id
            );

            if is_leg1 {
                info!(%order_id, ?status, "replaying buffered fill for Leg 1");
                let (price, size) = match &self.state.leg1_state {
                    OrderState::Posted { price, size, .. } => (*price, *size),
                    _ => unreachable!(),
                };
                match status {
                    TradeStatus::Matched | TradeStatus::Mined | TradeStatus::Confirmed => {
                        if let (Some(matched), Some(original)) = (size_matched, original_size) {
                            if matched < original {
                                warn!(%order_id, %matched, %original, "partial fill on Leg 1 (replay) — deferring alert to MINED");
                                self.pending_partial_fills.insert(order_id.clone(), PendingPartialFill {
                                    leg: "Leg 1", size_matched: matched, original_size: original,
                                });
                            }
                        }
                        let fill_size = size.round_dp(2);
                        self.state.leg1_state = OrderState::Filled {
                            order_id,
                            price,
                            size: fill_size,
                            fill_timestamp_ms: now_ms,
                        };
                        self.init_leg2(price, fill_size, now_ms);
                        self.live_trade_meta = LiveTradeMeta::default();

                        // Send opportunity alert for replayed Leg 1 fill.
                        if let (Some(reporter), Some(signal)) = (
                            self.reporter.clone(),
                            self.pending_leg1_signal.clone(),
                        ) {
                            let book = signal
                                .book_snapshot
                                .clone()
                                .or_else(|| self.state.poly_book.clone())
                                .unwrap_or_else(|| OrderBook {
                                    asset_id: signal.token_id.clone(),
                                    bids: vec![],
                                    asks: vec![],
                                    timestamp_ms: now_ms,
                                });
                            reporter.send_opportunity_alert(&signal, price, size, &book);
                            self.live_market_signals += 1;
                            if signal.bot_contested {
                                self.live_market_walls += 1;
                            }
                        }
                    }
                    TradeStatus::Failed => {
                        self.state.leg1_state = OrderState::None;
                    }
                    TradeStatus::Canceled => {
                        if self.leg1_cancel_inflight {
                            // Cancel in flight — defer cleanup to cancel result handler.
                            debug!(%order_id, "replayed Leg 1 CANCELED (cancel in flight) — deferring to cancel result");
                        } else {
                            warn!(%order_id, "replayed Leg 1 CANCELED unexpectedly — resetting");
                            self.state.leg1_state = OrderState::None;
                            self.pending_leg1_signal = None;
                        }
                    }
                    TradeStatus::Retrying => {}
                }
            } else if is_leg2 {
                info!(%order_id, ?status, "replaying buffered fill for Leg 2");
                let (price, size) = match &self.state.leg2_state {
                    OrderState::Posted { price, size, .. } => (*price, *size),
                    _ => unreachable!(),
                };
                match status {
                    TradeStatus::Matched | TradeStatus::Mined | TradeStatus::Confirmed => {
                        if let (Some(matched), Some(original)) = (size_matched, original_size) {
                            if matched < original {
                                warn!(%order_id, %matched, %original, "partial fill on Leg 2 (replay) — deferring alert to MINED");
                                self.pending_partial_fills.insert(order_id.clone(), PendingPartialFill {
                                    leg: "Leg 2", size_matched: matched, original_size: original,
                                });
                            }
                        }
                        self.state.leg2_state = OrderState::Filled {
                            order_id,
                            price,
                            size,
                            fill_timestamp_ms: now_ms,
                        };
                        // hedge cleared by on_trade_complete() after Telegram + recording
                    }
                    TradeStatus::Failed => {
                        self.state.leg2_state = OrderState::None;
                    }
                    TradeStatus::Canceled => {
                        self.state.leg2_state = OrderState::None;
                        self.prev_leg2_order = None;
                    }
                    TradeStatus::Retrying => {}
                }
            } else {
                // Still unmatched after replay — re-buffer.
                if self.pending_fills.len() < 8 {
                    self.pending_fills.push_back((order_id, status, size_matched, original_size));
                }
            }
        }
    }

    /// Record a completed live trade to QuestDB's `executed_trades` table.
    ///
    /// Call this when both legs are `Filled` — right before `on_trade_complete()`
    /// resets state. All required fields are extracted from engine state:
    /// `leg1_state`, `leg2_state`, `hedge`, `pending_leg1_signal`, `leg1_direction`.
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

        // Extract hedge metadata (if available).
        let (
            expected_pct,
            profit_tier,
            hedge_phase,
            exit_reason,
            alloc_amount,
            bot_contested,
            emergency_submitted,
            spike_magnitude,
        ) = match (&self.hedge, &self.pending_leg1_signal) {
            (Some(h), Some(sig)) => (
                h.expected_pct,
                h.tier.label(),
                h.phase as u8,
                h.exit_reason,
                sig.alloc_amount,
                sig.bot_contested,
                h.emergency_submitted,
                h.spike_info.magnitude,
            ),
            (Some(h), None) => (
                h.expected_pct,
                h.tier.label(),
                h.phase as u8,
                h.exit_reason,
                Decimal::ZERO,
                false,
                h.emergency_submitted,
                h.spike_info.magnitude,
            ),
            _ => (
                Decimal::ZERO,
                "LOW",
                0u8,
                None,
                Decimal::ZERO,
                false,
                false,
                Decimal::ZERO,
            ),
        };

        let leg2_was_taker = exit_reason.is_some();
        let favorable_taker = exit_reason == Some(ExitReason::FavorableTaker);
        // In live mode, we can't distinguish maker vs taker fills during emergency chase
        // (no executor feedback). Conservative: only true if emergency entered but no exit_reason
        // was set (meaning the maker order filled before FOK deadline).
        let emergency_maker = emergency_submitted && exit_reason.is_none();

        // Taker fee: CLOB deducts fees automatically; the REST response and User WS
        // do not return the actual amount charged. Recorded as zero in QuestDB.
        let taker_fee = Decimal::ZERO;
        // Leg 1 is always maker (post-only) — estimate rebate. Leg 2 maker rebate if not taker.
        let leg1_rebate = crate::executor::fill_engine::compute_maker_rebate(l1_price, l1_size);
        let leg2_rebate = if !leg2_was_taker {
            crate::executor::fill_engine::compute_maker_rebate(l2_price, l2_size)
        } else {
            Decimal::ZERO
        };
        let maker_rebate = leg1_rebate + leg2_rebate;

        let net_profit = gross_profit - taker_fee + maker_rebate;
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
            expected_pct,
            profit_tier,
            alloc_amount,
            hedge_phase,
            leg2_was_taker,
            bot_contested,
            l1_order_id,
            Some(l2_order_id),
            l1_fill_ts,
            exit_reason,
            favorable_taker,
            emergency_maker,
            spike_magnitude,
            maker_rebate,
        )
    }

    /// Called in live mode when both legs are filled (detected in main engine loop).
    /// Replicates the trade completion logic from `advance_simulation()`.
    pub fn on_trade_complete(&mut self) {
        let now_ms = now_epoch_ms();

        // Extract fill data before clearing state.
        let l1_data = match &self.state.leg1_state {
            OrderState::Filled { price, size, fill_timestamp_ms, .. } => {
                Some((*price, *size, *fill_timestamp_ms))
            }
            _ => None,
        };
        let l2_data = match &self.state.leg2_state {
            OrderState::Filled { price, size, .. } => Some((*price, *size)),
            _ => None,
        };

        if let (Some((l1_price, l1_size, l1_ts)), Some((l2_price, l2_size))) = (l1_data, l2_data) {
            let pair_cost = l1_price + l2_price;
            let net_profit = Decimal::ONE - pair_cost;
            info!(
                %l1_price, %l2_price,
                %pair_cost, %net_profit,
                "live trade pair complete — resetting for next trade"
            );

            // Build LiveTradeReport and send Telegram messages if reporter is set.
            if self.reporter.is_some() {
                if let Some(trade) =
                    self.build_live_sim_trade(l1_price, l1_size, l1_ts, l2_price, l2_size, now_ms)
                {
                    if let Some(ref reporter) = self.reporter.clone() {
                        reporter.send_trade_completed(&trade);
                    }
                    self.live_market_trades.push(trade.clone());
                    self.live_session_trades.push(trade);
                }
            }
        }

        self.in_trade_cooldown = true;
        self.last_trade_complete_ms = now_ms;

        self.state.leg1_state = OrderState::None;
        self.state.leg2_state = OrderState::None;
        self.hedge = None;
        self.leg1_direction = None;
        self.pending_leg1_signal = None;
        self.pending_tick_size_cmd = None;
        self.pending_leg1_cancel = None;
        self.pending_heartbeat_cancel = None;
        self.emergency_signal_in_flight = false;
        self.leg2_command_pending = false;
        self.prev_leg2_order = None;
        self.pending_fills.clear();
        self.state.leg1_posted_ask = None;
        self.leg1_cancel_inflight = false;
        // Dual-order tracking: clear IDs but keep post_trade_orphan (may fill after trade reset).
        self.leg2_phase1_order_id = None;
        self.leg2_phase2_order_id = None;
        self.last_leg2_was_phase2_alongside = false;
        // post_trade_orphan deliberately NOT cleared — survives trade reset.
        // cumulative_used is NOT reset — capital stays allocated within this market.
    }

    // ─── Simulation helpers ────────────────────────────────────────────────

    /// Initialize hedge state after a Leg 1 fill.
    ///
    /// Reused by both `TradeStatusUpdate` handler (live mode) and
    /// `advance_simulation()` (simulation mode).
    fn init_leg2(&mut self, fill_price: Decimal, fill_size: Decimal, now_ms: u64) {
        if let Some(buildup) = &self.state.last_buildup {
            let (conf, tier, initial_profit_target) = if let Some(sig) = &self.pending_leg1_signal {
                (sig.expected_pct, sig.profit_target_tier, sig.profit_target_pct)
            } else {
                // Fallback: recompute from buildup (rare path)
                let t_secs = self.state.time_remaining_ms(now_ms) / 1_000;
                let yes_mid = self
                    .state
                    .poly_yes_book
                    .as_ref()
                    .or(self.state.poly_book.as_ref())
                    .and_then(|b| {
                        let bid = b.best_bid()?.price;
                        let ask = b.best_ask()?.price;
                        Some((bid + ask) / Decimal::TWO)
                    })
                    .unwrap_or(Decimal::new(5, 1));
                let c = compute_expected_repricing(
                    buildup.composite_score,
                    Decimal::ZERO,  // min_strength (not used meaningfully with composite)
                    Decimal::ONE,   // strong_strength
                    yes_mid,
                    buildup.direction,
                    t_secs,
                    self.leg1.reprice_scale,
                    self.leg1.time_exponent,
                    self.leg1.max_time_factor,
                );
                let t = ProfitTier::from_expected_reprice(c, self.leg1.reprice_scale);
                let tick = self.state.tick_size;
                let target = round_to_tick(c * self.leg1.phase1_target_dampen, tick);
                (c, t, target)
            };
            let direction = self.leg1_direction.unwrap_or(buildup.direction);
            let tick = self.state.tick_size;
            let phase1_target_price = round_to_tick(
                Decimal::ONE - initial_profit_target - fill_price,
                tick,
            );
            let leg1_fee = self.pending_leg1_signal.as_ref()
                .map(|s| s.leg1_fee)
                .unwrap_or(Decimal::ZERO);
            self.hedge = Some(HedgeState::new(
                now_ms,
                fill_price,
                leg1_fee,
                tier,
                initial_profit_target,
                direction,
                SpikeInfo {
                    direction,
                    magnitude: buildup.composite_score,
                    sustained_ms: 0,
                    timestamp_ms: buildup.timestamp_ms,
                    atr_ratio: buildup.signal_atr_ratio,
                    obi: buildup.obi,
                },
                conf,
                phase1_target_price,
            ));
            info!(tier = tier.label(), %fill_price, %fill_size, %phase1_target_price, "leg2 hedge initialised");
        } else {
            warn!("Leg 1 filled but no buildup info — hedge not initialised");
        }
    }

    /// Advance simulation state: simulate Leg 1 and Leg 2 fills based on
    /// current orderbook conditions. Test-only — not called in live mode.
    ///
    /// Returns confirmed fill signals for the
    /// executor to record directly. Usually 0 or 1 items per call.
    #[cfg(test)]
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

            let tick = self.state.tick_size;
            let two_ticks = tick * Decimal::TWO;

            // Leg 1 fill check: maker post-only — fills when ask is within two ticks of
            // our bid price (ask has crossed or nearly crossed our level).
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
                            // Maker: fills when ask drops to within two ticks of our bid
                            let maker_valid = ask.is_some_and(|a| a <= fill_price + two_ticks);
                            let near_ask_depth: Decimal = book
                                .asks
                                .iter()
                                .filter(|lvl| lvl.price <= fill_price + two_ticks)
                                .map(|lvl| lvl.size)
                                .sum();
                            maker_valid && near_ask_depth > Decimal::ZERO
                        }
                        None => false,
                    }
                }
                Some(Direction::Down) => match self.state.poly_no_book.as_ref() {
                    Some(book) => {
                        let ask = book.best_ask().map(|a| a.price);
                        // Maker: fills when ask drops to within two ticks of our bid
                        let maker_valid = ask.is_some_and(|a| a <= fill_price + two_ticks);
                        let near_ask_depth: Decimal = book
                            .asks
                            .iter()
                            .filter(|lvl| lvl.price <= fill_price + two_ticks)
                            .map(|lvl| lvl.size)
                            .sum();
                        maker_valid && near_ask_depth > Decimal::ZERO
                    }
                    None => false,
                },
                None => false,
            };

            if should_fill {
                self.diag_leg1_fills += 1;
                info!(
                    %fill_price, %fill_size,
                    "advance_simulation: Leg 1 simulated fill (FAK taker at ask)"
                );
                if let Some(sig) = self.pending_leg1_signal.take() {
                    signals.push(sig);
                }
                self.state.leg1_state = OrderState::Filled {
                    order_id: format!("sim-leg1-{}", timestamp_ms),
                    price: fill_price,
                    size: fill_size,
                    fill_timestamp_ms: now_ms,
                };
                self.init_leg2(fill_price, fill_size, now_ms);
            }
        }

        // ── Leg 2: Posted → Filled ─────────────────────────────────────
        if let OrderState::Posted { price, size, .. } = &self.state.leg2_state {
            let posted_price = *price;
            let posted_size = *size;

            let is_fok = self.hedge.as_ref().is_some_and(|e| e.fok_emitted);

            // Determine fill outcome: (should_fill, is_favorable_taker, fill_price, sim_was_taker).
            // FOK orders fill immediately at ask (taker). Non-FOK use normal maker fill logic:
            //   ask < posted_price → favorable taker fill at ask_price
            //   ask == posted_price → normal maker fill at posted_price
            //   ask > posted_price or no ask → no fill (order rests)
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

            // Dual-order: also check if Phase 1 target price would fill (when in Phase 2).
            let phase1_fill = if !is_fok {
                if let Some(hedge) = &self.hedge {
                    if hedge.phase == HedgePhase::Phase2 {
                        let p1_price = hedge.phase1_target_price;
                        match best_ask {
                            Some(ask) if ask <= p1_price => Some((p1_price, false)),
                            _ => None,
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };

            let (should_fill, is_favorable_taker, fill_price, sim_was_taker) = if is_fok {
                // FOK orders fill immediately at ask (taker).
                match best_ask {
                    Some(ask) => (true, false, ask, true),
                    None => (false, false, posted_price, false),
                }
            } else if let Some((p1_fill_price, _)) = phase1_fill {
                // Phase 1 order fills first (better price for us = lower fill price).
                // Compare: which fills? Phase 1 fills if ask <= p1_target.
                // Phase 2 fills if ask <= p2_posted. Pick the one with lower price (better).
                match best_ask {
                    Some(ask) if ask < posted_price => {
                        // Both would fill — Phase 2 is favorable taker (ask < posted).
                        // Pick Phase 1 if its price is lower (better for us).
                        if p1_fill_price <= ask {
                            (true, false, p1_fill_price, false) // Phase 1 maker fill
                        } else {
                            (true, true, ask, false) // Phase 2 favorable taker
                        }
                    }
                    Some(ask) if ask <= posted_price => {
                        // Phase 2 would fill at posted, Phase 1 also fills.
                        if p1_fill_price <= posted_price {
                            (true, false, p1_fill_price, false) // Phase 1 fills (better price)
                        } else {
                            (true, false, posted_price, false) // Phase 2 fills
                        }
                    }
                    _ => {
                        // Phase 2 wouldn't fill, but Phase 1 does.
                        (true, false, p1_fill_price, false)
                    }
                }
            } else {
                match best_ask {
                    Some(ask) if ask < posted_price => (true, true, ask, false),
                    Some(ask) if ask <= posted_price => (true, false, posted_price, false),
                    _ => (false, false, posted_price, false),
                }
            };

            let is_emergency = self.hedge.as_ref().is_some_and(|e| e.emergency_submitted);

            if should_fill {
                if sim_was_taker {
                    self.diag_leg2_fills_taker += 1;
                } else {
                    self.diag_leg2_fills_maker += 1;
                }
                if is_emergency {
                    if sim_was_taker {
                        self.diag_emg_taker += 1;
                    } else {
                        self.diag_emg_maker += 1;
                    }
                }
                info!(
                    %posted_price, %posted_size, %fill_price, is_emergency, is_favorable_taker, sim_was_taker,
                    "advance_simulation: Leg 2 simulated fill"
                );
                // Build signal BEFORE setting Filled state — hedge is still valid.
                if let Some(mut sig) =
                    self.build_sim_leg2_fill_signal(fill_price, posted_size, now_ms)
                {
                    if is_favorable_taker {
                        sig.exit_reason = Some(ExitReason::FavorableTaker);
                    }
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
            self.hedge = None;
            self.leg1_direction = None;
            // cumulative_used is NOT reset — capital stays allocated within this market.
        }

        signals
    }

    /// Build a Leg 2 fill [`TradeSignal`] for routing to the `SimulationExecutor`.
    ///
    /// Uses the current hedge state and hedge book to construct a confirmed fill
    /// signal. Reads `exit_reason` from hedge state (set on emergency) so the
    /// executor can categorize the exit correctly.
    /// Returns `None` if hedge state or hedge token ID is unavailable.
    fn build_sim_leg2_fill_signal(
        &self,
        price: Decimal,
        size: Decimal,
        now_ms: u64,
    ) -> Option<TradeSignal> {
        let hedge = self.hedge.as_ref()?;
        let exit_reason = if hedge.emergency_submitted {
            hedge.exit_reason
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
        let signal = make_leg2_signal(
            &hedge_token_id,
            self.state.active_condition_id.as_deref().unwrap_or(""),
            price,
            size,
            self.state.binance_price.unwrap_or(Decimal::ZERO),
            hedge.expected_pct,
            hedge.tier,
            hedge.initial_profit_target,
            hedge.direction,
            hedge.spike_info,
            hedge.leg1_fill_price,
            now_ms,
            self.state.market_end_timestamp_ms,
            self.state.tick_size,
            self.state.atr.unwrap_or(Decimal::ZERO),
            false,
            None,
            hedge_book,
            exit_reason,
        );
        Some(signal)
    }

    // ─── Live mode Telegram reporting ────────────────────────────────────────

    /// Attach a Telegram reporter for live mode reporting.
    /// Sets `live_session_start_ms` to now so uptime is tracked from bot startup.
    pub fn set_reporter(&mut self, reporter: TelegramReporter) {
        self.reporter = Some(reporter);
        self.live_session_start_ms = now_epoch_ms();
    }

    /// Send a full market summary via Telegram for the current market.
    ///
    /// Called by main.rs just before `MarketRotation` is sent to the executor,
    /// so counters still reflect the outgoing market.
    pub fn send_live_market_summary(&self) {
        let reporter = match &self.reporter {
            Some(r) => r,
            None => return,
        };

        let condition_id = match &self.state.active_condition_id {
            Some(id) => id.as_str(),
            None => return,
        };

        let market_end_ms = self.state.market_end_timestamp_ms;
        let end_secs = market_end_ms / 1_000;
        let market_duration_secs = 300u64;
        let start_secs = end_secs.saturating_sub(market_duration_secs);
        let start_h = (start_secs / 3600) % 24;
        let start_m = (start_secs % 3600) / 60;
        let end_h = (end_secs / 3600) % 24;
        let end_m = (end_secs % 3600) / 60;
        let period_label = format!(
            "{:02}:{:02} - {:02}:{:02} UTC",
            start_h, start_m, end_h, end_m
        );

        let trades: Vec<LiveTradeReport> = self
            .live_market_trades
            .iter()
            .filter(|t| t.market_id == condition_id)
            .cloned()
            .collect();

        let leg1_fills = trades.len() as u32;
        let trades_hedged = trades.iter().filter(|t| t.leg2.is_some()).count() as u32;
        let emergency_taker_fills = trades.iter().filter(|t| t.leg2_was_taker).count() as u32;
        let emergency_maker_fills = trades.iter().filter(|t| t.emergency_maker).count() as u32;
        let favorable_taker_fills = trades.iter().filter(|t| t.favorable_taker).count() as u32;
        let favorable_maker_fills = trades.iter().filter(|t| t.favorable_maker).count() as u32;

        let mut allocation_used = Decimal::ZERO;
        let mut taker_fees_paid = Decimal::ZERO;
        let mut maker_rebates_earned = Decimal::ZERO;
        let mut gross_market_pnl = Decimal::ZERO;
        let mut net_market_pnl = Decimal::ZERO;
        let mut capital_locked = Decimal::ZERO;
        for t in &trades {
            allocation_used += t.alloc_amount;
            taker_fees_paid += t.taker_fee;
            maker_rebates_earned += t.maker_rebate;
            gross_market_pnl += t.gross_profit;
            net_market_pnl += t.net_profit;
            capital_locked += t.pair_cost * t.leg1.size;
        }

        let summary = MarketSummary {
            market_id: condition_id.to_owned(),
            period_label,
            resolution: "pending".to_owned(),
            uma_hours_remaining: Some(2),
            signals_detected: self.live_market_signals,
            leg1_fills,
            trades_hedged,
            total_trades: leg1_fills,
            walls_outbid: self.live_market_walls,
            emergency_taker_fills,
            emergency_maker_fills,
            favorable_taker_fills,
            favorable_maker_fills,
            trades,
            allocation_used,
            allocation_cap: self.leg1.max_alloc_per_trade,
            taker_fees_paid,
            maker_rebates_earned,
            gross_market_pnl,
            net_market_pnl,
            capital_locked,
        };

        reporter.send_market_summary(&summary);
    }

    /// Send a session summary via Telegram.
    ///
    /// Called by main.rs on graceful shutdown in live mode.
    pub fn send_live_session_summary(&self) {
        // Persist current market's condition ID if we traded in it.
        if !self.live_market_trades.is_empty()
            && let Some(ref cid) = self.state.active_condition_id
        {
            crate::control::wallet::append_condition_id_sync(cid);
        }

        let reporter = match &self.reporter {
            Some(r) => r,
            None => return,
        };

        let now_ms = now_epoch_ms();
        let uptime_secs = now_ms.saturating_sub(self.live_session_start_ms) / 1_000;

        let trades = &self.live_session_trades;
        let total_trades = trades.len() as u32;
        let leg1_fills = total_trades;
        let trades_hedged = trades.iter().filter(|t| t.leg2.is_some()).count() as u32;
        let signals_detected = self.diag_leg1_signals as u32;
        let unfilled_post_only = (self.diag_leg1_signals as u32).saturating_sub(leg1_fills);

        let mut high_count: u32 = 0;
        let mut med_count: u32 = 0;
        let mut low_count: u32 = 0;
        let mut high_alloc_sum = Decimal::ZERO;
        let mut med_alloc_sum = Decimal::ZERO;
        let mut low_alloc_sum = Decimal::ZERO;
        let mut reprice_sum = Decimal::ZERO;
        let mut gross_pnl = Decimal::ZERO;
        let mut taker_fees = Decimal::ZERO;
        let mut maker_rebates = Decimal::ZERO;
        let mut profit_pct_sum = Decimal::ZERO;
        let mut best_pct = Decimal::MIN;
        let mut best_market = String::new();
        let mut best_reprice = Decimal::ZERO;
        let mut worst_pct = Decimal::MAX;
        let mut worst_market = String::new();
        let mut worst_reprice = Decimal::ZERO;
        let mut walls_outbid: u32 = 0;
        let mut breach_fok: u32 = 0;
        let mut timeout_fok: u32 = 0;
        let mut emergency_taker: u32 = 0;
        let mut emergency_maker: u32 = 0;
        let mut favorable_taker: u32 = 0;
        let mut favorable_maker: u32 = 0;

        for t in trades {
            gross_pnl += t.gross_profit;
            taker_fees += t.taker_fee;
            maker_rebates += t.maker_rebate;
            reprice_sum += t.expected_pct;
            profit_pct_sum += t.profit_pct;
            if t.bot_contested { walls_outbid += 1; }
            if t.leg2_was_taker { emergency_taker += 1; }
            if t.emergency_maker { emergency_maker += 1; }
            if t.favorable_taker { favorable_taker += 1; }
            if t.favorable_maker { favorable_maker += 1; }
            match t.exit_reason {
                Some(ExitReason::BreakEvenBreach)
                | Some(ExitReason::Phase2PriceBreach)
                | Some(ExitReason::Phase1Breach) => {
                    breach_fok += 1;
                }
                Some(ExitReason::Phase2Timeout)
                | Some(ExitReason::MarketExpiry) => timeout_fok += 1,
                _ => {}
            }
            match t.profit_target_tier {
                ProfitTier::High => { high_count += 1; high_alloc_sum += t.alloc_amount; }
                ProfitTier::Med => { med_count += 1; med_alloc_sum += t.alloc_amount; }
                ProfitTier::Low => { low_count += 1; low_alloc_sum += t.alloc_amount; }
            }
            if t.profit_pct > best_pct {
                best_pct = t.profit_pct;
                best_market = t.market_id.clone();
                best_reprice = t.expected_pct;
            }
            if t.profit_pct < worst_pct {
                worst_pct = t.profit_pct;
                worst_market = t.market_id.clone();
                worst_reprice = t.expected_pct;
            }
        }

        if total_trades == 0 {
            best_pct = Decimal::ZERO;
            worst_pct = Decimal::ZERO;
        }

        let net_pnl = gross_pnl - taker_fees + maker_rebates;
        let avg_expected_pct = if total_trades > 0 {
            reprice_sum / Decimal::from(total_trades)
        } else {
            Decimal::ZERO
        };
        let avg_net_profit_pct = if total_trades > 0 {
            profit_pct_sum / Decimal::from(total_trades)
        } else {
            Decimal::ZERO
        };
        let win_rate_pct = if total_trades > 0 {
            let wins = trades.iter().filter(|t| t.net_profit > Decimal::ZERO).count() as u64;
            Decimal::from(wins) / Decimal::from(total_trades) * Decimal::ONE_HUNDRED
        } else {
            Decimal::ZERO
        };

        let summary = SessionSummary {
            uptime_secs,
            markets_observed: self.diag_markets_rotated as u32,
            signals_detected,
            leg1_fills,
            trades_hedged,
            total_trades,
            walls_outbid,
            breach_fok,
            timeout_fok,
            emergency_taker_fills: emergency_taker,
            emergency_maker_fills: emergency_maker,
            favorable_taker_fills: favorable_taker,
            favorable_maker_fills: favorable_maker,
            high_tier_trades: high_count,
            high_tier_avg_alloc: if high_count > 0 { high_alloc_sum / Decimal::from(high_count) } else { Decimal::ZERO },
            med_tier_trades: med_count,
            med_tier_avg_alloc: if med_count > 0 { med_alloc_sum / Decimal::from(med_count) } else { Decimal::ZERO },
            low_tier_trades: low_count,
            low_tier_avg_alloc: if low_count > 0 { low_alloc_sum / Decimal::from(low_count) } else { Decimal::ZERO },
            avg_expected_pct,
            gross_pnl,
            emergency_taker_fees: taker_fees,
            est_maker_rebates: maker_rebates,
            net_pnl,
            win_rate_pct,
            avg_net_profit_pct,
            best_trade_pct: best_pct,
            best_trade_market: best_market,
            best_trade_reprice: best_reprice,
            worst_trade_pct: worst_pct,
            worst_trade_market: worst_market,
            worst_trade_reprice: worst_reprice,
            unfilled_signals: unfilled_post_only,
            unfilled_post_only,
            unfilled_liquidity: 0,
            unfilled_spread_wide: 0,
            capital_locked: Decimal::ZERO,
            virtual_balance: Decimal::ZERO,
            starting_balance: Decimal::ZERO,
        };

        reporter.send_session_summary(&summary);
    }

    /// Build a `LiveTradeReport` from live fill data for Telegram reporting.
    ///
    /// Returns `None` if required state (hedge or pending signal) is unavailable.
    fn build_live_sim_trade(
        &self,
        l1_price: Decimal,
        l1_size: Decimal,
        l1_ts: u64,
        l2_price: Decimal,
        l2_size: Decimal,
        now_ms: u64,
    ) -> Option<LiveTradeReport> {
        use crate::executor::fill_engine::{compute_taker_fee, compute_maker_rebate};

        let hedge = self.hedge.as_ref()?;
        let signal = self.pending_leg1_signal.as_ref()?;

        let pair_cost = l1_price + l2_price;
        let paired_size = l1_size.min(l2_size);
        let gross_profit = (Decimal::ONE - pair_cost) * paired_size;
        // Leg 1 is always maker (post-only) — rebate, no taker fee.
        let leg1_rebate = compute_maker_rebate(l1_price, l1_size);
        let leg2_taker_fee = if self.live_trade_meta.leg2_was_taker {
            compute_taker_fee(l2_price, l2_size)
        } else {
            Decimal::ZERO
        };
        let taker_fee = leg2_taker_fee;
        // Leg 2 rebate only if maker fill.
        let leg2_rebate = if self.live_trade_meta.leg2_was_taker {
            Decimal::ZERO
        } else {
            compute_maker_rebate(l2_price, l2_size)
        };
        let maker_rebate = leg1_rebate + leg2_rebate;
        let net_profit = gross_profit - taker_fee + maker_rebate;
        let total_cost = l1_price * l1_size + l2_price * l2_size;
        let profit_pct = if total_cost.is_zero() {
            Decimal::ZERO
        } else {
            net_profit / total_cost * Decimal::ONE_HUNDRED
        };
        let sizes_differ = l1_size != l2_size;

        let leg1_side = Side::Buy;
        let leg2_side = leg1_side;

        let leg1_fill = FillInfo {
            side: leg1_side,
            price: l1_price,
            size: l1_size,
            timestamp_ms: l1_ts,
            was_partial: sizes_differ && l1_size > l2_size,
            was_taker: false,
            taker_fee: Decimal::ZERO,
            maker_rebate: leg1_rebate,
        };
        let leg2_fill = FillInfo {
            side: leg2_side,
            price: l2_price,
            size: l2_size,
            timestamp_ms: now_ms,
            was_partial: sizes_differ && l2_size > l1_size,
            was_taker: self.live_trade_meta.leg2_was_taker,
            taker_fee: leg2_taker_fee,
            maker_rebate: leg2_rebate,
        };

        Some(LiveTradeReport {
            market_id: signal.condition_id.clone(),
            direction: hedge.direction,
            leg1: leg1_fill,
            leg2: Some(leg2_fill),
            expected_pct: hedge.expected_pct,
            profit_target_tier: hedge.tier,
            alloc_amount: signal.alloc_amount,
            pair_cost,
            gross_profit,
            taker_fee,
            maker_rebate,
            net_profit,
            profit_pct,
            hedge_phase: hedge.phase as u8,
            leg2_was_taker: self.live_trade_meta.leg2_was_taker,
            bot_contested: signal.bot_contested,
            favorable_taker: self.live_trade_meta.favorable_taker,
            favorable_maker: self.live_trade_meta.favorable_maker,
            emergency_maker: self.live_trade_meta.emergency_maker,
            exit_reason: self.live_trade_meta.exit_reason,
            spike_magnitude: hedge.spike_info.magnitude,
            leg1_cancel_race: false,
            phase1_breach: self.live_trade_meta.phase1_breach,
            phase1_dual_fill: self.live_trade_meta.phase1_dual_fill,
            whipsaw_reversal: matches!(self.live_trade_meta.exit_reason, Some(ExitReason::WhipsawReversal)),
            open_timestamp_ms: l1_ts,
            close_timestamp_ms: now_ms,
        })
    }

    // ─── Drain & Status ───────────────────────────────────────────────────

    /// Enter drain mode: block new Leg 1 entries, let Leg 2 continue.
    pub fn set_draining(&mut self) {
        self.draining = true;
    }

    /// Pause trading: block new Leg 1 entries, keep connections alive.
    pub fn set_paused(&mut self, v: bool) {
        self.paused = v;
    }

    /// Returns `true` if trading is paused.
    #[allow(dead_code)] // public API for /status and future use
    pub fn is_paused(&self) -> bool {
        self.paused
    }

    /// Reset Leg 1 state to `None` and clear associated tracking fields.
    pub fn reset_leg1_state(&mut self) {
        self.state.leg1_state = OrderState::None;
        self.pending_leg1_signal = None;
        self.leg1_direction = None;
        self.state.buildup_detected = false;
        self.state.last_buildup = None;
        self.state.leg1_posted_ask = None;
        self.leg1_cancel_inflight = false;
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
            leg1_state: leg1_str,
            leg2_state: leg2_str,
            spikes_received: self.diag_buildups_received,
            signals_emitted: self.diag_leg1_signals,
            trades_completed: self.diag_leg2_fills_maker + self.diag_leg2_fills_taker,
            trades_enabled: true,  // updated by main loop from NotifyFlags
            summary_enabled: true, // updated by main loop from NotifyFlags
            draining: self.draining,
            paused: self.paused,
            heartbeat_healthy: self.connectivity.heartbeat_healthy,
            heartbeat_failures: self.connectivity.consecutive_heartbeat_failures,
            heartbeat_latency_ms: self.connectivity.last_heartbeat_latency_ms,
        }
    }
}

#[cfg(test)]
impl Default for StrategyEngine {
    fn default() -> Self {
        Self::new(&Config::test_defaults())
    }
}


#[cfg(test)]
#[path = "tests/strategy_tests.rs"]
mod tests;
