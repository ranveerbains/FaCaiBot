mod config;
mod engine;
mod executor;
mod gateway;
mod reporting;
mod storage;
mod types;
mod utils;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use anyhow::Result;
use crossbeam_channel::{Receiver, Sender, bounded};
use tracing::{debug, error, info, warn};

use crate::config::{Config, Mode};
use crate::engine::strategy::StrategyEngine;
use crate::executor::live::LiveExecutor;
use crate::executor::simulation::SimulationExecutor;
use crate::gateway::binance::BinanceGateway;
use crate::gateway::polymarket::PolymarketGateway;
use crate::gateway::polymarket::PolymarketWsGateway;
use crate::reporting::telegram::TelegramReporter;
use crate::storage::cold::ColdStorage;
use crate::types::market::OrderState;
use crate::types::order::ExecutorFeedback;
use crate::types::{ExecutorCommand, IngestorEvent};

/// Channel capacity between layers. Sized to absorb burst without back-pressure.
const CHANNEL_CAP: usize = 8192;

/// Minimum interval (ms) between recording Binance ticks to QuestDB.
/// The engine processes every tick for real-time trading decisions, but QuestDB
/// only needs periodic snapshots for analytics. 1 tick/sec keeps 7-day storage
/// under ~600K rows (~30MB) instead of ~18-30M rows at full SBE rate.
const TICK_RECORD_INTERVAL_MS: u64 = 1_000;

fn main() -> Result<()> {
    // ── Bootstrap ────────────────────────────────────────────────────
    // Install the ring crypto provider process-wide before any TLS connections.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "facaibot=info".parse().unwrap()),
        )
        .with_target(true)
        .init();

    // Build the main tokio runtime with 2 worker threads pinned to cores 1-2.
    // Core 0 is reserved for the ingestor (dedicated OS thread).
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .on_thread_start(|| {
            let core_ids = core_affinity::get_core_ids().unwrap_or_default();
            if core_ids.len() >= 3 {
                use std::sync::atomic::{AtomicUsize, Ordering};
                static THREAD_ID: AtomicUsize = AtomicUsize::new(0);
                let id = THREAD_ID.fetch_add(1, Ordering::Relaxed);
                let target_core = if id % 2 == 0 { 1 } else { 2 };
                if let Some(core) = core_ids.get(target_core) {
                    core_affinity::set_for_current(*core);
                }
            }
        })
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    runtime.block_on(async_main())
}

async fn async_main() -> Result<()> {
    let config = Config::load()?;
    info!(
        mode = ?config.mode,
        max_alloc_per_trade = %config.max_alloc_per_trade,
        spike_multiplier = config.bot.spike_detection.multiplier,
        max_spread_ticks = config.bot.entry_guards.max_spread_ticks,
        stale_book_ms = config.bot.entry_guards.stale_book_ms,
        sustain_ms = config.bot.spike_detection.sustain_ms,
        "FaCaiBot starting"
    );

    // ── Lock-free channels ───────────────────────────────────────────
    let (ingestor_tx, ingestor_rx): (Sender<IngestorEvent>, Receiver<IngestorEvent>) =
        bounded(CHANNEL_CAP);
    let (executor_tx, executor_rx): (Sender<ExecutorCommand>, Receiver<ExecutorCommand>) =
        bounded(CHANNEL_CAP);
    // Reverse channel: live executor → engine (for CLOB order ID feedback).
    let (feedback_tx, feedback_rx): (Sender<ExecutorFeedback>, Receiver<ExecutorFeedback>) =
        bounded(CHANNEL_CAP);

    // ── Layer 1: Ingestor (The Ear) ─────────────────────────────────
    // CPU-pinned to core 0 for minimal context-switch jitter.
    let ingestor_config = config.clone();
    let ingestor_tx_binance = ingestor_tx.clone();
    let ingestor_tx_poly = ingestor_tx;

    let ingestor_handle = std::thread::spawn(move || {
        // Pin to core 0.
        let core_ids = core_affinity::get_core_ids().unwrap_or_default();
        if let Some(core) = core_ids.first() {
            core_affinity::set_for_current(*core);
            info!(core = core.id, "ingestor pinned to CPU core");
        }

        // Build a dedicated single-threaded tokio runtime for the ingestor.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build ingestor runtime");

        rt.block_on(async move {
            // Binance gateway — always active (both modes need price feeds).
            let binance = BinanceGateway::new(
                ingestor_config.binance_sbe_ws_url.clone(),
                ingestor_config.binance_ed25519_api_key.clone(),
                ingestor_config.bot.spike_detection.clone(),
            );

            // Polymarket WS gateway — live mode passes creds, sim mode passes None.
            let poly_ws = if ingestor_config.mode == Mode::Live {
                PolymarketWsGateway::new(Some(ingestor_config.polymarket_api_key.clone()))
            } else {
                // Simulation: no User WS or heartbeat, but Market WS still runs
                // for live orderbook data.
                PolymarketWsGateway::new(None)
            };

            let tx_binance = ingestor_tx_binance;
            let tx_poly_ws = ingestor_tx_poly.clone();
            let tx_user_ws = ingestor_tx_poly.clone();
            let tx_heartbeat = ingestor_tx_poly.clone();
            let tx_rotation = ingestor_tx_poly;
            let stale_threshold = ingestor_config.stale_event_threshold_ms;
            let prewarm_lead_ms = ingestor_config.bot.rotation.prewarm_lead_secs * 1_000;

            // Watch channel: rotation manager pushes token IDs → Market WS subscribes.
            let (token_tx, token_rx) = tokio::sync::watch::channel(Vec::<String>::new());

            // Run all ingestor streams concurrently.
            // In sim mode, run_user_ws and run_heartbeat park indefinitely
            // (std::future::pending) without consuming resources.
            tokio::select! {
                res = binance.run(tx_binance, stale_threshold) => {
                    if let Err(e) = res { error!(error = %e, "binance stream crashed"); }
                }
                res = poly_ws.run_market_ws(token_rx, tx_poly_ws) => {
                    if let Err(e) = res { error!(error = %e, "polymarket market WS crashed"); }
                }
                res = poly_ws.run_market_rotation(tx_rotation, token_tx, prewarm_lead_ms) => {
                    if let Err(e) = res { error!(error = %e, "polymarket market rotation crashed"); }
                }
                res = poly_ws.run_user_ws(tx_user_ws) => {
                    if let Err(e) = res { error!(error = %e, "polymarket user WS crashed"); }
                }
                res = poly_ws.run_heartbeat(tx_heartbeat) => {
                    if let Err(e) = res { error!(error = %e, "polymarket heartbeat crashed"); }
                }
            }
        });
    });

    // ── Layer 2: Strategy Engine (The Brain) ─────────────────────────
    let engine_mode = config.mode;
    let engine_config = config.clone();
    let engine_handle = tokio::spawn(async move {
        let mut engine = StrategyEngine::new(&engine_config);

        // QuestDB analytics — fire-and-forget, not on the execution path.
        let mut cold = match ColdStorage::new(&engine_config.questdb_url) {
            Ok(c) => Some(c),
            Err(e) => {
                error!(error = %e, "failed to init QuestDB for engine analytics — recording disabled");
                None
            }
        };
        let mut last_book_snapshot_ms: u64 = 0;
        let mut last_tick_record_ms: u64 = 0;

        while let Ok(event) = ingestor_rx.recv() {
            // Drain executor feedback (non-blocking). In live mode, the executor
            // sends CLOB order IDs back so the engine can match User WS fills.
            while let Ok(fb) = feedback_rx.try_recv() {
                match fb {
                    ExecutorFeedback::OrderPosted {
                        is_leg2,
                        order_id,
                        price,
                        size,
                    } => {
                        engine.on_order_posted(is_leg2, order_id, price, size);
                    }
                    ExecutorFeedback::OrderFailed { is_leg2 } => {
                        engine.on_order_failed(is_leg2);
                    }
                }
            }

            // Detect market rotation to notify executor.
            let rotation_info = if let IngestorEvent::MarketRotation {
                ref condition_id,
                ref yes_token_id,
                ref no_token_id,
                ..
            } = event
            {
                Some((
                    condition_id.clone(),
                    yes_token_id.clone(),
                    no_token_id.clone(),
                ))
            } else {
                None
            };

            // Record Binance ticks to QuestDB (fire-and-forget analytics).
            // Downsampled: only record ~1 tick/sec to keep 7-day storage manageable.
            if let IngestorEvent::BinanceTick(ref tick) = event {
                if tick.timestamp_ms.saturating_sub(last_tick_record_ms) >= TICK_RECORD_INTERVAL_MS
                {
                    if let Some(ref mut c) = cold {
                        if let Err(e) = c.record_tick(tick) {
                            debug!(error = %e, "failed to record tick to QuestDB");
                        }
                    }
                    last_tick_record_ms = tick.timestamp_ms;
                }
            }

            engine.on_event(event);

            // Record Polymarket book snapshots every 5 seconds.
            let now_ms = epoch_ms();
            if now_ms.saturating_sub(last_book_snapshot_ms) >= 5_000 {
                if let Some(ref mut c) = cold {
                    // Snapshot YES book.
                    if let Some(ref book) = engine.state().poly_yes_book {
                        let best_bid = book.best_bid().map(|l| l.price).unwrap_or_default();
                        let best_ask = book.best_ask().map(|l| l.price).unwrap_or_default();
                        let bid_depth = book.total_bid_depth();
                        let ask_depth = book.total_ask_depth();
                        let spread = best_ask - best_bid;
                        if let Err(e) = c.record_book_snapshot(
                            &book.asset_id,
                            best_bid,
                            best_ask,
                            bid_depth,
                            ask_depth,
                            spread,
                        ) {
                            debug!(error = %e, "failed to record YES book snapshot");
                        }
                    }
                    // Snapshot NO book.
                    if let Some(ref book) = engine.state().poly_no_book {
                        let best_bid = book.best_bid().map(|l| l.price).unwrap_or_default();
                        let best_ask = book.best_ask().map(|l| l.price).unwrap_or_default();
                        let bid_depth = book.total_bid_depth();
                        let ask_depth = book.total_ask_depth();
                        let spread = best_ask - best_bid;
                        if let Err(e) = c.record_book_snapshot(
                            &book.asset_id,
                            best_bid,
                            best_ask,
                            bid_depth,
                            ask_depth,
                            spread,
                        ) {
                            debug!(error = %e, "failed to record NO book snapshot");
                        }
                    }
                }
                last_book_snapshot_ms = now_ms;
            }

            // Drain rotation emergency signals (Leg 2 FOK for open positions)
            // BEFORE sending MarketRotation so the executor hedges the old
            // position before cleaning up the old market's state.
            for emergency_signal in engine.take_rotation_emergencies() {
                if let Err(e) = executor_tx.send(ExecutorCommand::Signal(emergency_signal)) {
                    error!(error = %e, "failed to send rotation emergency to executor");
                    break;
                }
            }

            // Notify executor of market rotation (before evaluating signals,
            // so the executor can close positions before receiving new ones).
            if let Some((cond_id, yes_id, no_id)) = rotation_info {
                if let Err(e) = executor_tx.send(ExecutorCommand::MarketRotation {
                    condition_id: cond_id,
                    yes_token_id: yes_id,
                    no_token_id: no_id,
                    tick_size: engine.state().tick_size,
                }) {
                    error!(error = %e, "failed to send MarketRotation to executor");
                    break;
                }
            }

            // Send cutoff command once when entering the 3-min window.
            if let Some((cond_id, market_end_ms)) = engine.take_cutoff_trigger() {
                if let Err(e) = executor_tx.send(ExecutorCommand::MarketCutoff {
                    condition_id: cond_id,
                    market_end_ms,
                }) {
                    error!(error = %e, "failed to send MarketCutoff to executor");
                }
            }

            // In simulation mode, advance the fill state machine before
            // evaluating new signals. This simulates Leg 1/2 fills and
            // resets state after trade completion.
            if engine_mode == Mode::Simulation {
                for sim_signal in engine.advance_simulation() {
                    if let Err(e) = executor_tx.send(ExecutorCommand::Signal(sim_signal)) {
                        error!(error = %e, "failed to send sim fill to executor");
                        break;
                    }
                }
            }

            // Drain spike cancel (speculative Leg 1 cancelled after SpikeFailed).
            if let Some(cancel_cmd) = engine.take_spike_cancel()
                && let Err(e) = executor_tx.send(cancel_cmd)
            {
                error!(error = %e, "failed to send spike cancel to executor");
            }

            // Check for stale Leg 1 orders (applies in both sim and live modes).
            if let Some(cancel_cmd) = engine.check_leg1_staleness() {
                if let Err(e) = executor_tx.send(cancel_cmd) {
                    error!(error = %e, "failed to send CancelLeg1 to executor");
                }
            }

            // Evaluate Leg 1 signals.
            if let Some(signal) = engine.evaluate() {
                if let Err(e) = executor_tx.send(ExecutorCommand::Signal(signal)) {
                    error!(error = %e, "failed to send Leg 1 signal to executor");
                    break;
                }
            }

            // Evaluate Leg 2 signals (erosion cascade, emergency hedge).
            if let Some(signal) = engine.evaluate_leg2() {
                let should_send = if engine_mode == Mode::Simulation {
                    // Sim: only erosion steps. Emergencies handled by advance_simulation().
                    signal.exit_reason.is_none()
                } else {
                    // Live: ALL signals (including emergency FOK) must reach executor.
                    true
                };
                if should_send {
                    if let Err(e) = executor_tx.send(ExecutorCommand::Signal(signal)) {
                        error!(error = %e, "failed to send Leg 2 signal to executor");
                        break;
                    }
                }
            }

            // In live mode, detect trade completion (both legs filled via User WS).
            if engine_mode == Mode::Live
                && matches!(engine.state().leg1_state, OrderState::Filled { .. })
                && matches!(engine.state().leg2_state, OrderState::Filled { .. })
            {
                // Record to QuestDB before state reset.
                if let Some(ref mut c) = cold
                    && let Err(e) = engine.record_live_trade(c)
                {
                    warn!(error = %e, "failed to record live trade to QuestDB");
                }
                engine.on_trade_complete();
            }

            engine.check_diagnostic();
        }
        // Flush any remaining buffered ticks before exiting.
        if let Some(ref mut c) = cold {
            if let Err(e) = c.flush() {
                error!(error = %e, "failed to flush QuestDB on engine shutdown");
            }
        }
        info!("engine loop exited");
    });

    // ── Layer 3: Executor (The Hand) ─────────────────────────────────
    let executor_config = config.clone();
    let executor_handle = tokio::spawn(async move {
        match executor_config.mode {
            Mode::Simulation => {
                info!("starting simulation executor");

                // Build Telegram reporter.
                let reporter = TelegramReporter::new(
                    executor_config.telegram_bot_token.clone(),
                    executor_config.telegram_chat_id.clone(),
                );

                // Build QuestDB cold storage (optional — executor runs without it).
                let cold = match ColdStorage::new(&executor_config.questdb_url) {
                    Ok(c) => Some(c),
                    Err(e) => {
                        warn!(error = %e, "QuestDB unavailable for simulation — analytics recording disabled");
                        None
                    }
                };

                let now_ms = epoch_ms();
                let sim_executor = SimulationExecutor::new(
                    reporter,
                    cold,
                    executor_config.max_alloc_per_trade,
                    now_ms,
                );

                if let Err(e) = sim_executor.run(executor_rx).await {
                    error!(error = %e, "simulation executor crashed");
                }
            }
            Mode::Live => {
                info!("starting live executor");

                let poly = PolymarketGateway::new(executor_config.clone()).await;
                let cold = match ColdStorage::new(&executor_config.questdb_url) {
                    Ok(c) => Some(c),
                    Err(e) => {
                        warn!(error = %e, "QuestDB unavailable for live executor — analytics recording disabled");
                        None
                    }
                };
                let reporter = TelegramReporter::new(
                    executor_config.telegram_bot_token.clone(),
                    executor_config.telegram_chat_id.clone(),
                );

                let live_executor = LiveExecutor::new(poly, feedback_tx, reporter, cold);

                if let Err(e) = live_executor.run(executor_rx).await {
                    error!(error = %e, "live executor crashed");
                }
            }
        }
    });

    // ── Wait ─────────────────────────────────────────────────────────
    // The ingestor runs on a dedicated OS thread; the other two are tokio tasks.
    let _ = engine_handle.await;
    let _ = executor_handle.await;
    let _ = ingestor_handle.join();

    info!("FaCaiBot shutdown complete");
    Ok(())
}

/// Current epoch milliseconds.
use crate::utils::time::epoch_ms;
