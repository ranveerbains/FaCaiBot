mod config;
mod engine;
mod executor;
mod gateway;
mod reporting;
mod storage;
mod types;
mod utils;

use anyhow::Result;
use crossbeam_channel::{Receiver, Sender, bounded};
use tracing::{error, info, warn};

use crate::config::{Config, Mode};
use crate::engine::strategy::StrategyEngine;
use crate::executor::simulation::SimulationExecutor;
use crate::gateway::binance::BinanceGateway;
use crate::gateway::polymarket::PolymarketGateway;
use crate::gateway::polymarket::PolymarketWsGateway;
use crate::reporting::telegram::TelegramReporter;
use crate::storage::cold::ColdStorage;
use crate::storage::hot::HotStorage;
use crate::types::{ExecutorCommand, IngestorEvent};

/// Channel capacity between layers. Sized to absorb burst without back-pressure.
const CHANNEL_CAP: usize = 8192;

#[tokio::main]
async fn main() -> Result<()> {
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

    let config = Config::load()?;
    info!(
        mode = ?config.mode,
        fixed_alloc = %config.fixed_alloc,
        spike_multiplier = config.bot.spike_detection.multiplier,
        max_spread_pct = config.bot.entry_guards.max_spread_pct,
        stale_book_ms = config.bot.entry_guards.stale_book_ms,
        sustain_ms = config.bot.spike_detection.sustain_ms,
        "FaCaiBot starting"
    );

    // ── Lock-free channels ───────────────────────────────────────────
    let (ingestor_tx, ingestor_rx): (Sender<IngestorEvent>, Receiver<IngestorEvent>) =
        bounded(CHANNEL_CAP);
    let (executor_tx, executor_rx): (Sender<ExecutorCommand>, Receiver<ExecutorCommand>) =
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
                ingestor_config.binance_ws_url.clone(),
                ingestor_config.bot.spike_detection.clone(),
            );

            // Polymarket WS gateway — live mode passes creds, sim mode passes None.
            let poly_ws = if ingestor_config.mode == Mode::Live {
                PolymarketWsGateway::new(
                    Some(ingestor_config.polymarket_api_key.clone()),
                    Some(ingestor_config.polymarket_secret.clone()),
                    Some(ingestor_config.polymarket_passphrase.clone()),
                )
            } else {
                // Simulation: no User WS or heartbeat, but Market WS still runs
                // for live orderbook data.
                PolymarketWsGateway::new(None, None, None)
            };

            let tx_binance = ingestor_tx_binance;
            let tx_poly_ws = ingestor_tx_poly.clone();
            let tx_rotation = ingestor_tx_poly;
            let stale_threshold = ingestor_config.stale_event_threshold_ms;

            // Watch channel: rotation manager pushes token IDs → Market WS subscribes.
            let (token_tx, token_rx) = tokio::sync::watch::channel(Vec::<String>::new());

            // Run all ingestor streams concurrently.
            // Market rotation discovers 15-min markets via Gamma API and emits
            // MarketRotation events; the market WS subscribes to live orderbook
            // data for the active token IDs.
            tokio::select! {
                res = binance.run(tx_binance, stale_threshold) => {
                    if let Err(e) = res { error!(error = %e, "binance stream crashed"); }
                }
                res = poly_ws.run_market_ws(token_rx, tx_poly_ws) => {
                    if let Err(e) = res { error!(error = %e, "polymarket market WS crashed"); }
                }
                res = poly_ws.run_market_rotation(tx_rotation, token_tx) => {
                    if let Err(e) = res { error!(error = %e, "polymarket market rotation crashed"); }
                }
            }
        });
    });

    // ── Layer 2: Strategy Engine (The Brain) ─────────────────────────
    let engine_mode = config.mode;
    let engine_config = config.clone();
    let engine_handle = tokio::spawn(async move {
        let mut engine = StrategyEngine::new(&engine_config);

        while let Ok(event) = ingestor_rx.recv() {
            // Detect market rotation to notify executor.
            let rotation_condition_id = if let IngestorEvent::MarketRotation {
                ref condition_id,
                ..
            } = event
            {
                Some(condition_id.clone())
            } else {
                None
            };

            engine.on_event(event);

            // Notify executor of market rotation (before evaluating signals,
            // so the executor can close positions before receiving new ones).
            if let Some(cond_id) = rotation_condition_id {
                if let Err(e) = executor_tx.send(ExecutorCommand::MarketRotation {
                    condition_id: cond_id,
                }) {
                    error!(error = %e, "failed to send MarketRotation to executor");
                    break;
                }
            }

            // In simulation mode, advance the fill state machine before
            // evaluating new signals. This simulates Leg 1/2 fills and
            // resets state after trade completion.
            if engine_mode == Mode::Simulation {
                if let Some(sim_signal) = engine.advance_simulation() {
                    if let Err(e) =
                        executor_tx.send(ExecutorCommand::Signal(sim_signal))
                    {
                        error!(error = %e, "failed to send Leg 2 sim fill to executor");
                        break;
                    }
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
                if let Err(e) = executor_tx.send(ExecutorCommand::Signal(signal)) {
                    error!(error = %e, "failed to send Leg 2 signal to executor");
                    break;
                }
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

                // Build QuestDB cold storage.
                let cold = match ColdStorage::new(&executor_config.questdb_url) {
                    Ok(c) => c,
                    Err(e) => {
                        error!(error = %e, "failed to init QuestDB for simulation");
                        return;
                    }
                };

                let now_ms = epoch_ms();
                let sim_executor =
                    SimulationExecutor::new(reporter, cold, executor_config.fixed_alloc, now_ms);

                if let Err(e) = sim_executor.run(executor_rx).await {
                    error!(error = %e, "simulation executor crashed");
                }
            }
            Mode::Live => {
                info!("starting live executor");

                let poly = PolymarketGateway::new(executor_config.clone());
                let _hot = match HotStorage::new(&executor_config.redis_url) {
                    Ok(h) => h,
                    Err(e) => {
                        error!(error = %e, "failed to init Redis");
                        return;
                    }
                };
                let mut cold = match ColdStorage::new(&executor_config.questdb_url) {
                    Ok(c) => c,
                    Err(e) => {
                        error!(error = %e, "failed to init QuestDB");
                        return;
                    }
                };

                while let Ok(cmd) = executor_rx.recv() {
                    let signal = match cmd {
                        ExecutorCommand::Signal(s) => s,
                        ExecutorCommand::MarketRotation { .. } => continue,
                    };

                    info!(
                        side = ?signal.side,
                        token = %signal.token_id,
                        price = %signal.price,
                        size = %signal.size,
                        is_leg2 = signal.is_leg2,
                        confidence = %signal.confidence,
                        tier = signal.profit_target_tier.label(),
                        "executing trade signal"
                    );

                    let order = crate::types::OrderRequest::post_only_gtc(
                        signal.token_id.clone(),
                        signal.side,
                        signal.price,
                        signal.size,
                    );

                    match poly.place_order(&order).await {
                        Ok(resp) => {
                            info!(
                                order_id = %resp.order_id,
                                status = ?resp.status,
                                "order placed"
                            );
                        }
                        Err(e) => {
                            error!(error = %e, "order placement failed");
                        }
                    }

                    // Record signal to QuestDB.
                    let direction_str = match signal.direction {
                        crate::types::market::Direction::Up => "YES",
                        crate::types::market::Direction::Down => "NO",
                    };
                    if let Err(e) = cold.record_signal(
                        &signal.token_id,
                        direction_str,
                        signal.confidence,
                        signal.spike_info.magnitude,
                        rust_decimal::Decimal::ZERO, // ATR from engine state
                        rust_decimal::Decimal::ZERO, // book depth
                        0,                           // time remaining
                        signal.alloc_amount,
                        "submitted",
                    ) {
                        warn!(error = %e, "failed to record signal to QuestDB");
                    }
                }
                info!("live executor loop exited");
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
fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
