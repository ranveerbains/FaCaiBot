mod config;
mod engine;
mod gateway;
mod storage;
mod types;
mod utils;

use anyhow::Result;
use crossbeam_channel::{Receiver, Sender, bounded};
use tracing::{error, info};

use crate::config::Config;
use crate::engine::strategy::StrategyEngine;
use crate::gateway::binance::BinanceGateway;
use crate::gateway::polymarket::PolymarketGateway;
use crate::storage::cold::ColdStorage;
use crate::storage::hot::HotStorage;
use crate::types::{IngestorEvent, TradeSignal};

/// Channel capacity between layers. Sized to absorb burst without back-pressure.
const CHANNEL_CAP: usize = 8192;

#[tokio::main]
async fn main() -> Result<()> {
    // ── Bootstrap ────────────────────────────────────────────────────
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "facaibot=info".parse().unwrap()),
        )
        .with_target(true)
        .init();

    let config = Config::from_env()?;
    info!("FaCaiBot starting");

    // ── Lock-free channels ───────────────────────────────────────────
    let (ingestor_tx, ingestor_rx): (Sender<IngestorEvent>, Receiver<IngestorEvent>) =
        bounded(CHANNEL_CAP);
    let (executor_tx, executor_rx): (Sender<TradeSignal>, Receiver<TradeSignal>) =
        bounded(CHANNEL_CAP);

    // ── Layer 1: Ingestor (The Ear) ─────────────────────────────────
    // CPU-pinned to core 0 for minimal context-switch jitter.
    let ingestor_config = config.clone();
    let ingestor_tx_poly = ingestor_tx.clone();
    let ingestor_tx_binance = ingestor_tx;

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
            let poly = PolymarketGateway::new(ingestor_config.clone());
            let binance = BinanceGateway::new(ingestor_config.binance_ws_url.clone());

            // Run both streams concurrently.
            tokio::select! {
                res = poly.stream_orderbook("TODO_TOKEN_ID", ingestor_tx_poly) => {
                    if let Err(e) = res { error!(error = %e, "polymarket stream crashed"); }
                }
                res = binance.run(ingestor_tx_binance) => {
                    if let Err(e) = res { error!(error = %e, "binance stream crashed"); }
                }
            }
        });
    });

    // ── Layer 2: Strategy Engine (The Brain) ─────────────────────────
    let engine_handle = tokio::spawn(async move {
        let mut engine = StrategyEngine::new();

        while let Ok(event) = ingestor_rx.recv() {
            engine.on_event(event);

            if let Some(signal) = engine.evaluate()
                && let Err(e) = executor_tx.send(signal)
            {
                error!(error = %e, "failed to send trade signal to executor");
                break;
            }
        }
        info!("engine loop exited");
    });

    // ── Layer 3: Executor (The Hand) ─────────────────────────────────
    let executor_config = config.clone();
    let executor_handle = tokio::spawn(async move {
        let poly = PolymarketGateway::new(executor_config.clone());
        let hot = match HotStorage::new(&executor_config.redis_url) {
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

        while let Ok(signal) = executor_rx.recv() {
            info!(
                side = ?signal.side,
                token = %signal.token_id,
                price = %signal.price,
                size = %signal.size,
                "executing trade signal"
            );

            let order = crate::types::OrderRequest {
                token_id: signal.token_id.clone(),
                side: signal.side,
                price: signal.price,
                size: signal.size,
            };

            match poly.place_order(&order).await {
                Ok(resp) => info!(order_id = %resp.order_id, status = ?resp.status, "order placed"),
                Err(e) => error!(error = %e, "order placement failed"),
            }

            // Suppress unused-variable warnings; these will be wired in once
            // tick recording is fully integrated.
            let _ = (&hot, &mut cold);
        }
        info!("executor loop exited");
    });

    // ── Wait ─────────────────────────────────────────────────────────
    // The ingestor runs on a dedicated OS thread; the other two are tokio tasks.
    let _ = engine_handle.await;
    let _ = executor_handle.await;
    let _ = ingestor_handle.join();

    info!("FaCaiBot shutdown complete");
    Ok(())
}
