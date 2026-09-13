mod config;
mod control;
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

use std::sync::Arc;

use anyhow::Result;
use crossbeam_channel::{Receiver, Sender, bounded};
use tracing::{debug, error, info};

use crate::config::Config;
use crate::control::listener::TelegramCommandListener;
use crate::control::types::{BotStatus, DrainStatus, NotifyFlags};
use crate::engine::strategy::V2StrategyEngine;
use crate::executor::live::LiveExecutor;
use crate::gateway::binance::{BinanceGateway, FuturesGateway};
use crate::gateway::polymarket::PolymarketGateway;
use crate::gateway::polymarket::PolymarketWsGateway;
use crate::reporting::telegram::TelegramReporter;
use crate::storage::cold::ColdStorage;
use crate::types::order::{V2ExecutorCommand, V2ExecutorFeedback};
use crate::types::IngestorEvent;

const CHANNEL_CAP: usize = 8192;
const TICK_RECORD_INTERVAL_MS: u64 = 1_000;

fn main() -> Result<()> {
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
    info!("FaCaiBot v2 starting");

    // ── Channels ──
    let (ingestor_tx, ingestor_rx): (Sender<IngestorEvent>, Receiver<IngestorEvent>) =
        bounded(CHANNEL_CAP);
    let (executor_tx, executor_rx): (Sender<V2ExecutorCommand>, Receiver<V2ExecutorCommand>) =
        bounded(CHANNEL_CAP);
    let (feedback_tx, feedback_rx): (Sender<V2ExecutorFeedback>, Receiver<V2ExecutorFeedback>) =
        bounded(CHANNEL_CAP);

    // ── Control plane ──
    let redeem_notify = Arc::new(tokio::sync::Notify::new());
    let notify_flags = Arc::new(NotifyFlags::new());
    let (status_tx, status_rx) = tokio::sync::watch::channel(BotStatus::default());
    let (drain_status_tx, drain_status_rx) = tokio::sync::watch::channel(DrainStatus::Idle);
    let ingestor_tx_control = ingestor_tx.clone();

    // ── Layer 1: Ingestor ──
    let ingestor_config = config.clone();
    let ingestor_tx_binance = ingestor_tx.clone();
    let ingestor_tx_poly = ingestor_tx;

    let ingestor_handle = std::thread::spawn(move || {
        let core_ids = core_affinity::get_core_ids().unwrap_or_default();
        if let Some(core) = core_ids.first() {
            core_affinity::set_for_current(*core);
            info!(core = core.id, "ingestor pinned to CPU core");
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build ingestor runtime");

        rt.block_on(async move {
            let binance = BinanceGateway::new(
                ingestor_config.binance_sbe_ws_url.clone(),
                ingestor_config.binance_ed25519_api_key.clone(),
            );
            let futures = FuturesGateway::new(
                ingestor_config.binance_futures_ws_url.clone(),
            );
            let poly_ws = PolymarketWsGateway::new(
                Some(ingestor_config.polymarket_api_key.clone()),
                Some(ingestor_config.polymarket_secret.clone()),
                Some(ingestor_config.polymarket_passphrase.clone()),
                Some(ingestor_config.private_key.clone()),
            );

            let tx_binance = ingestor_tx_binance.clone();
            let tx_futures = ingestor_tx_binance;
            let tx_poly_ws = ingestor_tx_poly.clone();
            let tx_user_ws = ingestor_tx_poly.clone();
            let tx_heartbeat = ingestor_tx_poly.clone();
            let tx_rotation = ingestor_tx_poly;
            let stale_threshold = ingestor_config.stale_event_threshold_ms;
            let prewarm_lead_ms = ingestor_config.bot.rotation.prewarm_lead_secs * 1_000;

            let (token_tx, token_rx) = tokio::sync::watch::channel(Vec::<String>::new());

            tokio::select! {
                res = binance.run(tx_binance, stale_threshold) => {
                    if let Err(e) = res { error!(error = %e, "binance spot SBE crashed"); }
                }
                res = futures.run(tx_futures, stale_threshold) => {
                    if let Err(e) = res { error!(error = %e, "binance futures WS crashed"); }
                }
                res = poly_ws.run_market_ws(token_rx, tx_poly_ws) => {
                    if let Err(e) = res { error!(error = %e, "polymarket market WS crashed"); }
                }
                res = poly_ws.run_market_rotation(tx_rotation, token_tx, prewarm_lead_ms) => {
                    if let Err(e) = res { error!(error = %e, "market rotation crashed"); }
                }
                res = poly_ws.run_user_ws(tx_user_ws) => {
                    if let Err(e) = res { error!(error = %e, "polymarket user WS crashed"); }
                }
                res = poly_ws.run_heartbeat(tx_heartbeat) => {
                    if let Err(e) = res { error!(error = %e, "heartbeat crashed"); }
                }
            }
        });
    });

    // ── Layer 2: V2 Strategy Engine ──
    let engine_config = config.clone();
    let engine_notify_flags = Arc::clone(&notify_flags);
    let diag_tls = crate::reporting::telegram::build_tls_connector();
    let diag_bot_token = config.telegram_bot_token.clone();
    let diag_chat_id = config.telegram_chat_id.clone();

    let live_reporter_for_engine = TelegramReporter::new(
        config.telegram_bot_token.clone(),
        config.telegram_chat_id.clone(),
    );
    live_reporter_for_engine.spawn_cleanup_task();

    let listener_reporter = live_reporter_for_engine.clone();
    let wallet_reporter = live_reporter_for_engine.clone();
    let diag_reporter = live_reporter_for_engine.clone();

    let redeem_notify_engine = Arc::clone(&redeem_notify);
    let engine_handle = tokio::task::spawn_blocking(move || {
        let mut engine = V2StrategyEngine::new(&engine_config);
        engine.set_reporter(live_reporter_for_engine);

        let mut cold = match ColdStorage::new(&engine_config.questdb_url) {
            Ok(c) => Some(c),
            Err(e) => {
                error!(error = %e, "QuestDB init failed — recording disabled");
                None
            }
        };
        let mut last_tick_record_ms: u64 = 0;
        let mut last_status_publish_ms: u64 = 0;
        let mut pending_exit_code: Option<i32> = None;

        while let Ok(event) = ingestor_rx.recv() {
            // 1. Drain executor feedback
            while let Ok(fb) = feedback_rx.try_recv() {
                engine.on_feedback(fb);
            }

            // 2. Handle control events
            match event {
                IngestorEvent::Shutdown => {
                    engine.set_draining();
                    pending_exit_code = Some(0);
                    if engine.has_no_open_position() {
                        engine.send_session_summary();
                        let _ = drain_status_tx.send(DrainStatus::Complete {
                            exit_code: 0,
                            summary: "No open positions. Bot stopped.".into(),
                        });
                        break;
                    }
                    let _ = drain_status_tx.send(DrainStatus::Draining {
                        reason: "shutdown".into(),
                        position_info: "Waiting for market rotation".into(),
                    });
                    continue;
                }
                IngestorEvent::DrainAndRestart => {
                    engine.set_draining();
                    pending_exit_code = Some(42);
                    if engine.has_no_open_position() {
                        engine.send_session_summary();
                        let _ = drain_status_tx.send(DrainStatus::Complete {
                            exit_code: 42,
                            summary: "No open positions. Restarting...".into(),
                        });
                        break;
                    }
                    let _ = drain_status_tx.send(DrainStatus::Draining {
                        reason: "config change".into(),
                        position_info: "Waiting for market rotation".into(),
                    });
                    continue;
                }
                IngestorEvent::PauseTrading => {
                    let cancel_cmds = engine.set_paused(true);
                    for cmd in cancel_cmds {
                        if let Err(e) = executor_tx.send(cmd) {
                            error!(error = %e, "failed to send pause cancel command");
                        }
                    }
                    info!("trading PAUSED");
                    continue;
                }
                IngestorEvent::ResumeTrading => {
                    let _ = engine.set_paused(false);
                    info!("trading RESUMED");
                    continue;
                }
                _ => {}
            }

            // Record Binance ticks to QuestDB (downsampled)
            if let IngestorEvent::BinanceTick(ref tick) = event {
                if tick.timestamp_ms.saturating_sub(last_tick_record_ms) >= TICK_RECORD_INTERVAL_MS {
                    if let Some(ref mut c) = cold {
                        if let Err(e) = c.record_tick(tick) {
                            debug!(error = %e, "failed to record tick");
                        }
                    }
                    last_tick_record_ms = tick.timestamp_ms;
                }
            }

            // Detect rotation for redeem trigger
            let is_rotation = matches!(event, IngestorEvent::MarketRotation { .. });

            // 3. Process event (updates fair value, books, phase transitions)
            engine.on_event(event);

            // 4. Drain pending engine commands (rotation forwarding, tick size change)
            for cmd in engine.take_pending_commands() {
                if let Err(e) = executor_tx.send(cmd) {
                    error!(error = %e, "failed to send pending command to executor");
                }
            }
            if let Some(tick_cmd) = engine.take_tick_size_change() {
                if let Err(e) = executor_tx.send(tick_cmd) {
                    error!(error = %e, "failed to send tick size change");
                }
            }

            // Trigger auto-redeem on rotation
            if is_rotation {
                redeem_notify_engine.notify_one();
            }

            // 5. Quoting decisions
            for cmd in engine.quote_tick() {
                if let Err(e) = executor_tx.send(cmd) {
                    error!(error = %e, "failed to send quote command");
                    break;
                }
            }

            // Check drain completion
            if let Some(exit_code) = pending_exit_code {
                if engine.has_no_open_position() {
                    engine.send_session_summary();
                    let summary = if exit_code == 0 {
                        "Bot stopped.".to_string()
                    } else {
                        "Restarting with new config...".to_string()
                    };
                    let _ = drain_status_tx.send(DrainStatus::Complete { exit_code, summary });
                    break;
                }
            }

            // 7. Diagnostics
            engine.check_diagnostic();
            if let Some(diag_msg) = engine.take_pending_telegram_diag() {
                if engine_notify_flags.diagnostics_on() {
                    let tls = diag_tls.clone();
                    let token = diag_bot_token.clone();
                    let chat = diag_chat_id.clone();
                    let rep = diag_reporter.clone();
                    tokio::spawn(async move {
                        if let Ok(Some(id)) = crate::reporting::telegram::post_telegram_message(
                            &tls, &token, &chat, &diag_msg,
                        ).await {
                            rep.track_msg_id(id).await;
                        }
                    });
                }
            }

            // 8. Fill notifications (gated by trades_enabled)
            for fill_msg in engine.take_pending_fill_messages() {
                if engine_notify_flags.trades_on() {
                    let tls = diag_tls.clone();
                    let token = diag_bot_token.clone();
                    let chat = diag_chat_id.clone();
                    let rep = diag_reporter.clone();
                    tokio::spawn(async move {
                        if let Ok(Some(id)) = crate::reporting::telegram::post_telegram_message(
                            &tls, &token, &chat, &fill_msg,
                        ).await {
                            rep.track_msg_id(id).await;
                        }
                    });
                }
            }

            // 9. Market report (gated by summary_enabled)
            if let Some(report) = engine.take_pending_market_report() {
                if engine_notify_flags.summary_on() {
                    if let Some(reporter) = engine.reporter() {
                        reporter.fire_critical(report);
                    }
                }
            }

            // 10. QuestDB fill records
            for record in engine.take_pending_fill_records() {
                if let Some(ref mut c) = cold {
                    if let Err(e) = c.record_fill(&record) {
                        debug!(error = %e, "failed to record fill to QuestDB");
                    }
                }
            }

            // 11. QuestDB market summary
            if let Some(summary) = engine.take_pending_market_summary() {
                if let Some(ref mut c) = cold {
                    if let Err(e) = c.record_market_summary(&summary) {
                        debug!(error = %e, "failed to record market summary to QuestDB");
                    }
                }
            }

            // 12. QuestDB risk score records
            for record in engine.take_pending_risk_records() {
                if let Some(ref mut c) = cold {
                    if let Err(e) = c.record_risk_score(&record) {
                        debug!(error = %e, "failed to record risk score to QuestDB");
                    }
                }
            }

            // Publish status every 5s
            let now = crate::utils::time::epoch_ms();
            if now.saturating_sub(last_status_publish_ms) >= 5_000 {
                let mut status = engine.build_status("v2-live");
                status.trades_enabled = engine_notify_flags.trades_on();
                status.summary_enabled = engine_notify_flags.summary_on();
                let _ = status_tx.send(status);
                last_status_publish_ms = now;
            }
        }

        // Dispatch pending session summary (gated by summary_enabled)
        if let Some(summary_text) = engine.take_pending_session_summary() {
            if engine_notify_flags.summary_on() {
                if let Some(reporter) = engine.reporter() {
                    reporter.fire_critical(summary_text);
                }
            }
        }

        if let Some(ref mut c) = cold {
            if let Err(e) = c.flush() {
                error!(error = %e, "failed to flush QuestDB on shutdown");
            }
        }
        info!("v2 engine loop exited");
    });

    // ── Layer 3: V2 Executor ──
    let executor_config = config.clone();
    let executor_handle = tokio::spawn(async move {
        info!("starting v2 executor");

        let poly = PolymarketGateway::new(executor_config.clone()).await;
        let reporter = TelegramReporter::new(
            executor_config.telegram_bot_token.clone(),
            executor_config.telegram_chat_id.clone(),
        );
        reporter.spawn_cleanup_task();

        let live_executor = LiveExecutor::new(poly, feedback_tx, reporter);

        if let Err(e) = live_executor.run(executor_rx).await {
            error!(error = %e, "v2 executor crashed");
        }
    });

    // ── Command Listener ──
    if let Some(user_id) = config.telegram_allowed_user_id {
        let tls_connector = crate::reporting::telegram::build_tls_connector();
        let listener = TelegramCommandListener::new(
            config.telegram_bot_token.clone(),
            config.telegram_chat_id.clone(),
            user_id,
            tls_connector,
            Arc::clone(&notify_flags),
            ingestor_tx_control,
            status_rx,
            drain_status_rx,
            listener_reporter,
        );
        tokio::spawn(listener.run());
        info!(user_id, "command listener spawned");
    }

    // ── Auto-Redeem ──
    {
        let redeem_tls = crate::reporting::telegram::build_tls_connector();
        let redeem_bot_token = config.telegram_bot_token.clone();
        let redeem_chat_id = config.telegram_chat_id.clone();
        tokio::spawn(crate::control::wallet::auto_redeem_loop(
            redeem_tls,
            redeem_bot_token,
            redeem_chat_id,
            redeem_notify,
            wallet_reporter,
        ));
    }

    // ── QuestDB Retention (drop old partitions every 24h, keep 7 days) ──
    {
        let retention_url = config.questdb_http_url.clone();
        tokio::spawn(async move {
            let tables = ["binance_ticks", "v2_fills", "v2_market_summaries"];
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(86_400)).await;
                for table in &tables {
                    if let Err(e) = crate::storage::cold::drop_old_partitions(&retention_url, table, 7).await {
                        error!(table, error = %e, "QuestDB retention failed");
                    } else {
                        info!(table, "QuestDB retention: dropped partitions >7d");
                    }
                }
            }
        });
    }

    // ── Wait ──
    let _ = engine_handle.await;
    let _ = executor_handle.await;
    let _ = ingestor_handle.join();

    info!("FaCaiBot v2 shutdown complete");
    Ok(())
}
