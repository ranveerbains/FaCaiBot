//! Heartbeat loop — sends `POST /v1/heartbeats` every 5 seconds via SDK.
//!
//! Tracks consecutive failures and logs alerts on 2+ consecutive misses.
//! Skipped in simulation mode.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::signers::Signer as _;
use anyhow::{Context, Result, anyhow};
use crossbeam_channel::Sender;
use polymarket_client_sdk::POLYGON;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::{Credentials, Normal};
use polymarket_client_sdk::clob::{Client as SdkClient, Config as SdkConfig};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::{
    HEARTBEAT_FAIL_ALERT_THRESHOLD, HEARTBEAT_INTERVAL_MS, MATCHING_ENGINE_RESTART_ET_SECS,
    MONDAY, PRE_CANCEL_ET_START_SECS,
};
use crate::types::IngestorEvent;
use crate::utils::signing::build_signer;

/// Send `POST /v1/heartbeats` every 5 seconds to the CLOB.
///
/// Uses the SDK's `post_heartbeat()` method which handles the correct endpoint
/// path, body format, and L2 HMAC authentication automatically.
///
/// - Tracks `heartbeat_id`: `None` on first call; updated from server response.
/// - Emits `IngestorEvent::HeartbeatStatus` after each attempt.
/// - Logs an error if 2 consecutive heartbeats fail (Section 5.4).
/// - Skipped in simulation mode.
///
/// This method is intended to run as a standalone `tokio::spawn`-ed task.
pub(super) async fn run_heartbeat(
    shutdown: Arc<AtomicBool>,
    sim_mode: bool,
    api_key: Option<String>,
    secret: Option<String>,
    passphrase: Option<String>,
    private_key: Option<String>,
    tx: Sender<IngestorEvent>,
) -> Result<()> {
    if sim_mode {
        info!("simulation mode — heartbeat loop skipped");
        std::future::pending::<()>().await;
        return Ok(());
    }

    // Initialize SDK client for authenticated heartbeat calls.
    let api_key = api_key.context("api_key required for heartbeat")?;
    let secret = secret.context("secret required for heartbeat")?;
    let passphrase = passphrase.context("passphrase required for heartbeat")?;
    let private_key = private_key.context("private_key required for heartbeat")?;

    let signer = build_signer(&private_key)
        .context("failed to build signer for heartbeat")?
        .with_chain_id(Some(POLYGON));

    let uuid: Uuid = api_key
        .parse()
        .context("POLYMARKET_API_KEY must be a valid UUID")?;

    let creds = Credentials::new(uuid, secret, passphrase);

    let sdk: SdkClient<Authenticated<Normal>> =
        SdkClient::new("https://clob.polymarket.com", SdkConfig::default())
            .map_err(|e| anyhow!("failed to create SDK client for heartbeat: {e}"))?
            .authentication_builder(&signer)
            .credentials(creds)
            .authenticate()
            .await
            .map_err(|e| anyhow!("SDK heartbeat auth failed: {e}"))?;

    info!("heartbeat SDK client authenticated");

    let mut heartbeat_id: Option<Uuid> = None;
    let mut consecutive_failures: u32 = 0;
    let mut interval = tokio::time::interval(Duration::from_millis(HEARTBEAT_INTERVAL_MS));

    loop {
        interval.tick().await;

        if shutdown.load(Ordering::Relaxed) {
            info!("heartbeat loop shutdown requested — exiting");
            return Ok(());
        }

        // Matching engine restart guard: warn at 19:55 ET on Mondays.
        check_matching_engine_restart_window();

        let start = SystemTime::now();
        match sdk.post_heartbeat(heartbeat_id).await {
            Ok(resp) => {
                let latency_ms = start.elapsed().unwrap_or_default().as_millis() as u64;

                debug!(latency_ms, heartbeat_id = %resp.heartbeat_id, "heartbeat OK");
                heartbeat_id = Some(resp.heartbeat_id);
                consecutive_failures = 0;

                let _ = tx.try_send(IngestorEvent::HeartbeatStatus {
                    success: true,
                    latency_ms,
                });
            }
            Err(e) => {
                consecutive_failures += 1;
                // Reset heartbeat_id on error — server will issue a new session.
                heartbeat_id = None;

                warn!(
                    error = %e,
                    consecutive_failures,
                    "heartbeat failed"
                );

                let _ = tx.try_send(IngestorEvent::HeartbeatStatus {
                    success: false,
                    latency_ms: 0,
                });

                if consecutive_failures >= HEARTBEAT_FAIL_ALERT_THRESHOLD {
                    error!(
                        consecutive_failures,
                        "HEARTBEAT ALERT: {} consecutive failures — \
                         CLOB may have cancelled all orders. \
                         Reset executor order state and re-establish session.",
                        consecutive_failures
                    );
                }
            }
        }
    }
}

// ─── Matching engine restart guard ───────────────────────────────────────────

/// Log a warning if we are within the 5-minute pre-cancel window before the
/// Monday 20:00 ET matching engine restart.
///
/// The matching engine restarts every Monday at ~20:00 ET (~90s downtime).
/// At 19:55 ET the bot should cancel all open orders and enter cancel-only mode.
///
/// This function only **logs** — the executor is responsible for acting on the
/// warning (issuing `DELETE /cancel-all`). The ingestor emits the warning so the
/// executor can detect it via the tracing log stream or a future event variant.
///
/// Note: This uses UTC time and a fixed ET=-5 offset (no DST). For production,
/// integrate proper timezone handling.
pub(super) fn check_matching_engine_restart_window() {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // ET offset: UTC-5 (no DST adjustment — approximation).
    let et_secs = now_secs.wrapping_sub(5 * 3600);

    // Day of week: 0=Sunday, 1=Monday, ... 6=Saturday.
    let days_since_epoch = et_secs / 86_400;
    let day_of_week = ((days_since_epoch + 4) % 7) as u32; // epoch was Thursday (4)
    let secs_in_day = (et_secs % 86_400) as u32;

    if day_of_week == MONDAY {
        if secs_in_day >= PRE_CANCEL_ET_START_SECS && secs_in_day < MATCHING_ENGINE_RESTART_ET_SECS
        {
            warn!(
                secs_in_day,
                "MATCHING ENGINE RESTART WINDOW: 19:55–20:00 ET Monday — \
                 cancel all open orders and enter cancel-only mode!"
            );
        } else if secs_in_day >= MATCHING_ENGINE_RESTART_ET_SECS
            && secs_in_day < MATCHING_ENGINE_RESTART_ET_SECS + 120
        {
            warn!(
                secs_in_day,
                "MATCHING ENGINE RESTART: ~20:00 ET Monday — \
                 expect HTTP 425 for ~90s. Retrying with exponential backoff."
            );
        }
    }
}
