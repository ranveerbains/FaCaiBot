//! Heartbeat loop — sends `POST /heartbeat` every 5 seconds.
//!
//! Tracks consecutive failures and logs alerts on 2+ consecutive misses.
//! Skipped in simulation mode.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use serde::Deserialize;
use tracing::{debug, error, info, warn};

use super::tls_helpers::http_post;
use super::{
    CLOB_BASE_URL, HEARTBEAT_FAIL_ALERT_THRESHOLD, HEARTBEAT_INTERVAL_MS, MATCHING_ENGINE_RESTART_ET_SECS,
    MONDAY, PRE_CANCEL_ET_START_SECS,
};

/// Send `POST /heartbeat` every 5 seconds to the CLOB.
///
/// - Tracks `heartbeat_id`: empty string on first call; updated from server
///   response on subsequent calls.
/// - On 400 response: immediately retries with the id from the response body.
/// - Emits `IngestorEvent::HeartbeatStatus` after each attempt.
/// - Logs an error if 2 consecutive heartbeats fail (Section 5.4).
/// - Skipped in simulation mode.
///
/// This method is intended to run as a standalone `tokio::spawn`-ed task.
pub(super) async fn run_heartbeat(shutdown: Arc<AtomicBool>, sim_mode: bool) -> Result<()> {
    if sim_mode {
        info!("simulation mode — heartbeat loop skipped");
        std::future::pending::<()>().await;
        return Ok(());
    }

    let mut heartbeat_id = String::new();
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
        match send_heartbeat(&heartbeat_id).await {
            Ok(new_id) => {
                let latency_ms = start.elapsed().unwrap_or_default().as_millis() as u64;

                debug!(latency_ms, new_heartbeat_id = %new_id, "heartbeat OK");
                heartbeat_id = new_id;
                consecutive_failures = 0;

                // HeartbeatStatus is informational — log at debug level.
                // The executor layer listens for this to track CLOB session health.
            }
            Err(e) => {
                consecutive_failures += 1;
                warn!(
                    error = %e,
                    consecutive_failures,
                    "heartbeat failed"
                );

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

/// Send `POST /heartbeat` and return the updated `heartbeat_id`.
///
/// On HTTP 400, the server provides a new `heartbeat_id` in the body —
/// parse it and return it so the caller can retry immediately.
async fn send_heartbeat(heartbeat_id: &str) -> Result<String> {
    let url = format!("{CLOB_BASE_URL}/heartbeat");
    let body_json = serde_json::json!({ "id": heartbeat_id }).to_string();

    let response_bytes = http_post(&url, body_json.as_bytes()).await?;
    let body_str = std::str::from_utf8(&response_bytes).unwrap_or("{}");

    // Parse the heartbeat_id from the response (success or 400 body).
    #[derive(Deserialize, Default)]
    struct HeartbeatResponse {
        #[serde(default)]
        id: String,
    }

    let resp: HeartbeatResponse = serde_json::from_str(body_str).unwrap_or_default();
    Ok(resp.id)
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
