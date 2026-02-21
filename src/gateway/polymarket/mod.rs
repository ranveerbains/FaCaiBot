//! Polymarket gateway — Layer 1 Ingestor ("The Ear") + Layer 3 Executor REST.
//!
//! This module is split into focused sub-modules:
//!
//! - [`rest`] — CLOB REST gateway: EIP-712 signing, order placement, book queries.
//! - [`market_ws`] — Public Market WebSocket: book, price, tick events.
//! - [`user_ws`] — Authenticated User WebSocket: trade fills, order events.
//! - [`heartbeat`] — `POST /heartbeat` loop (every 5 seconds).
//! - [`rotation`] — Gamma API market discovery and rotation manager.
//! - [`tls_helpers`] — Shared TLS WebSocket + HTTP helpers.
//!
//! # Thread model
//! The WS sub-modules run on the **ingestor's dedicated OS thread** with its
//! own single-threaded tokio runtime. Must never touch the main multi-threaded
//! runtime.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use crossbeam_channel::Sender;

use crate::types::IngestorEvent;

pub mod heartbeat;
pub mod market_ws;
pub mod rest;
pub mod rotation;
pub mod tls_helpers;
pub mod user_ws;

// Re-export key public types.
pub use rest::PolymarketGateway;
pub use rotation::MarketInfo;

// ─── Endpoints ───────────────────────────────────────────────────────────────

/// Public Market WebSocket — book + price events for any token IDs.
pub(super) const MARKET_WS_URL: &str =
    "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// Authenticated User WebSocket — trade fills + order events.
pub(super) const USER_WS_URL: &str =
    "wss://ws-subscriptions-clob.polymarket.com/ws/user";

/// CLOB REST base URL.
pub(super) const CLOB_BASE_URL: &str = "https://clob.polymarket.com";

/// Gamma API base URL — market metadata / discovery.
pub(super) const GAMMA_BASE_URL: &str = "https://gamma-api.polymarket.com";

/// Gamma query for upcoming 15-minute crypto markets.
///
/// Uses tag_id=102467 ("15M") to find all 15-minute prediction markets.
/// Returns up to 10 active, non-closed events; we filter client-side
/// for BTC/ETH by slug prefix (`btc-updown-15m-` / `eth-updown-15m-`).
pub(super) const GAMMA_EVENTS_PATH: &str =
    "/events?tag_id=102467&active=true&closed=false&limit=10";

// ─── Timing constants ─────────────────────────────────────────────────────────

/// Heartbeat interval (ms). PRD: 5 000 ms (Section 5.4).
pub(super) const HEARTBEAT_INTERVAL_MS: u64 = 5_000;

/// Gamma API polling interval (ms). PRD: 10 minutes (Section 5.3).
pub(super) const GAMMA_POLL_INTERVAL_MS: u64 = 600_000;

/// Reconnection backoff: initial delay (ms).
pub(super) const BACKOFF_INITIAL_MS: u64 = 1_000;

/// Reconnection backoff: maximum delay (ms).
pub(super) const BACKOFF_MAX_MS: u64 = 30_000;

/// Consecutive heartbeat failures before logging an alert.
pub(super) const HEARTBEAT_FAIL_ALERT_THRESHOLD: u32 = 2;

/// Monday weekday index (0=Sunday, 1=Monday … in our helper).
/// Used for matching engine restart guard.
pub(super) const MONDAY: u32 = 1;

/// 19:55 ET in seconds-since-midnight (UTC-5 / no DST adjustment — approximation).
/// Full restart pre-cancel window: 19:55–20:00 ET Monday.
/// The bot sends `DELETE /cancel-all` and pauses order placement.
pub(super) const PRE_CANCEL_ET_START_SECS: u32 = 19 * 3600 + 55 * 60; // 71 700
pub(super) const MATCHING_ENGINE_RESTART_ET_SECS: u32 = 20 * 3600; // 72 000

// ─── Shared utility ───────────────────────────────────────────────────────────

/// Current wall-clock time as epoch milliseconds.
#[inline]
pub(super) fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ─── Gateway facade ───────────────────────────────────────────────────────────

/// Polymarket WebSocket + REST gateway for the Ingestor layer.
///
/// Holds credentials for the User WS (optional in simulation mode).
/// The Market WS and Gamma API calls are always public (no auth required).
pub struct PolymarketWsGateway {
    /// CLOB API key (L2 HMAC credential). `None` in simulation mode.
    api_key: Option<String>,
    /// CLOB API secret (L2 HMAC credential). `None` in simulation mode.
    #[allow(dead_code)]
    secret: Option<String>,
    /// CLOB API passphrase (L2 HMAC credential). `None` in simulation mode.
    #[allow(dead_code)]
    passphrase: Option<String>,
    /// `true` = simulation mode — User WS and heartbeat loop are skipped.
    sim_mode: bool,
    /// Shared flag allowing callers to signal a graceful shutdown.
    /// Set to `true` to stop all background loops.
    shutdown: Arc<AtomicBool>,
}

impl PolymarketWsGateway {
    /// Create a new gateway instance.
    ///
    /// - Pass `api_key / secret / passphrase` as `Some(...)` in live mode.
    /// - Pass all as `None` in simulation mode — User WS and heartbeat will be skipped.
    pub fn new(
        api_key: Option<String>,
        secret: Option<String>,
        passphrase: Option<String>,
    ) -> Self {
        let sim_mode = api_key.is_none();
        Self {
            api_key,
            secret,
            passphrase,
            sim_mode,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Signal all background loops (heartbeat, market WS, user WS) to shut down.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Market WS
    // ─────────────────────────────────────────────────────────────────────────

    /// Connect to the public Market WS and stream events for the active token IDs.
    ///
    /// Watches `token_rx` for new token lists pushed by `run_market_rotation`.
    /// When tokens change, the current WS session is dropped and a new one
    /// opens with the updated subscription. Runs forever with exponential-backoff
    /// reconnection.
    pub async fn run_market_ws(
        &self,
        token_rx: tokio::sync::watch::Receiver<Vec<String>>,
        tx: Sender<IngestorEvent>,
    ) -> Result<()> {
        market_ws::run_market_ws(self.shutdown.clone(), token_rx, tx).await
    }

    // ─────────────────────────────────────────────────────────────────────────
    // User WS
    // ─────────────────────────────────────────────────────────────────────────

    /// Connect to the authenticated User WS and stream trade / order events.
    ///
    /// Skipped automatically in simulation mode (no credentials → no connection).
    /// Runs forever with exponential-backoff reconnection.
    pub async fn run_user_ws(&self, tx: Sender<IngestorEvent>) -> Result<()> {
        user_ws::run_user_ws(
            self.shutdown.clone(),
            self.sim_mode,
            self.api_key.clone(),
            tx,
        )
        .await
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Heartbeat Loop
    // ─────────────────────────────────────────────────────────────────────────

    /// Send `POST /heartbeat` every 5 seconds to the CLOB.
    ///
    /// Skipped in simulation mode.
    pub async fn run_heartbeat(&self) -> Result<()> {
        heartbeat::run_heartbeat(self.shutdown.clone(), self.sim_mode).await
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Gamma API — Market Discovery & Rotation
    // ─────────────────────────────────────────────────────────────────────────

    /// Query the Gamma API for the next upcoming BTC 15-minute market.
    ///
    /// Returns info for the market with the soonest non-expired `endTimestamp`.
    /// PRD: poll every 10 minutes (Section 5.3).
    pub async fn discover_next_market(&self) -> Result<MarketInfo> {
        rotation::discover_next_market().await
    }

    /// Long-running market rotation manager.
    ///
    /// - Polls Gamma API every 10 minutes for the next market.
    /// - At <180s remaining on the current market, emits `MarketRotation`
    ///   and the caller should re-subscribe WS to new token IDs.
    /// - Emits `IngestorEvent::MarketRotation` once per market transition.
    pub async fn run_market_rotation(
        &self,
        tx: Sender<IngestorEvent>,
        token_tx: tokio::sync::watch::Sender<Vec<String>>,
    ) -> Result<()> {
        rotation::run_market_rotation(self.shutdown.clone(), tx, token_tx).await
    }
}
