// Telegram reporter — sends pre-formatted alerts via HTTP POST to the
// Telegram Bot API (https://api.telegram.org/bot{token}/sendMessage).
//
// Transport: hyper 1.x + tokio-rustls (direct connection, no legacy Client).
// We establish a TLS connection with tokio-rustls, then use hyper's connection
// API to send a single HTTP/1.1 request per message.
//
// All sends are fire-and-forget: spawned as detached tokio tasks so they never
// block the executor. Failures are logged via `tracing` and silently
// dropped.
//
// In v2, the strategy engine formats its own messages and calls
// `reporter.fire_critical(text)` directly. The reporter is a pure transport
// layer — no domain-specific formatting logic lives here.
//
// Message format: HTML parse_mode (bold via <b>, code via <code>).

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::header::{CONTENT_TYPE, HOST};
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use serde_json::json;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_rustls::TlsConnector;
use tracing::{debug, error, info, warn};

// ─── TelegramReporter ────────────────────────────────────────────────────────

/// Sends pre-formatted messages to a Telegram bot via raw HTTPS POST to the
/// Telegram Bot API. All methods are fire-and-forget: they spawn a detached
/// tokio task and return immediately, never blocking the executor.
///
/// In v2, callers format their own messages and use `fire_critical()` or
/// `fire_and_forget()` directly. The reporter owns no formatting logic.
#[derive(Clone)]
pub struct TelegramReporter {
    inner: Arc<ReporterInner>,
}

struct ReporterInner {
    bot_token: String,
    chat_id: String,
    /// Shared TLS connector built once; cheap to clone.
    tls_connector: TlsConnector,
    /// Epoch ms of the last dispatched message. Rate limiting: 5s minimum interval.
    last_send_ms: AtomicU64,
    /// Tracked sent message IDs for periodic cleanup (bounded to last 200).
    sent_messages: Mutex<VecDeque<i64>>,
}

impl TelegramReporter {
    /// Construct a new reporter.
    ///
    /// # Panics
    /// Panics if the TLS root certificate store cannot be built (should not
    /// happen with bundled webpki-roots).
    pub fn new(bot_token: String, chat_id: String) -> Self {
        let tls_config = crate::utils::tls::build_tls_config()
            .expect("failed to build TLS config");
        let tls_connector = TlsConnector::from(Arc::new(tls_config));

        Self {
            inner: Arc::new(ReporterInner {
                bot_token,
                chat_id,
                tls_connector,
                last_send_ms: AtomicU64::new(0),
                sent_messages: Mutex::new(VecDeque::new()),
            }),
        }
    }

    // ─── Public API ───────────────────────────────────────────────────────────

    /// Send a startup notice — "FaCaiBot live started".
    pub fn send_live_startup_message(&self) {
        let text = concat!(
            "<b>FaCaiBot live started</b>\n\n",
            "Connected to Binance SBE + Polymarket CLOB. ",
            "Real orders will be placed.",
        )
        .to_owned();
        self.fire_and_forget(text);
    }

    /// Spawn a detached tokio task that POSTs `text` to the Telegram sendMessage
    /// endpoint. Never blocks the caller. Errors are logged and dropped.
    ///
    /// Rate-limited: minimum 5s between sends to avoid Telegram 429. Messages
    /// that arrive too soon are silently dropped.
    pub fn fire_and_forget(&self, text: String) {
        // Rate limit: minimum 5s between sends to avoid Telegram 429.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let last = self.inner.last_send_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last) < 5_000 {
            debug!("Telegram rate limited — dropping message");
            return;
        }
        self.inner.last_send_ms.store(now_ms, Ordering::Relaxed);

        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            for chunk in split_message(&text) {
                match post_telegram_message(
                    &inner.tls_connector,
                    &inner.bot_token,
                    &inner.chat_id,
                    &chunk,
                )
                .await
                {
                    Ok(Some(msg_id)) => {
                        track_message_id(&inner.sent_messages, msg_id).await;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        error!(error = %e, "Telegram send failed");
                    }
                }
            }
        });
    }

    /// Fire a message unconditionally (never dropped). Spaces critical messages
    /// at least 1.5s apart so they don't pile up on Telegram.
    pub fn fire_critical(&self, text: String) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let last = self.inner.last_send_ms.swap(now_ms, Ordering::Relaxed);
        let gap_ms = now_ms.saturating_sub(last);

        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            // Space critical messages at least 1.5s apart so they don't pile up on Telegram.
            if gap_ms < 1_500 {
                tokio::time::sleep(std::time::Duration::from_millis(1_500 - gap_ms)).await;
            }
            for chunk in split_message(&text) {
                match post_telegram_message(
                    &inner.tls_connector,
                    &inner.bot_token,
                    &inner.chat_id,
                    &chunk,
                )
                .await
                {
                    Ok(Some(msg_id)) => {
                        track_message_id(&inner.sent_messages, msg_id).await;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        error!(error = %e, "Telegram critical send failed");
                    }
                }
            }
        });
    }

    /// Track an externally sent message ID for cleanup (e.g. from direct
    /// `post_telegram_message` calls outside the reporter's send methods).
    pub async fn track_msg_id(&self, msg_id: i64) {
        track_message_id(&self.inner.sent_messages, msg_id).await;
    }

    /// Spawn a background task that deletes tracked messages every hour.
    /// Call once after reporter construction. Silently ignores deletion failures.
    pub fn spawn_cleanup_task(&self) {
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3_600)).await;
                let ids: Vec<i64> = {
                    let mut lock = inner.sent_messages.lock().await;
                    lock.drain(..).collect()
                };
                if ids.is_empty() {
                    continue;
                }
                info!(count = ids.len(), "cleaning up Telegram messages");
                for msg_id in ids {
                    let _ = delete_telegram_message(
                        &inner.tls_connector,
                        &inner.bot_token,
                        &inner.chat_id,
                        msg_id,
                    )
                    .await;
                }
            }
        });
    }
}

// ─── Message Chunking ─────────────────────────────────────────────────────────

const TELEGRAM_MAX_LEN: usize = 4096;

/// Split a message into chunks that fit within Telegram's 4096-character limit.
/// Splits at newline boundaries where possible to avoid cutting mid-line.
fn split_message(text: &str) -> Vec<String> {
    if text.len() <= TELEGRAM_MAX_LEN {
        return vec![text.to_owned()];
    }

    let mut chunks: Vec<String> = Vec::new();
    let mut current = String::new();

    for line in text.split('\n') {
        // +1 for the '\n' we'd add between lines
        let needed = if current.is_empty() {
            line.len()
        } else {
            current.len() + 1 + line.len()
        };

        if needed > TELEGRAM_MAX_LEN {
            // Flush the current chunk and start a new one.
            if !current.is_empty() {
                chunks.push(current.clone());
                current.clear();
            }
            // If a single line is itself over the limit, hard-truncate it.
            if line.len() > TELEGRAM_MAX_LEN {
                chunks.push(line[..TELEGRAM_MAX_LEN].to_owned());
            } else {
                current.push_str(line);
            }
        } else {
            if !current.is_empty() {
                current.push('\n');
            }
            current.push_str(line);
        }
    }

    if !current.is_empty() {
        chunks.push(current);
    }

    chunks
}

// ─── HTTP Transport ───────────────────────────────────────────────────────────

pub(crate) const TELEGRAM_HOST: &str = "api.telegram.org";
const TELEGRAM_PORT: u16 = 443;

/// Build a TLS connector for Telegram API calls.
pub(crate) fn build_tls_connector() -> TlsConnector {
    let tls_config = crate::utils::tls::build_tls_config()
        .expect("failed to build TLS config");
    TlsConnector::from(Arc::new(tls_config))
}

/// POST a single JSON message to the Telegram Bot API.
///
/// Opens a fresh TLS connection per call. This is intentionally simple
/// (no connection pooling) — Telegram calls are infrequent (< 1/s) so
/// connection overhead is acceptable.
pub(crate) async fn post_telegram_message(
    tls_connector: &TlsConnector,
    bot_token: &str,
    chat_id: &str,
    text: &str,
) -> anyhow::Result<Option<i64>> {
    // ── Build JSON body ───────────────────────────────────────────────────────
    let body_json = json!({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": true,
    });
    let body_str = serde_json::to_string(&body_json)?;
    let body_bytes = Bytes::from(body_str);
    let content_len = body_bytes.len();

    // ── Establish TLS connection ───────────────────────────────────────────────
    let tcp = TcpStream::connect((TELEGRAM_HOST, TELEGRAM_PORT)).await?;
    let server_name = rustls::pki_types::ServerName::try_from(TELEGRAM_HOST)
        .map_err(|e| anyhow::anyhow!("invalid server name: {e}"))?
        .to_owned();
    let tls_stream = tls_connector.connect(server_name, tcp).await?;
    let io = TokioIo::new(tls_stream);

    // ── HTTP/1.1 handshake ────────────────────────────────────────────────────
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;

    // Drive the connection in the background.
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            warn!(error = %e, "Telegram HTTP connection error");
        }
    });

    // ── Build and send request ────────────────────────────────────────────────
    let path = format!("/bot{}/sendMessage", bot_token);
    let req = Request::builder()
        .method(Method::POST)
        .uri(&path)
        .header(HOST, TELEGRAM_HOST)
        .header(CONTENT_TYPE, "application/json")
        .header("Content-Length", content_len.to_string())
        .body(Full::new(body_bytes))?;

    let resp = sender.send_request(req).await?;
    let status = resp.status();

    let body = resp.into_body().collect().await?.to_bytes();
    if !status.is_success() {
        let body_text = String::from_utf8_lossy(&body);
        warn!(
            status = %status,
            body = %body_text,
            "Telegram API returned non-2xx response"
        );
        return Ok(None);
    }

    // Extract message_id from response for cleanup tracking.
    let message_id = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v.get("result")?.get("message_id")?.as_i64());

    Ok(message_id)
}

/// Track a sent message ID for later cleanup (bounded to 200).
async fn track_message_id(sent_messages: &Mutex<VecDeque<i64>>, msg_id: i64) {
    let mut lock = sent_messages.lock().await;
    lock.push_back(msg_id);
    while lock.len() > 200 {
        lock.pop_front();
    }
}

/// Delete a single Telegram message. Silently ignores failures.
async fn delete_telegram_message(
    tls_connector: &TlsConnector,
    bot_token: &str,
    chat_id: &str,
    message_id: i64,
) -> anyhow::Result<()> {
    let body_json = json!({
        "chat_id": chat_id,
        "message_id": message_id,
    });
    let body_str = serde_json::to_string(&body_json)?;
    let body_bytes = Bytes::from(body_str);
    let content_len = body_bytes.len();

    let tcp = TcpStream::connect((TELEGRAM_HOST, TELEGRAM_PORT)).await?;
    let server_name = rustls::pki_types::ServerName::try_from(TELEGRAM_HOST)
        .map_err(|e| anyhow::anyhow!("invalid server name: {e}"))?
        .to_owned();
    let tls_stream = tls_connector.connect(server_name, tcp).await?;
    let io = TokioIo::new(tls_stream);

    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            warn!(error = %e, "Telegram deleteMessage connection error");
        }
    });

    let path = format!("/bot{}/deleteMessage", bot_token);
    let req = Request::builder()
        .method(Method::POST)
        .uri(&path)
        .header(HOST, TELEGRAM_HOST)
        .header(CONTENT_TYPE, "application/json")
        .header("Content-Length", content_len.to_string())
        .body(Full::new(body_bytes))?;

    let resp = sender.send_request(req).await?;
    if !resp.status().is_success() {
        debug!(message_id, "deleteMessage failed (may already be deleted)");
    }

    Ok(())
}
