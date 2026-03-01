use std::sync::Arc;

use crossbeam_channel::Sender;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::header::{CONTENT_TYPE, HOST};
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_rustls::TlsConnector;
use tracing::{debug, info, warn};

use crate::control::handlers;
use crate::control::types::{BotStatus, DrainStatus, NotifyFlags};
use crate::reporting::telegram::{TELEGRAM_HOST, post_telegram_message};
use crate::types::market::IngestorEvent;

const TELEGRAM_PORT: u16 = 443;
/// Minimum interval between processing commands (ms).
const COMMAND_RATE_LIMIT_MS: u64 = 2_000;

/// Polls Telegram `getUpdates` for commands, dispatches to handlers,
/// and watches DrainStatus for progress updates.
pub struct TelegramCommandListener {
    bot_token: String,
    chat_id: String,
    allowed_user_id: i64,
    tls_connector: TlsConnector,
    notify_flags: Arc<NotifyFlags>,
    ingestor_tx: Sender<IngestorEvent>,
    status_rx: watch::Receiver<BotStatus>,
    drain_status_rx: watch::Receiver<DrainStatus>,
}

impl TelegramCommandListener {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bot_token: String,
        chat_id: String,
        allowed_user_id: i64,
        tls_connector: TlsConnector,
        notify_flags: Arc<NotifyFlags>,
        ingestor_tx: Sender<IngestorEvent>,
        status_rx: watch::Receiver<BotStatus>,
        drain_status_rx: watch::Receiver<DrainStatus>,
    ) -> Self {
        Self {
            bot_token,
            chat_id,
            allowed_user_id,
            tls_connector,
            notify_flags,
            ingestor_tx,
            status_rx,
            drain_status_rx,
        }
    }

    /// Main loop. Splits into two concurrent tasks:
    /// 1. Poll getUpdates for commands
    /// 2. Watch drain status for progress updates
    pub async fn run(self) {
        info!("command listener started (user_id={})", self.allowed_user_id);

        let bot_token = self.bot_token;
        let chat_id = self.chat_id;
        let allowed_user_id = self.allowed_user_id;
        let tls_connector = self.tls_connector;
        let notify_flags = self.notify_flags;
        let ingestor_tx = self.ingestor_tx;
        let status_rx = self.status_rx;
        let mut drain_status_rx = self.drain_status_rx;

        // Clone for the drain watcher task.
        let drain_tls = tls_connector.clone();
        let drain_bot_token = bot_token.clone();
        let drain_chat_id = chat_id.clone();

        // Task 1: Watch drain status → send Telegram progress updates.
        let _drain_watcher = tokio::spawn(async move {
            loop {
                if drain_status_rx.changed().await.is_err() {
                    break; // channel closed
                }
                let status = drain_status_rx.borrow_and_update().clone();
                match status {
                    DrainStatus::Idle => {}
                    DrainStatus::Draining {
                        reason,
                        position_info,
                    } => {
                        let msg =
                            format!("Drain mode activated ({reason}) — {position_info}");
                        let _ = post_telegram_message(
                            &drain_tls,
                            &drain_bot_token,
                            &drain_chat_id,
                            &msg,
                        )
                        .await;
                    }
                    DrainStatus::Complete { exit_code, summary } => {
                        let _ = post_telegram_message(
                            &drain_tls,
                            &drain_bot_token,
                            &drain_chat_id,
                            &summary,
                        )
                        .await;
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        info!(exit_code, "drain complete — exiting");
                        std::process::exit(exit_code);
                    }
                }
            }
        });

        // Task 2: Poll getUpdates for commands.
        let mut last_update_id: i64 = 0;
        let mut last_command_ms: u64 = 0;

        loop {
            match poll_updates(&tls_connector, &bot_token, &mut last_update_id).await {
                Ok(messages) => {
                    for msg in messages {
                        let reply = handle_message(
                            &msg,
                            allowed_user_id,
                            &chat_id,
                            &mut last_command_ms,
                            &notify_flags,
                            &ingestor_tx,
                            &status_rx,
                        );
                        if let Some(reply_text) = reply {
                            let _ = post_telegram_message(
                                &tls_connector,
                                &bot_token,
                                &chat_id,
                                &reply_text,
                            )
                            .await;
                        }
                    }
                }
                Err(e) => {
                    debug!(error = %e, "getUpdates failed — retrying in 5s");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        }

        // drain_watcher runs indefinitely; suppress the "unreachable" warning.
        #[allow(unreachable_code)]
        {
            let _ = _drain_watcher.await;
        }
    }
}

/// Process a single incoming message: auth check, parse command, dispatch.
/// Returns the reply text, or None if the message should be ignored.
fn handle_message(
    msg: &TelegramMessage,
    allowed_user_id: i64,
    chat_id: &str,
    last_command_ms: &mut u64,
    notify_flags: &Arc<NotifyFlags>,
    ingestor_tx: &Sender<IngestorEvent>,
    status_rx: &watch::Receiver<BotStatus>,
) -> Option<String> {
    let from_id = msg.from.as_ref().map(|f| f.id).unwrap_or(0);
    let msg_chat_id = msg.chat.id.to_string();

    if from_id != allowed_user_id || msg_chat_id != chat_id {
        warn!(
            from_id,
            chat_id = msg_chat_id,
            "rejected command from unauthorized source"
        );
        return None;
    }

    let text = msg.text.as_deref()?.trim();

    // Rate limit: 2s between commands.
    let now_ms = crate::utils::time::epoch_ms();
    if now_ms.saturating_sub(*last_command_ms) < COMMAND_RATE_LIMIT_MS {
        debug!("command rate limited");
        return None;
    }
    *last_command_ms = now_ms;

    // Parse command and args.
    let rest = text.strip_prefix('/')?;
    let (cmd, args) = match rest.split_once(' ') {
        Some((c, a)) => (c, a),
        None => (rest, ""),
    };

    // Strip bot username suffix (e.g. /status@f4c4ibot).
    let cmd = cmd.split('@').next().unwrap_or(cmd);

    let reply = match cmd {
        "trades" => handlers::handle_trades(args, notify_flags),
        "summary" => handlers::handle_summary(args, notify_flags),
        "stop" => handlers::handle_stop(ingestor_tx),
        "set" => handlers::handle_set(args, ingestor_tx),
        "config" => handlers::handle_config(args),
        "status" => {
            let status = status_rx.borrow().clone();
            handlers::handle_status(&status)
        }
        "help" => handlers::handle_help(),
        _ => format!("Unknown command: /{cmd}. Send /help for usage."),
    };

    Some(reply)
}

/// Long-poll Telegram `getUpdates` API.
async fn poll_updates(
    tls_connector: &TlsConnector,
    bot_token: &str,
    last_update_id: &mut i64,
) -> anyhow::Result<Vec<TelegramMessage>> {
    let path = format!(
        "/bot{}/getUpdates?offset={}&timeout=30&allowed_updates=[\"message\"]",
        bot_token,
        *last_update_id + 1
    );

    let tcp = TcpStream::connect((TELEGRAM_HOST, TELEGRAM_PORT)).await?;
    let server_name = rustls::pki_types::ServerName::try_from(TELEGRAM_HOST)
        .map_err(|e| anyhow::anyhow!("invalid server name: {e}"))?
        .to_owned();
    let tls_stream = tls_connector.connect(server_name, tcp).await?;
    let io = TokioIo::new(tls_stream);

    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            warn!(error = %e, "getUpdates connection error");
        }
    });

    let req = Request::builder()
        .method(Method::GET)
        .uri(&path)
        .header(HOST, TELEGRAM_HOST)
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::new()))?;

    let resp = sender.send_request(req).await?;
    let body = resp.into_body().collect().await?.to_bytes();
    let response: GetUpdatesResponse = serde_json::from_slice(&body)?;

    let mut messages = Vec::new();
    if response.ok {
        for update in response.result {
            if update.update_id > *last_update_id {
                *last_update_id = update.update_id;
            }
            if let Some(msg) = update.message {
                messages.push(msg);
            }
        }
    }

    Ok(messages)
}

// ─── Telegram API Response Types ─────────────────────────────────────────────

#[derive(Deserialize)]
struct GetUpdatesResponse {
    ok: bool,
    #[serde(default)]
    result: Vec<Update>,
}

#[derive(Deserialize)]
struct Update {
    update_id: i64,
    message: Option<TelegramMessage>,
}

#[derive(Deserialize)]
struct TelegramMessage {
    text: Option<String>,
    from: Option<TelegramUser>,
    chat: TelegramChat,
}

#[derive(Deserialize)]
struct TelegramUser {
    id: i64,
}

#[derive(Deserialize)]
struct TelegramChat {
    id: i64,
}
