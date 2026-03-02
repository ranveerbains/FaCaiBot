//! Shared TLS WebSocket and HTTP helpers used by market_ws, user_ws, heartbeat, and rotation.

use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use fastwebsockets::{WebSocket, handshake};
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::header::{CONNECTION, UPGRADE};
use hyper::upgrade::Upgraded;
use hyper::{Method, Request, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use rustls::ClientConfig as TlsClientConfig;
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tracing::warn;

// ─── TLS WebSocket helpers ────────────────────────────────────────────────────

/// Open a TLS WebSocket connection to any `wss://` URL.
///
/// Uses the same TLS + fastwebsockets handshake pattern as `binance.rs`.
pub(super) async fn tls_connect(url: &str) -> Result<WebSocket<TokioIo<Upgraded>>> {
    let uri: Uri = url.parse().context("invalid WS URL")?;
    let host = uri.host().context("WS URL has no host")?.to_string();
    let port = uri.port_u16().unwrap_or(443);
    let addr = format!("{host}:{port}");

    // TCP.
    let tcp = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("TCP connect to {addr} failed"))?;
    tcp.set_nodelay(true).context("TCP_NODELAY failed")?;

    // TLS.
    let tls_config = build_tls_config()?;
    let connector = TlsConnector::from(Arc::new(tls_config));
    let server_name = ServerName::try_from(host.as_str())
        .map_err(|e| anyhow!("invalid TLS server name '{}': {e}", host))?
        .to_owned();
    let tls_stream = connector
        .connect(server_name, tcp)
        .await
        .context("TLS handshake failed")?;

    // WebSocket upgrade.
    let path = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
    let request = Request::builder()
        .method("GET")
        .uri(path)
        .header("Host", &host)
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "upgrade")
        .header("Sec-WebSocket-Key", handshake::generate_key())
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::<Bytes>::new())
        .context("failed to build WS upgrade request")?;

    let (ws, _resp) = handshake::client(&SpawnExecutor, request, tls_stream)
        .await
        .context("WebSocket handshake failed")?;

    Ok(ws)
}

/// Build a standard `rustls::ClientConfig` with the system CA roots.
pub(super) fn build_tls_config() -> Result<TlsClientConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Ok(TlsClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth())
}

// ─── HTTP helpers (for REST calls) ────────────────────────────────────────────

/// Make a GET request to `url` over TLS and return the response body.
///
/// Uses a simple hyper + tokio-rustls stack (no pooling — these are infrequent
/// REST calls, not hot-path). Handles HTTP 425 (matching engine restart) with
/// a warning log.
pub(super) async fn http_get(url: &str) -> Result<Vec<u8>> {
    let uri: Uri = url.parse().context("invalid URL")?;
    let host = uri.host().context("URL missing host")?.to_string();
    let port = uri.port_u16().unwrap_or(443);
    let addr = format!("{host}:{port}");

    let tcp = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("TCP connect to {addr} failed"))?;
    let tls_config = build_tls_config()?;
    let connector = TlsConnector::from(Arc::new(tls_config));
    let server_name = ServerName::try_from(host.as_str())
        .map_err(|e| anyhow!("TLS server name error: {e}"))?
        .to_owned();
    let tls_stream = connector
        .connect(server_name, tcp)
        .await
        .context("TLS handshake failed")?;

    let io = TokioIo::new(tls_stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .context("HTTP/1.1 handshake failed")?;
    tokio::spawn(conn);

    let path = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
    let req = Request::builder()
        .method(Method::GET)
        .uri(path)
        .header("Host", &host)
        .header("User-Agent", "facaibot/0.1")
        .body(Empty::<Bytes>::new())
        .context("GET request build failed")?;

    let resp = sender.send_request(req).await.context("GET send failed")?;
    let status = resp.status();

    if status == StatusCode::from_u16(425).unwrap() {
        warn!(
            url = %url,
            "HTTP 425 — Polymarket matching engine restart in progress; retrying"
        );
    } else if !status.is_success() {
        warn!(url = %url, status = %status, "HTTP GET non-2xx response");
    }

    let body = resp
        .into_body()
        .collect()
        .await
        .context("GET body collect failed")?
        .to_bytes();

    Ok(body.to_vec())
}

// ─── fastwebsockets executor adapter ─────────────────────────────────────────

/// Minimal hyper executor that spawns futures onto the current tokio runtime.
/// Required by `fastwebsockets::handshake::client`.
pub(super) struct SpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
    Fut: std::future::Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::task::spawn(fut);
    }
}
