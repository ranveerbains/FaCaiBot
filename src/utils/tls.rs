//! Shared TLS utilities used across the codebase.

use anyhow::Result;
use rustls::ClientConfig;

/// Build a standard `rustls::ClientConfig` with the webpki CA roots.
pub(crate) fn build_tls_config() -> Result<ClientConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Ok(ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth())
}

/// Minimal `hyper::rt::Executor` that spawns onto the current tokio runtime.
/// Required by `fastwebsockets::handshake::client`.
pub(crate) struct SpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
    Fut: std::future::Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::task::spawn(fut);
    }
}
