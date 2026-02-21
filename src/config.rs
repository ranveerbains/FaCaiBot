use anyhow::{Context, Result};

/// Bot-wide configuration loaded from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    // Polymarket CLOB credentials (L2 auth)
    pub polymarket_api_key: String,
    pub polymarket_secret: String,
    pub polymarket_passphrase: String,

    // Wallet private key for EIP-712 signing
    pub private_key: String,

    // Infrastructure
    pub redis_url: String,
    pub questdb_url: String,

    // Binance
    pub binance_ws_url: String,
}

impl Config {
    /// Load config from environment. Call `dotenvy::dotenv()` before this.
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            polymarket_api_key: std::env::var("POLYMARKET_API_KEY")
                .context("POLYMARKET_API_KEY not set")?,
            polymarket_secret: std::env::var("POLYMARKET_SECRET")
                .context("POLYMARKET_SECRET not set")?,
            polymarket_passphrase: std::env::var("POLYMARKET_PASSPHRASE")
                .context("POLYMARKET_PASSPHRASE not set")?,
            private_key: std::env::var("PRIVATE_KEY").context("PRIVATE_KEY not set")?,
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1:6379".into()),
            questdb_url: std::env::var("QUESTDB_URL").unwrap_or_else(|_| "127.0.0.1:9009".into()),
            binance_ws_url: std::env::var("BINANCE_WS_URL")
                .unwrap_or_else(|_| "wss://stream.binance.com:9443".into()),
        })
    }
}
