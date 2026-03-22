use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::info;

// ─── TOML Config Sub-structs ─────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RotationConfig {
    pub prewarm_lead_secs: u64,
}

impl Default for RotationConfig {
    fn default() -> Self {
        Self { prewarm_lead_secs: 30 }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct FairValueTomlConfig {
    pub momentum_weight_basis: f64,
    pub momentum_weight_cvd: f64,
    pub momentum_weight_obi: f64,
    pub max_momentum_adj: f64,
    // Edge scaling
    pub vol_edge_scale: f64,
    pub time_edge_scale: f64,
    pub baseline_vol: f64,
    // Vol tracker params
    pub vol_ring_capacity: usize,
    pub vol_session_capacity: usize,
    pub vol_freshness_ms: u64,
    pub vol_min_warmup: usize,
    pub vol_default: f64,
    pub vol_ticks_per_sec: f64,
    // Model params
    pub tail_compression_factor: f64,
    pub stale_data_edge_penalty: f64,
    pub regime_spike_threshold: f64,
    pub regime_spike_penalty: f64,
    pub strike_warmup_count: usize,
    // Metric tracker params
    pub basis_halflife_ms: f64,
    pub basis_freshness_ms: u64,
    pub basis_min: f64,
    pub basis_saturation: f64,
    pub cvd_fast_halflife_ms: f64,
    pub cvd_slow_halflife_ms: f64,
    pub cvd_freshness_ms: u64,
    pub cvd_min: f64,
    pub cvd_saturation: f64,
    pub obi_halflife_ms: f64,
    pub obi_freshness_ms: u64,
    pub obi_min: f64,
    pub obi_saturation: f64,
}

impl Default for FairValueTomlConfig {
    fn default() -> Self {
        Self {
            momentum_weight_basis: 0.10,
            momentum_weight_cvd: 0.05,
            momentum_weight_obi: 0.05,
            max_momentum_adj: 0.08,
            vol_edge_scale: 0.5,
            time_edge_scale: 0.02,
            baseline_vol: 0.00003,
            vol_ring_capacity: 600,
            vol_session_capacity: 6000,
            vol_freshness_ms: 500,
            vol_min_warmup: 10,
            vol_default: 0.00003,
            vol_ticks_per_sec: 20.0,
            tail_compression_factor: 0.85,
            stale_data_edge_penalty: 0.01,
            regime_spike_threshold: 2.0,
            regime_spike_penalty: 0.01,
            strike_warmup_count: 5,
            basis_halflife_ms: 250.0,
            basis_freshness_ms: 150,
            basis_min: 0.0,
            basis_saturation: 0.05,
            cvd_fast_halflife_ms: 200.0,
            cvd_slow_halflife_ms: 500.0,
            cvd_freshness_ms: 150,
            cvd_min: 0.0,
            cvd_saturation: 0.6,
            obi_halflife_ms: 250.0,
            obi_freshness_ms: 120,
            obi_min: 0.0,
            obi_saturation: 0.2,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct QuotingTomlConfig {
    pub min_edge: f64,
    pub requote_threshold: f64,
    pub min_requote_interval_ms: u64,
    pub max_order_size: f64,
    pub min_order_size: f64,
    pub emergency_requote_threshold: f64,
    pub max_imbalance_skew: f64,
    pub max_one_sided_shares: f64,
    pub max_fair_value_extremity: f64,
}

impl Default for QuotingTomlConfig {
    fn default() -> Self {
        Self {
            min_edge: 0.04,
            requote_threshold: 0.01,
            min_requote_interval_ms: 2000,
            max_order_size: 100.0,
            min_order_size: 5.0,
            emergency_requote_threshold: 0.05,
            max_imbalance_skew: 0.02,
            max_one_sided_shares: 5.0,
            max_fair_value_extremity: 0.85,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RiskV2TomlConfig {
    pub max_unpaired_shares: f64,
    pub max_unpaired_usdc: f64,
    pub max_capital_per_market: f64,
    pub closing_phase_secs: u64,
    pub rotation_quiet_ms: u64,
    pub max_closing_pair_cost: f64,
    pub max_closing_attempts: u32,
    pub closing_retry_price_increment: f64,
    pub rebalance_threshold: f64,
    pub rebalance_size: f64,
    pub rebalance_max_pair_cost: f64,
    pub min_rebalance_interval_ms: u64,
    pub stale_book_ms: u64,
    pub max_entry_spread: f64,
    pub heartbeat_dead_threshold: u32,
    pub binance_stale_event_ms: u64,
}

impl Default for RiskV2TomlConfig {
    fn default() -> Self {
        Self {
            max_unpaired_shares: 30.0,
            max_unpaired_usdc: 25.0,
            max_capital_per_market: 100.0,
            closing_phase_secs: 45,
            rotation_quiet_ms: 5000,
            max_closing_pair_cost: 0.97,
            max_closing_attempts: 3,
            closing_retry_price_increment: 0.01,
            rebalance_threshold: 20.0,
            rebalance_size: 10.0,
            rebalance_max_pair_cost: 0.96,
            min_rebalance_interval_ms: 10000,
            stale_book_ms: 850,
            max_entry_spread: 0.04,
            heartbeat_dead_threshold: 5,
            binance_stale_event_ms: 150,
        }
    }
}

// ─── Top-level TOML config ──────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BotConfig {
    pub rotation: RotationConfig,
    pub fair_value: FairValueTomlConfig,
    pub quoting: QuotingTomlConfig,
    pub risk_v2: RiskV2TomlConfig,
}

impl Default for BotConfig {
    fn default() -> Self {
        Self {
            rotation: RotationConfig::default(),
            fair_value: FairValueTomlConfig::default(),
            quoting: QuotingTomlConfig::default(),
            risk_v2: RiskV2TomlConfig::default(),
        }
    }
}

// ─── Main Config struct ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Config {
    // ── Polymarket CLOB credentials ──
    pub polymarket_api_key: String,
    pub polymarket_secret: String,
    pub polymarket_passphrase: String,
    pub private_key: String,

    // ── Infrastructure ──
    pub questdb_url: String,
    pub questdb_http_url: String,

    // ── Binance ──
    pub binance_sbe_ws_url: String,
    pub binance_ed25519_api_key: String,
    pub binance_futures_ws_url: String,

    // ── Telegram ──
    pub telegram_bot_token: String,
    pub telegram_chat_id: String,
    pub telegram_allowed_user_id: Option<i64>,

    // ── Tuning (from config.toml) ──
    pub bot: BotConfig,

    // ── Derived Decimal values ──
    pub max_capital_per_market: Decimal,
    pub stale_event_threshold_ms: u64,
}

impl Config {
    pub fn load() -> Result<Self> {
        let config_path =
            std::env::var("CONFIG_FILE").unwrap_or_else(|_| "config.toml".to_string());
        let bot = if std::path::Path::new(&config_path).exists() {
            let contents = std::fs::read_to_string(&config_path)
                .with_context(|| format!("failed to read config file: {config_path}"))?;
            let parsed: BotConfig = toml::from_str(&contents)
                .with_context(|| format!("failed to parse config file: {config_path}"))?;
            info!(path = %config_path, "loaded config from TOML file");
            parsed
        } else {
            info!(path = %config_path, "config file not found — using defaults");
            BotConfig::default()
        };

        let polymarket_api_key = std::env::var("POLYMARKET_API_KEY")
            .context("POLYMARKET_API_KEY not set (required)")?;
        let polymarket_secret = std::env::var("POLYMARKET_SECRET")
            .context("POLYMARKET_SECRET not set (required)")?;
        let polymarket_passphrase = std::env::var("POLYMARKET_PASSPHRASE")
            .context("POLYMARKET_PASSPHRASE not set (required)")?;
        let private_key = std::env::var("PRIVATE_KEY")
            .context("PRIVATE_KEY not set (required)")?;
        let telegram_bot_token = std::env::var("TELEGRAM_BOT_TOKEN")
            .context("TELEGRAM_BOT_TOKEN not set (required)")?;
        let telegram_chat_id = std::env::var("TELEGRAM_CHAT_ID")
            .context("TELEGRAM_CHAT_ID not set (required)")?;
        let telegram_allowed_user_id = std::env::var("TELEGRAM_ALLOWED_USER_ID")
            .ok()
            .and_then(|s| s.parse::<i64>().ok());

        let max_capital_per_market = Decimal::try_from(bot.risk_v2.max_capital_per_market)
            .context("risk_v2.max_capital_per_market: invalid decimal")?;
        let stale_event_threshold_ms = bot.risk_v2.binance_stale_event_ms;

        let config = Self {
            polymarket_api_key,
            polymarket_secret,
            polymarket_passphrase,
            private_key,
            questdb_url: std::env::var("QUESTDB_URL").unwrap_or_else(|_| "127.0.0.1:9009".into()),
            questdb_http_url: std::env::var("QUESTDB_HTTP_URL").unwrap_or_else(|_| "http://127.0.0.1:9000".into()),
            binance_sbe_ws_url: std::env::var("BINANCE_SBE_WS_URL")
                .unwrap_or_else(|_| "wss://stream-sbe.binance.com:9443".into()),
            binance_ed25519_api_key: std::env::var("BINANCE_ED25519_API_KEY")
                .context("BINANCE_ED25519_API_KEY not set (required for SBE market data)")?,
            binance_futures_ws_url: std::env::var("BINANCE_FUTURES_WS_URL")
                .unwrap_or_else(|_| "wss://fstream.binance.com".into()),
            telegram_bot_token,
            telegram_chat_id,
            telegram_allowed_user_id,
            bot,
            max_capital_per_market,
            stale_event_threshold_ms,
        };

        if config.max_capital_per_market <= Decimal::ZERO {
            anyhow::bail!("risk_v2.max_capital_per_market must be positive");
        }
        Ok(config)
    }

}
