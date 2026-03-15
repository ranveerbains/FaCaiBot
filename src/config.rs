use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::info;

// ─── TOML Config Sub-structs ─────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct EntryGuardsConfig {
    /// No entries within this many seconds of market expiry.
    pub entry_cutoff_secs: u64,
    /// Max age (ms) of a Binance SBE event before it is discarded.
    pub binance_stale_event_ms: u64,
    /// Max book age (ms) before blocking entry.
    pub stale_book_ms: u64,
    /// Quiet period (ms) after market rotation — no new Leg 1 entries.
    pub rotation_quiet_ms: u64,
    /// Cooldown (ms) after trade completion before allowing new entries.
    pub trade_cooldown_ms: u64,
    /// Consecutive heartbeat failures before assuming CLOB cancelled all resting orders.
    pub heartbeat_dead_threshold: u32,
    /// Maximum bid-ask spread on the directional book to allow Leg 1 entry.
    /// Prevents entries on stale/illiquid Polymarket books (e.g. off-hours).
    pub max_entry_spread: f64,
}

impl Default for EntryGuardsConfig {
    fn default() -> Self {
        Self {
            entry_cutoff_secs: 180,
            binance_stale_event_ms: 50,
            stale_book_ms: 1000,
            rotation_quiet_ms: 30000,
            trade_cooldown_ms: 5000,
            heartbeat_dead_threshold: 5,
            max_entry_spread: 0.03,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CapitalConfig {
    /// Maximum USDC to allocate per leg per trade (nearest dollar, $1 floor).
    pub max_alloc_per_trade: f64,
}

impl Default for CapitalConfig {
    fn default() -> Self {
        Self {
            max_alloc_per_trade: 10.0,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RiskConfig {
    /// Phase 1 timeout (ms) — time at profit target before transitioning to Phase 2.
    pub phase1_timeout_ms: u64,
    /// Phase 1 breach threshold: pair cost > this triggers immediate FOK taker.
    /// Must be > 1.00. Default 1.05 (overridden in config.toml).
    pub phase1_breach_threshold: f64,
    /// Phase 2 timeout (ms) — time at break-even pursuit before FOK taker exit.
    pub phase2_timeout_ms: u64,
    /// Timeout (ms) for favorable maker try before FOK fallback on crosses-book.
    pub favorable_maker_timeout_ms: u64,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            phase1_timeout_ms: 2000,
            phase1_breach_threshold: 1.05,
            phase2_timeout_ms: 2000,
            favorable_maker_timeout_ms: 1000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RotationConfig {
    /// How many seconds before current market expiry to discover and pre-warm the next market.
    pub prewarm_lead_secs: u64,
}

impl Default for RotationConfig {
    fn default() -> Self {
        Self {
            prewarm_lead_secs: 30,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RepricingConfig {
    /// Model output ceiling / 100% allocation threshold (0.015 = 1.5%).
    pub reprice_scale: f64,
    /// Minimum model output to enter a trade (0.005 = 0.5%).
    pub min_reprice_pct: f64,
    /// Floor allocation fraction (0.3 = always use at least 30% of max_alloc).
    pub min_alloc_pct: f64,
    /// Hard reject: YES mid beyond this regardless of model.
    pub hard_skew_cap: f64,
    /// Exponent for time amplification (0.5 = sqrt, 0 = disabled, 1.0 = linear).
    pub time_exponent: f64,
    /// Maximum time amplification factor (2.0 = time can at most double the output).
    pub max_time_factor: f64,
    /// Phase 1 target dampening factor (0.8 = target 80% of expected repricing).
    /// Only affects Phase 1 profit target — entry gate and allocation use raw expected_pct.
    pub phase1_target_dampen: f64,
}

impl Default for RepricingConfig {
    fn default() -> Self {
        Self {
            reprice_scale: 0.015,
            min_reprice_pct: 0.005,
            min_alloc_pct: 0.3,
            hard_skew_cap: 0.90,
            time_exponent: 0.5,
            max_time_factor: 2.0,
            phase1_target_dampen: 0.8,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BuildupTomlConfig {
    /// Entry threshold — composite must exceed to trigger Leg 1 entry.
    pub entry_threshold: f64,
    /// Cancel threshold — cancel unfilled Leg 1 if composite drops below.
    pub cancel_threshold: f64,
    /// Max wait (ms) for Leg 1 maker fill before cancelling.
    pub cancel_window_ms: u64,
    /// Maximum dissenting directional metrics allowed in consensus vote (default 1).
    pub max_dissenters: u32,
    // Metric weights (must sum to 1.0)
    pub w_cvd: f64,
    pub w_spot_flow: f64,
    pub w_obi: f64,
    pub w_basis: f64,
    pub w_liq: f64,
    pub w_atr: f64,
    // Freshness gates (ms)
    pub freshness_cvd_ms: u64,
    pub freshness_spot_flow_ms: u64,
    pub freshness_obi_ms: u64,
    pub freshness_basis_ms: u64,
    pub freshness_liq_ms: u64,
    pub freshness_atr_ms: u64,
    // Normalization bounds
    pub cvd_min: f64,
    pub cvd_saturation: f64,
    pub spot_flow_min: f64,
    pub spot_flow_saturation: f64,
    pub obi_min: f64,
    pub obi_saturation: f64,
    pub basis_min: f64,
    pub basis_saturation: f64,
    pub liq_min: f64,
    pub liq_saturation: f64,
    pub atr_min: f64,
    pub atr_saturation: f64,
    // EMA half-lives (ms) — time for old value to decay to 50% weight
    pub cvd_fast_halflife_ms: f64,
    pub cvd_slow_halflife_ms: f64,
    pub spot_flow_halflife_ms: f64,
    pub obi_velocity_halflife_ms: f64,
    pub basis_halflife_ms: f64,
}

impl Default for BuildupTomlConfig {
    fn default() -> Self {
        Self {
            entry_threshold: 0.40,
            cancel_threshold: 0.25,
            cancel_window_ms: 500,
            max_dissenters: 1,
            w_cvd: 0.30,
            w_spot_flow: 0.15,
            w_obi: 0.20,
            w_basis: 0.20,
            w_liq: 0.05,
            w_atr: 0.10,
            freshness_cvd_ms: 300,
            freshness_spot_flow_ms: 200,
            freshness_obi_ms: 100,
            freshness_basis_ms: 300,
            freshness_liq_ms: 3000,
            freshness_atr_ms: 100,
            cvd_min: 0.0,
            cvd_saturation: 1.0,
            spot_flow_min: 0.0,
            spot_flow_saturation: 1.0,
            obi_min: 0.0,
            obi_saturation: 0.5,
            basis_min: 0.0,
            basis_saturation: 2.0,
            liq_min: 0.0,
            liq_saturation: 10.0,
            atr_min: 0.0,
            atr_saturation: 15.0,
            cvd_fast_halflife_ms: 150.0,
            cvd_slow_halflife_ms: 700.0,
            spot_flow_halflife_ms: 300.0,
            obi_velocity_halflife_ms: 300.0,
            basis_halflife_ms: 300.0,
        }
    }
}

// ─── Top-level TOML config ──────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BotConfig {
    pub rotation: RotationConfig,
    pub entry_guards: EntryGuardsConfig,
    pub capital: CapitalConfig,
    pub risk: RiskConfig,
    pub repricing: RepricingConfig,
    pub buildup: BuildupTomlConfig,
}

impl Default for BotConfig {
    fn default() -> Self {
        Self {
            rotation: RotationConfig::default(),
            entry_guards: EntryGuardsConfig::default(),
            capital: CapitalConfig::default(),
            risk: RiskConfig::default(),
            repricing: RepricingConfig::default(),
            buildup: BuildupTomlConfig::default(),
        }
    }
}

// ─── Main Config struct ─────────────────────────────────────────────────────

/// Bot-wide configuration loaded from `.env` (secrets/infra) + `config.toml` (tuning).
#[derive(Debug, Clone)]
pub struct Config {
    // ── Polymarket CLOB credentials (L2 auth) ─────────────────────────
    pub polymarket_api_key: String,
    pub polymarket_secret: String,
    pub polymarket_passphrase: String,

    // ── Wallet ────────────────────────────────────────────────────────
    pub private_key: String,

    // ── Infrastructure ────────────────────────────────────────────────
    pub questdb_url: String,

    // ── Binance SBE ────────────────────────────────────────────────────
    /// Binance SBE WebSocket endpoint (binary market data streams).
    pub binance_sbe_ws_url: String,
    /// Binance Ed25519 API key string for SBE stream authentication.
    pub binance_ed25519_api_key: String,
    /// Binance USDT-M Futures WebSocket endpoint (JSON streams).
    pub binance_futures_ws_url: String,

    // ── Telegram ──────────────────────────────────────────────────────
    pub telegram_bot_token: String,
    pub telegram_chat_id: String,
    /// If set, enables bidirectional Telegram command control.
    /// Only messages from this user ID are accepted.
    pub telegram_allowed_user_id: Option<i64>,

    // ── Tuning parameters (from config.toml) ──────────────────────────
    pub bot: BotConfig,

    // ── Derived Decimal values (computed from BotConfig at load time) ─
    pub max_alloc_per_trade: Decimal,
    pub phase1_breach_threshold: Decimal,
    pub stale_event_threshold_ms: u64,
    /// Repricing model: output ceiling / 100% allocation threshold.
    pub reprice_scale: Decimal,
    /// Repricing model: minimum output to enter a trade.
    pub min_reprice_pct: Decimal,
    /// Repricing model: floor allocation fraction.
    pub min_alloc_pct: Decimal,
    /// Repricing model: hard reject cap on YES mid price.
    pub hard_skew_cap: Decimal,
    /// Repricing model: exponent for time amplification (stays f64 for powf).
    pub time_exponent: f64,
    /// Repricing model: maximum time amplification factor.
    pub max_time_factor: f64,
    /// Phase 1 target dampening factor (only affects profit target, not entry gate or allocation).
    pub phase1_target_dampen: Decimal,
}

impl Config {
    /// Load config from `.env` (secrets/infrastructure) + `config.toml` (tuning).
    ///
    /// 1. Load `.env` via `dotenvy::dotenv()`
    /// 2. Read `CONFIG_FILE` env var (default `"config.toml"`)
    /// 3. Parse TOML file into `BotConfig` (or use defaults if file missing)
    /// 4. Load secrets/infra from env vars
    /// 5. Validate
    pub fn load() -> Result<Self> {
        // ── TOML config file ───────────────────────────────────────────
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
            info!(
                path = %config_path,
                "config file not found — using defaults"
            );
            BotConfig::default()
        };

        // ── Secrets & infrastructure from env ─────────────────────────
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

        // ── Derive Decimal values from BotConfig ─────────────────────
        let max_alloc_per_trade = Decimal::try_from(bot.capital.max_alloc_per_trade)
            .context("capital.max_alloc_per_trade: invalid decimal")?;
        let phase1_breach_threshold = Decimal::try_from(bot.risk.phase1_breach_threshold)
            .context("risk.phase1_breach_threshold: invalid decimal")?;
        let stale_event_threshold_ms = bot.entry_guards.binance_stale_event_ms;
        let reprice_scale = Decimal::try_from(bot.repricing.reprice_scale)
            .context("repricing.reprice_scale: invalid decimal")?;
        let min_reprice_pct = Decimal::try_from(bot.repricing.min_reprice_pct)
            .context("repricing.min_reprice_pct: invalid decimal")?;
        let min_alloc_pct = Decimal::try_from(bot.repricing.min_alloc_pct)
            .context("repricing.min_alloc_pct: invalid decimal")?;
        let hard_skew_cap = Decimal::try_from(bot.repricing.hard_skew_cap)
            .context("repricing.hard_skew_cap: invalid decimal")?;
        let time_exponent = bot.repricing.time_exponent;
        let max_time_factor = bot.repricing.max_time_factor;
        let phase1_target_dampen = Decimal::try_from(bot.repricing.phase1_target_dampen)
            .context("repricing.phase1_target_dampen: invalid decimal")?;
        let config = Self {
            polymarket_api_key,
            polymarket_secret,
            polymarket_passphrase,
            private_key,
            questdb_url: std::env::var("QUESTDB_URL").unwrap_or_else(|_| "127.0.0.1:9009".into()),
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
            max_alloc_per_trade,
            phase1_breach_threshold,
            stale_event_threshold_ms,
            reprice_scale,
            min_reprice_pct,
            min_alloc_pct,
            hard_skew_cap,
            time_exponent,
            max_time_factor,
            phase1_target_dampen,
        };

        // ── Validation ───────────────────────────────────────────────
        if config.max_alloc_per_trade <= Decimal::ZERO {
            anyhow::bail!("capital.max_alloc_per_trade must be positive");
        }
        Ok(config)
    }

    /// Test-only constructor with defaults. Secrets are empty strings.
    #[cfg(test)]
    pub fn test_defaults() -> Self {
        let bot = BotConfig::default();

        let max_alloc_per_trade = Decimal::try_from(bot.capital.max_alloc_per_trade).unwrap();
        let phase1_breach_threshold = Decimal::try_from(bot.risk.phase1_breach_threshold).unwrap();
        let stale_event_threshold_ms = bot.entry_guards.binance_stale_event_ms;
        let reprice_scale = Decimal::try_from(bot.repricing.reprice_scale).unwrap();
        let min_reprice_pct = Decimal::try_from(bot.repricing.min_reprice_pct).unwrap();
        let min_alloc_pct = Decimal::try_from(bot.repricing.min_alloc_pct).unwrap();
        let hard_skew_cap = Decimal::try_from(bot.repricing.hard_skew_cap).unwrap();
        let time_exponent = bot.repricing.time_exponent;
        let max_time_factor = bot.repricing.max_time_factor;
        let phase1_target_dampen = Decimal::try_from(bot.repricing.phase1_target_dampen).unwrap();
        Self {
            polymarket_api_key: String::new(),
            polymarket_secret: String::new(),
            polymarket_passphrase: String::new(),
            private_key: String::new(),
            questdb_url: "127.0.0.1:9009".into(),
            binance_sbe_ws_url: "wss://stream-sbe.binance.com:9443".into(),
            binance_ed25519_api_key: "test-key".into(),
            binance_futures_ws_url: "wss://fstream.binance.com".into(),
            telegram_bot_token: "test-token".into(),
            telegram_chat_id: "test-chat".into(),
            telegram_allowed_user_id: None,
            bot,
            max_alloc_per_trade,
            phase1_breach_threshold,
            stale_event_threshold_ms,
            reprice_scale,
            min_reprice_pct,
            min_alloc_pct,
            hard_skew_cap,
            time_exponent,
            max_time_factor,
            phase1_target_dampen,
        }
    }
}
