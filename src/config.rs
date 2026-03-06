use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::info;

/// Operating mode of the bot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Live trading — real orders submitted to Polymarket CLOB.
    Live,
    /// Simulation — same pipeline, but executor simulates fills + reports to Telegram.
    Simulation,
}

// ─── TOML Config Sub-structs ─────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SpikeDetectionConfig {
    /// `|delta| > multiplier × ATR` triggers spike candidate.
    pub multiplier: f64,
    /// EMA smoothing alpha for ATR. No spikes emitted for first MIN_ATR_SAMPLES ticks (warmup).
    pub atr_alpha: f64,
    /// Minimum sustain duration (ms) before spike confirmation.
    pub sustain_ms: u64,
    /// Minimum spike magnitude (%) to emit a signal. Below this → discard.
    pub min_magnitude_pct: f64,
    /// Minimum momentum ratio (displacement/peak) at sustain time. Below this → discard.
    pub momentum_ratio_min: f64,
}

impl Default for SpikeDetectionConfig {
    fn default() -> Self {
        Self {
            multiplier: 2.0,
            atr_alpha: 0.002,
            sustain_ms: 300,
            min_magnitude_pct: 0.01,
            momentum_ratio_min: 0.5,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct EntryGuardsConfig {
    /// Max bid-ask spread in absolute dollars (e.g. 0.025 = $0.025).
    pub max_spread: f64,
    /// Min book depth as fraction of required depth.
    pub depth_min_pct: f64,
    /// No entries within this many seconds of market expiry.
    pub entry_cutoff_secs: u64,
    /// Max age (ms) of a Binance SBE event before it is discarded.
    pub binance_stale_event_ms: u64,
    /// Max book age (ms) before blocking entry.
    pub stale_book_ms: u64,
    /// Block entry if YES mid price exceeds this (or falls below 1 - this).
    pub max_price_skew: f64,
    /// Maximum time (ms) a Leg 1 post-only order can rest unfilled before cancellation.
    pub leg1_timeout_ms: u64,
    /// Quiet period (ms) after market rotation — no new Leg 1 entries.
    pub rotation_quiet_ms: u64,
    /// Cooldown (ms) after trade completion before allowing new entries.
    pub trade_cooldown_ms: u64,
}

impl Default for EntryGuardsConfig {
    fn default() -> Self {
        Self {
            max_spread: 0.025,
            depth_min_pct: 0.15,
            entry_cutoff_secs: 180,
            binance_stale_event_ms: 50,
            stale_book_ms: 1000,
            max_price_skew: 0.80,
            leg1_timeout_ms: 5000,
            rotation_quiet_ms: 30000,
            trade_cooldown_ms: 5000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CapitalConfig {
    /// Maximum USDC to allocate per leg per trade (nearest dollar, $1 floor).
    pub max_alloc_per_trade: f64,
    /// Fraction of max_alloc_per_trade for HIGH confidence tier.
    pub high_alloc_pct: f64,
    /// Fraction of max_alloc_per_trade for MED confidence tier.
    pub med_alloc_pct: f64,
    /// Fraction of max_alloc_per_trade for LOW confidence tier.
    pub low_alloc_pct: f64,
}

impl Default for CapitalConfig {
    fn default() -> Self {
        Self {
            max_alloc_per_trade: 10.0,
            high_alloc_pct: 1.00,
            med_alloc_pct: 0.50,
            low_alloc_pct: 0.25,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ConfidenceConfig {
    /// Confidence >= this → HIGH tier.
    pub high_threshold: f64,
    /// Confidence >= this → MED tier.
    pub med_threshold: f64,
    /// Profit target % for HIGH tier (e.g. 0.025 = 2.5%).
    pub high_target_pct: f64,
    /// Profit target % for MED tier (e.g. 0.015 = 1.5%).
    pub med_target_pct: f64,
    /// Profit target % for LOW tier (e.g. 0.010 = 1.0%).
    pub low_target_pct: f64,
    /// Minimum spike ATR ratio to trade. Spikes below this → f1 = 0.
    pub min_spike_atr_ratio: f64,
    /// Spike ATR ratio at which spike quality factor = 1.0.
    pub strong_spike_atr_ratio: f64,
}

impl Default for ConfidenceConfig {
    fn default() -> Self {
        Self {
            high_threshold: 0.6,
            med_threshold: 0.3,
            high_target_pct: 0.025,
            med_target_pct: 0.015,
            low_target_pct: 0.010,
            min_spike_atr_ratio: 25.0,
            strong_spike_atr_ratio: 75.0,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RiskConfig {
    /// Phase 1 timeout (ms) — time at profit target before transitioning to Phase 2.
    pub phase1_timeout_ms: u64,
    /// Depth level > X × avg = competitor wall.
    pub depth_wall_multiplier: f64,
    /// Phase 1 breach threshold: pair cost > this triggers transition to Phase 2.
    /// Default 1.05 ($1.05). Must be > 1.00.
    pub phase1_breach_threshold: f64,
    /// Phase 2 timeout (ms) — time at break-even pursuit before FOK taker exit.
    pub phase2_timeout_ms: u64,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            phase1_timeout_ms: 2000,
            depth_wall_multiplier: 4.0,
            phase1_breach_threshold: 1.05,
            phase2_timeout_ms: 2000,
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

// ─── Top-level TOML config ──────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BotConfig {
    pub rotation: RotationConfig,
    pub spike_detection: SpikeDetectionConfig,
    pub entry_guards: EntryGuardsConfig,
    pub capital: CapitalConfig,
    pub confidence: ConfidenceConfig,
    pub risk: RiskConfig,
}

impl Default for BotConfig {
    fn default() -> Self {
        Self {
            rotation: RotationConfig::default(),
            spike_detection: SpikeDetectionConfig::default(),
            entry_guards: EntryGuardsConfig::default(),
            capital: CapitalConfig::default(),
            confidence: ConfidenceConfig::default(),
            risk: RiskConfig::default(),
        }
    }
}

// ─── Main Config struct ─────────────────────────────────────────────────────

/// Bot-wide configuration loaded from `.env` (secrets/infra) + `config.toml` (tuning).
#[derive(Debug, Clone)]
pub struct Config {
    // ── Operating mode ────────────────────────────────────────────────
    pub mode: Mode,

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
    pub high_alloc_pct: Decimal,
    pub med_alloc_pct: Decimal,
    pub low_alloc_pct: Decimal,
    pub phase1_breach_threshold: Decimal,
    pub stale_event_threshold_ms: u64,
    /// Profit target for HIGH tier (from config).
    pub high_target_pct: Decimal,
    /// Profit target for MED tier (from config).
    pub med_target_pct: Decimal,
    /// Profit target for LOW tier (from config).
    pub low_target_pct: Decimal,
    /// Minimum spike magnitude (from spike_detection config), as Decimal.
    pub min_magnitude_pct: Decimal,
    /// Minimum spike ATR ratio to trade, as Decimal.
    pub min_spike_atr_ratio: Decimal,
    /// Spike ATR ratio at which spike quality factor = 1.0, as Decimal.
    pub strong_spike_atr_ratio: Decimal,
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

        // ── Mode ──────────────────────────────────────────────────────
        let mode_str = std::env::var("MODE").unwrap_or_else(|_| "simulation".into());
        let mode = match mode_str.to_lowercase().as_str() {
            "live" | "production" => Mode::Live,
            _ => Mode::Simulation,
        };

        // ── Secrets & infrastructure from env ─────────────────────────
        let polymarket_api_key = if mode == Mode::Live {
            std::env::var("POLYMARKET_API_KEY")
                .context("POLYMARKET_API_KEY not set (required for live mode)")?
        } else {
            std::env::var("POLYMARKET_API_KEY").unwrap_or_default()
        };

        let polymarket_secret = if mode == Mode::Live {
            std::env::var("POLYMARKET_SECRET")
                .context("POLYMARKET_SECRET not set (required for live mode)")?
        } else {
            std::env::var("POLYMARKET_SECRET").unwrap_or_default()
        };

        let polymarket_passphrase = if mode == Mode::Live {
            std::env::var("POLYMARKET_PASSPHRASE")
                .context("POLYMARKET_PASSPHRASE not set (required for live mode)")?
        } else {
            std::env::var("POLYMARKET_PASSPHRASE").unwrap_or_default()
        };

        let private_key = if mode == Mode::Live {
            std::env::var("PRIVATE_KEY").context("PRIVATE_KEY not set (required for live mode)")?
        } else {
            std::env::var("PRIVATE_KEY").unwrap_or_default()
        };

        let telegram_bot_token = if mode == Mode::Simulation {
            std::env::var("TELEGRAM_BOT_TOKEN")
                .context("TELEGRAM_BOT_TOKEN not set (required for simulation mode)")?
        } else {
            std::env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default()
        };

        let telegram_chat_id = if mode == Mode::Simulation {
            std::env::var("TELEGRAM_CHAT_ID")
                .context("TELEGRAM_CHAT_ID not set (required for simulation mode)")?
        } else {
            std::env::var("TELEGRAM_CHAT_ID").unwrap_or_default()
        };

        let telegram_allowed_user_id = std::env::var("TELEGRAM_ALLOWED_USER_ID")
            .ok()
            .and_then(|s| s.parse::<i64>().ok());

        // ── Derive Decimal values from BotConfig ─────────────────────
        let max_alloc_per_trade = Decimal::try_from(bot.capital.max_alloc_per_trade)
            .context("capital.max_alloc_per_trade: invalid decimal")?;
        let high_alloc_pct = Decimal::try_from(bot.capital.high_alloc_pct)
            .context("capital.high_alloc_pct: invalid decimal")?;
        let med_alloc_pct = Decimal::try_from(bot.capital.med_alloc_pct)
            .context("capital.med_alloc_pct: invalid decimal")?;
        let low_alloc_pct = Decimal::try_from(bot.capital.low_alloc_pct)
            .context("capital.low_alloc_pct: invalid decimal")?;
        let phase1_breach_threshold = Decimal::try_from(bot.risk.phase1_breach_threshold)
            .context("risk.phase1_breach_threshold: invalid decimal")?;
        let stale_event_threshold_ms = bot.entry_guards.binance_stale_event_ms;
        let high_target_pct = Decimal::try_from(bot.confidence.high_target_pct)
            .context("confidence.high_target_pct: invalid decimal")?;
        let med_target_pct = Decimal::try_from(bot.confidence.med_target_pct)
            .context("confidence.med_target_pct: invalid decimal")?;
        let low_target_pct = Decimal::try_from(bot.confidence.low_target_pct)
            .context("confidence.low_target_pct: invalid decimal")?;
        let min_magnitude_pct = Decimal::try_from(bot.spike_detection.min_magnitude_pct)
            .context("spike_detection.min_magnitude_pct: invalid decimal")?;
        let min_spike_atr_ratio = Decimal::try_from(bot.confidence.min_spike_atr_ratio)
            .context("confidence.min_spike_atr_ratio: invalid decimal")?;
        let strong_spike_atr_ratio = Decimal::try_from(bot.confidence.strong_spike_atr_ratio)
            .context("confidence.strong_spike_atr_ratio: invalid decimal")?;

        let config = Self {
            mode,
            polymarket_api_key,
            polymarket_secret,
            polymarket_passphrase,
            private_key,
            questdb_url: std::env::var("QUESTDB_URL").unwrap_or_else(|_| "127.0.0.1:9009".into()),
            binance_sbe_ws_url: std::env::var("BINANCE_SBE_WS_URL")
                .unwrap_or_else(|_| "wss://stream-sbe.binance.com:9443".into()),
            binance_ed25519_api_key: std::env::var("BINANCE_ED25519_API_KEY")
                .context("BINANCE_ED25519_API_KEY not set (required for SBE market data)")?,
            telegram_bot_token,
            telegram_chat_id,
            telegram_allowed_user_id,
            bot,
            max_alloc_per_trade,
            high_alloc_pct,
            med_alloc_pct,
            low_alloc_pct,
            phase1_breach_threshold,
            stale_event_threshold_ms,
            high_target_pct,
            med_target_pct,
            low_target_pct,
            min_magnitude_pct,
            min_spike_atr_ratio,
            strong_spike_atr_ratio,
        };

        // ── Validation ───────────────────────────────────────────────
        if config.max_alloc_per_trade <= Decimal::ZERO {
            anyhow::bail!("capital.max_alloc_per_trade must be positive");
        }
        if config.bot.spike_detection.multiplier <= 0.0 {
            anyhow::bail!("spike_detection.multiplier must be positive");
        }
        if config.bot.confidence.high_threshold <= config.bot.confidence.med_threshold {
            anyhow::bail!("confidence.high_threshold must be > med_threshold");
        }

        Ok(config)
    }

    /// Test-only constructor using the same defaults as config.toml.
    /// Secrets are empty strings; mode is Simulation.
    #[cfg(test)]
    pub fn test_defaults() -> Self {
        let bot = BotConfig::default();

        let max_alloc_per_trade = Decimal::try_from(bot.capital.max_alloc_per_trade).unwrap();
        let high_alloc_pct = Decimal::try_from(bot.capital.high_alloc_pct).unwrap();
        let med_alloc_pct = Decimal::try_from(bot.capital.med_alloc_pct).unwrap();
        let low_alloc_pct = Decimal::try_from(bot.capital.low_alloc_pct).unwrap();
        let phase1_breach_threshold = Decimal::try_from(bot.risk.phase1_breach_threshold).unwrap();
        let stale_event_threshold_ms = bot.entry_guards.binance_stale_event_ms;
        let high_target_pct = Decimal::try_from(bot.confidence.high_target_pct).unwrap();
        let med_target_pct = Decimal::try_from(bot.confidence.med_target_pct).unwrap();
        let low_target_pct = Decimal::try_from(bot.confidence.low_target_pct).unwrap();
        let min_magnitude_pct = Decimal::try_from(bot.spike_detection.min_magnitude_pct).unwrap();
        let min_spike_atr_ratio = Decimal::try_from(bot.confidence.min_spike_atr_ratio).unwrap();
        let strong_spike_atr_ratio = Decimal::try_from(bot.confidence.strong_spike_atr_ratio).unwrap();

        Self {
            mode: Mode::Simulation,
            polymarket_api_key: String::new(),
            polymarket_secret: String::new(),
            polymarket_passphrase: String::new(),
            private_key: String::new(),
            questdb_url: "127.0.0.1:9009".into(),
            binance_sbe_ws_url: "wss://stream-sbe.binance.com:9443".into(),
            binance_ed25519_api_key: "test-key".into(),
            telegram_bot_token: "test-token".into(),
            telegram_chat_id: "test-chat".into(),
            telegram_allowed_user_id: None,
            bot,
            max_alloc_per_trade,
            high_alloc_pct,
            med_alloc_pct,
            low_alloc_pct,
            phase1_breach_threshold,
            stale_event_threshold_ms,
            high_target_pct,
            med_target_pct,
            low_target_pct,
            min_magnitude_pct,
            min_spike_atr_ratio,
            strong_spike_atr_ratio,
        }
    }
}
