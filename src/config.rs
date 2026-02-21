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
    /// EMA smoothing alpha for 1-minute ATR (fast window).
    pub atr_alpha: f64,
    /// Time window (ms) within which price delta must exceed threshold.
    pub window_ms: u64,
    /// Minimum sustain duration (ms) before spike confirmation.
    pub sustain_ms: u64,
    /// Extra sustain time (ms) in low-volatility conditions.
    pub sustain_low_vol_ext_ms: u64,
    /// Phantom filter: reject if price reverts > this fraction of spike delta.
    pub phantom_revert_fraction: f64,
    /// Time window (ms) after sustain confirmation for phantom check.
    pub phantom_check_ms: u64,
}

impl Default for SpikeDetectionConfig {
    fn default() -> Self {
        Self {
            multiplier: 2.0,
            atr_alpha: 0.1,
            window_ms: 400,
            sustain_ms: 300,
            sustain_low_vol_ext_ms: 300,
            phantom_revert_fraction: 0.5,
            phantom_check_ms: 100,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct EntryGuardsConfig {
    /// Max bid-ask spread as fraction of mid price.
    pub max_spread_pct: f64,
    /// Min book depth as fraction of required depth.
    pub depth_min_pct: f64,
    /// No entries within this many seconds of market expiry.
    pub entry_cutoff_secs: u64,
    /// Max book age (ms) before blocking entry.
    pub stale_book_ms: u64,
}

impl Default for EntryGuardsConfig {
    fn default() -> Self {
        Self {
            max_spread_pct: 0.10,
            depth_min_pct: 0.15,
            entry_cutoff_secs: 180,
            stale_book_ms: 1000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CapitalConfig {
    /// Total USDC capital available per session.
    pub fixed_alloc: f64,
    /// Maximum allocation per trade as fraction.
    pub max_alloc_pct: f64,
}

impl Default for CapitalConfig {
    fn default() -> Self {
        Self {
            fixed_alloc: 100.0,
            max_alloc_pct: 0.30,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ConfidenceConfig {
    /// Confidence >= this → HIGH tier (2.5% target, 30% alloc).
    pub high_threshold: f64,
    /// Confidence >= this → MED tier (1.5% target, 20% alloc).
    pub med_threshold: f64,
    /// Sustain window (ms) for confidence scoring normalization.
    pub sustain_window_ms: u64,
}

impl Default for ConfidenceConfig {
    fn default() -> Self {
        Self {
            high_threshold: 0.8,
            med_threshold: 0.5,
            sustain_window_ms: 200,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RiskConfig {
    /// Price reversal threshold triggering emergency FOK.
    pub adverse_threshold: f64,
    /// Grace period (ms) after fill before adverse monitoring.
    pub adverse_grace_period_ms: u64,
    /// Seconds before expiry to force FOK hedge.
    pub emergency_deadline_secs: u64,
    /// Leg 2 erosion step interval (ms).
    pub erosion_interval_ms: u64,
    /// Depth level > X × avg = competitor wall.
    pub depth_wall_multiplier: f64,
    /// Quick cancel threshold within 100ms of fill.
    pub quick_reversal_threshold: f64,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            adverse_threshold: 0.003,
            adverse_grace_period_ms: 3000,
            emergency_deadline_secs: 90,
            erosion_interval_ms: 2000,
            depth_wall_multiplier: 4.0,
            quick_reversal_threshold: 0.0005,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SimulationConfig {
    /// Simulated fill latency (ms).
    pub fill_delay_ms: u64,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self { fill_delay_ms: 500 }
    }
}

// ─── Top-level TOML config ──────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BotConfig {
    pub spike_detection: SpikeDetectionConfig,
    pub entry_guards: EntryGuardsConfig,
    pub capital: CapitalConfig,
    pub confidence: ConfidenceConfig,
    pub risk: RiskConfig,
    pub simulation: SimulationConfig,
}

impl Default for BotConfig {
    fn default() -> Self {
        Self {
            spike_detection: SpikeDetectionConfig::default(),
            entry_guards: EntryGuardsConfig::default(),
            capital: CapitalConfig::default(),
            confidence: ConfidenceConfig::default(),
            risk: RiskConfig::default(),
            simulation: SimulationConfig::default(),
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
    pub redis_url: String,
    pub questdb_url: String,

    // ── Binance ───────────────────────────────────────────────────────
    pub binance_ws_url: String,

    // ── Telegram ──────────────────────────────────────────────────────
    pub telegram_bot_token: String,
    pub telegram_chat_id: String,

    // ── Tuning parameters (from config.toml) ──────────────────────────
    pub bot: BotConfig,

    // ── Derived Decimal values (computed from BotConfig at load time) ─
    pub fixed_alloc: Decimal,
    pub max_alloc_pct: Decimal,
    pub adverse_threshold: Decimal,
    pub stale_event_threshold_ms: u64,
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

        // ── Derive Decimal values from BotConfig ─────────────────────
        let fixed_alloc = Decimal::try_from(bot.capital.fixed_alloc)
            .context("capital.fixed_alloc: invalid decimal")?;
        let max_alloc_pct = Decimal::try_from(bot.capital.max_alloc_pct)
            .context("capital.max_alloc_pct: invalid decimal")?;
        let adverse_threshold = Decimal::try_from(bot.risk.adverse_threshold)
            .context("risk.adverse_threshold: invalid decimal")?;
        let stale_event_threshold_ms = bot.entry_guards.stale_book_ms;

        let config = Self {
            mode,
            polymarket_api_key,
            polymarket_secret,
            polymarket_passphrase,
            private_key,
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1:6379".into()),
            questdb_url: std::env::var("QUESTDB_URL").unwrap_or_else(|_| "127.0.0.1:9009".into()),
            binance_ws_url: std::env::var("BINANCE_WS_URL")
                .unwrap_or_else(|_| "wss://stream.binance.com:9443".into()),
            telegram_bot_token,
            telegram_chat_id,
            bot,
            fixed_alloc,
            max_alloc_pct,
            adverse_threshold,
            stale_event_threshold_ms,
        };

        // ── Validation ───────────────────────────────────────────────
        if config.fixed_alloc <= Decimal::ZERO {
            anyhow::bail!("capital.fixed_alloc must be positive");
        }
        if config.max_alloc_pct <= Decimal::ZERO || config.max_alloc_pct > Decimal::ONE {
            anyhow::bail!("capital.max_alloc_pct must be in (0, 1]");
        }
        if config.bot.spike_detection.multiplier <= 0.0 {
            anyhow::bail!("spike_detection.multiplier must be positive");
        }
        if config.bot.confidence.high_threshold <= config.bot.confidence.med_threshold {
            anyhow::bail!("confidence.high_threshold must be > med_threshold");
        }

        Ok(config)
    }

    /// Whether we are in simulation mode.
    pub fn is_simulation(&self) -> bool {
        self.mode == Mode::Simulation
    }

    /// Test-only constructor with legacy defaults (pre-TOML constant values).
    /// Ensures backward-compatible tests without requiring a config file.
    #[cfg(test)]
    pub fn test_defaults() -> Self {
        let bot = BotConfig {
            spike_detection: SpikeDetectionConfig {
                multiplier: 1.5,
                atr_alpha: 0.1,
                window_ms: 400,
                sustain_ms: 200,
                sustain_low_vol_ext_ms: 300,
                phantom_revert_fraction: 0.5,
                phantom_check_ms: 100,
            },
            entry_guards: EntryGuardsConfig {
                max_spread_pct: 0.03,
                depth_min_pct: 0.15,
                entry_cutoff_secs: 180,
                stale_book_ms: 500,
            },
            capital: CapitalConfig {
                fixed_alloc: 100.0,
                max_alloc_pct: 0.30,
            },
            confidence: ConfidenceConfig {
                high_threshold: 0.8,
                med_threshold: 0.5,
                sustain_window_ms: 200,
            },
            risk: RiskConfig {
                adverse_threshold: 0.003,
                adverse_grace_period_ms: 3000,
                emergency_deadline_secs: 90,
                erosion_interval_ms: 2000,
                depth_wall_multiplier: 4.0,
                quick_reversal_threshold: 0.0005,
            },
            simulation: SimulationConfig { fill_delay_ms: 500 },
        };

        let fixed_alloc = Decimal::try_from(bot.capital.fixed_alloc).unwrap();
        let max_alloc_pct = Decimal::try_from(bot.capital.max_alloc_pct).unwrap();
        let adverse_threshold = Decimal::try_from(bot.risk.adverse_threshold).unwrap();
        let stale_event_threshold_ms = bot.entry_guards.stale_book_ms;

        Self {
            mode: Mode::Simulation,
            polymarket_api_key: String::new(),
            polymarket_secret: String::new(),
            polymarket_passphrase: String::new(),
            private_key: String::new(),
            redis_url: "redis://127.0.0.1:6379".into(),
            questdb_url: "127.0.0.1:9009".into(),
            binance_ws_url: "wss://stream.binance.com:9443".into(),
            telegram_bot_token: "test-token".into(),
            telegram_chat_id: "test-chat".into(),
            bot,
            fixed_alloc,
            max_alloc_pct,
            adverse_threshold,
            stale_event_threshold_ms,
        }
    }
}
