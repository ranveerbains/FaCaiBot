use std::sync::atomic::{AtomicBool, Ordering};

/// Atomic notification flags. Shared between CommandListener and TelegramReporter.
///
/// All loads/stores use `Relaxed` ordering — these are advisory toggles with
/// no happens-before requirements.
pub struct NotifyFlags {
    pub trades_enabled: AtomicBool,
    pub summary_enabled: AtomicBool,
    pub diagnostics_enabled: AtomicBool,
}

impl NotifyFlags {
    pub fn new() -> Self {
        Self {
            trades_enabled: AtomicBool::new(true),
            summary_enabled: AtomicBool::new(true),
            diagnostics_enabled: AtomicBool::new(false),
        }
    }

    pub fn trades_on(&self) -> bool {
        self.trades_enabled.load(Ordering::Relaxed)
    }

    pub fn summary_on(&self) -> bool {
        self.summary_enabled.load(Ordering::Relaxed)
    }

    pub fn diagnostics_on(&self) -> bool {
        self.diagnostics_enabled.load(Ordering::Relaxed)
    }
}

impl Default for NotifyFlags {
    fn default() -> Self {
        Self::new()
    }
}

/// Lightweight engine status snapshot for `/status` command.
#[derive(Debug, Clone)]
pub struct BotStatus {
    pub uptime_secs: u64,
    pub mode: String,
    pub current_market: Option<String>,
    pub phase: String,
    pub position_summary: String,
    pub pairing_summary: String,
    pub unpaired_yes: String,
    pub unpaired_no: String,
    pub markets_traded: u64,
    pub total_fills: u64,
    pub trades_enabled: bool,
    pub summary_enabled: bool,
    pub draining: bool,
    pub paused: bool,
    pub heartbeat_healthy: bool,
    pub heartbeat_failures: u32,
    pub heartbeat_latency_ms: u64,
}

impl Default for BotStatus {
    fn default() -> Self {
        Self {
            uptime_secs: 0,
            mode: "live".into(),
            current_market: None,
            phase: "Idle".into(),
            position_summary: "YES:0 NO:0".into(),
            pairing_summary: "paired:0 locked:$0.00".into(),
            unpaired_yes: "0".into(),
            unpaired_no: "0".into(),
            markets_traded: 0,
            total_fills: 0,
            trades_enabled: true,
            summary_enabled: true,
            draining: false,
            paused: false,
            heartbeat_healthy: true,
            heartbeat_failures: 0,
            heartbeat_latency_ms: 0,
        }
    }
}

/// Drain lifecycle status. Published by engine via watch channel,
/// consumed by command listener to send Telegram progress updates.
#[derive(Debug, Clone)]
pub enum DrainStatus {
    /// Normal operation.
    Idle,
    /// Drain activated — engine is waiting for open position to close.
    Draining {
        reason: String,
        position_info: String,
    },
    /// Drain complete (or no position was open). Ready to exit.
    Complete { exit_code: i32, summary: String },
}
