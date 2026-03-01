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
    #[allow(dead_code)] // reserved for future /status display
    pub market_end_ms: u64,
    pub leg1_state: String,
    pub leg2_state: String,
    pub spikes_received: u64,
    pub signals_emitted: u64,
    pub trades_completed: u64,
    pub trades_enabled: bool,
    pub summary_enabled: bool,
    pub draining: bool,
}

impl Default for BotStatus {
    fn default() -> Self {
        Self {
            uptime_secs: 0,
            mode: "simulation".into(),
            current_market: None,
            market_end_ms: 0,
            leg1_state: "None".into(),
            leg2_state: "None".into(),
            spikes_received: 0,
            signals_emitted: 0,
            trades_completed: 0,
            trades_enabled: true,
            summary_enabled: true,
            draining: false,
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
