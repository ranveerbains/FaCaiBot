use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crossbeam_channel::Sender;

use crate::control::config_editor;
use crate::control::types::{BotStatus, NotifyFlags};
use crate::types::market::IngestorEvent;

/// Handle `/trades on|off`.
pub fn handle_trades(args: &str, flags: &Arc<NotifyFlags>) -> String {
    match args.trim().to_lowercase().as_str() {
        "on" => {
            flags.trades_enabled.store(true, Ordering::Relaxed);
            "Trade notifications enabled.".into()
        }
        "off" => {
            flags.trades_enabled.store(false, Ordering::Relaxed);
            "Trade notifications disabled.".into()
        }
        _ => "Usage: /trades on|off".into(),
    }
}

/// Handle `/summary on|off`.
pub fn handle_summary(args: &str, flags: &Arc<NotifyFlags>) -> String {
    match args.trim().to_lowercase().as_str() {
        "on" => {
            flags.summary_enabled.store(true, Ordering::Relaxed);
            "Market summary notifications enabled.".into()
        }
        "off" => {
            flags.summary_enabled.store(false, Ordering::Relaxed);
            "Market summary notifications disabled.".into()
        }
        _ => "Usage: /summary on|off".into(),
    }
}

/// Handle `/diag on|off`.
pub fn handle_diag(args: &str, flags: &Arc<NotifyFlags>) -> String {
    match args.trim().to_lowercase().as_str() {
        "on" => {
            flags.diagnostics_enabled.store(true, Ordering::Relaxed);
            "Diagnostic forwarding enabled.".into()
        }
        "off" => {
            flags.diagnostics_enabled.store(false, Ordering::Relaxed);
            "Diagnostic forwarding disabled.".into()
        }
        _ => "Usage: /diag on|off".into(),
    }
}

/// Handle `/stop`. Pauses quoting. Closing phase still runs if active. Bot stays alive.
pub fn handle_stop(ingestor_tx: &Sender<IngestorEvent>) -> String {
    match ingestor_tx.try_send(IngestorEvent::PauseTrading) {
        Ok(()) => "Trading paused. Use /resume to restart.".into(),
        Err(e) => format!("Failed: {e}"),
    }
}

/// Handle `/resume`. Resumes trading after a /stop pause.
pub fn handle_resume(ingestor_tx: &Sender<IngestorEvent>) -> String {
    match ingestor_tx.try_send(IngestorEvent::ResumeTrading) {
        Ok(()) => "Trading resumed.".into(),
        Err(e) => format!("Failed: {e}"),
    }
}

/// Handle `/shutdown`. Full graceful shutdown (drain + exit).
pub fn handle_shutdown(ingestor_tx: &Sender<IngestorEvent>) -> String {
    match ingestor_tx.try_send(IngestorEvent::Shutdown) {
        Ok(()) => "Shutting down...".into(),
        Err(e) => format!("Failed: {e}"),
    }
}

/// Handle `/set <section.param> <value>`.
/// Validates, writes config, then sends DrainAndRestart to engine.
pub fn handle_set(args: &str, ingestor_tx: &Sender<IngestorEvent>) -> String {
    let parts: Vec<&str> = args.trim().splitn(2, ' ').collect();
    if parts.len() != 2 {
        return "Usage: /set <section.param> <value>".into();
    }

    let param_name = parts[0];
    let value_str = parts[1].trim();

    // Validate param name and value range.
    let value = match config_editor::validate_param(param_name, value_str) {
        Ok(v) => v,
        Err(e) => return format!("Validation error: {e}"),
    };

    // Determine config file path.
    let config_path = std::env::var("CONFIG_FILE").unwrap_or_else(|_| "config.toml".into());
    let path = Path::new(&config_path);

    // Write the new value.
    if let Err(e) = config_editor::update_config_file(path, param_name, value) {
        return format!("Config write error: {e}");
    }

    // Send drain-and-restart to engine.
    if let Err(e) = ingestor_tx.try_send(IngestorEvent::DrainAndRestart) {
        return format!("Config updated but restart failed: {e}");
    }

    format!("Config updated: {param_name} = {value_str}. Restarting...")
}

/// Handle `/config [section]`.
pub fn handle_config(args: &str) -> String {
    let config_path = std::env::var("CONFIG_FILE").unwrap_or_else(|_| "config.toml".into());
    let path = Path::new(&config_path);

    let section = args.trim();
    let result = if section.is_empty() {
        config_editor::read_config_all(path)
    } else {
        config_editor::read_config_section(path, section)
    };

    match result {
        Ok(text) => text,
        Err(e) => format!("Error reading config: {e}"),
    }
}

/// Handle `/status`. Reads latest BotStatus from watch channel.
pub fn handle_status(status: &BotStatus) -> String {
    let uptime_h = status.uptime_secs / 3600;
    let uptime_m = (status.uptime_secs % 3600) / 60;
    let uptime_s = status.uptime_secs % 60;

    let market = status
        .current_market
        .as_deref()
        .map(|m| {
            if m.len() > 8 {
                format!("...{}", &m[m.len() - 5..])
            } else {
                m.to_string()
            }
        })
        .unwrap_or_else(|| "none".into());

    let drain_status = if status.draining {
        " [DRAINING]"
    } else if status.paused {
        " [PAUSED]"
    } else {
        ""
    };

    let heartbeat_str = if status.heartbeat_healthy {
        format!("OK ({}ms)", status.heartbeat_latency_ms)
    } else {
        format!("DOWN ({} consecutive)", status.heartbeat_failures)
    };

    format!(
        "Uptime: {uptime_h}h {uptime_m:02}m {uptime_s:02}s{drain}\n\
         Mode: {mode} [{phase}]\n\
         Heartbeat: {heartbeat}\n\
         Market: {market}\n\
         Position: {position}\n\
         Paired: {pairing}\n\
         Unpaired: YES:{up_yes} | NO:{up_no}\n\
         Markets: {markets} | Fills: {fills}\n\
         Trades notify: {trades_on} | Summary notify: {summary_on}",
        drain = drain_status,
        mode = status.mode,
        phase = status.phase,
        heartbeat = heartbeat_str,
        market = market,
        position = status.position_summary,
        pairing = status.pairing_summary,
        up_yes = status.unpaired_yes,
        up_no = status.unpaired_no,
        markets = status.markets_traded,
        fills = status.total_fills,
        trades_on = if status.trades_enabled { "on" } else { "off" },
        summary_on = if status.summary_enabled { "on" } else { "off" },
    )
}

/// Handle `/help`.
pub fn handle_help() -> String {
    "/trades on|off — Toggle trade notifications\n\
     /summary on|off — Toggle market summary notifications\n\
     /diag on|off — Toggle 60s diagnostic forwarding\n\
     /stop — Pause trading (keeps connections alive)\n\
     /resume — Resume trading after /stop\n\
     /shutdown — Graceful shutdown (drains open position)\n\
     /set <param> <value> — Update config + restart\n\
     /config [section] — Show current config\n\
     /status — Bot status and counters\n\
     /balance — Wallet balance (USDC.e + POL)\n\
     /polybalance — Polymarket positions and value\n\
     /redeem — Redeem resolved positions to USDC.e\n\
     /redeem <condition_id> — Redeem a specific condition ID\n\
     /help — This message"
        .into()
}
