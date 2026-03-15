use std::path::Path;

use anyhow::{Context, Result, bail};

/// Specification for a single tunable parameter.
struct ParamSpec {
    name: &'static str,
    min: f64,
    max: f64,
}

/// Allowlist of all tunable parameters with valid ranges.
const ALLOWED_PARAMS: &[ParamSpec] = &[
    // entry_guards
    ParamSpec { name: "entry_guards.entry_cutoff_secs", min: 30.0, max: 600.0 },
    ParamSpec { name: "entry_guards.binance_stale_event_ms", min: 10.0, max: 1000.0 },
    ParamSpec { name: "entry_guards.stale_book_ms", min: 50.0, max: 5000.0 },
    ParamSpec { name: "entry_guards.rotation_quiet_ms", min: 0.0, max: 120000.0 },
    ParamSpec { name: "entry_guards.trade_cooldown_ms", min: 0.0, max: 60000.0 },
    ParamSpec { name: "entry_guards.heartbeat_dead_threshold", min: 2.0, max: 20.0 },
    ParamSpec { name: "entry_guards.max_entry_spread", min: 0.005, max: 0.20 },
    // capital
    ParamSpec { name: "capital.max_alloc_per_trade", min: 0.01, max: 10000.0 },
    // repricing
    ParamSpec { name: "repricing.reprice_scale", min: 0.001, max: 0.10 },
    ParamSpec { name: "repricing.min_reprice_pct", min: 0.001, max: 0.10 },
    ParamSpec { name: "repricing.min_alloc_pct", min: 0.01, max: 1.0 },
    ParamSpec { name: "repricing.hard_skew_cap", min: 0.5, max: 0.99 },
    ParamSpec { name: "repricing.time_exponent", min: 0.0, max: 2.0 },
    ParamSpec { name: "repricing.max_time_factor", min: 1.0, max: 10.0 },
    ParamSpec { name: "repricing.phase1_target_dampen", min: 0.1, max: 1.0 },
    // risk
    ParamSpec { name: "risk.phase1_timeout_ms", min: 500.0, max: 10000.0 },
ParamSpec { name: "risk.phase1_breach_threshold", min: 1.001, max: 1.10 },
    ParamSpec { name: "risk.phase2_timeout_ms", min: 500.0, max: 10000.0 },
    ParamSpec { name: "risk.favorable_maker_timeout_ms", min: 200.0, max: 5000.0 },
    // rotation
    ParamSpec { name: "rotation.prewarm_lead_secs", min: 5.0, max: 300.0 },
    // buildup
    ParamSpec { name: "buildup.entry_threshold", min: 0.0, max: 1.0 },
    ParamSpec { name: "buildup.cancel_threshold", min: 0.0, max: 1.0 },
    ParamSpec { name: "buildup.ask_drift_cancel_cents", min: 0.005, max: 0.10 },
    ParamSpec { name: "buildup.max_dissenters", min: 0.0, max: 4.0 },
    ParamSpec { name: "buildup.w_cvd", min: 0.0, max: 1.0 },
    ParamSpec { name: "buildup.w_spot_flow", min: 0.0, max: 1.0 },
    ParamSpec { name: "buildup.w_obi", min: 0.0, max: 1.0 },
    ParamSpec { name: "buildup.w_basis", min: 0.0, max: 1.0 },
    ParamSpec { name: "buildup.w_liq", min: 0.0, max: 1.0 },
    ParamSpec { name: "buildup.w_atr", min: 0.0, max: 1.0 },
    ParamSpec { name: "buildup.freshness_cvd_ms", min: 50.0, max: 5000.0 },
    ParamSpec { name: "buildup.freshness_spot_flow_ms", min: 50.0, max: 5000.0 },
    ParamSpec { name: "buildup.freshness_obi_ms", min: 50.0, max: 5000.0 },
    ParamSpec { name: "buildup.freshness_basis_ms", min: 50.0, max: 5000.0 },
    ParamSpec { name: "buildup.freshness_liq_ms", min: 100.0, max: 30000.0 },
    ParamSpec { name: "buildup.freshness_atr_ms", min: 50.0, max: 5000.0 },
    ParamSpec { name: "buildup.cvd_min", min: 0.0, max: 10.0 },
    ParamSpec { name: "buildup.cvd_saturation", min: 0.01, max: 100.0 },
    ParamSpec { name: "buildup.spot_flow_min", min: 0.0, max: 10.0 },
    ParamSpec { name: "buildup.spot_flow_saturation", min: 0.01, max: 100.0 },
    ParamSpec { name: "buildup.obi_min", min: 0.0, max: 1.0 },
    ParamSpec { name: "buildup.obi_saturation", min: 0.01, max: 1.0 },
    ParamSpec { name: "buildup.basis_min", min: 0.0, max: 10.0 },
    ParamSpec { name: "buildup.basis_saturation", min: 0.01, max: 100.0 },
    ParamSpec { name: "buildup.liq_min", min: 0.0, max: 100.0 },
    ParamSpec { name: "buildup.liq_saturation", min: 0.01, max: 1000.0 },
    ParamSpec { name: "buildup.atr_min", min: 0.0, max: 100.0 },
    ParamSpec { name: "buildup.atr_saturation", min: 0.01, max: 1000.0 },
    ParamSpec { name: "buildup.cvd_fast_halflife_ms", min: 10.0, max: 5000.0 },
    ParamSpec { name: "buildup.cvd_slow_halflife_ms", min: 50.0, max: 10000.0 },
    ParamSpec { name: "buildup.spot_flow_halflife_ms", min: 10.0, max: 5000.0 },
    ParamSpec { name: "buildup.obi_velocity_halflife_ms", min: 10.0, max: 5000.0 },
    ParamSpec { name: "buildup.basis_halflife_ms", min: 10.0, max: 5000.0 },
];

/// Validate a parameter name against the allowlist and check value range.
/// Returns the parsed f64 value on success.
pub fn validate_param(name: &str, value: &str) -> Result<f64> {
    let spec = ALLOWED_PARAMS
        .iter()
        .find(|s| s.name == name)
        .with_context(|| format!("unknown parameter: {name}"))?;

    let v: f64 = value
        .parse()
        .with_context(|| format!("invalid number: {value}"))?;

    if v < spec.min || v > spec.max {
        bail!(
            "{name} must be between {} and {} (got {v})",
            spec.min,
            spec.max
        );
    }

    Ok(v)
}

/// Update a single parameter in the config TOML file.
///
/// Uses line-based replacement to preserve comments and formatting.
/// Atomic write (temp file + rename) prevents partial writes.
pub fn update_config_file(path: &Path, param_name: &str, value: f64) -> Result<()> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config file: {}", path.display()))?;

    // param_name is "section.key" e.g. "repricing.reprice_scale"
    let (_section, key) = param_name
        .split_once('.')
        .with_context(|| format!("invalid param format: {param_name}"))?;

    // Format the new value: integer-like values get no decimal point.
    let value_str = if value.fract() == 0.0 && value.abs() < i64::MAX as f64 {
        format!("{}", value as i64)
    } else {
        format!("{value}")
    };

    // Find and replace the line matching `key = ...` (preserving inline comments).
    let mut found = false;
    let mut output = String::with_capacity(contents.len());
    for line in contents.lines() {
        let trimmed = line.trim();
        // Match lines like "key = value" or "key = value  # comment"
        if let Some(after_key) = trimmed.strip_prefix(key) {
            let after_key = after_key.trim_start();
            if let Some(after_eq) = after_key.strip_prefix('=') {
                // Preserve leading whitespace from the original line.
                let leading_ws: &str =
                    &line[..line.len() - line.trim_start().len()];
                // Preserve inline comment if any.
                let rest_after_eq = after_eq.trim_start();
                let inline_comment = if let Some(hash_pos) = find_inline_comment(rest_after_eq) {
                    let comment = rest_after_eq[hash_pos..].trim_start();
                    format!("  {comment}")
                } else {
                    String::new()
                };
                output.push_str(&format!(
                    "{leading_ws}{key} = {value_str}{inline_comment}\n"
                ));
                found = true;
                continue;
            }
        }
        output.push_str(line);
        output.push('\n');
    }

    if !found {
        bail!("parameter '{key}' not found in config file");
    }

    // Atomic write: temp file + rename.
    let tmp_path = path.with_extension("toml.tmp");
    std::fs::write(&tmp_path, &output)
        .with_context(|| format!("failed to write temp config: {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("failed to rename temp config to {}", path.display()))?;

    Ok(())
}

/// Find the position of an inline comment (`#`) in a TOML value string.
/// Returns None if there's no inline comment.
fn find_inline_comment(s: &str) -> Option<usize> {
    // Simple heuristic: find `#` that's preceded by whitespace.
    // TOML values in our config are always simple numbers, no strings with `#`.
    for (i, c) in s.char_indices() {
        if c == '#' {
            return Some(i);
        }
    }
    None
}

/// Read and format a specific section of the config for display.
pub fn read_config_section(path: &Path, section: &str) -> Result<String> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config file: {}", path.display()))?;

    let doc: toml::Value = contents
        .parse()
        .with_context(|| format!("failed to parse TOML: {}", path.display()))?;

    let table = doc
        .get(section)
        .and_then(|v| v.as_table())
        .with_context(|| format!("section [{section}] not found"))?;

    let mut out = format!("[{section}]\n");
    for (key, value) in table {
        out.push_str(&format!("  {section}.{key} = {}\n", format_toml_value(value)));
    }
    Ok(out)
}

/// Read and format all config sections for display.
pub fn read_config_all(path: &Path) -> Result<String> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config file: {}", path.display()))?;

    let doc: toml::Value = contents
        .parse()
        .with_context(|| format!("failed to parse TOML: {}", path.display()))?;

    let mut out = String::new();
    if let Some(table) = doc.as_table() {
        for (section_name, item) in table {
            if let Some(inner) = item.as_table() {
                out.push_str(&format!("[{section_name}]\n"));
                for (key, value) in inner {
                    out.push_str(&format!("  {section_name}.{key} = {}\n", format_toml_value(value)));
                }
                out.push('\n');
            }
        }
    }
    Ok(out)
}

/// Format a TOML value for display (concise, no quotes on numbers).
fn format_toml_value(v: &toml::Value) -> String {
    match v {
        toml::Value::Integer(i) => i.to_string(),
        toml::Value::Float(f) => format!("{f}"),
        toml::Value::String(s) => format!("\"{s}\""),
        toml::Value::Boolean(b) => b.to_string(),
        other => format!("{other}"),
    }
}

#[cfg(test)]
#[path = "tests/config_editor_tests.rs"]
mod tests;
