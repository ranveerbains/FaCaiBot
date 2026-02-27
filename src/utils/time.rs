//! Shared time utility.

use std::time::{SystemTime, UNIX_EPOCH};

/// Current wall-clock time as epoch milliseconds.
#[inline]
pub fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
