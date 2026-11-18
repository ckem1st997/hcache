//! Wall-clock time in milliseconds, with an offset that tests move forward
//! instead of sleeping.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch, by the system clock.
pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

#[derive(Default)]
pub struct Clock {
    offset: AtomicU64,
}

impl Clock {
    pub fn now_ms(&self) -> u64 {
        unix_ms().saturating_add(self.offset.load(Ordering::Relaxed))
    }

    /// Move this clock forward. Time never goes back.
    pub fn advance(&self, ms: u64) {
        self.offset.fetch_add(ms, Ordering::Relaxed);
    }
}
