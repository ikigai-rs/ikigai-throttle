//! The default clock on a native target: monotonic, and the only place this crate
//! reads time from the platform.
//!
//! **Native only**, like [`Timeout`](crate::Timeout): `Instant::now()` and
//! `SystemTime::now()` compile on wasm32-unknown-unknown and panic when called. On
//! wasm32 this module is not compiled, so a governor there has no default clock and
//! the host installs one (`with_clock`), as it does for its kernel.
//!
//! Monotonic on purpose. Core's [`SystemClock`](ikigai_core::SystemClock) reads the
//! wall clock, which can step backward or forward (NTP, a laptop waking), and a rate
//! window or a breaker cooldown measured on it would stretch or vanish when it does.
//! This clock is anchored once at the wall clock, so its readings are still
//! milliseconds since the Unix epoch, and then advances only by `Instant`'s elapsed
//! time. That is the behavior every governor here had before it read a clock, kept.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ikigai_core::{Clock, Time};

/// A [`Clock`] that never runs backward: the wall clock's reading when it was made,
/// advanced by the monotonic time elapsed since.
pub(crate) struct Monotonic {
    origin: Instant,
    epoch: u64,
}

impl Monotonic {
    pub(crate) fn new() -> Self {
        Monotonic {
            origin: Instant::now(),
            epoch: millis(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default(),
            ),
        }
    }
}

impl Clock for Monotonic {
    fn now(&self) -> Time {
        Time::from_millis(self.epoch.saturating_add(millis(self.origin.elapsed())))
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
