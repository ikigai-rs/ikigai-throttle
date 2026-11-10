//! Full jitter separates clients that would otherwise retry in lockstep.
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Select a backoff delay in the inclusive range `0..=upper`.
/// A host can inject a deterministic sampler for tests with [`crate::Retry::with_jitter`].
pub trait Jitter: Send + Sync {
    /// The overlay clamps the answer to `upper`, even for a custom sampler.
    fn sample(&self, upper: Duration) -> Duration;
}

/// Uniform full jitter at millisecond resolution: sample from zero through the
/// exponential cap, rather than adding a fixed delay that keeps callers synchronized.
/// This is a scheduling generator, not a source of security tokens.
pub struct FullJitter(AtomicU64);

impl Default for FullJitter {
    fn default() -> Self {
        // Use the standard library's per-instance random hash seed, without
        // reading platform time in an overlay. Hosts can supply their own sampler.
        Self::seeded(
            std::collections::hash_map::RandomState::new()
                .build_hasher()
                .finish(),
        )
    }
}

impl FullJitter {
    /// A repeatable sequence, useful for simulations and tests.
    pub fn seeded(seed: u64) -> Self {
        Self(AtomicU64::new(seed))
    }

    fn next(&self) -> u64 {
        // SplitMix64: each concurrent sample gets a distinct counter value.
        let mut value = self.0.fetch_add(0x9e3779b97f4a7c15, Ordering::Relaxed);
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }
}

impl Jitter for FullJitter {
    fn sample(&self, upper: Duration) -> Duration {
        let max = u64::try_from(upper.as_millis()).unwrap_or(u64::MAX);
        let Some(range) = max.checked_add(1) else {
            return Duration::from_millis(self.next());
        };
        // Rejection avoids modulo bias when the range does not divide 2^64.
        let threshold = range.wrapping_neg() % range;
        loop {
            let value = self.next();
            if value >= threshold {
                return Duration::from_millis(value % range);
            }
        }
    }
}
