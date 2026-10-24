//! [`Timer`]: what a [`Retry`](crate::Retry) waits a [`RetryAfter`](ikigai_core::Error::RetryAfter)
//! delay out on, and what a [`Timeout`](crate::Timeout) races its budget against.
//!
//! A wait is not a [`Clock`](ikigai_core::Clock): a clock answers "what time is it" and
//! never "wake me when", so an overlay that waits needs something that can suspend a
//! future. Core has no such seam, so the overlays take one, injected like a clock: a
//! native target defaults to futures-timer, whose backend runs a helper thread;
//! wasm32-unknown-unknown has no default. A browser has timers, but only behind
//! JavaScript imports, so a browser host supplies one (ledger #2189). A `Retry` without a
//! timer returns every `RetryAfter` unchanged, so the caller owns the wait; a `Timeout`
//! without one refuses what it governs.
//!
//! A browser timer's future is not `Send` and this trait's is, so a browser host resolves
//! a `setTimeout` promise in a `spawn_local` task and awaits a `oneshot` channel that the
//! task completes, which is `Send`: the same bridge a wasm host already uses for `fetch`.
//!
//! A test injects one that moves its own clock instead of sleeping, so a delay is
//! "waited" in no real time and the boundary arithmetic stays exact.

use std::sync::Arc;
use std::time::Duration;

/// Suspends the caller for a delay. Injected into [`Retry`](crate::Retry) and
/// [`Timeout`](crate::Timeout) with their `with_timer` builders.
///
/// ```
/// use std::sync::{Arc, Mutex};
/// use std::time::Duration;
/// use ikigai_throttle::Timer;
///
/// /// A timer that records what it was asked to wait and returns at once.
/// #[derive(Default)]
/// struct Recording(Mutex<Vec<Duration>>);
///
/// #[async_trait::async_trait]
/// impl Timer for Recording {
///     async fn sleep(&self, delay: Duration) {
///         self.0.lock().unwrap().push(delay);
///     }
/// }
///
/// let timer = Arc::new(Recording::default());
/// futures::executor::block_on(timer.sleep(Duration::from_millis(250)));
/// assert_eq!(*timer.0.lock().unwrap(), [Duration::from_millis(250)]);
/// ```
#[async_trait::async_trait]
pub trait Timer: Send + Sync {
    /// Return after `delay` has passed.
    async fn sleep(&self, delay: Duration);
}

/// The timer a [`Retry`](crate::Retry) or [`Timeout`](crate::Timeout) uses when its host
/// installed none: futures-timer on
/// a native target, none on wasm32 (see the module doc).
pub(crate) fn default_timer() -> Option<Arc<dyn Timer>> {
    #[cfg(not(target_arch = "wasm32"))]
    let timer: Option<Arc<dyn Timer>> = Some(Arc::new(crate::native_timer::Native));
    #[cfg(target_arch = "wasm32")]
    let timer: Option<Arc<dyn Timer>> = None;
    timer
}
