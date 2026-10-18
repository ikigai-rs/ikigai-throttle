//! The [`Timer`](crate::Timer) a native `Retry` defaults to. **Native only**, declared
//! behind `cfg(not(target_arch = "wasm32"))` in `lib.rs`: futures-timer's native backend
//! reads platform time and runs a helper thread, which compile on wasm32 and fail there at
//! runtime (`tests/wasm_clean.rs` holds the line).

use std::time::Duration;

use crate::Timer;

/// futures-timer's `Delay`: runtime-agnostic, so it works under the kernel's `block_on`.
pub(crate) struct Native;

#[async_trait::async_trait]
impl Timer for Native {
    async fn sleep(&self, delay: Duration) {
        futures_timer::Delay::new(delay).await;
    }
}
