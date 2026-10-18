//! [`Timeout`]: bound an invocation by a timer.
//!
//! **Native only.** A timeout needs something that WAKES a future when the budget
//! elapses, and that is not a clock: core's [`Clock`](ikigai_core::Clock) answers
//! "what time is it", never "tell me when". This module races the work against
//! `futures_timer::Delay`, whose native backend reads `Instant::now()` and runs a
//! helper thread; wasm32-unknown-unknown has neither a platform clock nor threads,
//! and both compile there and fail only at runtime, which `cargo check` cannot see.
//! So on wasm32 the module is not compiled and
//! `Timeout` does not exist: a host that names it gets a compile error, never a
//! panic on the first governed call. A wasm `Timeout` needs a timer the host
//! supplies, which core has no seam for yet (ledger #1507).

use std::sync::Arc;
use std::time::Duration;

use crate::memo::{address, digest, Encloses, Wrappers};
use ikigai_core::{
    Description, Endpoint, Error, Invocation, Iri, Representation, Request, Resolution, Scope,
    Space, SpaceEntry, Topology,
};

/// A [`Space`] overlay that **bounds how long** an invocation may run. It races the
/// inner endpoint's invoke against a timer; if the budget elapses first, the work
/// is dropped and it returns a **transient** [`Timeout`](Error::Timeout) — so a
/// [`Retry`](crate::Retry)/[`Failover`](crate::Failover) above can move on. Applies
/// to every verb (a slow `Sink` is bounded too); the re-issue safety of a timed-out
/// mutation is the verb's concern, not the timeout's. NOTE: this bounds genuinely *async* work — a purely
/// synchronous blocking call inside the invoke (e.g. a blocking socket read) never
/// yields, so a single-threaded executor can't fire the timer; that hang is fixed
/// at the transport (a socket read timeout), complementary to this.
///
/// ★ **Why it stays [`Timeout`](Error::Timeout), class transient, and never
/// [`OverBudget`](Error::OverBudget)** (ledger #1546, #972): this overlay cannot tell
/// what ran out. The work it dropped may have been WAITING on something slow (an
/// upstream, a lock), which a repeat may well beat, or it may have been an expensive
/// COMPUTATION, which under the same bound costs the same and fails the same. Only the
/// work itself knows which, so only the work can say `OverBudget`. Calling every
/// elapse over-budget would stop a [`Retry`](crate::Retry) from riding out a slow
/// upstream; calling it transient is the reading that was already true. A host that
/// bounds a computation (a script, a query) should bound it with the computation's own
/// budget, which can answer `OverBudget`, rather than with this overlay.
pub struct Timeout<S> {
    inner: S,
    budget: Duration,
    /// One wrapper per (inner endpoint, target), since a wrapper names its target
    /// when the budget elapses: see [`memo`](crate::memo).
    wrappers: Wrappers<(usize, u64), TimeoutEndpoint>,
}

impl<S: Space> Timeout<S> {
    /// Wrap `inner`, bounding each invocation to `budget`.
    pub fn new(inner: S, budget: Duration) -> Self {
        Timeout {
            inner,
            budget,
            wrappers: Wrappers::new(),
        }
    }
}

impl<S: Space> Space for Timeout<S> {
    fn resolve(&self, request: &Request, scope: &Scope) -> Resolution {
        self.inner.resolve(request, scope).map_endpoint(|inner| {
            let target = request.target.as_str();
            self.wrappers.get_or_wrap(
                (address(&inner), digest(target)),
                |wrapper| wrapper.target == target,
                || TimeoutEndpoint {
                    inner,
                    target: target.to_string(),
                    budget: self.budget,
                },
            ) as Arc<dyn Endpoint>
        })
    }

    fn entries(&self) -> Option<Vec<SpaceEntry>> {
        self.inner.entries()
    }

    fn id(&self) -> Option<Iri> {
        self.inner.id()
    }

    fn topology(&self) -> Topology {
        self.inner.topology()
    }
}

/// The endpoint a [`Timeout`] resolves to: race the inner invoke against the budget.
struct TimeoutEndpoint {
    inner: Arc<dyn Endpoint>,
    target: String,
    budget: Duration,
}

impl Encloses for TimeoutEndpoint {
    fn enclosed(&self) -> Vec<&Arc<dyn Endpoint>> {
        vec![&self.inner]
    }
}

#[async_trait::async_trait]
impl Endpoint for TimeoutEndpoint {
    async fn invoke(&self, inv: &Invocation<'_>) -> Result<Representation, Error> {
        let work = self.inner.invoke(inv);
        futures::pin_mut!(work);
        match futures::future::select(work, futures_timer::Delay::new(self.budget)).await {
            // The work finished within budget.
            futures::future::Either::Left((result, _timer)) => result,
            // The timer won — drop the in-flight work and report a transient timeout.
            futures::future::Either::Right((_elapsed, _work)) => Err(Error::Timeout(format!(
                "`{}` exceeded {}ms",
                self.target,
                self.budget.as_millis()
            ))),
        }
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn describe(&self) -> Description {
        self.inner.describe()
    }
}
