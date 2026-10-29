//! [`Timeout`]: bound an invocation by a timer.
//!
//! A timeout needs something that WAKES a future when the budget elapses, and that is
//! not a clock: core's [`Clock`](ikigai_core::Clock) answers "what time is it", never
//! "tell me when". So the overlay races the work against a [`Timer`], injected like a
//! clock (ledger #2189): a native target defaults to futures-timer, and on
//! wasm32-unknown-unknown the HOST supplies one. A browser has timers (`setTimeout`), but
//! this library cannot reach them without JavaScript imports, and the same target also
//! runs where there is no JavaScript at all (wasmtime). A `Timeout` there without a timer
//! refuses what it governs, naming [`with_timer`](Timeout::with_timer): never a panic, and
//! never an unbounded call passed off as a bounded one.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::{BoxFuture, Either};
use futures::{FutureExt, Stream};

use crate::memo::{address, digest, Encloses, Wrappers};
use crate::timer::{self, Timer};
use ikigai_core::{
    Body, ByteStream, Bytes, Description, Endpoint, Error, Forward, Invocation, Iri,
    Representation, Request, Resolution, Scope, Space, SpaceEntry, Topology,
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
///
/// **A one-shot body is bounded to its end** (ledger #2263): the budget runs from the
/// call to the body's last chunk, so a body still streaming when it elapses ends with the
/// same transient [`Timeout`](Error::Timeout), never a quiet shorter body. A buffer or a
/// body that can be read again is complete when it is handed over, and is not bounded
/// past the call.
pub struct Timeout<S> {
    inner: S,
    budget: Duration,
    /// What the budget is raced against: see [`with_timer`](Self::with_timer).
    timer: Option<Arc<dyn Timer>>,
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
            timer: timer::default_timer(),
            wrappers: Wrappers::new(),
        }
    }

    /// Race the budget against `timer` (builder), instead of the default: futures-timer on
    /// a native target, and **none on wasm32**, where a `Timeout` without a timer refuses
    /// what it governs. A browser host hands it a timer over `setTimeout`; a test hands it
    /// one that fires when the test says. Call it before the first resolution: a wrapper
    /// built by then keeps the timer it was built with.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use ikigai_core::{EndpointSpace, Kernel};
    /// use ikigai_throttle::{Timeout, Timer};
    ///
    /// /// A timer that never fires: the work always finishes first.
    /// struct Never;
    ///
    /// #[async_trait::async_trait]
    /// impl Timer for Never {
    ///     async fn sleep(&self, _delay: Duration) {
    ///         futures::future::pending::<()>().await
    ///     }
    /// }
    ///
    /// let space = Timeout::new(EndpointSpace::new(), Duration::from_secs(5))
    ///     .with_timer(Arc::new(Never));
    /// let _kernel = Kernel::new(Arc::new(space));
    /// ```
    pub fn with_timer(mut self, timer: Arc<dyn Timer>) -> Self {
        self.timer = Some(timer);
        self
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
                    timer: self.timer.clone(),
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
    timer: Option<Arc<dyn Timer>>,
}

impl Encloses for TimeoutEndpoint {
    fn enclosed(&self) -> Vec<&Arc<dyn Endpoint>> {
        vec![&self.inner]
    }
}

impl TimeoutEndpoint {
    /// The refusal for having no timer.
    fn untimed(&self) -> Error {
        Error::Endpoint(format!(
            "timeout: `{}` has no timer to bound it — install one with Timeout::with_timer",
            self.target
        ))
    }

    /// What this endpoint answers when its budget elapses.
    fn elapsed(&self) -> Error {
        Error::Timeout(format!(
            "`{}` exceeded {}ms",
            self.target,
            self.budget.as_millis()
        ))
    }
}

#[async_trait::async_trait]
impl Endpoint for TimeoutEndpoint {
    async fn invoke(&self, inv: &Invocation<'_>) -> Result<Representation, Error> {
        // No timer, no bound: refuse before the work starts rather than run it unbounded.
        let Some(timer) = &self.timer else {
            return Err(self.untimed());
        };
        let work = self.inner.invoke(inv);
        let elapse = timer.sleep(self.budget);
        futures::pin_mut!(work);
        futures::pin_mut!(elapse);
        match futures::future::select(work, elapse).await {
            // The work finished within budget.
            Either::Left((result, _timer)) => result,
            // The timer won — drop the in-flight work and report a transient timeout.
            Either::Right((_elapsed, _work)) => Err(self.elapsed()),
        }
    }

    async fn invoke_body(&self, inv: &Invocation<'_>) -> Result<Representation<Body>, Error> {
        let Some(timer) = self.timer.clone() else {
            return Err(self.untimed());
        };
        // Owned, so the same elapse goes on bounding a one-shot body after the call.
        let budget = self.budget;
        let elapse: BoxFuture<'static, ()> = async move { timer.sleep(budget).await }.boxed();
        let work = self.inner.invoke_body(inv);
        futures::pin_mut!(work);
        match futures::future::select(work, elapse).await {
            Either::Left((result, elapse)) => Ok(result?.map_body(|body| match body {
                Body::Once(stream) => Body::once(Bounded {
                    inner: Some(stream),
                    elapse,
                    elapsed: self.elapsed(),
                }),
                complete => complete,
            })),
            Either::Right((_elapsed, _work)) => Err(self.elapsed()),
        }
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn forwards(&self) -> Option<Forward> {
        self.inner.forwards()
    }

    fn describe(&self) -> Description {
        self.inner.describe()
    }
}

/// A one-shot body under the rest of a [`Timeout`]'s budget: it ends with the timeout
/// when the budget elapses before its last chunk.
struct Bounded {
    /// `None` once the body has ended.
    inner: Option<ByteStream>,
    elapse: BoxFuture<'static, ()>,
    /// The error it ends with on the elapse.
    elapsed: Error,
}

impl Stream for Bounded {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(inner) = self.inner.as_mut() else {
            return Poll::Ready(None);
        };
        // An end is the body's; otherwise the budget is checked on every poll, so a
        // producer that is always ready is bounded too.
        let polled = inner.as_mut().poll_next(cx);
        if let Poll::Ready(None | Some(Err(_))) = polled {
            self.inner = None;
            return polled;
        }
        if self.elapse.poll_unpin(cx).is_ready() {
            // Drop the producer now: its Drop releases whatever it holds.
            self.inner = None;
            return Poll::Ready(Some(Err(self.elapsed.clone())));
        }
        polled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use ikigai_core::{
        AsyncFnEndpoint, Capability, EndpointSpace, Exact, InvokeFuture, Kernel, ReprType, Verb,
    };
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::sync::Mutex;

    /// A timer the test controls: it fires at once or never, and records each budget.
    struct Scripted {
        fires: bool,
        asked: Mutex<Vec<Duration>>,
    }

    #[async_trait::async_trait]
    impl Timer for Scripted {
        async fn sleep(&self, delay: Duration) {
            self.asked.lock().unwrap().push(delay);
            if !self.fires {
                futures::future::pending::<()>().await
            }
        }
    }

    fn scripted(fires: bool) -> Arc<Scripted> {
        Arc::new(Scripted {
            fires,
            asked: Mutex::new(Vec::new()),
        })
    }

    /// An endpoint that answers once `release` is set, and counts its invocations.
    fn gated(release: Arc<AtomicBool>, seen: Arc<AtomicU32>) -> AsyncFnEndpoint {
        AsyncFnEndpoint::new("gated", move |_inv| -> InvokeFuture<'_> {
            seen.fetch_add(1, Ordering::SeqCst);
            let release = release.clone();
            Box::pin(async move {
                if !release.load(Ordering::SeqCst) {
                    futures::future::pending::<()>().await;
                }
                Ok(Representation::new(
                    ReprType::new("text/plain"),
                    b"done".to_vec(),
                ))
            })
        })
    }

    fn call(kernel: &Kernel) -> Result<Representation, Error> {
        block_on(kernel.issue(
            Request::new(Verb::Source, Iri::parse("urn:dep").unwrap()),
            &Capability::root(),
        ))
    }

    /// The injected timer decides: when it fires first, the work is dropped and the
    /// answer is a transient Timeout naming the target, raced against exactly the budget.
    #[test]
    fn an_injected_timer_that_fires_first_times_the_work_out() {
        let seen = Arc::new(AtomicU32::new(0));
        let space = EndpointSpace::new().bind(
            Exact::new("urn:dep"),
            gated(Arc::new(AtomicBool::new(false)), seen.clone()),
        );
        let timer = scripted(true);
        let kernel = Kernel::new(Arc::new(
            Timeout::new(space, Duration::from_millis(250)).with_timer(timer.clone()),
        ));
        let err = call(&kernel).unwrap_err();
        assert!(matches!(err, Error::Timeout(_)), "{err:?}");
        assert!(err.to_string().contains("urn:dep"), "{err}");
        assert_eq!(*timer.asked.lock().unwrap(), [Duration::from_millis(250)]);
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    /// Work that finishes while the timer has not fired is answered unchanged.
    #[test]
    fn work_that_finishes_first_is_answered() {
        let space = EndpointSpace::new().bind(
            Exact::new("urn:dep"),
            gated(Arc::new(AtomicBool::new(true)), Arc::default()),
        );
        let kernel = Kernel::new(Arc::new(
            Timeout::new(space, Duration::from_millis(250)).with_timer(scripted(false)),
        ));
        assert_eq!(call(&kernel).unwrap().bytes, b"done");
    }

    /// With no timer (wasm32 when the host installed none) a timeout cannot bound the
    /// work, so it REFUSES before starting it and says how to fix it: never a panic, and
    /// never the work run unbounded under a name that promises a bound.
    #[test]
    fn a_timeout_without_a_timer_refuses_and_names_the_fix() {
        let seen = Arc::new(AtomicU32::new(0));
        let space = EndpointSpace::new().bind(
            Exact::new("urn:dep"),
            gated(Arc::new(AtomicBool::new(true)), seen.clone()),
        );
        let mut timeout = Timeout::new(space, Duration::from_millis(250));
        timeout.timer = None; // what `new` leaves on wasm32
        let kernel = Kernel::new(Arc::new(timeout));
        let err = call(&kernel).unwrap_err();
        assert!(matches!(err, Error::Endpoint(_)), "{err:?}");
        assert!(err.to_string().contains("Timeout::with_timer"), "{err}");
        assert_eq!(seen.load(Ordering::SeqCst), 0, "the work never started");
    }
}
