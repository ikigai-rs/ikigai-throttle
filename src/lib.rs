//! **Reliability interception overlays** for ikigai.
//!
//! An interception overlay is a [`Space`] that wraps another `Space`, adding
//! cross-cutting behaviour to every resolution flowing through it without the
//! wrapped space knowing — the substrate's composition primitive turned to
//! reliability. Stack them in front of a leaf space, a `Fallback`, a remote
//! mount, or one another. In effect these are Michael Nygard's *Release It!*
//! stability patterns (Circuit Breaker, Timeouts, Bulkhead) as resolver
//! decorators.
//!
//! - [`RateLimit`] — reject resolutions over a per-URI-prefix rate (external
//!   politeness: a published rate you must not exceed).
//! - [`Retry`] — re-issue on a *transient* failure, or after a *retry-after* delay
//!   that fits the run, up to N times, for an *idempotent* verb only (a `Sink` is
//!   never blindly re-sent).
//! - [`CircuitBreaker`] — trip open after consecutive transient failures and fail
//!   fast (retry-after the cooldown left) for a cooldown, then half-open and probe to
//!   recover.
//! - [`Failover`] — try `[primary, backup, …]`, advancing on a transient or
//!   retry-after failure of an idempotent verb.
//! - [`Timeout`] — bound an invocation; on elapse, drop the work and return a
//!   transient timeout.
//! - [`Throttle`] — cap *concurrency* per prefix and **park** the excess until a
//!   slot frees (backpressure, never an error) — Nygard's Bulkhead.
//!
//! Every overlay here is **transparent to identity**: it decorates the endpoint
//! and forwards everything the inner resolution reported — bindings and
//! [`Resolved::canonical`](ikigai_core::Resolved::canonical), the name a rewriting
//! space actually resolved under. So an [`Alias`](ikigai_core::Alias) composed
//! *below* a governor still gives the logical and the backing name one cache entry
//! and one golden thread. [`Failover`] is the stated exception — it reports a
//! canonical only when every target agrees on one; see its `resolve`.
//!
//! Each governor also keeps **one wrapper per inner endpoint**, so a governed
//! resource resolves to the same endpoint `Arc` every time and the kernel's
//! per-endpoint floor memo hits (ledger #534); see the private `memo` module for
//! why a reused wrapper is always the right one and why the table cannot leak.
//!
//! The same transparency holds for **structure**: an overlay that encloses one
//! space reports that space's [`Space::topology`], [`Space::id`] and
//! [`Space::entries`], so `urn:kernel:topology`, explain and the diagram see
//! through a governor to the doors it guards. [`Failover`], enclosing several,
//! is reported opaque; see its `topology`.
//!
//! **Time comes from a core [`Clock`]**, never from the platform directly. The two
//! governors that measure time, [`RateLimit`] (its window) and [`CircuitBreaker`]
//! (its cooldown), read the clock they were given with `with_clock`, and default to
//! a monotonic clock on a native target. On wasm32-unknown-unknown, where Rust's own
//! platform time panics (a browser has a clock, but only behind JavaScript imports this
//! library does not make), there is no default: the host installs a clock as it does
//! for its kernel, and a governor without one refuses rather than guessing.
//! [`Timeout`] and [`Retry`] wait on a [`Timer`], which a clock cannot be, injected the
//! same way (`with_timer`; ledger #2189). Every overlay here builds for wasm32.
//!
//! **What the governors hold is readable, beside them.** A host observes a governor onto a
//! [`StateBoard`] (`observed(&board, "name")`) and mounts [`StateBoard::space`] next to
//! the governed space: `urn:throttle:state` then serves the windows, circuits, bulkheads
//! and failover routes as `text/plain` and Turtle, skolemized per governor and key, each
//! governor naming the space it governs by the [`Space::id`] it forwards. Reading it
//! needs `urn:cap:throttle:state:{governor}`, and a governor the caller may not read is
//! not found. The overlays themselves are unchanged: still transparent to identity,
//! enumeration and topology. See [`StateBoard`] and the `state` module's docs.
//!
//! The reliability overlays read the request **verb**
//! ([`Verb::is_idempotent`](ikigai_core::Verb::is_idempotent) governs whether a re-issue
//! is *safe*) and the failure's [**class**](ikigai_core::Error::class) (whether it is
//! *worth* repeating, and when). They decide from core's
//! [`FailureClass`], never from a table of their own (ledger
//! #1546, #200):
//!
//! | class received | [`Retry`] | [`Failover`] | [`CircuitBreaker`] |
//! | --- | --- | --- | --- |
//! | transient | re-issue, idempotent verbs only | advance to the next target | counts toward tripping |
//! | retry-after | wait the delay and re-issue, only when it fits | advance (another target is not limited) | not counted |
//! | over-budget, denied, refused, permanent, unknown | returned unchanged | returned unchanged | not counted |
//!
//! A re-issue or an advance spends one retry from the run's shared pool
//! ([`Invocation::retry_budget`](ikigai_core::Invocation::retry_budget)) when the host
//! configured one, and with none left the overlay returns the error it received: the
//! budget belongs to whoever issued the request, not to the overlay. A kernel with no
//! retry budget keeps each overlay's own `attempts`. Logging, egress-filtering, and load-balancing
//! overlays are later additions of the same shape. The motivating use is a standing
//! server (a dev server, a background dreamer, a red-team agent) where a runaway or
//! buggy agent must not hammer `urn:system:exec` or a remote API through the
//! substrate.
//!
//! ```
//! use ikigai_throttle::{RateLimit, Rate};
//! use std::time::Duration;
//! # fn wrap(inner: ikigai_core::EndpointSpace) {
//! let space = RateLimit::new(inner)
//!     .limit("urn:system:exec", Rate::new(3, Duration::from_secs(10)))
//!     .limit("urn:httpGet", Rate::new(30, Duration::from_secs(60)));
//! # let _ = space; }
//! ```

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Mutex;
use std::time::Duration;

mod memo;
use memo::{address, digest, Encloses, Wrappers};

mod timer;
pub use timer::Timer;

mod state;
pub use state::{
    governor_iri, governor_thread, state_cap, StateBoard, STATE, STATE_CAP_FAMILY, STATE_NS,
};
use state::{Circuit, Key, Kind, Observed, Reading, Watch};

/// The `thr:` vocabulary ([`STATE_NS`]) as Turtle: an `owl:Ontology` at
/// `https://ikigai-rs.dev/ns/throttle` defining every term the state face writes, with
/// the domain and range it writes it with. This text IS the published document, served
/// at that IRI (deployed by hand, checked against this repo by ikigai-devtools'
/// `ns-drift`), and its `owl:versionInfo` is this crate's version.
///
/// ```
/// let header = ikigai_throttle::VOCABULARY_TTL
///     .lines()
///     .find(|line| line.trim_start().starts_with("owl:versionInfo"))
///     .unwrap();
/// assert_eq!(header, format!("    owl:versionInfo \"{}\" .", env!("CARGO_PKG_VERSION")));
/// ```
pub const VOCABULARY_TTL: &str = include_str!("vocabulary.ttl");

#[cfg(not(target_arch = "wasm32"))]
mod native_clock;
#[cfg(not(target_arch = "wasm32"))]
mod native_timer;
mod timeout;
pub use timeout::Timeout;

use ikigai_core::{
    Bindings, Clock, Description, Endpoint, Error, FailureClass, Invocation, Iri, Representation,
    Request, Resolution, Resolved, Scope, Space, SpaceEntry, Time, Topology, Verb,
};
use std::sync::Arc;

/// The clock a governor reads when its host installed none: on a native target a
/// monotonic one (the private `native_clock` module); on wasm32 none at all, because
/// there is no platform time to read there without a host, and reading it anyway is
/// a runtime panic. A governor that has no clock REFUSES what it governs, naming
/// `with_clock`, rather than guessing at time (see [`RateLimit::with_clock`]).
fn default_clock() -> Option<Arc<dyn Clock>> {
    #[cfg(not(target_arch = "wasm32"))]
    let clock: Option<Arc<dyn Clock>> = Some(Arc::new(native_clock::Monotonic::new()));
    #[cfg(target_arch = "wasm32")]
    let clock: Option<Arc<dyn Clock>> = None;
    clock
}

/// A duration in whole milliseconds, the resolution of a core [`Time`].
pub(crate) fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Milliseconds from `since` to `now`; zero if the clock reads earlier than `since`
/// (a host's clock is allowed to, and a governor must not panic or wrap on it).
fn elapsed(now: Time, since: Time) -> u64 {
    now.as_millis().saturating_sub(since.as_millis())
}

/// Drop the hits in `window` that are a whole `rate.window` old or older at `now`.
fn slide(window: &mut VecDeque<Time>, now: Time, rate: Rate) {
    while window
        .front()
        .is_some_and(|&t| elapsed(now, t) >= millis(rate.window))
    {
        window.pop_front();
    }
}

/// A duration as an operator wrote it: whole seconds from a second up, else milliseconds.
pub(crate) fn span(duration: Duration) -> String {
    if duration >= Duration::from_secs(1) && duration.subsec_millis() == 0 {
        format!("{}s", duration.as_secs())
    } else {
        format!("{}ms", duration.as_millis())
    }
}

/// A velocity cap: at most `max` resolutions per `window`.
#[derive(Clone, Copy, Debug)]
pub struct Rate {
    /// The most resolutions allowed within one window.
    pub max: u32,
    /// The sliding window.
    pub window: Duration,
}

impl Rate {
    /// `max` resolutions per `window`.
    pub fn new(max: u32, window: Duration) -> Self {
        Rate { max, window }
    }
}

/// A [`Space`] overlay that rate-limits resolutions by URI prefix. Wrap any
/// space, then `limit` one or more prefixes. Longest-prefix wins; an unmatched
/// target is never limited.
///
/// A resolution over budget is refused with [`Error::RetryAfter`] (since 0.2.0,
/// ledger #1546, #200), whose delay is the time until the window next opens, so an
/// HTTP edge answers 429 with `Retry-After`:
///
/// ```
/// use std::{sync::Arc, time::Duration};
/// use futures::executor::block_on;
/// use ikigai_core::{Capability, EndpointSpace, Error, Exact, FailureClass, FixedClock,
///     FnEndpoint, Iri, Kernel, ReprType, Representation, Request, Verb};
/// use ikigai_throttle::{Rate, RateLimit};
///
/// let ok = FnEndpoint::new("x", |_| Ok(Representation::new(ReprType::new("text/plain"), b"ok".to_vec())));
/// let kernel = Kernel::new(Arc::new(
///     RateLimit::new(EndpointSpace::new().bind(Exact::new("urn:demo:x"), ok))
///         .limit("urn:demo:", Rate::new(1, Duration::from_secs(10)))
///         .with_clock(Arc::new(FixedClock::at(1_000))),
/// ));
/// let read = || block_on(kernel.issue(
///     Request::new(Verb::Source, Iri::parse("urn:demo:x").unwrap()),
///     &Capability::root(),
/// ));
/// assert!(read().is_ok());
/// let limited = read().unwrap_err();
/// assert_eq!(limited.class(), FailureClass::RetryAfter);
/// assert_eq!(limited.retry_after_millis(), Some(10_000));
/// assert_eq!(
///     limited.to_string(),
///     "retry after 10000 ms: rate-limited: `urn:demo:` is capped at 1 per 10s",
/// );
/// ```
pub struct RateLimit<S> {
    inner: S,
    rules: Vec<(String, Rate)>,
    /// Shared with every over-budget stand-in, which reads its retry hint here
    /// when it refuses rather than carrying one fixed when it was built.
    hits: Arc<Mutex<HashMap<String, VecDeque<Time>>>>,
    /// Where the window's time comes from: see [`with_clock`](Self::with_clock).
    clock: Option<Arc<dyn Clock>>,
    /// One stand-in per (wrapped endpoint, limited prefix): see [`memo`].
    stand_ins: Wrappers<(usize, String), RateLimited>,
    /// How a charged hit reaches the state face: see [`observed`](Self::observed).
    watch: Arc<Watch>,
}

impl<S: Space> RateLimit<S> {
    /// Wrap `inner`; add limits with [`limit`](Self::limit).
    pub fn new(inner: S) -> Self {
        RateLimit {
            inner,
            rules: Vec::new(),
            hits: Arc::new(Mutex::new(HashMap::new())),
            clock: default_clock(),
            stand_ins: Wrappers::new(),
            watch: Watch::new(),
        }
    }

    /// Serve this governor's windows on `board` as `name` (builder), at
    /// `urn:throttle:state:{name}`: one key per limited prefix, with its hits in the
    /// window. See [`StateBoard`]. Refused for a name that is not lowercase letters,
    /// digits and `-`, one already on the board, or a governor already observed.
    pub fn observed(self, board: &StateBoard, name: &str) -> Result<Self, Error> {
        state::observe(board, name, &self.watch, self.view())?;
        Ok(self)
    }

    fn view(&self) -> Arc<dyn Observed> {
        Arc::new(RateLimitView {
            governs: self.inner.id(),
            rules: self.rules.clone(),
            hits: Arc::clone(&self.hits),
            clock: self.clock.clone(),
        })
    }

    /// Measure the window on `clock` (builder), instead of the default: a monotonic
    /// clock on a native target, and **none on wasm32**, where a host must install
    /// one. Without a clock, every resolution under a limited prefix is refused with
    /// a permanent [`Error::Endpoint`] that names this method; nothing panics.
    ///
    /// Give it the clock the host gave its kernel (`Kernel::with_clock`) when the two
    /// should agree, or a test clock that advances only when the test says so. Time
    /// is read in whole milliseconds, a core [`Time`]'s resolution, so a window
    /// shorter than a millisecond is not meaningful.
    ///
    /// Not the request scope's clock: a temporal corridor pins time for the DOMAIN
    /// (an as-of read), and a window measured on a pinned clock would never slide.
    /// How fast a prefix is being hit is operational time, which core keeps on the
    /// kernel's clock for the same reason (ledger #517).
    ///
    /// ```
    /// use ikigai_core::{Capability, EndpointSpace, Exact, FixedClock, FnEndpoint,
    ///     Iri, Kernel, ReprType, Representation, Request, Verb};
    /// use ikigai_throttle::{Rate, RateLimit};
    /// use std::{sync::Arc, time::Duration};
    ///
    /// let leaf = EndpointSpace::new().bind(
    ///     Exact::new("urn:demo:x"),
    ///     FnEndpoint::new("x", |_| Ok(Representation::new(ReprType::new("text/plain"), b"ok".to_vec()))),
    /// );
    /// // A clock that never moves: the window never slides, however long the test runs.
    /// let kernel = Kernel::new(Arc::new(
    ///     RateLimit::new(leaf)
    ///         .limit("urn:demo:", Rate::new(1, Duration::from_millis(1)))
    ///         .with_clock(Arc::new(FixedClock::at(1_000))),
    /// ));
    /// let read = || futures::executor::block_on(kernel.issue(
    ///     Request::new(Verb::Source, Iri::parse("urn:demo:x").unwrap()),
    ///     &Capability::root(),
    /// ));
    /// assert!(read().is_ok());
    /// std::thread::sleep(Duration::from_millis(5));
    /// assert!(read().is_err(), "the window is measured on the clock it was given");
    /// ```
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self.watch.refresh(self.view());
        self
    }

    /// Cap resolutions of resources whose IRI starts with `prefix` at `rate`
    /// (builder).
    pub fn limit(mut self, prefix: impl Into<String>, rate: Rate) -> Self {
        self.rules.push((prefix.into(), rate));
        // Longest prefix first, so `rule_for` takes the most specific match.
        self.rules
            .sort_by_key(|(prefix, _)| std::cmp::Reverse(prefix.len()));
        self.watch.refresh(self.view());
        self
    }

    /// The most specific rule matching `target`, if any.
    fn rule_for(&self, target: &str) -> Option<&(String, Rate)> {
        self.rules
            .iter()
            .find(|(prefix, _)| target.starts_with(prefix))
    }
}

impl<S: Space> Space for RateLimit<S> {
    fn resolve(&self, request: &Request, scope: &Scope) -> Resolution {
        let Resolution::Hit(hit) = self.inner.resolve(request, scope) else {
            return Resolution::Miss; // a miss is nothing to throttle
        };
        // Never rate-limit self-description — describing a resource is cheap and an
        // agent must always be able to read what it may (or may not) invoke.
        if request.verb == Verb::Meta {
            return Resolution::Hit(hit);
        }
        let Some((prefix, rate)) = self.rule_for(request.target.as_str()) else {
            return Resolution::Hit(hit);
        };

        // No clock, no window: refuse rather than guess (see `with_clock`).
        let now = self.clock.as_ref().map(|clock| clock.now());
        let mut hits = self.hits.lock().expect("throttle lock");
        let window = hits.entry(prefix.clone()).or_default();
        if let Some(now) = now {
            slide(window, now, *rate);
        }
        if now.is_none() || window.len() as u32 >= rate.max {
            drop(hits);
            // Substitute the endpoint, keep everything else the inner resolution
            // reported. Being over budget does not make this a different resource:
            // if a rewrite underneath named it, that name still holds — and the
            // substitute describes itself as the endpoint it stands in for, so the
            // kernel's capability floor still holds too (see `RateLimited`). The
            // substitute is the SAME one every time for this endpoint and prefix,
            // so a refused resource keeps one identity (ledger #534).
            let inner = &hit.endpoint;
            let stand_in = self.stand_ins.get_or_wrap(
                (address(inner), prefix.clone()),
                |_| true,
                || RateLimited {
                    inner: Arc::clone(inner),
                    prefix: prefix.clone(),
                    rate: *rate,
                    hits: Arc::clone(&self.hits),
                    clock: self.clock.clone(),
                    watch: Arc::clone(&self.watch),
                },
            );
            return Resolution::Hit(hit.with_endpoint(stand_in));
        }
        if let Some(now) = now {
            window.push_back(now);
        }
        drop(hits);
        // A hit charged: the window's count changed (after the lock, see `Watch`).
        self.watch.changed();
        Resolution::Hit(hit)
    }

    fn entries(&self) -> Option<Vec<SpaceEntry>> {
        // The overlay is transparent to enumeration — the catalog/manifold sees
        // the wrapped bindings unchanged.
        self.inner.entries()
    }

    // ★ And transparent to STRUCTURE and NAME (ledger #978), as every single-space
    // overlay in this crate is. The default `topology` is an opaque node, which made
    // a governed space invisible to `urn:kernel:topology`, explain, the diagram and a
    // declared arrangement's harvest. Forwarding `id` is a claim that this overlay
    // holds the same doors as the space it wraps (ledger #987), and it does: the same
    // patterns, answered by the same endpoints, decorated. A decoration only ever
    // adds a REFUSAL, and the kernel never caches a refusal, so a successful answer
    // here is the bare space's answer and sharing its cache partition is sound.
    // `tests/topology.rs` holds every overlay to it.
    fn id(&self) -> Option<Iri> {
        self.inner.id()
    }

    fn topology(&self) -> Topology {
        self.inner.topology()
    }
}

/// The over-budget stand-in for a wrapped endpoint. It refuses on invoke, and it
/// **describes itself as the endpoint it stands in for**.
///
/// The kernel evaluates the declared-capability floor from the RESOLVED
/// endpoint's `describe()`, after resolution and before the cache lookup and the
/// invoke. This stand-in used to describe itself as `rate-limited`, declaring no
/// `requires` — so once a prefix was over budget the wrapped endpoint's floor
/// vanished with it: a caller holding no grant for a gated resource was told the
/// prefix is rate-limited instead of `Denied`, learning something the floor
/// exists to withhold, and the catalog's contract and the kernel's enforcement
/// disagreed for the length of the window. `ikigai-conformance`'s ENFORCED check
/// found it (`tests/conformance.rs`). Forwarding the description keeps declared =
/// enforced whatever the budget says.
///
/// ★ **The refusal is [`Error::RetryAfter`]** (since 0.2.0, ledger #1546, #200), with
/// the time to the window's next opening as its delay, so an HTTP edge answers 429 with
/// `Retry-After` and a [`Retry`] above waits instead of re-issuing straight into the
/// limit. It used to be a permanent [`Error::Endpoint`] with the delay only in prose,
/// because core had no typed "retry after". A `Rate::new(0, …)` prefix is never
/// allowed, so no window opening exists to name: its refusal carries no delay
/// (`delay: None`, "not now"), which no retry overlay waits on.
///
/// The decision at resolution is provisional. The stand-in **re-checks the window when
/// it is invoked**, and if the window has opened since (a [`Retry`] above waited the
/// delay out, and re-invokes the endpoint it resolved, which is this stand-in), it
/// charges the window and invokes the wrapped endpoint. Without that, a waited retry
/// would meet the same refusal however long it waited. The delay is computed then,
/// from the window as it is, because one stand-in serves every over-budget resolution
/// of its endpoint (ledger #534).
struct RateLimited {
    inner: Arc<dyn Endpoint>,
    prefix: String,
    rate: Rate,
    hits: Arc<Mutex<HashMap<String, VecDeque<Time>>>>,
    /// The governor's clock; `None` is the refusal for having none.
    clock: Option<Arc<dyn Clock>>,
    /// The governor's: a hit charged here changes its window too.
    watch: Arc<Watch>,
}

impl RateLimited {
    /// Admit this call if the window has room at `now`, charging it; otherwise the
    /// time until the OLDEST hit in the window ages out, which is when it next opens.
    /// `None` when it never opens: `Rate::new(0, …)` reads as "never allowed", a
    /// plausible operator input, and it is over budget on an EMPTY window. (Unwrapping
    /// the front there once panicked on the first resolve instead of refusing; a
    /// governor must refuse, never panic.)
    fn admit(&self, now: Time) -> Result<(), Option<Duration>> {
        let mut hits = self.hits.lock().expect("throttle lock");
        let window = hits.entry(self.prefix.clone()).or_default();
        slide(window, now, self.rate);
        if (window.len() as u32) < self.rate.max {
            window.push_back(now);
            return Ok(());
        }
        Err(window.front().map(|&t| {
            self.rate
                .window
                .saturating_sub(Duration::from_millis(elapsed(now, t)))
        }))
    }
}

impl Encloses for RateLimited {
    fn enclosed(&self) -> Vec<&Arc<dyn Endpoint>> {
        vec![&self.inner]
    }
}

#[async_trait::async_trait]
impl Endpoint for RateLimited {
    async fn invoke(&self, inv: &Invocation<'_>) -> Result<Representation, Error> {
        let Some(clock) = &self.clock else {
            return Err(Error::Endpoint(format!(
                "rate-limited: `{}` has no clock to measure its window — \
                 install one with RateLimit::with_clock",
                self.prefix
            )));
        };
        match self.admit(clock.now()) {
            // The window opened since resolution: this call is within budget now.
            Ok(()) => {
                self.watch.changed();
                self.inner.invoke(inv).await
            }
            Err(delay) => Err(Error::RetryAfter {
                delay,
                detail: format!(
                    "rate-limited: `{}` is capped at {} per {}",
                    self.prefix,
                    self.rate.max,
                    span(self.rate.window)
                ),
            }),
        }
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn describe(&self) -> Description {
        self.inner.describe()
    }
}

/// A [`Space`] overlay that **re-issues** a failed invocation when the failure's
/// [class](Error::class) says a repeat may succeed, for an
/// [idempotent](Verb::is_idempotent) verb only: a `Sink` is never blindly re-sent (a
/// re-send could double-write; that needs an idempotency key). Nygard's stability
/// family; sibling of [`RateLimit`], [`CircuitBreaker`] and [`Failover`].
///
/// What it does with each [`FailureClass`] it RECEIVES:
///
/// - **transient** ([`Timeout`](Error::Timeout), [`Unavailable`](Error::Unavailable)):
///   re-issue at once.
/// - **retry-after** ([`RetryAfter`](Error::RetryAfter)): wait the delay on its
///   [`Timer`] and re-issue, but only when the delay is known and fits: at most the
///   run's [`time_left`](Invocation::time_left) when the door set a deadline, and at
///   most this overlay's own bound ([`with_max_wait`](Self::with_max_wait)) otherwise.
///   An own bound never extends a deadline. A delay that does not fit is returned
///   unchanged, so the caller, who owns the time, decides; with no deadline and no
///   bound set, that is every `RetryAfter`.
/// - **over-budget, denied, refused, permanent, unknown**: returned on the first
///   attempt. Over-budget especially (ledger #972): under the same bound a repeat costs
///   the same and fails the same.
///
/// **The budget is the run's, not the overlay's** (ledger #1546). When the host
/// configured a retry budget, the run carries one shared pool
/// ([`Invocation::retry_budget`]); this overlay spends one retry from it before every
/// re-issue, and returns the error it received when none is left, so nested `Retry`
/// layers spend one pool instead of multiplying their attempts. `attempts` then caps
/// this layer under the pool. With no pool (a kernel that configured none), `attempts`
/// alone bounds it, as before.
///
/// ```
/// use std::sync::{Arc, atomic::{AtomicU32, Ordering}};
/// use futures::executor::block_on;
/// use ikigai_core::{Capability, EndpointSpace, Error, Exact, FnEndpoint, Iri, Kernel, Request, Verb};
/// use ikigai_throttle::Retry;
///
/// let seen = Arc::new(AtomicU32::new(0));
/// let counter = seen.clone();
/// let busy = FnEndpoint::new("busy", move |_| {
///     counter.fetch_add(1, Ordering::SeqCst);
///     Err(Error::Unavailable("busy".into()))
/// });
/// let leaf = EndpointSpace::new().bind(Exact::new("urn:busy"), busy);
/// let read = Request::new(Verb::Source, Iri::parse("urn:busy").unwrap());
///
/// // Two Retry layers of 3 attempts each, under a run allowed 2 retries in all:
/// // one first try plus two re-issues, never 3 x 3.
/// let kernel = Kernel::new(Arc::new(Retry::new(Retry::new(leaf, 3), 3))).with_retry_budget(0, 2);
/// assert!(block_on(kernel.issue(read, &Capability::root())).is_err());
/// assert_eq!(seen.load(Ordering::SeqCst), 3);
/// ```
pub struct Retry<S> {
    inner: S,
    attempts: u32,
    /// What a `RetryAfter` delay is waited out on: see [`with_timer`](Self::with_timer).
    timer: Option<Arc<dyn Timer>>,
    /// The longest delay waited when the run has no deadline.
    max_wait: Option<Duration>,
    /// One wrapper per inner endpoint: see [`memo`].
    wrappers: Wrappers<usize, RetryEndpoint>,
}

impl<S: Space> Retry<S> {
    /// Wrap `inner`, allowing up to `attempts` total invocations of a resolved
    /// endpoint (`1` = no retry).
    pub fn new(inner: S, attempts: u32) -> Self {
        Retry {
            inner,
            attempts: attempts.max(1),
            timer: timer::default_timer(),
            max_wait: None,
            wrappers: Wrappers::new(),
        }
    }

    /// Wait out [`RetryAfter`](Error::RetryAfter) delays on `timer` (builder), instead
    /// of the default: futures-timer on a native target, and **none on wasm32**, where
    /// a `Retry` without a timer returns every `RetryAfter` unchanged. A test hands it a
    /// timer that moves the test's clock. Call it before the first resolution: a wrapper
    /// built by then keeps the timer it was built with.
    pub fn with_timer(mut self, timer: Arc<dyn Timer>) -> Self {
        self.timer = Some(timer);
        self
    }

    /// The longest [`RetryAfter`](Error::RetryAfter) delay this overlay waits (builder).
    /// It is the whole bound when the run has no deadline, and it only lowers one that
    /// does. Unset, a run with no deadline waits for nothing. Call it before the first
    /// resolution, as [`with_timer`](Self::with_timer).
    pub fn with_max_wait(mut self, max_wait: Duration) -> Self {
        self.max_wait = Some(max_wait);
        self
    }
}

impl<S: Space> Space for Retry<S> {
    fn resolve(&self, request: &Request, scope: &Scope) -> Resolution {
        self.inner.resolve(request, scope).map_endpoint(|inner| {
            self.wrappers.get_or_wrap(
                address(&inner),
                |_| true,
                || RetryEndpoint {
                    inner,
                    attempts: self.attempts,
                    timer: self.timer.clone(),
                    max_wait: self.max_wait,
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

/// When a failure may be repeated, by the class core gives it.
enum Repeat {
    /// Re-issue now (transient).
    Now,
    /// Re-issue after this delay (retry-after, with a known delay).
    After(Duration),
    /// Return it (every other class, and retry-after with no delay).
    Never,
}

/// What a re-issuing overlay may do with `error` on `verb`: decided from core's
/// [`Error::class`] and [`Verb::is_idempotent`], never from a table of this crate's.
fn repeat(error: &Error, verb: Verb) -> Repeat {
    if !verb.is_idempotent() {
        return Repeat::Never;
    }
    match (error.class(), error) {
        (FailureClass::Transient, _) => Repeat::Now,
        (
            FailureClass::RetryAfter,
            Error::RetryAfter {
                delay: Some(delay), ..
            },
        ) => Repeat::After(*delay),
        _ => Repeat::Never,
    }
}

/// Spend one retry from the run's pool, when the run carries one. `true` with no pool:
/// the overlay's own bound (`attempts`, or the candidate list) is then the whole bound.
fn spend(inv: &Invocation<'_>) -> bool {
    inv.retry_budget().is_none_or(|pool| pool.try_spend())
}

/// The endpoint a [`Retry`] resolves to: re-invoke the inner endpoint while the
/// failure's class allows it, the verb is idempotent and the budget has a retry.
struct RetryEndpoint {
    inner: Arc<dyn Endpoint>,
    attempts: u32,
    timer: Option<Arc<dyn Timer>>,
    max_wait: Option<Duration>,
}

impl RetryEndpoint {
    /// Whether a `RetryAfter` delay is one this overlay may wait out for this run: the
    /// run's time left when it has a deadline (lowered by an own bound), the own bound
    /// when it has none, and never without a timer to wait on.
    fn fits(&self, delay: Duration, inv: &Invocation<'_>) -> bool {
        if self.timer.is_none() {
            return false;
        }
        let bound = match (inv.time_left(), self.max_wait) {
            (Some(left), Some(own)) => left.min(own),
            (Some(left), None) => left,
            (None, Some(own)) => own,
            (None, None) => Duration::ZERO,
        };
        delay <= bound
    }
}

impl Encloses for RetryEndpoint {
    fn enclosed(&self) -> Vec<&Arc<dyn Endpoint>> {
        vec![&self.inner]
    }
}

#[async_trait::async_trait]
impl Endpoint for RetryEndpoint {
    async fn invoke(&self, inv: &Invocation<'_>) -> Result<Representation, Error> {
        let mut attempt = 1;
        loop {
            let error = match self.inner.invoke(inv).await {
                Ok(representation) => return Ok(representation),
                Err(error) => error,
            };
            if attempt >= self.attempts {
                return Err(error);
            }
            let wait = match repeat(&error, inv.request.verb) {
                Repeat::Now => None,
                Repeat::After(delay) if self.fits(delay, inv) => Some(delay),
                Repeat::After(_) | Repeat::Never => return Err(error),
            };
            // Spend BEFORE waiting, so an empty pool never costs a wait.
            if !spend(inv) {
                return Err(error);
            }
            if let (Some(delay), Some(timer)) = (wait, &self.timer) {
                timer.sleep(delay).await;
            }
            attempt += 1;
        }
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn describe(&self) -> Description {
        self.inner.describe()
    }
}

/// Per-target circuit state for [`CircuitBreaker`].
#[derive(Default)]
struct Breaker {
    /// Consecutive transient failures while closed.
    failures: u32,
    /// When the circuit tripped open, if it is open.
    opened_at: Option<Time>,
}

/// A [`Space`] overlay implementing Nygard's **Circuit Breaker** (from *Release
/// It!*). It counts consecutive **transient** failures per target (class
/// [`Transient`](ikigai_core::FailureClass::Transient)); after `threshold` it **trips
/// open** and, for `cooldown`, fails fast *without touching the dependency*, so a dead
/// resource stops being hammered. Once `cooldown` elapses it goes **half-open**: the
/// next call probes the dependency — success **closes** the circuit, another failure
/// **re-opens** it. Every other class passes through uncounted: a `RetryAfter` says the
/// dependency is limited, not broken, and over-budget, denied, refused, permanent and
/// unknown failures say nothing about its health. Sibling of [`RateLimit`] and
/// [`Retry`]; its trip-open is the fast trigger a Failover reads.
///
/// ★ **The open circuit answers [`Error::RetryAfter`]** (since 0.2.0, ledger #1546),
/// with the cooldown left as its delay: a [`Retry`] above waits it out (when it fits
/// the run's time) instead of spending its attempts on a circuit it cannot close, a
/// [`Failover`] above moves to the next target, and an HTTP edge answers 429 with
/// `Retry-After`. It used to be a transient [`Unavailable`](Error::Unavailable).
///
/// ```
/// use std::{sync::Arc, time::Duration};
/// use futures::executor::block_on;
/// use ikigai_core::{Capability, EndpointSpace, Error, Exact, FixedClock, FnEndpoint, Iri,
///     Kernel, Request, Verb};
/// use ikigai_throttle::CircuitBreaker;
///
/// let down = FnEndpoint::new("down", |_| Err(Error::Unavailable("down".into())));
/// let kernel = Kernel::new(Arc::new(
///     CircuitBreaker::new(EndpointSpace::new().bind(Exact::new("urn:dep"), down), 1, Duration::from_secs(30))
///         .with_clock(Arc::new(FixedClock::at(1_000))),
/// ));
/// let read = || block_on(kernel.issue(
///     Request::new(Verb::Source, Iri::parse("urn:dep").unwrap()),
///     &Capability::root(),
/// ));
/// assert_eq!(read().unwrap_err(), Error::Unavailable("down".into()), "trips the circuit");
/// let open = read().unwrap_err();
/// assert_eq!(open.retry_after_millis(), Some(30_000), "the cooldown left");
/// assert_eq!(open.to_string(), "retry after 30000 ms: circuit open for `urn:dep`");
/// ```
pub struct CircuitBreaker<S> {
    inner: S,
    threshold: u32,
    cooldown: Duration,
    states: Arc<Mutex<HashMap<String, Breaker>>>,
    /// Where the cooldown's time comes from: see [`with_clock`](Self::with_clock).
    clock: Option<Arc<dyn Clock>>,
    /// One wrapper per (inner endpoint, target), since a wrapper carries the
    /// target its circuit is keyed on: see [`memo`].
    wrappers: Wrappers<(usize, u64), BreakerEndpoint>,
    /// How a circuit's change reaches the state face: see [`observed`](Self::observed).
    watch: Arc<Watch>,
}

impl<S: Space> CircuitBreaker<S> {
    /// Wrap `inner`; trip open after `threshold` consecutive transient failures,
    /// staying open for `cooldown` before a half-open probe.
    pub fn new(inner: S, threshold: u32, cooldown: Duration) -> Self {
        CircuitBreaker {
            inner,
            threshold: threshold.max(1),
            cooldown,
            states: Arc::new(Mutex::new(HashMap::new())),
            clock: default_clock(),
            wrappers: Wrappers::new(),
            watch: Watch::new(),
        }
    }

    /// Serve this governor's circuits on `board` as `name` (builder), at
    /// `urn:throttle:state:{name}`: one key per target that has failed through it, with
    /// its state (closed, open, half-open) and its count of consecutive transient
    /// failures. A target that never failed has no key: its circuit is closed. See
    /// [`StateBoard`]; refused as [`RateLimit::observed`] is.
    pub fn observed(self, board: &StateBoard, name: &str) -> Result<Self, Error> {
        state::observe(board, name, &self.watch, self.view())?;
        Ok(self)
    }

    fn view(&self) -> Arc<dyn Observed> {
        Arc::new(BreakerView {
            governs: self.inner.id(),
            threshold: self.threshold,
            cooldown: self.cooldown,
            states: Arc::clone(&self.states),
            clock: self.clock.clone(),
        })
    }

    /// Measure the cooldown on `clock` (builder), instead of the default: a
    /// monotonic clock on a native target, and **none on wasm32**, where a host must
    /// install one. Without a clock the breaker refuses every call with a permanent
    /// [`Error::Endpoint`] naming this method, before touching the dependency;
    /// nothing panics. The same reasoning as [`RateLimit::with_clock`]: the
    /// kernel's clock or a test clock, never a temporal corridor's pinned one.
    ///
    /// Call it before the first resolution: a wrapper built by then keeps the clock
    /// it was built with.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self.watch.refresh(self.view());
        self
    }
}

impl<S: Space> Space for CircuitBreaker<S> {
    fn resolve(&self, request: &Request, scope: &Scope) -> Resolution {
        self.inner.resolve(request, scope).map_endpoint(|inner| {
            let target = request.target.as_str();
            self.wrappers.get_or_wrap(
                (address(&inner), digest(target)),
                |wrapper| wrapper.target == target,
                || BreakerEndpoint {
                    inner,
                    target: target.to_string(),
                    threshold: self.threshold,
                    cooldown: self.cooldown,
                    states: Arc::clone(&self.states),
                    clock: self.clock.clone(),
                    watch: Arc::clone(&self.watch),
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

/// The endpoint a [`CircuitBreaker`] resolves to: gate on the per-target circuit
/// before invoking, and update it after.
struct BreakerEndpoint {
    inner: Arc<dyn Endpoint>,
    target: String,
    threshold: u32,
    cooldown: Duration,
    states: Arc<Mutex<HashMap<String, Breaker>>>,
    clock: Option<Arc<dyn Clock>>,
    watch: Arc<Watch>,
}

impl Encloses for BreakerEndpoint {
    fn enclosed(&self) -> Vec<&Arc<dyn Endpoint>> {
        vec![&self.inner]
    }
}

#[async_trait::async_trait]
impl Endpoint for BreakerEndpoint {
    async fn invoke(&self, inv: &Invocation<'_>) -> Result<Representation, Error> {
        // No clock, no cooldown to measure: refuse rather than guess (`with_clock`).
        let Some(clock) = &self.clock else {
            return Err(Error::Endpoint(format!(
                "circuit breaker for `{}` has no clock to measure its cooldown — \
                 install one with CircuitBreaker::with_clock",
                self.target
            )));
        };
        // Gate: if open and still cooling, fail fast without touching the dependency.
        // A target with no entry is closed; reading it never creates one, so the state
        // face lists only targets that have failed (a closed circuit with no history is
        // what an absent key means).
        {
            let states = self.states.lock().expect("breaker lock");
            if let Some(opened) = states.get(&self.target).and_then(|b| b.opened_at) {
                let cooled = elapsed(clock.now(), opened);
                if cooled < millis(self.cooldown) {
                    return Err(Error::RetryAfter {
                        delay: Some(self.cooldown.saturating_sub(Duration::from_millis(cooled))),
                        detail: format!("circuit open for `{}`", self.target),
                    });
                }
                // Cooldown elapsed → let this call through as a half-open probe.
            }
        }
        // Invoke (closed, or a half-open probe), then update the circuit.
        let outcome = self.inner.invoke(inv).await;
        let mut states = self.states.lock().expect("breaker lock");
        let changed = match &outcome {
            Ok(_) => match states.get_mut(&self.target) {
                // Success closes the circuit (and confirms a probe).
                Some(breaker) if breaker.failures != 0 || breaker.opened_at.is_some() => {
                    breaker.failures = 0;
                    breaker.opened_at = None;
                    true
                }
                _ => false, // already closed: nothing changed
            },
            Err(e) if e.class() == FailureClass::Transient => {
                let breaker = states.entry(self.target.clone()).or_default();
                breaker.failures = breaker.failures.saturating_add(1);
                if breaker.failures >= self.threshold {
                    breaker.opened_at = Some(clock.now()); // trip, or re-open a failed probe
                }
                true
            }
            Err(_) => false, // no other class says the dependency is broken
        };
        drop(states);
        if changed {
            self.watch.changed();
        }
        outcome
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn describe(&self) -> Description {
        self.inner.describe()
    }
}

/// A [`Space`] overlay that **fails over** across an ordered list of spaces
/// `[primary, backup, …]`. It resolves the request against each, then on invoke
/// tries them in order — advancing to the next only while the failure's
/// [class](Error::class) is **transient** or **retry-after** (this target is down, or
/// limited; another one is neither) **and** the verb is
/// [idempotent](Verb::is_idempotent), since failing a non-idempotent `Sink` over to
/// another node could double-apply it. Every other class stops immediately: a backup
/// would answer an over-budget, denied, refused, permanent or unknown failure the same
/// (an over-budget input costs the same anywhere, ledger #972). The DR ladder's core;
/// wrap a primary in a [`CircuitBreaker`] and its trip-open becomes the *fast* trigger
/// to move on. Sibling of [`Retry`] — Retry re-issues to the *same* target, Failover to
/// the *next*.
///
/// An advance is a re-issue, so when the run carries a retry pool
/// ([`Invocation::retry_budget`]) each one spends a retry from it, and with none left
/// the failover returns the error it received. With no pool the candidate list alone
/// bounds it.
///
/// Each candidate is invoked on the variables **its own** grammar captured, so
/// targets bound under different patterns (`urn:x/{id}` beside `urn:x/{name}`) are
/// each handed the arguments they declare.
pub struct Failover {
    spaces: Vec<Arc<dyn Space>>,
    /// One wrapper per (candidate endpoints, target), checked against each
    /// candidate's captures on reuse: see [`memo`].
    wrappers: Wrappers<(Vec<usize>, u64), FailoverEndpoint>,
    /// Per target, where its last call ended: kept only once observed.
    routes: Arc<Mutex<HashMap<String, Route>>>,
    /// How a route's change reaches the state face: see [`observed`](Self::observed).
    watch: Arc<Watch>,
}

/// Where a [`Failover`]'s last call to one target ended: the index of the space (into
/// the list it was built with) whose candidate answered or returned the error.
#[derive(Clone, Copy, PartialEq)]
struct Route {
    space: usize,
    answered: bool,
}

impl Failover {
    /// Fail over across `spaces` in order — the first is the primary.
    pub fn new(spaces: Vec<Arc<dyn Space>>) -> Self {
        Failover {
            spaces,
            wrappers: Wrappers::new(),
            routes: Arc::new(Mutex::new(HashMap::new())),
            watch: Watch::new(),
        }
    }

    /// Serve this failover on `board` as `name` (builder), at
    /// `urn:throttle:state:{name}`: its spaces in order (each named by its own
    /// [`Space::id`]), and one key per target called through it since, naming the space
    /// its last call ended at and whether that one answered. See [`StateBoard`]; refused
    /// as [`RateLimit::observed`] is.
    pub fn observed(self, board: &StateBoard, name: &str) -> Result<Self, Error> {
        let view = Arc::new(FailoverView {
            candidates: self.spaces.iter().map(|space| space.id()).collect(),
            routes: Arc::clone(&self.routes),
        });
        state::observe(board, name, &self.watch, view)?;
        Ok(self)
    }
}

impl Space for Failover {
    fn resolve(&self, request: &Request, scope: &Scope) -> Resolution {
        // Resolve against every target now (cheap — a remote space's resolve is a
        // local ForwardingEndpoint, no round-trip); the wire calls happen on invoke,
        // and only as far down the list as failures force.
        let mut hits: Vec<Resolved> = Vec::new();
        // Which space each hit came from, for the state face's routes.
        let mut from: Vec<usize> = Vec::new();
        for (index, space) in self.spaces.iter().enumerate() {
            if let Resolution::Hit(hit) = space.resolve(request, scope) {
                hits.push(hit);
                from.push(index);
            }
        }
        let Some(first) = hits.first() else {
            return Resolution::Miss;
        };
        // ★ EACH CANDIDATE KEEPS ITS OWN CAPTURES.
        //
        // The candidates are matched by their own grammars, which need not be the
        // same grammar: `urn:x/{id}` beside `urn:x/{name}` both match `urn:x/7`,
        // and each hit's `bindings` are the only ones ITS endpoint can read its
        // arguments out of. Resolution ends here; invoke picks the answering
        // candidate later, so this list is the last place the pairing still exists.
        // Dropping it — keeping only the endpoints and invoking every one of them
        // with the FIRST hit's bindings — made candidate 2 read `None` for its own
        // variable and behave as it would for the bare target, silently and on the
        // branch that looks like success.
        //
        // The wrapper is reused per (candidate endpoints, target) so the resource
        // keeps one identity (ledger #534), and only while every candidate's
        // captures still match: a space can rebind an endpoint under a different
        // grammar, and the captures are what this wrapper exists to carry.
        let key = (
            hits.iter().map(|hit| address(&hit.endpoint)).collect(),
            digest(request.target.as_str()),
        );
        let target = request.target.as_str();
        let fits = |wrapper: &FailoverEndpoint| {
            wrapper.target == target
                && wrapper.candidates.iter().zip(hits.iter().zip(&from)).all(
                    |(candidate, (hit, space))| {
                        candidate.bindings == hit.bindings && candidate.space == *space
                    },
                )
        };
        let failover = self.wrappers.get_or_wrap(key, fits, || FailoverEndpoint {
            candidates: hits
                .iter()
                .zip(&from)
                .map(|(hit, space)| Candidate {
                    endpoint: Arc::clone(&hit.endpoint),
                    bindings: hit.bindings.clone(),
                    space: *space,
                })
                .collect(),
            target: target.to_string(),
            routes: Arc::clone(&self.routes),
            watch: Arc::clone(&self.watch),
        });

        // ★ THE CANONICAL: report it only when every hit agrees, otherwise none.
        //
        // `Resolved::canonical` is the name a resolution actually resolved under
        // when something underneath rewrote the target, and the kernel keys the
        // cache entry, the golden-thread cut and the capability floor on it. Every
        // other overlay in this crate has one inner resolution and simply forwards
        // what it reported. Failover does not: it keeps ALL the hits and defers the
        // choice of which one answers to invoke — after the cache key has already
        // been derived. So "which canonical" has no answer here, and the three
        // shapes are not equally safe:
        //
        // - The FIRST hit's, matching what `bindings` does below, is wrong in
        //   exactly the case failover exists for. With `[alias→A, alias→B]` and A
        //   down, the answer comes from B and is cached, invalidated and authorized
        //   under A's name: a Sink to B leaves a stale read on the entry, and a
        //   Sink to A cuts a thread nothing hangs off. That is a correctness bug
        //   where reporting nothing is merely a missed optimization.
        // - REFUSING to compose over disagreeing rewrites is loud but wrong for an
        //   overlay whose entire job is tolerating difference between its targets —
        //   and a `Space` cannot error anyway, so it would mean resolving to a
        //   failing endpoint and turning a working failover into an outage.
        // - CONSENSUS, taken here: when every hit reports the same thing, that name
        //   is true whichever endpoint ends up serving — mirrors of one resource
        //   are one resource, which is the shape failover is actually configured
        //   for. When they disagree, `None` names nothing false: the names fall
        //   back to a cache entry and a thread each, which is exactly the behaviour
        //   before `canonical` existed. No hit is silently spoken for.
        //
        // `None` and `Some(_)` are a disagreement too, not a gap to fill in: a hit
        // reporting nothing resolved under the request's own target, which is a
        // different resource from one that rewrote.
        let agreed = hits.iter().all(|hit| hit.canonical == first.canonical);
        let mut resolved = hits.into_iter().next().expect("checked non-empty");
        resolved.endpoint = failover;
        if !agreed {
            resolved.canonical = None;
        }
        Resolution::Hit(resolved)
    }

    fn entries(&self) -> Option<Vec<SpaceEntry>> {
        let mut all = Vec::new();
        let mut any = false;
        for space in &self.spaces {
            if let Some(entries) = space.entries() {
                any = true;
                all.extend(entries);
            }
        }
        any.then_some(all)
    }

    /// Opaque, deliberately: the one overlay here that does not forward its
    /// structure. It encloses SEVERAL spaces and picks the one that answers at
    /// invoke time, and no core `SpaceKind` says that. `Fallback` is first hit at
    /// RESOLUTION, so a failover reported as one would read back (a declaration, a
    /// harvest) as an arrangement that never reaches the backup, a different
    /// behavior presented as the same structure. Nor does it claim a name: its
    /// targets differ by design, so no single claim over them is true. An opaque
    /// node says where structural knowledge stops, which is the honest answer
    /// until core has a kind for it.
    fn topology(&self) -> Topology {
        Topology::opaque(None)
    }
}

/// Whether a [`Failover`] moves past a target that failed with `error` on `verb`: a
/// transient or retry-after class, for an idempotent verb. A retry-after advances
/// whatever its delay, since the delay is THIS target's.
fn advances(error: &Error, verb: Verb) -> bool {
    verb.is_idempotent()
        && matches!(
            error.class(),
            FailureClass::Transient | FailureClass::RetryAfter
        )
}

/// The endpoint a [`Failover`] resolves to: try each target in order, advancing on
/// a transient or retry-after failure of an idempotent verb.
struct FailoverEndpoint {
    candidates: Vec<Candidate>,
    /// The target every candidate resolved, which the routes are keyed on.
    target: String,
    routes: Arc<Mutex<HashMap<String, Route>>>,
    watch: Arc<Watch>,
}

impl FailoverEndpoint {
    /// Record where this call ended, when a host observes the failover, and cut the
    /// state face only when that differs from where the last one did.
    fn ended(&self, at: &Candidate, answered: bool) {
        if !self.watch.is_observed() {
            return;
        }
        let route = Route {
            space: at.space,
            answered,
        };
        let previous = self
            .routes
            .lock()
            .expect("failover lock")
            .insert(self.target.clone(), route);
        if previous != Some(route) {
            self.watch.changed();
        }
    }
}

impl Encloses for FailoverEndpoint {
    fn enclosed(&self) -> Vec<&Arc<dyn Endpoint>> {
        self.candidates.iter().map(|c| &c.endpoint).collect()
    }
}

/// One failover target: an endpoint and the variables **its own** grammar captured
/// from the request. They travel together because the choice of which endpoint
/// answers is deferred past resolution, and a capture is only meaningful to the
/// grammar that took it.
struct Candidate {
    endpoint: Arc<dyn Endpoint>,
    bindings: Bindings,
    /// The index of the space it came from, in the list the failover was built with.
    space: usize,
}

#[async_trait::async_trait]
impl Endpoint for FailoverEndpoint {
    async fn invoke(&self, inv: &Invocation<'_>) -> Result<Representation, Error> {
        let last = self.candidates.len().saturating_sub(1);
        let mut latest = None;
        for (i, candidate) in self.candidates.iter().enumerate() {
            // Reborrow this invocation onto THIS candidate's captures. Two
            // properties of `with_bindings` matter here and are easy to lose:
            //
            // - It cannot change `request`, and does not need to: every failover
            //   candidate resolves the SAME target, and only the grammars that
            //   matched it — hence the captures — differ. A combinator that needed
            //   to invoke a *different* target would have to re-enter through
            //   `issue`, and should; this is not the seam for that.
            // - It SHARES the recording side (`deps`, `dep_threads`, `trace_notes`)
            //   rather than copying it. That is what keeps a sub-request issued by
            //   the answering candidate landing its expiry and its golden threads on
            //   the parent invocation. Do not build the handle some other way —
            //   `Invocation::detached` would compile here and would sever the
            //   endpoint from the kernel, dropping the recording on the floor.
            let attempt = inv.with_bindings(&candidate.bindings);
            match candidate.endpoint.invoke(&attempt).await {
                Ok(representation) => {
                    self.ended(candidate, true);
                    return Ok(representation);
                }
                Err(e) if i < last && advances(&e, inv.request.verb) => {
                    // This target is down or limited: try the next, if the run can
                    // afford another issue.
                    if !spend(inv) {
                        self.ended(candidate, false);
                        return Err(e);
                    }
                    latest = Some(e);
                }
                Err(e) => {
                    self.ended(candidate, false);
                    return Err(e);
                }
            }
        }
        Err(latest.unwrap_or_else(|| {
            Error::Unavailable("no failover target resolved the request".into())
        }))
    }

    fn name(&self) -> &str {
        self.candidates
            .first()
            .map(|c| c.endpoint.name())
            .unwrap_or("failover")
    }

    fn describe(&self) -> Description {
        // The first target that can actually DESCRIBE itself — not simply the
        // first target.
        //
        // A description is how the engine routes named arguments, and a mount to
        // an unreachable peer describes itself as a bare stub (its `describe` is a
        // best-effort wire call, and a dead peer answers nothing). Taking that stub
        // would strip every ArgSpec the resource really has, so `source urn:fn:toUpper
        // in=hi` would lose `in=` and pass the literal text `in=hi` as content —
        // failover that returns a WRONG answer instead of no answer. So skip targets
        // that declare nothing and keep looking; if none declares anything, the first
        // one's description is as good as any.
        let described = self
            .candidates
            .iter()
            .map(|c| c.endpoint.describe())
            .find(|d| !d.action_specs().is_empty());
        described
            .or_else(|| self.candidates.first().map(|c| c.endpoint.describe()))
            .unwrap_or_else(|| Description::new("failover"))
    }
}

/// A [`Space`] overlay that caps **concurrency** by URI prefix and **parks** the
/// excess: at most N invocations of resources under a prefix run at once, and the
/// N+1th *waits* for a slot to free rather than erroring. This is `RateLimit`'s
/// sibling — RateLimit **rejects** to respect an external rate; `Throttle`
/// **backpressures** to protect a local resource from overload (NetKernel's model:
/// don't drop the work, hold it until there's room). Nygard's **Bulkhead**:
/// isolate a hot prefix so it can't consume unbounded capacity. `Meta` and
/// unmatched targets pass through unthrottled.
pub struct Throttle<S> {
    inner: S,
    rules: Vec<(String, Arc<Bulkhead>)>,
    /// One wrapper per (inner endpoint, capped prefix): see [`memo`].
    wrappers: Wrappers<(usize, String), ThrottleEndpoint>,
    /// How work starting, parking or finishing reaches the state face: see
    /// [`observed`](Self::observed).
    watch: Arc<Watch>,
}

/// One capped prefix of a [`Throttle`]: its slots, and what holds and waits for them.
struct Bulkhead {
    max: usize,
    semaphore: Arc<async_lock::Semaphore>,
    running: AtomicUsize,
    parked: AtomicUsize,
}

impl<S: Space> Throttle<S> {
    /// Wrap `inner`; add concurrency caps with [`limit`](Self::limit).
    pub fn new(inner: S) -> Self {
        Throttle {
            inner,
            rules: Vec::new(),
            wrappers: Wrappers::new(),
            watch: Watch::new(),
        }
    }

    /// Allow at most `max` concurrent invocations of resources whose IRI starts
    /// with `prefix`; the excess parks until a slot frees (builder).
    pub fn limit(mut self, prefix: impl Into<String>, max: usize) -> Self {
        let max = max.max(1);
        self.rules.push((
            prefix.into(),
            Arc::new(Bulkhead {
                max,
                semaphore: Arc::new(async_lock::Semaphore::new(max)),
                running: AtomicUsize::new(0),
                parked: AtomicUsize::new(0),
            }),
        ));
        // Longest prefix first, so `permit_for` takes the most specific match.
        self.rules
            .sort_by_key(|(prefix, _)| std::cmp::Reverse(prefix.len()));
        self.watch.refresh(self.view());
        self
    }

    /// Serve this governor's bulkheads on `board` as `name` (builder), at
    /// `urn:throttle:state:{name}`: one key per capped prefix, with the work running
    /// and parked under it. See [`StateBoard`]; refused as [`RateLimit::observed`] is.
    pub fn observed(self, board: &StateBoard, name: &str) -> Result<Self, Error> {
        state::observe(board, name, &self.watch, self.view())?;
        Ok(self)
    }

    fn view(&self) -> Arc<dyn Observed> {
        Arc::new(ThrottleView {
            governs: self.inner.id(),
            rules: self.rules.clone(),
        })
    }

    /// The most specific rule matching `target` (its prefix and bulkhead), if any.
    fn rule_for(&self, target: &str) -> Option<&(String, Arc<Bulkhead>)> {
        self.rules
            .iter()
            .find(|(prefix, _)| target.starts_with(prefix))
    }
}

impl<S: Space> Space for Throttle<S> {
    fn resolve(&self, request: &Request, scope: &Scope) -> Resolution {
        let Resolution::Hit(hit) = self.inner.resolve(request, scope) else {
            return Resolution::Miss;
        };
        // Never throttle self-description; only cap a matched prefix.
        if request.verb == Verb::Meta {
            return Resolution::Hit(hit);
        }
        match self.rule_for(request.target.as_str()) {
            Some((prefix, bulkhead)) => {
                let inner = &hit.endpoint;
                let throttled = self.wrappers.get_or_wrap(
                    (address(inner), prefix.clone()),
                    |_| true,
                    || ThrottleEndpoint {
                        inner: Arc::clone(inner),
                        bulkhead: Arc::clone(bulkhead),
                        watch: Arc::clone(&self.watch),
                    },
                );
                Resolution::Hit(hit.with_endpoint(throttled))
            }
            None => Resolution::Hit(hit),
        }
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

/// The endpoint a [`Throttle`] resolves to: hold a permit for the invocation,
/// parking until one is free.
struct ThrottleEndpoint {
    inner: Arc<dyn Endpoint>,
    bulkhead: Arc<Bulkhead>,
    watch: Arc<Watch>,
}

/// One unit of a [`Bulkhead`] count, held for as long as the work it counts: taken
/// with a change, given back with one on drop, so a call dropped mid-flight (a
/// [`Timeout`] above elapsed) is uncounted too.
struct Counted<'a> {
    count: &'a AtomicUsize,
    watch: &'a Watch,
}

impl<'a> Counted<'a> {
    fn take(count: &'a AtomicUsize, watch: &'a Watch) -> Self {
        count.fetch_add(1, AtomicOrdering::SeqCst);
        watch.changed();
        Counted { count, watch }
    }
}

impl Drop for Counted<'_> {
    fn drop(&mut self) {
        self.count.fetch_sub(1, AtomicOrdering::SeqCst);
        self.watch.changed();
    }
}

impl Encloses for ThrottleEndpoint {
    fn enclosed(&self) -> Vec<&Arc<dyn Endpoint>> {
        vec![&self.inner]
    }
}

#[async_trait::async_trait]
impl Endpoint for ThrottleEndpoint {
    async fn invoke(&self, inv: &Invocation<'_>) -> Result<Representation, Error> {
        // Park here until a slot frees; the guard releases it on drop, after invoke.
        let bulkhead = &self.bulkhead;
        let _permit = match bulkhead.semaphore.try_acquire_arc() {
            Some(permit) => permit,
            None => {
                let _parked = Counted::take(&bulkhead.parked, &self.watch);
                bulkhead.semaphore.acquire_arc().await
            }
        };
        // Dropped before the permit: the count falls, then the slot frees.
        let _running = Counted::take(&bulkhead.running, &self.watch);
        self.inner.invoke(inv).await
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn describe(&self) -> Description {
        self.inner.describe()
    }
}

// -----------------------------------------------------------------------------------------
// What each governor shows the state face (`state`): a view over the same shared state its
// endpoints change, read under the same locks, never written.
// -----------------------------------------------------------------------------------------

/// The soonest of two pending changes.
fn sooner(a: Option<Duration>, b: Duration) -> Option<Duration> {
    Some(a.map_or(b, |a| a.min(b)))
}

/// Keep the first rule per prefix: the one `rule_for` takes (the sort is stable).
fn first_per_prefix<T: Clone>(rules: &[(String, T)]) -> Vec<(String, T)> {
    let mut seen = std::collections::BTreeMap::new();
    for (prefix, rule) in rules {
        seen.entry(prefix.clone()).or_insert_with(|| rule.clone());
    }
    seen.into_iter().collect()
}

struct RateLimitView {
    governs: Option<Iri>,
    rules: Vec<(String, Rate)>,
    hits: Arc<Mutex<HashMap<String, VecDeque<Time>>>>,
    clock: Option<Arc<dyn Clock>>,
}

impl Observed for RateLimitView {
    fn read(&self) -> Reading {
        let now = self.clock.as_ref().map(|clock| clock.now());
        let hits = self.hits.lock().expect("throttle lock");
        let mut next_change = None;
        let keys = first_per_prefix(&self.rules)
            .into_iter()
            .map(|(prefix, rate)| {
                // The hits still in the window at `now`: everything after those a whole
                // window old, which `slide` drops from the front, so it is the same count
                // `resolve` will see, found without walking the window. With no clock
                // the window never slides, as `resolve` treats it.
                let window = millis(rate.window);
                let empty = VecDeque::new();
                let charged = hits.get(&prefix).unwrap_or(&empty);
                let aged = now.map_or(0, |now| {
                    charged.partition_point(|&t| elapsed(now, t) >= window)
                });
                if let (Some(now), Some(&oldest)) = (now, charged.get(aged)) {
                    let left = window.saturating_sub(elapsed(now, oldest));
                    next_change = sooner(next_change, Duration::from_millis(left));
                }
                Key::Window {
                    prefix,
                    max: rate.max,
                    window: rate.window,
                    hits: u32::try_from(charged.len() - aged).unwrap_or(u32::MAX),
                }
            })
            .collect();
        Reading {
            kind: Kind::RateLimit {
                governs: self.governs.clone(),
            },
            keys,
            next_change,
        }
    }
}

struct BreakerView {
    governs: Option<Iri>,
    threshold: u32,
    cooldown: Duration,
    states: Arc<Mutex<HashMap<String, Breaker>>>,
    clock: Option<Arc<dyn Clock>>,
}

impl Observed for BreakerView {
    fn read(&self) -> Reading {
        let now = self.clock.as_ref().map(|clock| clock.now());
        let states = self.states.lock().expect("breaker lock");
        let mut targets: Vec<(&String, &Breaker)> = states.iter().collect();
        targets.sort_by(|a, b| a.0.cmp(b.0));
        let mut next_change = None;
        let keys = targets
            .into_iter()
            .map(|(target, breaker)| {
                let circuit = match (breaker.opened_at, now) {
                    (None, _) => Circuit::Closed,
                    (Some(opened), Some(now)) => {
                        let cooled = elapsed(now, opened);
                        let cooldown = millis(self.cooldown);
                        if cooled < cooldown {
                            next_change =
                                sooner(next_change, Duration::from_millis(cooldown - cooled));
                            Circuit::Open
                        } else {
                            Circuit::HalfOpen
                        }
                    }
                    // Opened needs a clock; a breaker without one never opens.
                    (Some(_), None) => Circuit::Open,
                };
                Key::Circuit {
                    target: target.clone(),
                    failures: breaker.failures,
                    circuit,
                }
            })
            .collect();
        Reading {
            kind: Kind::CircuitBreaker {
                governs: self.governs.clone(),
                threshold: self.threshold,
                cooldown: self.cooldown,
            },
            keys,
            next_change,
        }
    }
}

struct ThrottleView {
    governs: Option<Iri>,
    rules: Vec<(String, Arc<Bulkhead>)>,
}

impl Observed for ThrottleView {
    fn read(&self) -> Reading {
        let keys = first_per_prefix(&self.rules)
            .into_iter()
            .map(|(prefix, bulkhead)| Key::Bulkhead {
                prefix,
                max: bulkhead.max,
                running: bulkhead.running.load(AtomicOrdering::SeqCst),
                parked: bulkhead.parked.load(AtomicOrdering::SeqCst),
            })
            .collect();
        Reading {
            kind: Kind::Throttle {
                governs: self.governs.clone(),
            },
            keys,
            next_change: None,
        }
    }
}

struct FailoverView {
    candidates: Vec<Option<Iri>>,
    routes: Arc<Mutex<HashMap<String, Route>>>,
}

impl Observed for FailoverView {
    fn read(&self) -> Reading {
        let routes = self.routes.lock().expect("failover lock");
        let mut targets: Vec<(&String, &Route)> = routes.iter().collect();
        targets.sort_by(|a, b| a.0.cmp(b.0));
        let keys = targets
            .into_iter()
            .map(|(target, route)| Key::Route {
                target: target.clone(),
                candidate: route.space,
                answered: route.answered,
            })
            .collect();
        Reading {
            kind: Kind::Failover {
                candidates: self.candidates.clone(),
            },
            keys,
            next_change: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use ikigai_core::{
        Capability, EndpointSpace, Exact, FixedClock, FnEndpoint, Iri, Kernel, ReprType,
        UriTemplate,
    };
    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

    /// A clock that moves only when the test moves it. Core ships `FixedClock`, which
    /// never moves, and says a crate that must advance time writes its own.
    #[derive(Clone, Default)]
    struct Manual(Arc<AtomicU64>);
    impl Manual {
        fn advance(&self, millis: u64) {
            self.0.fetch_add(millis, Ordering::SeqCst);
        }
    }
    impl Clock for Manual {
        fn now(&self) -> Time {
            Time::from_millis(self.0.load(Ordering::SeqCst))
        }
    }

    /// An endpoint whose failure is toggleable, counting invocations — so a test
    /// can watch the breaker stop reaching it, then let it recover.
    struct Controlled {
        fail: Arc<AtomicBool>,
        seen: Arc<AtomicU32>,
    }
    #[async_trait::async_trait]
    impl Endpoint for Controlled {
        async fn invoke(&self, _inv: &Invocation<'_>) -> Result<Representation, Error> {
            self.seen.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                Err(Error::Unavailable("dependency down".into()))
            } else {
                Ok(Representation::new(
                    ReprType::new("text/plain"),
                    b"ok".to_vec(),
                ))
            }
        }
    }

    /// A stand-in for a mount to an unreachable peer: it answers nothing and, like
    /// a remote endpoint whose `describe` round-trip failed, declares nothing.
    struct Unreachable;
    #[async_trait::async_trait]
    impl Endpoint for Unreachable {
        async fn invoke(&self, _inv: &Invocation<'_>) -> Result<Representation, Error> {
            Err(Error::Unavailable("peer down".into()))
        }
        fn describe(&self) -> Description {
            Description::new("remote")
        }
    }

    /// The regression this guards: a `Failover` described itself with its FIRST
    /// target's description, so a dead primary made the whole thing look
    /// arg-less. The engine routes named arguments by that description, so
    /// `source urn:x in=hi` lost `in=` and passed the literal `in=hi` as content —
    /// a wrong answer, which is worse than no answer.
    #[test]
    fn a_failover_describes_itself_by_a_target_that_declares_something() {
        struct Real;
        #[async_trait::async_trait]
        impl Endpoint for Real {
            async fn invoke(&self, _inv: &Invocation<'_>) -> Result<Representation, Error> {
                Ok(Representation::new(
                    ReprType::new("text/plain"),
                    b"ok".to_vec(),
                ))
            }
            fn describe(&self) -> Description {
                Description::new("real").verb(Verb::Source)
            }
        }
        let candidate = |endpoint: Arc<dyn Endpoint>, space| Candidate {
            endpoint,
            bindings: Bindings::new(),
            space,
        };
        let failover = FailoverEndpoint {
            candidates: vec![
                candidate(Arc::new(Unreachable), 0),
                candidate(Arc::new(Real), 1),
            ],
            target: "urn:x".to_string(),
            routes: Arc::default(),
            watch: Watch::new(),
        };
        assert!(
            !failover.describe().action_specs().is_empty(),
            "the live target's contract must survive a dead primary"
        );
    }

    #[test]
    fn trips_open_fails_fast_then_recovers_after_cooldown() {
        let seen = Arc::new(AtomicU32::new(0));
        let fail = Arc::new(AtomicBool::new(true));
        let endpoint = Arc::new(Controlled {
            fail: fail.clone(),
            seen: seen.clone(),
        });
        let space = EndpointSpace::new().bind_arc(Exact::new("urn:dep"), endpoint);
        let clock = Manual::default();
        let kernel = Kernel::new(Arc::new(
            CircuitBreaker::new(space, 2, Duration::from_millis(20))
                .with_clock(Arc::new(clock.clone())),
        ));
        let src = || Request::new(Verb::Source, Iri::parse("urn:dep").unwrap());

        // Two transient failures trip the circuit — both reach the dependency.
        assert!(block_on(kernel.issue(src(), &Capability::root())).is_err());
        assert!(block_on(kernel.issue(src(), &Capability::root())).is_err());
        assert_eq!(seen.load(Ordering::SeqCst), 2);

        // Circuit OPEN → fail fast WITHOUT touching the dependency.
        let err = block_on(kernel.issue(src(), &Capability::root())).unwrap_err();
        assert!(format!("{err}").contains("circuit open"), "{err}");
        assert_eq!(
            seen.load(Ordering::SeqCst),
            2,
            "fast-failed, dependency untouched"
        );

        // Dependency recovers, but one millisecond short of the cooldown the circuit
        // is still open: the boundary is exact because the clock is the test's.
        fail.store(false, Ordering::SeqCst);
        clock.advance(19);
        let err = block_on(kernel.issue(src(), &Capability::root())).unwrap_err();
        assert!(format!("{err}").contains("circuit open"), "{err}");
        assert_eq!(seen.load(Ordering::SeqCst), 2, "still cooling: untouched");

        // At the cooldown, a half-open probe closes the circuit.
        clock.advance(1);
        assert!(block_on(kernel.issue(src(), &Capability::root())).is_ok());
        assert_eq!(
            seen.load(Ordering::SeqCst),
            3,
            "the probe reached the recovered dependency"
        );
        // Closed again → traffic flows.
        assert!(block_on(kernel.issue(src(), &Capability::root())).is_ok());
        assert_eq!(seen.load(Ordering::SeqCst), 4);
    }

    /// An endpoint that fails transiently (Timeout) its first `fail` invocations,
    /// then succeeds — counting invocations so a test can see how many ran.
    struct Flaky {
        fail: u32,
        seen: Arc<AtomicU32>,
    }
    #[async_trait::async_trait]
    impl Endpoint for Flaky {
        async fn invoke(&self, _inv: &Invocation<'_>) -> Result<Representation, Error> {
            let n = self.seen.fetch_add(1, Ordering::SeqCst);
            if n < self.fail {
                Err(Error::Timeout(format!("attempt {n}")))
            } else {
                Ok(Representation::new(
                    ReprType::new("text/plain"),
                    b"ok".to_vec(),
                ))
            }
        }
    }

    fn kernel_over(flaky: Arc<Flaky>, attempts: u32) -> Kernel {
        let inner = EndpointSpace::new().bind_arc(Exact::new("urn:flaky"), flaky);
        Kernel::new(Arc::new(Retry::new(inner, attempts)))
    }

    #[test]
    fn retries_transient_idempotent_but_not_sinks_or_permanent() {
        // (a) A Source that fails transiently twice then succeeds — retried to success.
        let seen = Arc::new(AtomicU32::new(0));
        let kernel = kernel_over(
            Arc::new(Flaky {
                fail: 2,
                seen: seen.clone(),
            }),
            3,
        );
        let out = block_on(kernel.issue(
            Request::new(Verb::Source, Iri::parse("urn:flaky").unwrap()),
            &Capability::root(),
        ));
        assert!(
            out.is_ok(),
            "transient failures retried to success: {out:?}"
        );
        assert_eq!(
            seen.load(Ordering::SeqCst),
            3,
            "2 transient fails + 1 success"
        );

        // (b) A non-idempotent Sink is never re-sent — one attempt, then the error.
        let seen = Arc::new(AtomicU32::new(0));
        let kernel = kernel_over(
            Arc::new(Flaky {
                fail: 2,
                seen: seen.clone(),
            }),
            3,
        );
        let out = block_on(kernel.issue(
            Request::new(Verb::Sink, Iri::parse("urn:flaky").unwrap()),
            &Capability::root(),
        ));
        assert!(out.is_err(), "a Sink is not blindly re-sent");
        assert_eq!(
            seen.load(Ordering::SeqCst),
            1,
            "the Sink was invoked exactly once"
        );
    }

    /// The regression this guards: `Failover` kept the FIRST hit's bindings and
    /// invoked EVERY candidate with them, so a candidate matched by a different
    /// grammar read `None` for its own variable and behaved as it would for the
    /// bare target — a wrong answer on the branch that looks like success.
    ///
    /// Two grammars over one target is the shape that exposes it: `urn:svc/{id}`
    /// and `urn:svc/{name}` both match `urn:svc/seven`, and only the backup's own
    /// captures contain `name`. The endpoint reports what it ACTUALLY saw, so the
    /// assertion is about the invocation the candidate received, not about the
    /// configuration it was built from.
    #[test]
    fn each_failover_candidate_reads_its_own_captures() {
        /// Answers with the captures it was handed, both the variable its own
        /// grammar declares and the one the *other* candidate's does.
        struct Reporter;
        #[async_trait::async_trait]
        impl Endpoint for Reporter {
            async fn invoke(&self, inv: &Invocation<'_>) -> Result<Representation, Error> {
                let name = inv.bindings.get("name").unwrap_or("<absent>");
                let id = inv.bindings.get("id").unwrap_or("<absent>");
                Ok(Representation::new(
                    ReprType::new("text/plain"),
                    format!("name={name} id={id}").into_bytes(),
                ))
            }
        }

        let tried = Arc::new(AtomicU32::new(0));
        // Candidate 1 matches `{id}` and is down; candidate 2 matches `{name}`.
        let primary = Arc::new(EndpointSpace::new().bind_arc(
            UriTemplate::parse("urn:svc/{id}").unwrap(),
            Arc::new(Controlled {
                fail: Arc::new(AtomicBool::new(true)),
                seen: tried.clone(),
            }),
        )) as Arc<dyn Space>;
        let backup = Arc::new(EndpointSpace::new().bind_arc(
            UriTemplate::parse("urn:svc/{name}").unwrap(),
            Arc::new(Reporter),
        )) as Arc<dyn Space>;

        let kernel = Kernel::new(Arc::new(Failover::new(vec![primary, backup])));
        let repr = block_on(kernel.issue(
            Request::new(Verb::Source, Iri::parse("urn:svc/seven").unwrap()),
            &Capability::root(),
        ))
        .expect("the backup serves the failover");

        assert_eq!(
            tried.load(Ordering::SeqCst),
            1,
            "the down primary was tried"
        );
        assert_eq!(
            String::from_utf8(repr.bytes).unwrap(),
            "name=seven id=<absent>",
            "candidate 2 must read ITS OWN grammar's capture, not candidate 1's"
        );
    }

    #[test]
    fn fails_over_to_a_backup_but_never_for_a_sink() {
        fn svc(fail: bool, seen: &Arc<AtomicU32>) -> Arc<dyn Space> {
            Arc::new(EndpointSpace::new().bind_arc(
                Exact::new("urn:svc"),
                Arc::new(Controlled {
                    fail: Arc::new(AtomicBool::new(fail)),
                    seen: seen.clone(),
                }),
            ))
        }
        let primary = Arc::new(AtomicU32::new(0));
        let backup = Arc::new(AtomicU32::new(0));
        let req = |verb| Request::new(verb, Iri::parse("urn:svc").unwrap());

        // (a) A Source fails over: the (down) primary is tried, the backup serves.
        let kernel = Kernel::new(Arc::new(Failover::new(vec![
            svc(true, &primary),
            svc(false, &backup),
        ])));
        assert!(block_on(kernel.issue(req(Verb::Source), &Capability::root())).is_ok());
        assert_eq!(primary.load(Ordering::SeqCst), 1, "primary tried first");
        assert_eq!(
            backup.load(Ordering::SeqCst),
            1,
            "backup served the failover"
        );

        // (b) A Sink never fails over — that could double-apply the write.
        primary.store(0, Ordering::SeqCst);
        backup.store(0, Ordering::SeqCst);
        let kernel = Kernel::new(Arc::new(Failover::new(vec![
            svc(true, &primary),
            svc(false, &backup),
        ])));
        assert!(block_on(kernel.issue(req(Verb::Sink), &Capability::root())).is_err());
        assert_eq!(primary.load(Ordering::SeqCst), 1);
        assert_eq!(
            backup.load(Ordering::SeqCst),
            0,
            "the backup is never touched by a Sink"
        );
    }

    #[test]
    fn times_out_slow_work_but_lets_fast_work_through() {
        struct Slow {
            delay: Duration,
        }
        #[async_trait::async_trait]
        impl Endpoint for Slow {
            async fn invoke(&self, _inv: &Invocation<'_>) -> Result<Representation, Error> {
                futures_timer::Delay::new(self.delay).await;
                Ok(Representation::new(
                    ReprType::new("text/plain"),
                    b"ok".to_vec(),
                ))
            }
        }
        let over = |delay, budget| {
            let space =
                EndpointSpace::new().bind_arc(Exact::new("urn:slow"), Arc::new(Slow { delay }));
            Kernel::new(Arc::new(Timeout::new(space, budget)))
        };
        let req = || Request::new(Verb::Source, Iri::parse("urn:slow").unwrap());

        // Work slower than the budget → a transient Timeout.
        let err = block_on(
            over(Duration::from_millis(60), Duration::from_millis(10))
                .issue(req(), &Capability::root()),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("timeout"), "{err}");
        assert!(err.is_transient(), "a timeout is transient");

        // Work well within the budget → it completes normally.
        let out = block_on(
            over(Duration::from_millis(5), Duration::from_millis(200))
                .issue(req(), &Capability::root()),
        );
        assert!(out.is_ok(), "fast work completes: {out:?}");
    }

    #[test]
    fn throttle_caps_concurrency_and_parks_the_excess() {
        use futures::future::join_all;
        use std::sync::atomic::AtomicUsize;

        // An endpoint that tracks how many invocations run simultaneously.
        struct Concurrent {
            live: Arc<AtomicUsize>,
            max_seen: Arc<AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl Endpoint for Concurrent {
            async fn invoke(&self, _inv: &Invocation<'_>) -> Result<Representation, Error> {
                let now = self.live.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_seen.fetch_max(now, Ordering::SeqCst);
                futures_timer::Delay::new(Duration::from_millis(30)).await; // hold the slot
                self.live.fetch_sub(1, Ordering::SeqCst);
                Ok(Representation::new(
                    ReprType::new("text/plain"),
                    b"ok".to_vec(),
                ))
            }
        }

        let live = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let endpoint = Arc::new(Concurrent {
            live: live.clone(),
            max_seen: max_seen.clone(),
        });
        let space = EndpointSpace::new().bind_arc(Exact::new("urn:work"), endpoint);
        let kernel = Kernel::new(Arc::new(Throttle::new(space).limit("urn:work", 2)));
        let req = || Request::new(Verb::Source, Iri::parse("urn:work").unwrap());
        let cap = Capability::root();

        // Fire five concurrently; at most two ever run together, but all complete
        // (parked, never dropped).
        let results = block_on(join_all((0..5).map(|_| kernel.issue(req(), &cap))));
        assert!(results.iter().all(|r| r.is_ok()), "all five completed");
        assert!(
            max_seen.load(Ordering::SeqCst) <= 2,
            "never more than 2 ran at once, saw {}",
            max_seen.load(Ordering::SeqCst)
        );
        assert_eq!(live.load(Ordering::SeqCst), 0, "every slot released");
    }

    fn always_ok() -> FnEndpoint {
        FnEndpoint::new("ok", |_inv| {
            Ok(Representation::new(
                ReprType::new("text/plain"),
                b"ok".to_vec(),
            ))
        })
    }

    fn kernel_with(rate: Rate) -> Kernel {
        let inner = EndpointSpace::new().bind(Exact::new("urn:demo:tick"), always_ok());
        let space = RateLimit::new(inner).limit("urn:demo:", rate);
        Kernel::new(Arc::new(space))
    }

    fn kernel_on(rate: Rate, clock: Arc<dyn Clock>) -> Kernel {
        let inner = EndpointSpace::new().bind(Exact::new("urn:demo:tick"), always_ok());
        let space = RateLimit::new(inner)
            .limit("urn:demo:", rate)
            .with_clock(clock);
        Kernel::new(Arc::new(space))
    }

    fn tick(kernel: &Kernel) -> Result<Representation, Error> {
        block_on(kernel.issue(
            Request::new(Verb::Source, Iri::parse("urn:demo:tick").unwrap()),
            &Capability::root(),
        ))
    }

    #[test]
    fn over_budget_resolutions_are_throttled() {
        // Three per (long) window; the fourth in the window is throttled.
        let kernel = kernel_with(Rate::new(3, Duration::from_secs(3600)));
        for i in 0..3 {
            assert!(tick(&kernel).is_ok(), "call {i} should pass");
        }
        let err = tick(&kernel).unwrap_err();
        assert!(
            matches!(err, Error::RetryAfter { delay: Some(_), .. }),
            "{err:?}"
        );
        assert!(err.to_string().contains("rate-limited"), "{err}");
        assert!(err.to_string().starts_with("retry after"), "{err}");
    }

    /// A governor must REFUSE, never panic. `Rate::new(0, …)` — "never allowed"
    /// — is over budget with an empty window, so there is no oldest hit to
    /// compute a retry hint from; unwrapping it panicked on the very first
    /// resolve, taking the host down instead of denying the request.
    #[test]
    fn a_zero_rate_refuses_instead_of_panicking() {
        let space = EndpointSpace::new().bind(
            Exact::new("urn:never"),
            FnEndpoint::new("never", |_inv| {
                Ok(Representation::new(
                    ReprType::new("text/plain"),
                    b"ok".to_vec(),
                ))
            }),
        );
        let kernel = Kernel::new(Arc::new(
            RateLimit::new(space).limit("urn:never", Rate::new(0, Duration::from_secs(60))),
        ));
        let out = block_on(kernel.issue(
            Request::new(Verb::Source, Iri::parse("urn:never").unwrap()),
            &Capability::root(),
        ));
        assert!(out.is_err(), "a zero rate denies every resolution");
    }

    #[test]
    fn the_window_slides() {
        // One per 10ms, on a clock the test moves: refused until the oldest hit is a
        // whole window old, and not a millisecond sooner.
        let clock = Manual::default();
        let kernel = kernel_on(
            Rate::new(1, Duration::from_millis(10)),
            Arc::new(clock.clone()),
        );
        assert!(tick(&kernel).is_ok());
        clock.advance(9);
        assert!(
            tick(&kernel).is_err(),
            "one millisecond short of the window"
        );
        clock.advance(1);
        assert!(tick(&kernel).is_ok(), "the window should have slid");
    }

    /// The window is measured on the governor's clock, never on the platform's: on
    /// a clock that does not move, real time passing slides nothing. Reading
    /// `Instant::now()` directly (as every governor here did before ledger #1507)
    /// slides it, and panics on wasm32.
    #[test]
    fn a_rate_window_is_measured_on_the_governors_clock() {
        let kernel = kernel_on(
            Rate::new(1, Duration::from_millis(1)),
            Arc::new(FixedClock::at(1_000)),
        );
        assert!(tick(&kernel).is_ok());
        std::thread::sleep(Duration::from_millis(5));
        assert!(
            tick(&kernel).is_err(),
            "real time passed; the clock did not"
        );
    }

    /// The breaker's cooldown likewise: tripped on a stopped clock, it stays open
    /// however long real time runs, without touching the dependency.
    #[test]
    fn a_breaker_cooldown_is_measured_on_the_governors_clock() {
        let seen = Arc::new(AtomicU32::new(0));
        let endpoint = Arc::new(Controlled {
            fail: Arc::new(AtomicBool::new(true)),
            seen: seen.clone(),
        });
        let space = EndpointSpace::new().bind_arc(Exact::new("urn:dep"), endpoint);
        let kernel = Kernel::new(Arc::new(
            CircuitBreaker::new(space, 1, Duration::from_millis(1))
                .with_clock(Arc::new(FixedClock::at(1_000))),
        ));
        let src = || Request::new(Verb::Source, Iri::parse("urn:dep").unwrap());
        assert!(block_on(kernel.issue(src(), &Capability::root())).is_err());
        std::thread::sleep(Duration::from_millis(5));
        let err = block_on(kernel.issue(src(), &Capability::root())).unwrap_err();
        assert!(format!("{err}").contains("circuit open"), "{err}");
        assert_eq!(seen.load(Ordering::SeqCst), 1, "no probe: still cooling");
    }

    /// With no clock (wasm32 when the host installed none), a rate limit cannot
    /// measure its window, so it REFUSES what it governs and says how to fix it:
    /// never a panic, and never a silent pass. Unlimited prefixes are untouched.
    #[test]
    fn a_rate_limit_without_a_clock_refuses_and_names_the_fix() {
        let inner = EndpointSpace::new()
            .bind(Exact::new("urn:demo:tick"), always_ok())
            .bind(Exact::new("urn:other:x"), always_ok());
        let mut space =
            RateLimit::new(inner).limit("urn:demo:", Rate::new(100, Duration::from_secs(1)));
        space.clock = None; // what `new` leaves on wasm32
        let kernel = Kernel::new(Arc::new(space));
        let err = tick(&kernel).unwrap_err();
        assert!(matches!(err, Error::Endpoint(_)), "{err:?}");
        assert!(err.to_string().contains("RateLimit::with_clock"), "{err}");
        let other = block_on(kernel.issue(
            Request::new(Verb::Source, Iri::parse("urn:other:x").unwrap()),
            &Capability::root(),
        ));
        assert!(
            other.is_ok(),
            "an unlimited prefix needs no clock: {other:?}"
        );
    }

    /// The breaker without a clock refuses before touching the dependency.
    #[test]
    fn a_breaker_without_a_clock_refuses_and_names_the_fix() {
        let seen = Arc::new(AtomicU32::new(0));
        let endpoint = Arc::new(Controlled {
            fail: Arc::new(AtomicBool::new(false)),
            seen: seen.clone(),
        });
        let space = EndpointSpace::new().bind_arc(Exact::new("urn:dep"), endpoint);
        let mut breaker = CircuitBreaker::new(space, 2, Duration::from_secs(1));
        breaker.clock = None; // what `new` leaves on wasm32
        let kernel = Kernel::new(Arc::new(breaker));
        let err = block_on(kernel.issue(
            Request::new(Verb::Source, Iri::parse("urn:dep").unwrap()),
            &Capability::root(),
        ))
        .unwrap_err();
        assert!(matches!(err, Error::Endpoint(_)), "{err:?}");
        assert!(
            err.to_string().contains("CircuitBreaker::with_clock"),
            "{err}"
        );
        assert_eq!(
            seen.load(Ordering::SeqCst),
            0,
            "the dependency was not touched"
        );
    }

    #[test]
    fn unmatched_prefixes_and_meta_pass_freely() {
        let inner = EndpointSpace::new().bind(Exact::new("urn:other:x"), always_ok());
        let space =
            RateLimit::new(inner).limit("urn:demo:", Rate::new(1, Duration::from_secs(3600)));
        let kernel = Kernel::new(Arc::new(space));
        // Not under the limited prefix → never throttled, however many times.
        for _ in 0..5 {
            let r = block_on(kernel.issue(
                Request::new(Verb::Source, Iri::parse("urn:other:x").unwrap()),
                &Capability::root(),
            ));
            assert!(r.is_ok());
        }
    }
}
