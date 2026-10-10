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
//! - [`Retry`] — re-issue on a *transient* failure, up to N times, for an
//!   *idempotent* verb only (a `Sink` is never blindly re-sent).
//! - [`CircuitBreaker`] — trip open after consecutive transient failures and fail
//!   fast for a cooldown, then half-open and probe to recover.
//! - [`Failover`] — try `[primary, backup, …]`, advancing on a transient,
//!   idempotent failure.
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
//! The reliability overlays read the request **verb** (idempotency governs whether
//! a re-issue is *safe*) and [`Error::is_transient`](ikigai_core::Error::is_transient)
//! (whether it's *worth* retrying). Logging, egress-filtering, and load-balancing
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
use std::sync::Mutex;
use std::time::{Duration, Instant};

mod memo;
use memo::{address, digest, Encloses, Wrappers};

use ikigai_core::{
    Bindings, Description, Endpoint, Error, Invocation, Iri, Representation, Request, Resolution,
    Resolved, Scope, Space, SpaceEntry, Topology, Verb,
};
use std::sync::Arc;

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
pub struct RateLimit<S> {
    inner: S,
    rules: Vec<(String, Rate)>,
    /// Shared with every over-budget stand-in, which reads its retry hint here
    /// when it refuses rather than carrying one fixed when it was built.
    hits: Arc<Mutex<HashMap<String, VecDeque<Instant>>>>,
    /// One stand-in per (wrapped endpoint, limited prefix): see [`memo`].
    stand_ins: Wrappers<(usize, String), RateLimited>,
}

impl<S: Space> RateLimit<S> {
    /// Wrap `inner`; add limits with [`limit`](Self::limit).
    pub fn new(inner: S) -> Self {
        RateLimit {
            inner,
            rules: Vec::new(),
            hits: Arc::new(Mutex::new(HashMap::new())),
            stand_ins: Wrappers::new(),
        }
    }

    /// Cap resolutions of resources whose IRI starts with `prefix` at `rate`
    /// (builder).
    pub fn limit(mut self, prefix: impl Into<String>, rate: Rate) -> Self {
        self.rules.push((prefix.into(), rate));
        // Longest prefix first, so `rule_for` takes the most specific match.
        self.rules
            .sort_by_key(|(prefix, _)| std::cmp::Reverse(prefix.len()));
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

        let now = Instant::now();
        let mut hits = self.hits.lock().expect("throttle lock");
        let window = hits.entry(prefix.clone()).or_default();
        // Drop timestamps older than the window.
        while window
            .front()
            .is_some_and(|&t| now.duration_since(t) >= rate.window)
        {
            window.pop_front();
        }
        if window.len() as u32 >= rate.max {
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
                },
            );
            return Resolution::Hit(hit.with_endpoint(stand_in));
        }
        window.push_back(now);
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
/// enforced whatever the budget says; the refusal itself is unchanged.
///
/// The refusal is a permanent [`Error::Endpoint`] carrying the retry hint: core
/// has no typed "retry after" error, and a transient one would have a `Retry`
/// above re-issue into the limit immediately, which is the opposite of the
/// politeness the limit exists for. The hint is computed when the stand-in
/// refuses, from the window as it is then, because one stand-in serves every
/// over-budget resolution of its endpoint (ledger #534).
struct RateLimited {
    inner: Arc<dyn Endpoint>,
    prefix: String,
    rate: Rate,
    hits: Arc<Mutex<HashMap<String, VecDeque<Instant>>>>,
}

impl RateLimited {
    /// How long until the OLDEST hit in the window ages out — unless there is no
    /// oldest hit. `Rate::new(0, …)` reads as "never allowed", a plausible
    /// operator input, and it is over budget on an EMPTY window: unwrapping the
    /// front there panicked on the first resolve instead of refusing. A governor
    /// must refuse, never panic, so fall back to the whole window.
    fn retry_after(&self) -> Duration {
        let now = Instant::now();
        let hits = self.hits.lock().expect("throttle lock");
        hits.get(&self.prefix)
            .and_then(|window| window.front())
            .map(|&t| self.rate.window.saturating_sub(now.duration_since(t)))
            .unwrap_or(self.rate.window)
    }
}

impl Encloses for RateLimited {
    fn enclosed(&self) -> Vec<&Arc<dyn Endpoint>> {
        vec![&self.inner]
    }
}

#[async_trait::async_trait]
impl Endpoint for RateLimited {
    async fn invoke(&self, _inv: &Invocation<'_>) -> Result<Representation, Error> {
        Err(Error::Endpoint(format!(
            "rate-limited: `{}` is capped at {}/{}s — retry after {}s",
            self.prefix,
            self.rate.max,
            self.rate.window.as_secs().max(1),
            self.retry_after().as_secs() + 1
        )))
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn describe(&self) -> Description {
        self.inner.describe()
    }
}

/// A [`Space`] overlay that **re-issues** a resolution on a transient failure. It
/// wraps any space; a resolved endpoint is re-invoked up to `attempts` times while
/// the error [`is_transient`](Error::is_transient) **and** the request verb is
/// idempotent (Source/Exists/Meta/Delete). A non-idempotent `Sink` is never retried
/// — a blind re-send could double-write; that needs an idempotency key. Permanent
/// errors (denied, not-found, bad-argument) return immediately. Nygard's stability
/// family; sibling of [`RateLimit`] (and of the coming CircuitBreaker/Failover).
pub struct Retry<S> {
    inner: S,
    attempts: u32,
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
            wrappers: Wrappers::new(),
        }
    }
}

impl<S: Space> Space for Retry<S> {
    fn resolve(&self, request: &Request, scope: &Scope) -> Resolution {
        let attempts = self.attempts;
        self.inner.resolve(request, scope).map_endpoint(|inner| {
            self.wrappers.get_or_wrap(
                address(&inner),
                |_| true,
                || RetryEndpoint { inner, attempts },
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

/// The endpoint a [`Retry`] resolves to: re-invoke the inner endpoint while the
/// failure is transient and the verb idempotent.
struct RetryEndpoint {
    inner: Arc<dyn Endpoint>,
    attempts: u32,
}

impl Encloses for RetryEndpoint {
    fn enclosed(&self) -> Vec<&Arc<dyn Endpoint>> {
        vec![&self.inner]
    }
}

#[async_trait::async_trait]
impl Endpoint for RetryEndpoint {
    async fn invoke(&self, inv: &Invocation<'_>) -> Result<Representation, Error> {
        let idempotent = matches!(
            inv.request.verb,
            Verb::Source | Verb::Exists | Verb::Meta | Verb::Delete
        );
        let mut attempt = 1;
        loop {
            match self.inner.invoke(inv).await {
                Ok(representation) => return Ok(representation),
                Err(e) if e.is_transient() && idempotent && attempt < self.attempts => {
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
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
    opened_at: Option<Instant>,
}

/// A [`Space`] overlay implementing Nygard's **Circuit Breaker** (from *Release
/// It!*). It counts consecutive transient failures per target; after `threshold`
/// it **trips open** and, for `cooldown`, fails fast — returning an
/// [`Unavailable`](Error::Unavailable) error *without touching the dependency*, so
/// a dead resource stops being hammered. Once `cooldown` elapses it goes
/// **half-open**: the next call probes the dependency — success **closes** the
/// circuit, another failure **re-opens** it. Only *transient* failures count;
/// permanent ones (denied, not-found) pass through untouched. Sibling of
/// [`RateLimit`] and [`Retry`]; its trip-open is the fast trigger a Failover reads.
pub struct CircuitBreaker<S> {
    inner: S,
    threshold: u32,
    cooldown: Duration,
    states: Arc<Mutex<HashMap<String, Breaker>>>,
    /// One wrapper per (inner endpoint, target), since a wrapper carries the
    /// target its circuit is keyed on: see [`memo`].
    wrappers: Wrappers<(usize, u64), BreakerEndpoint>,
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
            wrappers: Wrappers::new(),
        }
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
}

impl Encloses for BreakerEndpoint {
    fn enclosed(&self) -> Vec<&Arc<dyn Endpoint>> {
        vec![&self.inner]
    }
}

#[async_trait::async_trait]
impl Endpoint for BreakerEndpoint {
    async fn invoke(&self, inv: &Invocation<'_>) -> Result<Representation, Error> {
        // Gate: if open and still cooling, fail fast without touching the dependency.
        {
            let mut states = self.states.lock().expect("breaker lock");
            let breaker = states.entry(self.target.clone()).or_default();
            if let Some(opened) = breaker.opened_at {
                if Instant::now().duration_since(opened) < self.cooldown {
                    return Err(Error::Unavailable(format!(
                        "circuit open for `{}` — failing fast until it cools down",
                        self.target
                    )));
                }
                // Cooldown elapsed → let this call through as a half-open probe.
            }
        }
        // Invoke (closed, or a half-open probe), then update the circuit.
        let outcome = self.inner.invoke(inv).await;
        let mut states = self.states.lock().expect("breaker lock");
        let breaker = states.entry(self.target.clone()).or_default();
        match &outcome {
            Ok(_) => {
                // Success closes the circuit (and confirms a probe).
                breaker.failures = 0;
                breaker.opened_at = None;
            }
            Err(e) if e.is_transient() => {
                breaker.failures += 1;
                if breaker.failures >= self.threshold {
                    breaker.opened_at = Some(Instant::now()); // trip, or re-open a failed probe
                }
            }
            Err(_) => {} // permanent errors don't count toward tripping
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
/// tries them in order — advancing to the next only while the error
/// [`is_transient`](Error::is_transient) **and** the verb is idempotent
/// (Source/Exists/Meta/Delete), since failing a non-idempotent `Sink` over to
/// another node could double-apply it. A permanent error stops immediately (a
/// backup would answer the same). The DR ladder's core; wrap a primary in a
/// [`CircuitBreaker`] and its trip-open becomes the *fast* trigger to move on.
/// Sibling of [`Retry`] — Retry re-issues to the *same* target, Failover to the *next*.
///
/// Each candidate is invoked on the variables **its own** grammar captured, so
/// targets bound under different patterns (`urn:x/{id}` beside `urn:x/{name}`) are
/// each handed the arguments they declare.
pub struct Failover {
    spaces: Vec<Arc<dyn Space>>,
    /// One wrapper per (candidate endpoints, target), checked against each
    /// candidate's captures on reuse: see [`memo`].
    wrappers: Wrappers<(Vec<usize>, u64), FailoverEndpoint>,
}

impl Failover {
    /// Fail over across `spaces` in order — the first is the primary.
    pub fn new(spaces: Vec<Arc<dyn Space>>) -> Self {
        Failover {
            spaces,
            wrappers: Wrappers::new(),
        }
    }
}

impl Space for Failover {
    fn resolve(&self, request: &Request, scope: &Scope) -> Resolution {
        // Resolve against every target now (cheap — a remote space's resolve is a
        // local ForwardingEndpoint, no round-trip); the wire calls happen on invoke,
        // and only as far down the list as failures force.
        let mut hits: Vec<Resolved> = Vec::new();
        for space in &self.spaces {
            if let Resolution::Hit(hit) = space.resolve(request, scope) {
                hits.push(hit);
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
        let fits = |wrapper: &FailoverEndpoint| {
            wrapper
                .candidates
                .iter()
                .zip(&hits)
                .all(|(candidate, hit)| candidate.bindings == hit.bindings)
        };
        let failover = self.wrappers.get_or_wrap(key, fits, || FailoverEndpoint {
            candidates: hits
                .iter()
                .map(|hit| Candidate {
                    endpoint: Arc::clone(&hit.endpoint),
                    bindings: hit.bindings.clone(),
                })
                .collect(),
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

/// The endpoint a [`Failover`] resolves to: try each target in order, advancing on
/// a transient, idempotent failure.
struct FailoverEndpoint {
    candidates: Vec<Candidate>,
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
}

#[async_trait::async_trait]
impl Endpoint for FailoverEndpoint {
    async fn invoke(&self, inv: &Invocation<'_>) -> Result<Representation, Error> {
        let idempotent = matches!(
            inv.request.verb,
            Verb::Source | Verb::Exists | Verb::Meta | Verb::Delete
        );
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
                Ok(representation) => return Ok(representation),
                Err(e) if e.is_transient() && idempotent && i < last => {
                    latest = Some(e); // this target is down — try the next
                }
                Err(e) => return Err(e),
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

/// A [`Space`] overlay that **bounds how long** an invocation may run. It races the
/// inner endpoint's invoke against a timer; if the budget elapses first, the work
/// is dropped and it returns a **transient** [`Timeout`](Error::Timeout) — so a
/// [`Retry`]/[`Failover`] above can move on. Applies to every verb (a slow `Sink`
/// is bounded too); the re-issue safety of a timed-out mutation is the verb's
/// concern, not the timeout's. NOTE: this bounds genuinely *async* work — a purely
/// synchronous blocking call inside the invoke (e.g. a blocking socket read) never
/// yields, so a single-threaded executor can't fire the timer; that hang is fixed
/// at the transport (a socket read timeout), complementary to this.
pub struct Timeout<S> {
    inner: S,
    budget: Duration,
    /// One wrapper per (inner endpoint, target), since a wrapper names its target
    /// when the budget elapses: see [`memo`].
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
    rules: Vec<(String, Arc<async_lock::Semaphore>)>,
    /// One wrapper per (inner endpoint, capped prefix): see [`memo`].
    wrappers: Wrappers<(usize, String), ThrottleEndpoint>,
}

impl<S: Space> Throttle<S> {
    /// Wrap `inner`; add concurrency caps with [`limit`](Self::limit).
    pub fn new(inner: S) -> Self {
        Throttle {
            inner,
            rules: Vec::new(),
            wrappers: Wrappers::new(),
        }
    }

    /// Allow at most `max` concurrent invocations of resources whose IRI starts
    /// with `prefix`; the excess parks until a slot frees (builder).
    pub fn limit(mut self, prefix: impl Into<String>, max: usize) -> Self {
        self.rules.push((
            prefix.into(),
            Arc::new(async_lock::Semaphore::new(max.max(1))),
        ));
        // Longest prefix first, so `permit_for` takes the most specific match.
        self.rules
            .sort_by_key(|(prefix, _)| std::cmp::Reverse(prefix.len()));
        self
    }

    /// The most specific rule matching `target` (its prefix and semaphore), if any.
    fn rule_for(&self, target: &str) -> Option<&(String, Arc<async_lock::Semaphore>)> {
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
            Some((prefix, semaphore)) => {
                let inner = &hit.endpoint;
                let throttled = self.wrappers.get_or_wrap(
                    (address(inner), prefix.clone()),
                    |_| true,
                    || ThrottleEndpoint {
                        inner: Arc::clone(inner),
                        semaphore: Arc::clone(semaphore),
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
    semaphore: Arc<async_lock::Semaphore>,
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
        let _permit = self.semaphore.acquire_arc().await;
        self.inner.invoke(inv).await
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn describe(&self) -> Description {
        self.inner.describe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use ikigai_core::{
        Capability, EndpointSpace, Exact, FnEndpoint, Iri, Kernel, ReprType, UriTemplate,
    };
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

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
        let candidate = |endpoint: Arc<dyn Endpoint>| Candidate {
            endpoint,
            bindings: Bindings::new(),
        };
        let failover = FailoverEndpoint {
            candidates: vec![candidate(Arc::new(Unreachable)), candidate(Arc::new(Real))],
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
        let kernel = Kernel::new(Arc::new(CircuitBreaker::new(
            space,
            2,
            Duration::from_millis(20),
        )));
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

        // Dependency recovers; after the cooldown, a half-open probe closes the circuit.
        fail.store(false, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(30));
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
        assert!(format!("{err:?}").contains("rate-limited"), "{err:?}");
        assert!(format!("{err:?}").contains("retry after"), "{err:?}");
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
        // One per 1ms: after the window elapses, calls pass again.
        let kernel = kernel_with(Rate::new(1, Duration::from_millis(1)));
        assert!(tick(&kernel).is_ok());
        std::thread::sleep(Duration::from_millis(5));
        assert!(tick(&kernel).is_ok(), "the window should have slid");
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
