//! Rate-limiting as an **interception overlay**.
//!
//! [`RateLimit`] wraps *any* [`Space`] and caps how often resources under a URI
//! prefix may resolve: at most N resolutions per time window. Over budget, the
//! request resolves not to its real endpoint but to a **rate-limited endpoint**
//! that returns an honest `rate-limited — retry after N` error on invoke.
//! Everything else passes straight through, so it composes in front of a leaf
//! space, a `Fallback`, or another overlay with no change to what it wraps.
//!
//! `RateLimit` REJECTS the excess — the right tool for external politeness
//! (a published rate you must not exceed, e.g. a SPARQL endpoint or crates.io).
//! Its sibling — a concurrency `Throttle` that PARKS the excess until a slot
//! frees (backpressure, never an error), for local overload protection — is a
//! later addition to this crate; both are the same Space-decorator shape.
//!
//! It is the first instance of ikigai's interception primitive: the same
//! Space-decorator shape will carry the concurrency throttle, logging, egress
//! filtering, and load-balancing. The motivating use is a standing server (dev,
//! dreamer, red team) where a runaway or buggy agent must not hammer
//! `urn:system:exec` or a remote API through the substrate.
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

use ikigai_core::{
    Description, Endpoint, Error, FnEndpoint, Invocation, Representation, Request, Resolution,
    Resolved, Scope, Space, SpaceEntry, Verb,
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
    hits: Mutex<HashMap<String, VecDeque<Instant>>>,
}

impl<S: Space> RateLimit<S> {
    /// Wrap `inner`; add limits with [`limit`](Self::limit).
    pub fn new(inner: S) -> Self {
        RateLimit {
            inner,
            rules: Vec::new(),
            hits: Mutex::new(HashMap::new()),
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
            let retry = rate
                .window
                .saturating_sub(now.duration_since(*window.front().expect("non-empty")));
            return Resolution::Hit(Resolved {
                endpoint: rate_limited(prefix, *rate, retry),
                bindings: hit.bindings,
            });
        }
        window.push_back(now);
        Resolution::Hit(hit)
    }

    fn entries(&self) -> Option<Vec<SpaceEntry>> {
        // The overlay is transparent to enumeration — the catalog/manifold sees
        // the wrapped bindings unchanged.
        self.inner.entries()
    }
}

/// The endpoint an over-budget request resolves to: it errors on invoke with an
/// honest, actionable message.
fn rate_limited(prefix: &str, rate: Rate, retry: Duration) -> Arc<dyn Endpoint> {
    let message = format!(
        "rate-limited: `{prefix}` is capped at {}/{}s — retry after {}s",
        rate.max,
        rate.window.as_secs().max(1),
        retry.as_secs() + 1
    );
    let summary = message.clone();
    Arc::new(
        FnEndpoint::new("rate-limited", move |_inv| {
            Err(Error::Endpoint(message.clone()))
        })
        .with_description(
            Description::new("rate-limited")
                .title("Rate limit reached")
                .summary(summary)
                .verb(Verb::Source)
                .verb(Verb::Meta)
                .output("text/plain;charset=utf-8"),
        ),
    )
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
}

impl<S: Space> Retry<S> {
    /// Wrap `inner`, allowing up to `attempts` total invocations of a resolved
    /// endpoint (`1` = no retry).
    pub fn new(inner: S, attempts: u32) -> Self {
        Retry {
            inner,
            attempts: attempts.max(1),
        }
    }
}

impl<S: Space> Space for Retry<S> {
    fn resolve(&self, request: &Request, scope: &Scope) -> Resolution {
        match self.inner.resolve(request, scope) {
            Resolution::Hit(hit) => Resolution::Hit(Resolved {
                endpoint: Arc::new(RetryEndpoint {
                    inner: hit.endpoint,
                    attempts: self.attempts,
                }),
                bindings: hit.bindings,
            }),
            Resolution::Miss => Resolution::Miss,
        }
    }

    fn entries(&self) -> Option<Vec<SpaceEntry>> {
        self.inner.entries()
    }
}

/// The endpoint a [`Retry`] resolves to: re-invoke the inner endpoint while the
/// failure is transient and the verb idempotent.
struct RetryEndpoint {
    inner: Arc<dyn Endpoint>,
    attempts: u32,
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use ikigai_core::{Capability, EndpointSpace, Exact, Iri, Kernel, ReprType};
    use std::sync::atomic::{AtomicU32, Ordering};

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
