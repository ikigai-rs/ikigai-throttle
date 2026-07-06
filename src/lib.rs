//! Rate-limiting as an **interception overlay**.
//!
//! [`Throttle`] wraps *any* [`Space`] and caps how often resources under a URI
//! prefix may resolve. When a prefix is over budget, the request resolves not to
//! its real endpoint but to a **throttled endpoint** that returns an honest
//! `throttled — retry after N` error on invoke. Everything else passes straight
//! through, so a throttle composes in front of a leaf space, a `Fallback`, or
//! another overlay with no change to what it wraps.
//!
//! It is the first instance of ikigai's interception primitive: the same
//! Space-decorator shape will carry logging, egress filtering, and
//! load-balancing. The motivating use is a standing server (dev, dreamer, red
//! team) where a runaway or buggy agent must not hammer `urn:system:exec` or a
//! remote API through the substrate.
//!
//! ```
//! use ikigai_throttle::{Throttle, Rate};
//! use std::time::Duration;
//! # fn wrap(inner: ikigai_core::EndpointSpace) {
//! let space = Throttle::new(inner)
//!     .limit("urn:system:exec", Rate::new(3, Duration::from_secs(10)))
//!     .limit("urn:httpGet", Rate::new(30, Duration::from_secs(60)));
//! # let _ = space; }
//! ```

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use ikigai_core::{
    Description, Endpoint, Error, FnEndpoint, Request, Resolution, Resolved, Scope, Space,
    SpaceEntry, Verb,
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
/// target is never throttled.
pub struct Throttle<S> {
    inner: S,
    rules: Vec<(String, Rate)>,
    hits: Mutex<HashMap<String, VecDeque<Instant>>>,
}

impl<S: Space> Throttle<S> {
    /// Wrap `inner`; add limits with [`limit`](Self::limit).
    pub fn new(inner: S) -> Self {
        Throttle {
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
        self.rules.sort_by_key(|(prefix, _)| std::cmp::Reverse(prefix.len()));
        self
    }

    /// The most specific rule matching `target`, if any.
    fn rule_for(&self, target: &str) -> Option<&(String, Rate)> {
        self.rules
            .iter()
            .find(|(prefix, _)| target.starts_with(prefix))
    }
}

impl<S: Space> Space for Throttle<S> {
    fn resolve(&self, request: &Request, scope: &Scope) -> Resolution {
        let Resolution::Hit(hit) = self.inner.resolve(request, scope) else {
            return Resolution::Miss; // a miss is nothing to throttle
        };
        // Never throttle self-description — describing a resource is cheap and an
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
                endpoint: throttled(prefix, *rate, retry),
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
fn throttled(prefix: &str, rate: Rate, retry: Duration) -> Arc<dyn Endpoint> {
    let message = format!(
        "throttled: `{prefix}` is capped at {}/{}s — retry after {}s",
        rate.max,
        rate.window.as_secs().max(1),
        retry.as_secs() + 1
    );
    let summary = message.clone();
    Arc::new(
        FnEndpoint::new("throttled", move |_inv| {
            Err(Error::Endpoint(message.clone()))
        })
        .with_description(
            Description::new("throttled")
                .title("Rate limit reached")
                .summary(summary)
                .verb(Verb::Source)
                .verb(Verb::Meta)
                .output("text/plain;charset=utf-8"),
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use ikigai_core::{Capability, EndpointSpace, Exact, Iri, Kernel, ReprType, Representation};

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
        let space = Throttle::new(inner).limit("urn:demo:", rate);
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
        assert!(format!("{err:?}").contains("throttled"), "{err:?}");
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
            Throttle::new(inner).limit("urn:demo:", Rate::new(1, Duration::from_secs(3600)));
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
