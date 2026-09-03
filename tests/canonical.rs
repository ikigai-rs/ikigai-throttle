//! **No overlay in this family is exempt from forwarding a rewrite.**
//!
//! `Resolved::canonical` (ikigai-core 0.1.64) is how a space that rewrote the
//! request's target tells the kernel what name it actually resolved under. The
//! kernel adopts it *before* it derives the cache id, fires the golden-thread cut
//! and evaluates the capability floor — so a logical name and its backing name are
//! ONE resource, one cache entry, one thread, however the rewriting overlay was
//! composed.
//!
//! An overlay that rebuilds a `Resolved` from parts instead of forwarding it drops
//! that report, and the two names split back into two resources that merely agree
//! until one of them is written. Nothing about that failure is loud: the
//! resolutions are correct, the reads are correct, the types are identical, and
//! the only symptom is a stale representation served through the other name after
//! a `Sink`.
//!
//! So every overlay gets the same test, composed over the same `Alias`. Six
//! near-identical tests is the point: the guard has to name the overlay that
//! broke, and an overlay added later without one is the bug this file exists to
//! stop from recurring.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::executor::block_on;
use ikigai_core::{
    Alias, AliasTable, ArgRef, Capability, Description, EndpointSpace, Exact, FnEndpoint, Iri,
    Kernel, ReprType, Representation, Request, Resolution, Rewrite, Scope, Space, Verb,
};
use ikigai_throttle::{CircuitBreaker, Failover, Rate, RateLimit, Retry, Throttle, Timeout};

fn iri(s: &str) -> Iri {
    Iri::parse(s).unwrap()
}

fn text() -> ReprType {
    ReprType::new("text/plain").with_param("charset", "utf-8")
}

/// A mutable cell exposed as a resource: `Source` is cacheable and hangs off the
/// golden thread named after the resource it was resolved under, `Sink` replaces
/// the value. The kernel cuts that thread on a successful mutating verb, so a
/// stale `Source` recomputes — *if* both names agree on which thread that is.
fn cell(value: Arc<Mutex<String>>) -> FnEndpoint {
    FnEndpoint::new("cellX", move |inv| {
        let target = inv.request.target.as_str().to_string();
        match inv.request.verb {
            Verb::Sink => {
                *value.lock().unwrap() = inv.inline_str("content")?.to_string();
                Ok(Representation::new(text(), b"ok".to_vec()))
            }
            _ => {
                let current = value.lock().unwrap().clone();
                Ok(Representation::new(text(), current.into_bytes())
                    .cacheable()
                    .depends_on(target))
            }
        }
    })
    .with_description(
        Description::new("cellX")
            .verb(Verb::Source)
            .verb(Verb::Sink)
            .output("text/plain;charset=utf-8"),
    )
}

/// Only the BACKING name is bound. `urn:store:x` reaches it solely through the
/// rewrite, which is what makes the rewrite load-bearing rather than decorative.
fn backing(value: Arc<Mutex<String>>) -> Arc<dyn Space> {
    Arc::new(EndpointSpace::new().bind(Exact::new("urn:iki:store:x"), cell(value)))
}

fn migration() -> Arc<AliasTable> {
    Arc::new(AliasTable::new().prefix("urn:store:", "urn:iki:store:"))
}

/// The real shape: an `Alias` composed by hand UNDER a governor, on a kernel that
/// holds no alias table of its own. The only thing that can make the two names one
/// resource is the canonical riding back up through the overlay.
///
/// ★ Returned CONCRETE, not as `Arc<dyn Space>`: core implements `Space` for the
/// space types but not for `Arc<dyn Space>`, and every governor here is generic
/// over `S: Space`, so an erased inner space cannot be wrapped without a
/// delegating newtype. Reported up.
fn aliased(value: Arc<Mutex<String>>) -> Alias {
    Alias::new(migration(), backing(value))
}

fn source(kernel: &Kernel, target: &str) -> Vec<u8> {
    block_on(kernel.issue(Request::new(Verb::Source, iri(target)), &Capability::root()))
        .expect("source")
        .bytes
}

fn sink(kernel: &Kernel, target: &str, body: &[u8]) {
    block_on(kernel.issue(
        Request::new(Verb::Sink, iri(target)).with_arg("content", ArgRef::Inline(body.to_vec())),
        &Capability::root(),
    ))
    .expect("sink");
}

/// The acceptance property, once: with an `Alias` composed under `compose`'s
/// overlay, the logical and the backing name share **one cache entry**, and a
/// `Sink` through either **cuts the other's** golden thread.
fn one_resource_through(label: &str, compose: impl FnOnce(Alias) -> Arc<dyn Space>) {
    let value = Arc::new(Mutex::new("one".to_string()));
    let kernel = Kernel::new(compose(aliased(value)));

    assert_eq!(source(&kernel, "urn:store:x"), b"one");
    assert_eq!(source(&kernel, "urn:iki:store:x"), b"one");
    assert_eq!(
        kernel.cache_len(),
        1,
        "{label}: two names, one resource — a second entry means the overlay \
         dropped the canonical its inner `Alias` reported"
    );

    sink(&kernel, "urn:iki:store:x", b"two");
    assert_eq!(
        source(&kernel, "urn:store:x"),
        b"two",
        "{label}: the logical name served a stale read after a sink through the backing name"
    );

    sink(&kernel, "urn:store:x", b"three");
    assert_eq!(
        source(&kernel, "urn:iki:store:x"),
        b"three",
        "{label}: the backing name served a stale read after a sink through the logical name"
    );
}

#[test]
fn rate_limit_forwards_the_canonical() {
    // Under budget. RateLimit's over-budget path is the one that rebuilds, and it
    // resolves to an erroring endpoint — nothing to cache — so it is guarded at the
    // resolution level in `every_overlay_forwards_a_reported_canonical` below.
    one_resource_through("RateLimit", |inner| {
        Arc::new(RateLimit::new(inner).limit("urn:", Rate::new(1000, Duration::from_secs(60))))
    });
}

#[test]
fn retry_forwards_the_canonical() {
    one_resource_through("Retry", |inner| Arc::new(Retry::new(inner, 3)));
}

#[test]
fn circuit_breaker_forwards_the_canonical() {
    one_resource_through("CircuitBreaker", |inner| {
        Arc::new(CircuitBreaker::new(inner, 3, Duration::from_millis(50)))
    });
}

#[test]
fn failover_forwards_the_canonical() {
    one_resource_through("Failover", |inner| {
        Arc::new(Failover::new(vec![Arc::new(inner)]))
    });
}

#[test]
fn timeout_forwards_the_canonical() {
    one_resource_through("Timeout", |inner| {
        Arc::new(Timeout::new(inner, Duration::from_secs(5)))
    });
}

#[test]
fn throttle_forwards_the_canonical() {
    one_resource_through("Throttle", |inner| {
        Arc::new(Throttle::new(inner).limit("urn:", 4))
    });
}

// ------------------------------------------------------------ resolution level

/// A space that reports a rewrite, without an `AliasTable` — the minimum an
/// overlay has to hand on.
fn rewriting(to: &'static str) -> Rewrite {
    // Bound under `to`, reachable only through the rewrite — so the space really
    // hits, and really reports. (A rewrite onto an unbound name misses, and a
    // Failover over a missing candidate has nothing to disagree with.)
    let inner: Arc<dyn Space> = Arc::new(EndpointSpace::new().bind(
        Exact::new(to),
        cell(Arc::new(Mutex::new("one".to_string()))),
    ));
    Rewrite::new(inner, move |target| {
        (target.as_str() == "urn:store:x").then(|| iri(to))
    })
}

fn canonical_of(space: &dyn Space) -> Option<String> {
    let request = Request::new(Verb::Source, iri("urn:store:x"));
    match space.resolve(&request, &Scope::empty()) {
        Resolution::Hit(hit) => hit.canonical.as_ref().map(|c| c.as_str().to_string()),
        Resolution::Miss => panic!("missed"),
    }
}

#[test]
fn every_overlay_forwards_a_reported_canonical() {
    // The per-resolution guard, including the paths the cache property cannot
    // reach: RateLimit's over-budget branch resolves to an endpoint that errors on
    // invoke, so it never caches anything — but it is still THIS resource, and it
    // is one of the six sites that used to rebuild the `Resolved`.
    let over_budget = RateLimit::new(rewriting("urn:iki:store:x"))
        .limit("urn:", Rate::new(1, Duration::from_secs(60)));
    assert_eq!(
        canonical_of(&over_budget),
        Some("urn:iki:store:x".to_string()),
        "RateLimit (under budget) dropped the canonical"
    );
    assert_eq!(
        canonical_of(&over_budget),
        Some("urn:iki:store:x".to_string()),
        "RateLimit (over budget) dropped the canonical — the refusal is still this resource"
    );

    let overlays: Vec<(&str, Arc<dyn Space>)> = vec![
        (
            "Retry",
            Arc::new(Retry::new(rewriting("urn:iki:store:x"), 3)),
        ),
        (
            "CircuitBreaker",
            Arc::new(CircuitBreaker::new(
                rewriting("urn:iki:store:x"),
                3,
                Duration::from_millis(50),
            )),
        ),
        (
            "Failover",
            Arc::new(Failover::new(vec![Arc::new(rewriting("urn:iki:store:x"))])),
        ),
        (
            "Timeout",
            Arc::new(Timeout::new(
                rewriting("urn:iki:store:x"),
                Duration::from_secs(5),
            )),
        ),
        (
            "Throttle",
            Arc::new(Throttle::new(rewriting("urn:iki:store:x")).limit("urn:", 4)),
        ),
    ];
    for (label, space) in overlays {
        assert_eq!(
            canonical_of(space.as_ref()),
            Some("urn:iki:store:x".to_string()),
            "{label} dropped the canonical its inner space reported"
        );
    }
}

// ------------------------------------------------------------------- failover

#[test]
fn a_failover_reports_the_canonical_its_targets_agree_on() {
    // Mirrors of one resource ARE one resource: whichever endpoint ends up serving
    // at invoke, the name is true, so the two names share an entry and a thread.
    let space = Failover::new(vec![
        Arc::new(rewriting("urn:iki:store:x")),
        Arc::new(rewriting("urn:iki:store:x")),
    ]);
    assert_eq!(
        canonical_of(&space),
        Some("urn:iki:store:x".to_string()),
        "every target reported the same name and the failover reported nothing"
    );
}

#[test]
fn a_failover_reports_nothing_when_its_targets_disagree() {
    // ★ The decision, pinned. Failover defers the choice of which endpoint answers
    // to invoke, and the cache key is derived before that. Reporting the FIRST
    // hit's name would be a claim about a resource that may never serve — wrong in
    // exactly the case failover exists for. Reporting nothing costs the two names
    // an entry each and claims nothing false.
    let space = Failover::new(vec![
        Arc::new(rewriting("urn:iki:store:x")),
        Arc::new(rewriting("urn:mirror:store:x")),
    ]);
    assert_eq!(
        canonical_of(&space),
        None,
        "disagreeing targets must not be spoken for by the first one"
    );
}

#[test]
fn a_failover_treats_an_unrewritten_target_as_a_disagreement() {
    // `None` is not a gap to fill in with someone else's answer: a target that
    // reported nothing resolved under the REQUEST's name, which is a different
    // resource from one that rewrote.
    let unrewritten: Arc<dyn Space> = Arc::new(EndpointSpace::new().bind(
        Exact::new("urn:store:x"),
        cell(Arc::new(Mutex::new("one".to_string()))),
    ));
    let space = Failover::new(vec![Arc::new(rewriting("urn:iki:store:x")), unrewritten]);
    assert_eq!(canonical_of(&space), None);
}
