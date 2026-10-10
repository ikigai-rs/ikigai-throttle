//! **A governed resource resolves to the SAME endpoint every time** (ledger #534).
//!
//! The kernel memoizes each resolved endpoint's capability floor by the address of
//! its `Arc` (core 0.1.74), so a resource bound once is described once. A governor
//! that wrapped the inner endpoint in a fresh `Arc` on every resolution presented a
//! new address every time: every request missed the memo, ran `describe()` again,
//! and inserted an entry that died at once. Each overlay now keeps one wrapper per
//! inner endpoint, so two resolutions of one name come back `Arc::ptr_eq`.
//!
//! One test per overlay so a failure names the overlay that broke, then the three
//! properties that make reuse safe rather than merely fast: a REBOUND endpoint gets
//! a new wrapper (never the old one's), a per-target wrapper is per target, and a
//! space that allocates a fresh endpoint per resolution cannot grow the memo
//! without bound or keep the dropped endpoints alive.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::executor::block_on;
use ikigai_core::{
    Capability, Endpoint, EndpointSpace, Error, Exact, FnEndpoint, Invocation, Iri, Kernel,
    ReprType, Representation, Request, Resolution, Scope, Space, UriTemplate, Verb,
};
use ikigai_throttle::{CircuitBreaker, Failover, Rate, RateLimit, Retry, Throttle, Timeout};

fn iri(s: &str) -> Iri {
    Iri::parse(s).unwrap()
}

fn ok(name: &'static str) -> FnEndpoint {
    FnEndpoint::new(name, move |_inv| {
        Ok(Representation::new(
            ReprType::new("text/plain"),
            name.as_bytes().to_vec(),
        ))
    })
}

fn leaf() -> EndpointSpace {
    EndpointSpace::new().bind(Exact::new("urn:demo:x"), ok("x"))
}

fn resolved(space: &dyn Space, target: &str) -> Arc<dyn Endpoint> {
    match space.resolve(&Request::new(Verb::Source, iri(target)), &Scope::empty()) {
        Resolution::Hit(hit) => hit.endpoint,
        Resolution::Miss => panic!("`{target}` must resolve"),
    }
}

/// Resolve `target` twice through `space`: the same allocation both times.
fn stable(name: &str, space: &dyn Space, target: &str) {
    let first = resolved(space, target);
    let second = resolved(space, target);
    assert!(
        Arc::ptr_eq(&first, &second),
        "{name}: two resolutions of `{target}` must yield the same endpoint Arc"
    );
}

#[test]
fn retry_resolves_to_one_wrapper() {
    stable("Retry", &Retry::new(leaf(), 3), "urn:demo:x");
}

#[test]
fn circuit_breaker_resolves_to_one_wrapper() {
    let overlay = CircuitBreaker::new(leaf(), 3, Duration::from_secs(1));
    stable("CircuitBreaker", &overlay, "urn:demo:x");
}

#[test]
fn timeout_resolves_to_one_wrapper() {
    stable(
        "Timeout",
        &Timeout::new(leaf(), Duration::from_secs(1)),
        "urn:demo:x",
    );
}

#[test]
fn throttle_resolves_to_one_wrapper() {
    stable(
        "Throttle",
        &Throttle::new(leaf()).limit("urn:demo:", 2),
        "urn:demo:x",
    );
}

#[test]
fn failover_resolves_to_one_wrapper() {
    let failover = Failover::new(vec![
        Arc::new(leaf()) as Arc<dyn Space>,
        Arc::new(leaf()) as Arc<dyn Space>,
    ]);
    stable("Failover", &failover, "urn:demo:x");
}

/// The over-budget stand-in is an identity too: a caller hammering a limited
/// prefix resolves the same refusal, and it still refuses with a live retry hint.
#[test]
fn rate_limits_stand_in_resolves_to_one_wrapper() {
    let overlay = RateLimit::new(leaf()).limit("urn:demo:", Rate::new(0, Duration::from_secs(60)));
    stable("RateLimit (over budget)", &overlay, "urn:demo:x");

    let kernel = Kernel::new(Arc::new(overlay));
    let err = block_on(kernel.issue(
        Request::new(Verb::Source, iri("urn:demo:x")),
        &Capability::root(),
    ))
    .unwrap_err();
    assert!(format!("{err}").contains("rate-limited"), "{err}");
    assert!(format!("{err}").contains("retry after"), "{err}");
}

/// A stack is stable at every layer, so the outermost wrapper is too.
#[test]
fn a_stack_of_governors_resolves_to_one_wrapper() {
    let stack = Retry::new(
        CircuitBreaker::new(
            Timeout::new(leaf(), Duration::from_secs(1)),
            3,
            Duration::from_secs(1),
        ),
        2,
    );
    stable("Retry(CircuitBreaker(Timeout))", &stack, "urn:demo:x");
}

/// A space whose binding can be replaced at runtime.
struct Swappable(Mutex<Arc<dyn Endpoint>>);

impl Space for Swappable {
    fn resolve(&self, request: &Request, _scope: &Scope) -> Resolution {
        if request.target.as_str() != "urn:demo:x" {
            return Resolution::Miss;
        }
        let endpoint = Arc::clone(&self.0.lock().unwrap());
        Resolution::Hit(ikigai_core::Resolved::new(
            endpoint,
            ikigai_core::Bindings::new(),
        ))
    }
}

/// Reuse is keyed on the inner endpoint's identity, so a rebind is a new wrapper
/// that invokes the NEW endpoint, never the old wrapper serving the old one.
#[test]
fn a_rebound_endpoint_gets_a_new_wrapper() {
    let swappable = Arc::new(Swappable(Mutex::new(Arc::new(ok("old")))));
    let overlay = Arc::new(Retry::new(Arc::clone(&swappable), 3));
    let before = resolved(&*overlay, "urn:demo:x");

    *swappable.0.lock().unwrap() = Arc::new(ok("new"));
    let after = resolved(&*overlay, "urn:demo:x");
    assert!(
        !Arc::ptr_eq(&before, &after),
        "a rebound endpoint must not be served the old wrapper"
    );

    let kernel = Kernel::new(overlay);
    let answer = block_on(kernel.issue(
        Request::new(Verb::Source, iri("urn:demo:x")),
        &Capability::root(),
    ))
    .unwrap();
    assert_eq!(answer.bytes, b"new");
}

/// A breaker's state is per TARGET, so one endpoint under a template is one
/// wrapper per target: reuse never hands target A's breaker to target B.
#[test]
fn a_per_target_wrapper_is_per_target() {
    let templated =
        EndpointSpace::new().bind(UriTemplate::parse("urn:dep/{id}").unwrap(), ok("dep"));
    let overlay = CircuitBreaker::new(templated, 1, Duration::from_secs(60));
    let a = resolved(&overlay, "urn:dep/a");
    let b = resolved(&overlay, "urn:dep/b");
    assert!(!Arc::ptr_eq(&a, &b), "two targets, two breakers");
    assert!(Arc::ptr_eq(&a, &resolved(&overlay, "urn:dep/a")));
}

/// Two failover resolutions of one target through templates whose captures differ
/// are different candidates' arguments, so they are different wrappers.
#[test]
fn failover_reuse_keeps_each_candidates_captures() {
    let primary =
        Arc::new(EndpointSpace::new().bind(UriTemplate::parse("urn:svc/{id}").unwrap(), ok("p")))
            as Arc<dyn Space>;
    let failover = Failover::new(vec![primary]);
    let seven = resolved(&failover, "urn:svc/seven");
    let eight = resolved(&failover, "urn:svc/eight");
    assert!(!Arc::ptr_eq(&seven, &eight));
    assert!(Arc::ptr_eq(&seven, &resolved(&failover, "urn:svc/seven")));
}

/// A space that allocates a FRESH endpoint per resolution (a remote's forwarding
/// endpoint is built this way) can never hit the memo. It must not make the memo a
/// leak: the endpoints nobody else holds are swept, so they are dropped.
#[test]
fn per_resolution_endpoints_are_swept_not_kept_alive() {
    static LIVE: AtomicUsize = AtomicUsize::new(0);

    struct Counted;
    impl Counted {
        fn new() -> Self {
            LIVE.fetch_add(1, Ordering::SeqCst);
            Counted
        }
    }
    impl Drop for Counted {
        fn drop(&mut self) {
            LIVE.fetch_sub(1, Ordering::SeqCst);
        }
    }
    #[async_trait::async_trait]
    impl Endpoint for Counted {
        async fn invoke(&self, _inv: &Invocation<'_>) -> Result<Representation, Error> {
            Ok(Representation::new(
                ReprType::new("text/plain"),
                b"c".to_vec(),
            ))
        }
    }

    struct Fresh;
    impl Space for Fresh {
        fn resolve(&self, _request: &Request, _scope: &Scope) -> Resolution {
            Resolution::Hit(ikigai_core::Resolved::new(
                Arc::new(Counted::new()),
                ikigai_core::Bindings::new(),
            ))
        }
    }

    let overlay = Timeout::new(Fresh, Duration::from_secs(1));
    for _ in 0..10_000 {
        drop(resolved(&overlay, "urn:demo:x"));
    }
    let live = LIVE.load(Ordering::SeqCst);
    assert!(
        live <= 1024,
        "the memo must sweep endpoints only it holds: {live} still alive after 10k"
    );
}
