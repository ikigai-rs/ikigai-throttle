//! The module recipe as one test: `ikigai-conformance` walks a kernel and reports
//! every violation at once. This crate is the pass-through case — it binds no
//! endpoint of its own. An overlay decorates whatever it wraps, so what the suite
//! can hold an overlay to is exactly this: **the report through the overlay is the
//! report without it.**
//!
//! ## The fixture: a small conforming space, walked bare and through every overlay
//!
//! [`Leaf`] binds four endpoints that pass the suite on their own (the suite
//! walks fixture endpoints as module endpoints — conformance PENDING #17 — so
//! they must), one per cacheability shape an overlay has to carry through
//! unchanged:
//!
//! - `cell` (`urn:cell`) — a threaded read: `Source` is `.cacheable()` under the
//!   golden thread named after the resource, `Sink` replaces the value and cuts
//!   it. Declared `cacheable`.
//! - `live` (`urn:live`) — a live read: uncacheable, and it answers a different
//!   byte string every time so a cache hit would be visible.
//! - `upper` (`urn:upper`) — a pure function of its one input: cacheable with an
//!   empty thread set. Declared `pure` and `cacheable`.
//! - `gated` (`urn:gated`) — declares `urn:cap:demo:read`; the kernel's floor
//!   enforces it (ENFORCED proves core's gate, not the module's — PENDING #46 —
//!   and this module has no gate of its own to prove).
//!
//! [`conforms`] runs the same suite over the bare space, over each of the six
//! overlays alone, and over the README's stack, and requires every report to be
//! clean with the same shape. What the walk cannot see — that a read cached
//! through an overlay is the SAME cache entry, with the same thread, that a live
//! read stayed live, that a refusal was never stored — the hand tests below pin.
//!
//! ## Declarations, and why each
//!
//! - `cacheable("cell")`, `cacheable("upper")`: an overlay must not turn a cached
//!   read into a recomputation (the ~2000× shape the declaration exists for). The
//!   declaration is what makes an overlay that rebuilt the representation, or
//!   resolved through something volatile, a red line rather than a silent
//!   downgrade.
//! - `pure("upper")`: its empty thread set is correct by construction.
//! - No opt-outs, no namespace (no RDF face: the overlays keep no state a Turtle
//!   face could serve), NAMES runs (every id is kebab-case).
//!
//! ## What the suite cannot hold, pinned by hand
//!
//! - **The contract is the wrapped endpoint's, verbatim**
//!   ([`every_overlay_forwards_the_wrapped_contract`]): the catalog entries and
//!   every description through an overlay equal the bare space's. The suite's
//!   static checks passing through an overlay is weaker than this — a description
//!   that conformed but was not the wrapped one's would also pass.
//! - **Cacheability is inherited, never manufactured**
//!   ([`cacheability_is_inherited_not_manufactured`]): a threaded read is served
//!   from the cache on the second resolution with the wrapped endpoint's thread,
//!   a `Sink` through the overlay cuts it, a live read reaches the endpoint every
//!   time, a pure read caches with no thread. The suite sees the second half of
//!   this (cache hit, non-empty threads) but not WHICH thread, and it cannot say
//!   "live on purpose" (PENDING #22).
//! - **Typed errors pass through unchanged**
//!   ([`typed_errors_pass_through_unchanged`]): every `Error` variant an endpoint
//!   can raise comes out of every overlay as the same variant with the same
//!   message. The only overlay that re-issues is `Retry`, and it re-issues a
//!   transient error exactly `attempts` times and then returns the LAST one
//!   unchanged; a permanent error is returned on the first.
//! - **Each overlay's own refusal is typed, and never cached**
//!   ([`refusals_are_typed_and_never_cached`]): an open circuit is a transient
//!   `Unavailable`; an elapsed budget is a transient `Timeout`; a rate-limit
//!   refusal is a permanent `Endpoint` carrying a retry hint (pinned as what the
//!   code does — core has no typed "retry after" error, reported up). None of
//!   them stores anything.
//! - **An over-budget refusal keeps the wrapped contract**
//!   ([`an_over_budget_refusal_keeps_the_wrapped_contract`] and
//!   [`over_budget_the_only_findings_are_the_reads_a_refusal_cannot_serve`]): the
//!   endpoint `RateLimit` substitutes when a prefix is over budget describes
//!   itself as the WRAPPED endpoint, so the kernel's capability floor — evaluated
//!   from the resolved endpoint's description, before the cache lookup and before
//!   invoke — still refuses an ungranted caller with `Denied` rather than telling
//!   them the prefix is rate-limited. Before this arc the substitute described
//!   itself as `rate-limited` with no `requires`, and the suite's ENFORCED check
//!   found it: `gated  ENFORCED  source: declares requires `urn:cap:demo:read`;
//!   under no grants expected a typed `Denied`, got `rate-limited: …``.
//! - **A cached read is served over budget** (in
//!   [`refusals_are_typed_and_never_cached`]): `RateLimit` acts at resolution,
//!   which the kernel runs before its cache lookup, so a representation already
//!   in the cache is served to an over-budget caller — the external resource the
//!   limit protects is not touched. The same ordering means a cache hit is
//!   charged against the window. Pinned as what the code does; a `Space` sees no
//!   cache and no capability, so this is the kernel's ordering, reported up.
//! - **`Failover` over mirrors doubles the catalog** (in [`conforms`]): its
//!   `entries()` concatenates every target's, so two mirrors of one space are
//!   walked twice — 4 endpoints, 10 actions. Every action is fired twice.

use futures::executor::block_on;
use ikigai_conformance::{Check, Report, Suite};
use ikigai_core::{
    ActionSpec, ArgRef, ArgSpec, Capability, Description, Endpoint, EndpointSpace, Error, Exact,
    Expiry, FnEndpoint, Invocation, Iri, Kernel, ReprType, Representation, Request, Space, Thread,
    Verb,
};
use ikigai_throttle::{CircuitBreaker, Failover, Rate, RateLimit, Retry, Throttle, Timeout};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
const READ_CAP: &str = "urn:cap:demo:read";
const TEXT: &str = "text/plain;charset=utf-8";

/// The four fixture endpoints: id, bound IRI, verbs.
const LEAVES: [(&str, &str, &[Verb]); 4] = [
    ("cell", "urn:cell", &[Verb::Source, Verb::Sink]),
    ("live", "urn:live", &[Verb::Source]),
    ("upper", "urn:upper", &[Verb::Source]),
    ("gated", "urn:gated", &[Verb::Source]),
];

fn text() -> ReprType {
    ReprType::new("text/plain").with_param("charset", "utf-8")
}

fn ok(bytes: impl Into<Vec<u8>>) -> Representation {
    Representation::new(text(), bytes)
}

/// The fixture space's endpoints and the counters that say what reached them.
/// The endpoints are shared `Arc`s so two spaces over one `Leaf` are mirrors of
/// ONE state — the shape `Failover` is configured for.
struct Leaf {
    cell: Arc<dyn Endpoint>,
    live: Arc<dyn Endpoint>,
    upper: Arc<dyn Endpoint>,
    gated: Arc<dyn Endpoint>,
    cell_reads: Arc<AtomicU32>,
    live_reads: Arc<AtomicU32>,
    gated_reads: Arc<AtomicU32>,
}

impl Leaf {
    fn new() -> Self {
        let value = Arc::new(Mutex::new("one".to_string()));
        let cell_reads = Arc::new(AtomicU32::new(0));
        let live_reads = Arc::new(AtomicU32::new(0));
        let gated_reads = Arc::new(AtomicU32::new(0));

        let cell = {
            let (value, reads) = (value.clone(), cell_reads.clone());
            FnEndpoint::new("cell", move |inv| match inv.request.verb {
                Verb::Sink => {
                    *value.lock().unwrap() = inv.inline_str("content")?.to_string();
                    Ok(ok("ok"))
                }
                _ => {
                    reads.fetch_add(1, Ordering::SeqCst);
                    let current = value.lock().unwrap().clone();
                    // The thread is named after the resource: the kernel cuts it on
                    // a successful Sink to the same target.
                    Ok(ok(current)
                        .cacheable()
                        .depends_on(inv.request.target.as_str()))
                }
            })
            .with_description(
                Description::new("cell")
                    .title("A mutable cell")
                    .action(ActionSpec::new(Verb::Source).output(TEXT))
                    .action(
                        ActionSpec::new(Verb::Sink)
                            .input(ArgSpec::new("content").class(XSD_STRING))
                            .output(TEXT),
                    ),
            )
        };
        let live = {
            let reads = live_reads.clone();
            FnEndpoint::new("live", move |_inv| {
                let n = reads.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(ok(format!("tick {n}")))
            })
            .with_description(
                Description::new("live")
                    .title("A live counter")
                    .verb(Verb::Source)
                    .output(TEXT),
            )
        };
        let upper = FnEndpoint::new("upper", |inv| {
            Ok(ok(inv.inline_str("in")?.to_uppercase()).cacheable())
        })
        .with_description(
            Description::new("upper")
                .title("Uppercase")
                .verb(Verb::Source)
                .input(ArgSpec::new("in").class(XSD_STRING))
                .output(TEXT),
        );
        let gated = {
            let reads = gated_reads.clone();
            FnEndpoint::new("gated", move |_inv| {
                reads.fetch_add(1, Ordering::SeqCst);
                Ok(ok("secret"))
            })
            .with_description(
                Description::new("gated")
                    .title("A gated read")
                    .verb(Verb::Source)
                    .requires(READ_CAP)
                    .output(TEXT),
            )
        };
        Leaf {
            cell: Arc::new(cell),
            live: Arc::new(live),
            upper: Arc::new(upper),
            gated: Arc::new(gated),
            cell_reads,
            live_reads,
            gated_reads,
        }
    }

    /// A space binding the four endpoints. Call it twice for two mirrors.
    fn space(&self) -> Arc<dyn Space> {
        Arc::new(
            EndpointSpace::new()
                .bind_arc(Exact::new("urn:cell"), Arc::clone(&self.cell))
                .bind_arc(Exact::new("urn:live"), Arc::clone(&self.live))
                .bind_arc(Exact::new("urn:upper"), Arc::clone(&self.upper))
                .bind_arc(Exact::new("urn:gated"), Arc::clone(&self.gated)),
        )
    }

    fn cell_reads(&self) -> u32 {
        self.cell_reads.load(Ordering::SeqCst)
    }

    fn live_reads(&self) -> u32 {
        self.live_reads.load(Ordering::SeqCst)
    }

    fn gated_reads(&self) -> u32 {
        self.gated_reads.load(Ordering::SeqCst)
    }
}

/// One way to wrap a space, by name.
type Compose = fn(Arc<dyn Space>) -> Arc<dyn Space>;

/// A budget no walk here exhausts.
fn generous() -> Rate {
    Rate::new(10_000, Duration::from_secs(3600))
}

/// Every overlay alone, then the README's stack: bound each attempt, ride out
/// blips, give up on a corpse, fail over — under a rate limit and a bulkhead.
/// `retries` is how many times the composition invokes an endpoint that keeps
/// failing transiently — `Retry`'s `attempts`, or one.
fn overlays() -> Vec<(&'static str, Compose, u32)> {
    vec![
        (
            "RateLimit",
            |inner| Arc::new(RateLimit::new(inner).limit("urn:", generous())),
            1,
        ),
        ("Retry", |inner| Arc::new(Retry::new(inner, 3)), 3),
        (
            "CircuitBreaker",
            |inner| Arc::new(CircuitBreaker::new(inner, 5, Duration::from_millis(50))),
            1,
        ),
        ("Failover", |inner| Arc::new(Failover::new(vec![inner])), 1),
        (
            "Timeout",
            |inner| Arc::new(Timeout::new(inner, Duration::from_secs(5))),
            1,
        ),
        (
            "Throttle",
            |inner| Arc::new(Throttle::new(inner).limit("urn:", 4)),
            1,
        ),
        (
            "stack",
            |inner| {
                let primary = CircuitBreaker::new(
                    Retry::new(Timeout::new(inner, Duration::from_secs(5)), 3),
                    5,
                    Duration::from_millis(50),
                );
                let resilient = Failover::new(vec![Arc::new(primary)]);
                Arc::new(
                    Throttle::new(RateLimit::new(resilient).limit("urn:", generous()))
                        .limit("urn:", 4),
                )
            },
            3,
        ),
    ]
}

fn suite() -> Suite {
    Suite::new()
        .cacheable("cell")
        .cacheable("upper")
        .pure("upper")
}

/// Four endpoints, five actions, nothing skipped, nothing opted out, the three
/// declarations recorded.
fn assert_shape(label: &str, report: &Report, actions: usize) {
    assert_eq!(report.endpoints, LEAVES.len(), "{label}: {report}");
    assert_eq!(report.actions, actions, "{label}: {report}");
    assert_eq!(
        report.checks.skipped().count(),
        0,
        "{label}: nothing skipped: {report}"
    );
    assert!(report.declared.opted_out.is_empty(), "{label}: {report}");
    assert_eq!(report.declared.cacheable, ["cell", "upper"], "{label}");
    assert_eq!(report.declared.pure, ["upper"], "{label}");
}

fn request(verb: Verb, iri: &str, args: &[(&str, &str)]) -> Request {
    let mut request = Request::new(verb, Iri::parse(iri).unwrap());
    for (name, value) in args {
        request = request.with_arg(*name, ArgRef::Inline(value.as_bytes().to_vec()));
    }
    request
}

fn issue(
    kernel: &Kernel,
    verb: Verb,
    iri: &str,
    args: &[(&str, &str)],
    capability: &Capability,
) -> Result<Representation, Error> {
    block_on(kernel.issue(request(verb, iri, args), capability))
}

fn source(kernel: &Kernel, iri: &str, args: &[(&str, &str)]) -> Result<Representation, Error> {
    issue(kernel, Verb::Source, iri, args, &Capability::root())
}

fn none() -> Capability {
    Capability::scoped(Vec::<String>::new())
}

/// The walk, bare and through every overlay: every report clean, every report
/// the same shape. Then `Failover` over two mirrors of the same space, which
/// lists every entry twice.
#[test]
fn conforms() {
    let bare = Kernel::new(Leaf::new().space());
    let report = suite().run_blocking(&bare);
    // Printed even when clean (`--nocapture`): the report is the record.
    eprintln!("bare: {report}");
    assert!(report.is_clean(), "bare: {report}");
    assert_shape("bare", &report, 5);

    for (label, compose, _) in overlays() {
        let kernel = Kernel::new(compose(Leaf::new().space()));
        let report = suite().run_blocking(&kernel);
        eprintln!("{label}: {report}");
        assert!(report.is_clean(), "{label}: {report}");
        assert_shape(label, &report, 5);
    }

    let leaf = Leaf::new();
    let mirrored = Kernel::new(Arc::new(Failover::new(vec![leaf.space(), leaf.space()])));
    let report = suite().run_blocking(&mirrored);
    eprintln!("Failover over mirrors: {report}");
    assert!(report.is_clean(), "mirrors: {report}");
    assert_shape("mirrors", &report, 10);
}

/// The catalog and every description through an overlay are the bare space's,
/// verbatim — not merely conforming.
#[test]
fn every_overlay_forwards_the_wrapped_contract() {
    let bare = Kernel::new(Leaf::new().space());
    let entries = bare.entries().expect("enumerable");
    let described: Vec<Description> = LEAVES
        .iter()
        .map(|(_, iri, _)| bare.describe_pattern(iri).unwrap())
        .collect();
    for ((id, _, verbs), description) in LEAVES.iter().zip(&described) {
        assert_eq!(description.id, *id);
        let declared: Vec<Verb> = description.action_specs().iter().map(|a| a.verb).collect();
        assert_eq!(
            &declared, verbs,
            "{id}: the fixture declares what it answers"
        );
    }
    for (label, compose, _) in overlays() {
        let kernel = Kernel::new(compose(Leaf::new().space()));
        assert_eq!(
            kernel.entries().expect("enumerable"),
            entries,
            "{label}: transparent to enumeration"
        );
        for ((id, iri, _), expected) in LEAVES.iter().zip(&described) {
            assert_eq!(
                kernel.describe_pattern(iri).as_ref(),
                Some(expected),
                "{label}: `{id}` describes itself as the wrapped endpoint does"
            );
        }
    }
}

/// A read through an overlay is exactly as cacheable as the wrapped read: same
/// expiry, same threads, same cache entry, cut by the same Sink. A live read is
/// not made cacheable; a pure read is not made live.
#[test]
fn cacheability_is_inherited_not_manufactured() {
    for (label, compose, _) in overlays() {
        let leaf = Leaf::new();
        let kernel = Kernel::new(compose(leaf.space()));
        let root = Capability::root();

        // Threaded: cached under the wrapped endpoint's thread, cut by its Sink.
        let first = source(&kernel, "urn:cell", &[]).unwrap();
        assert_eq!(first.expiry, Expiry::Never, "{label}: cell is cacheable");
        assert_eq!(
            first.threads().iter().cloned().collect::<Vec<_>>(),
            [Thread::new("urn:cell")],
            "{label}: the thread is the wrapped endpoint's"
        );
        let second = source(&kernel, "urn:cell", &[]).unwrap();
        assert_eq!(second.bytes, b"one");
        assert_eq!(
            leaf.cell_reads(),
            1,
            "{label}: the second read was a cache hit"
        );
        assert!(
            kernel.is_cached(&request(Verb::Source, "urn:cell", &[]), &root),
            "{label}: the entry is the kernel's"
        );
        issue(
            &kernel,
            Verb::Sink,
            "urn:cell",
            &[("content", "two")],
            &root,
        )
        .unwrap();
        assert!(
            !kernel.is_cached(&request(Verb::Source, "urn:cell", &[]), &root),
            "{label}: a Sink through the overlay cut the thread"
        );
        let third = source(&kernel, "urn:cell", &[]).unwrap();
        assert_eq!(third.bytes, b"two", "{label}: recomputed after the cut");
        assert_eq!(leaf.cell_reads(), 2, "{label}");

        // Live: every read reaches the endpoint, nothing is stored.
        let a = source(&kernel, "urn:live", &[]).unwrap();
        let b = source(&kernel, "urn:live", &[]).unwrap();
        assert_eq!(a.expiry, Expiry::Always, "{label}: live stays live");
        assert_ne!(a.bytes, b.bytes, "{label}: two live reads, two answers");
        assert_eq!(leaf.live_reads(), 2, "{label}");
        assert!(
            !kernel.is_cached(&request(Verb::Source, "urn:live", &[]), &root),
            "{label}: a live read is never stored"
        );

        // Pure: cached with no thread, byte-identical.
        let a = source(&kernel, "urn:upper", &[("in", "hi")]).unwrap();
        let b = source(&kernel, "urn:upper", &[("in", "hi")]).unwrap();
        assert_eq!(a.bytes, b"HI");
        assert_eq!(a.bytes, b.bytes);
        assert_eq!(a.expiry, Expiry::Never, "{label}");
        assert!(a.threads().is_empty(), "{label}: pure has no thread");
        assert!(
            kernel.is_cached(&request(Verb::Source, "urn:upper", &[("in", "hi")]), &root),
            "{label}"
        );
    }
}

/// An endpoint that fails with whatever error it is handed, counting attempts.
struct Failing {
    error: Error,
    seen: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl Endpoint for Failing {
    async fn invoke(&self, _inv: &Invocation<'_>) -> Result<Representation, Error> {
        self.seen.fetch_add(1, Ordering::SeqCst);
        Err(self.error.clone())
    }
    fn describe(&self) -> Description {
        Description::new("failing").verb(Verb::Source).output(TEXT)
    }
}

/// Every `Error` variant an endpoint can raise, permanent and transient.
fn every_error() -> Vec<Error> {
    vec![
        Error::MissingArgument("in".into()),
        Error::InvalidArgument {
            name: "in".into(),
            detail: "not a thing".into(),
        },
        Error::Endpoint("boom".into()),
        Error::Denied("not yours".into()),
        Error::NotFound("gone".into()),
        Error::Timeout("slow".into()),
        Error::Unavailable("down".into()),
    ]
}

/// Every variant comes out of every overlay as it went in — same variant, same
/// message. `Retry` (alone, and inside the stack) re-issues a transient failure
/// `attempts` times and returns the last unchanged; everything permanent is
/// returned on the first attempt.
#[test]
fn typed_errors_pass_through_unchanged() {
    for (label, compose, retries) in overlays() {
        for error in every_error() {
            let seen = Arc::new(AtomicU32::new(0));
            let inner: Arc<dyn Space> = Arc::new(EndpointSpace::new().bind_arc(
                Exact::new("urn:failing"),
                Arc::new(Failing {
                    error: error.clone(),
                    seen: seen.clone(),
                }),
            ));
            let kernel = Kernel::new(compose(inner));
            let out = source(&kernel, "urn:failing", &[]).unwrap_err();
            assert_eq!(out, error, "{label}: the error is the wrapped endpoint's");
            let expected = if error.is_transient() { retries } else { 1 };
            assert_eq!(
                seen.load(Ordering::SeqCst),
                expected,
                "{label}: attempts for {error:?}"
            );
            assert_eq!(kernel.cache_len(), 0, "{label}: an error stores nothing");
        }
    }
}

/// An endpoint whose failure is toggleable.
struct Controlled {
    fail: Arc<Mutex<bool>>,
    seen: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl Endpoint for Controlled {
    async fn invoke(&self, _inv: &Invocation<'_>) -> Result<Representation, Error> {
        self.seen.fetch_add(1, Ordering::SeqCst);
        if *self.fail.lock().unwrap() {
            Err(Error::Unavailable("dependency down".into()))
        } else {
            Ok(ok("ok"))
        }
    }
    fn describe(&self) -> Description {
        Description::new("controlled")
            .verb(Verb::Source)
            .output(TEXT)
    }
}

/// Each overlay's own refusal is a typed error of the kind its semantics say —
/// transient where a Retry/Failover above should move on, permanent where it
/// should not — and none of them is ever stored. A representation ALREADY in the
/// cache is served over budget and while the circuit is open, because the kernel
/// looks the cache up after resolution and before invoke.
#[test]
fn refusals_are_typed_and_never_cached() {
    let root = Capability::root();

    // RateLimit at a zero budget: permanent, carries a retry hint, reaches nothing.
    let leaf = Leaf::new();
    let kernel = Kernel::new(Arc::new(
        RateLimit::new(leaf.space()).limit("urn:", Rate::new(0, Duration::from_secs(60))),
    ));
    let err = source(&kernel, "urn:cell", &[]).unwrap_err();
    assert!(matches!(err, Error::Endpoint(_)), "{err:?}");
    assert!(
        !err.is_transient(),
        "a rate-limit refusal is permanent: {err:?}"
    );
    let text = err.to_string();
    assert!(
        text.contains("rate-limited") && text.contains("retry after"),
        "{text}"
    );
    assert_eq!(leaf.cell_reads(), 0, "the wrapped endpoint was not reached");
    assert_eq!(kernel.cache_len(), 0, "the refusal stored nothing");
    assert!(!kernel.is_cached(&request(Verb::Source, "urn:cell", &[]), &root));

    // RateLimit at a budget of one: the first read is cached; the second is over
    // budget at resolution and served from the cache anyway — the wrapped
    // endpoint is not touched, which is what the limit protects. The window was
    // charged for it. A live read over budget is refused.
    let leaf = Leaf::new();
    let kernel = Kernel::new(Arc::new(
        RateLimit::new(leaf.space()).limit("urn:", Rate::new(1, Duration::from_secs(60))),
    ));
    assert_eq!(source(&kernel, "urn:cell", &[]).unwrap().bytes, b"one");
    assert_eq!(
        source(&kernel, "urn:cell", &[]).unwrap().bytes,
        b"one",
        "over budget, a cached representation is still served"
    );
    assert_eq!(leaf.cell_reads(), 1, "the endpoint was read once");
    let err = source(&kernel, "urn:live", &[]).unwrap_err();
    assert!(err.to_string().contains("rate-limited"), "{err}");
    assert_eq!(leaf.live_reads(), 0);

    // CircuitBreaker: trip it, then the open circuit is a transient Unavailable
    // that touches nothing and stores nothing; after the cooldown a probe recovers.
    let fail = Arc::new(Mutex::new(true));
    let seen = Arc::new(AtomicU32::new(0));
    let inner: Arc<dyn Space> = Arc::new(EndpointSpace::new().bind_arc(
        Exact::new("urn:dep"),
        Arc::new(Controlled {
            fail: fail.clone(),
            seen: seen.clone(),
        }),
    ));
    let kernel = Kernel::new(Arc::new(CircuitBreaker::new(
        inner,
        2,
        Duration::from_millis(30),
    )));
    for _ in 0..2 {
        let err = source(&kernel, "urn:dep", &[]).unwrap_err();
        assert_eq!(err, Error::Unavailable("dependency down".into()));
    }
    let err = source(&kernel, "urn:dep", &[]).unwrap_err();
    assert!(matches!(err, Error::Unavailable(_)), "{err:?}");
    assert!(err.is_transient(), "an open circuit is transient: {err:?}");
    assert!(err.to_string().contains("circuit open"), "{err}");
    assert_eq!(
        seen.load(Ordering::SeqCst),
        2,
        "fast-failed: the dependency was not touched"
    );
    assert_eq!(kernel.cache_len(), 0);
    *fail.lock().unwrap() = false;
    std::thread::sleep(Duration::from_millis(40));
    assert!(
        source(&kernel, "urn:dep", &[]).is_ok(),
        "the half-open probe closed it"
    );
    assert_eq!(seen.load(Ordering::SeqCst), 3);

    // Timeout: an elapsed budget is a transient Timeout naming the target.
    struct Slow;
    #[async_trait::async_trait]
    impl Endpoint for Slow {
        async fn invoke(&self, _inv: &Invocation<'_>) -> Result<Representation, Error> {
            futures_timer::Delay::new(Duration::from_millis(100)).await;
            Ok(ok("late").cacheable())
        }
        fn describe(&self) -> Description {
            Description::new("slow").verb(Verb::Source).output(TEXT)
        }
    }
    let inner: Arc<dyn Space> =
        Arc::new(EndpointSpace::new().bind_arc(Exact::new("urn:slow"), Arc::new(Slow)));
    let kernel = Kernel::new(Arc::new(Timeout::new(inner, Duration::from_millis(10))));
    let err = source(&kernel, "urn:slow", &[]).unwrap_err();
    assert!(matches!(err, Error::Timeout(_)), "{err:?}");
    assert!(err.is_transient());
    assert!(err.to_string().contains("urn:slow"), "{err}");
    assert_eq!(
        kernel.cache_len(),
        0,
        "a timed-out cacheable read stored nothing"
    );
}

/// Over budget, `RateLimit` resolves to a substitute endpoint. That endpoint
/// describes itself as the WRAPPED endpoint, so the capability floor the kernel
/// evaluates from the resolved endpoint's description still holds: an ungranted
/// caller is refused with `Denied` (and never learns the prefix is limited); a
/// granted caller gets the rate-limit refusal; the description the catalog shows
/// is unchanged.
#[test]
fn an_over_budget_refusal_keeps_the_wrapped_contract() {
    let leaf = Leaf::new();
    let kernel = Kernel::new(Arc::new(
        RateLimit::new(leaf.space()).limit("urn:", Rate::new(0, Duration::from_secs(60))),
    ));
    let description = kernel.describe_pattern("urn:gated").unwrap();
    assert_eq!(description.action_specs()[0].requires, [READ_CAP]);

    let err = issue(&kernel, Verb::Source, "urn:gated", &[], &none()).unwrap_err();
    assert!(
        matches!(err, Error::Denied(_)),
        "an ungranted caller is denied before the limit is consulted: {err:?}"
    );
    assert!(err.to_string().contains(READ_CAP), "{err}");

    let err = issue(
        &kernel,
        Verb::Source,
        "urn:gated",
        &[],
        &Capability::scoped([READ_CAP.to_string()]),
    )
    .unwrap_err();
    assert!(
        matches!(err, Error::Endpoint(_)) && err.to_string().contains("rate-limited"),
        "a granted caller meets the limit: {err:?}"
    );
    assert_eq!(leaf.gated_reads(), 0);
}

/// The suite over a zero budget: every read is refused, so the four cacheable
/// probes report "did not resolve" (attributed to CACHEABLE — conformance PENDING
/// #33) and nothing else. In particular ENFORCED is clean: the floor precedes the
/// refusal. Before this arc it was not — the substitute endpoint declared no
/// `requires`, and `gated` was refused with the rate-limit message instead of
/// `Denied`.
#[test]
fn over_budget_the_only_findings_are_the_reads_a_refusal_cannot_serve() {
    let kernel = Kernel::new(Arc::new(
        RateLimit::new(Leaf::new().space()).limit("urn:", Rate::new(0, Duration::from_secs(60))),
    ));
    let report = suite().run_blocking(&kernel);
    eprintln!("over budget: {report}");
    assert_eq!(report.of(Check::Enforced).count(), 0, "{report}");
    let mut refused: Vec<&str> = report
        .of(Check::Cacheable)
        .map(|f| f.endpoint.as_str())
        .collect();
    refused.sort_unstable();
    assert_eq!(refused, ["cell", "gated", "live", "upper"], "{report}");
    for finding in report.of(Check::Cacheable) {
        assert!(
            finding.detail.contains("did not resolve") && finding.detail.contains("rate-limited"),
            "{finding}"
        );
    }
    assert_eq!(report.findings.len(), 4, "{report}");
}
