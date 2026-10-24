//! **The state face** (ledger #198): what the governors hold, as resources beside them.
//!
//! The contract a consumer reads (ikigai-diagram's state overlay, ledger #2173) is pinned
//! here byte for byte: the names, the Turtle shape and the text face. Then what makes it
//! safe to read: the capability and the sensitive-existence rule (ledger #14, D28), and
//! what makes it cheap: cacheable, cut on each change, expiring at the next change time
//! alone makes. Last, the overlays stay transparent to structure once observed, the face
//! keeps to its published vocabulary (`src/vocabulary.ttl`, ledger #2175), and the
//! conformance suite holds the face.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::executor::block_on;
use ikigai_conformance::{Fixture, Suite};
use ikigai_core::{
    ArgRef, Capability, Clock, EndpointSpace, Error, Exact, Expiry, Fallback, FnEndpoint, Iri,
    Kernel, ReprType, Representation, Request, Space, Time, Verb,
};
use ikigai_throttle::{
    governor_iri, state_cap, CircuitBreaker, Failover, Rate, RateLimit, StateBoard, Throttle,
    STATE, STATE_CAP_FAMILY,
};
use oxrdf::{NamedOrBlankNode, Term, Triple};
use oxttl::TurtleParser;

/// A clock that moves only when the test moves it, shared by the governors and the kernel
/// (the module docs ask for one clock: a deadline is measured on the governor's and set on
/// the kernel's).
#[derive(Clone)]
struct Manual(Arc<AtomicU64>);
impl Manual {
    fn new() -> Self {
        Manual(Arc::new(AtomicU64::new(1_000_000)))
    }
    fn advance(&self, millis: u64) {
        self.0.fetch_add(millis, Ordering::SeqCst);
    }
}
impl Clock for Manual {
    fn now(&self) -> Time {
        Time::from_millis(self.0.load(Ordering::SeqCst))
    }
}

fn text() -> ReprType {
    ReprType::new("text/plain").with_param("charset", "utf-8")
}

/// A named leaf for one governor: `urn:demo:{name}:ok` answers, `…:down` is unavailable.
fn leaf(name: &str) -> EndpointSpace {
    EndpointSpace::new()
        .bind(
            Exact::new(format!("urn:demo:{name}:ok")),
            FnEndpoint::new("ok", |_| Ok(Representation::new(text(), "ok"))),
        )
        .bind(
            Exact::new(format!("urn:demo:{name}:down")),
            FnEndpoint::new("down", |_| Err(Error::Unavailable("down".into()))),
        )
        .named(Iri::parse(format!("urn:iki:space:demo-{name}")).unwrap())
}

/// A host: four observed governors, each over its own named leaf, and the board mounted
/// beside them.
struct Host {
    kernel: Arc<Kernel>,
    board: StateBoard,
    clock: Manual,
}

fn host(attach: bool) -> Host {
    let clock = Manual::new();
    let board = StateBoard::new();
    let limit = RateLimit::new(leaf("limit"))
        .limit("urn:demo:limit:", Rate::new(3, Duration::from_secs(10)))
        .with_clock(Arc::new(clock.clone()))
        .observed(&board, "limit")
        .unwrap();
    let breaker = CircuitBreaker::new(leaf("breaker"), 2, Duration::from_secs(30))
        .with_clock(Arc::new(clock.clone()))
        .observed(&board, "breaker")
        .unwrap();
    let pool = Throttle::new(leaf("pool"))
        .limit("urn:demo:pool:", 2)
        .observed(&board, "pool")
        .unwrap();
    // The primary is down for `urn:demo:dr:x`, the backup answers it.
    let primary = EndpointSpace::new()
        .bind(
            Exact::new("urn:demo:dr:x"),
            FnEndpoint::new("primary", |_| {
                Err(Error::Unavailable("primary down".into()))
            }),
        )
        .named(Iri::parse("urn:iki:space:demo-primary").unwrap());
    let backup = EndpointSpace::new()
        .bind(
            Exact::new("urn:demo:dr:x"),
            FnEndpoint::new("backup", |_| Ok(Representation::new(text(), "backup"))),
        )
        .named(Iri::parse("urn:iki:space:demo-backup").unwrap());
    let failover = Failover::new(vec![Arc::new(primary), Arc::new(backup)])
        .observed(&board, "dr")
        .unwrap();
    let spaces: Vec<Arc<dyn Space>> = vec![
        Arc::new(board.space()),
        Arc::new(limit),
        Arc::new(breaker),
        Arc::new(pool),
        Arc::new(failover),
    ];
    let kernel =
        Arc::new(Kernel::new(Arc::new(Fallback::new(spaces))).with_clock(Arc::new(clock.clone())));
    if attach {
        board.attach(&kernel);
    }
    Host {
        kernel,
        board,
        clock,
    }
}

fn request(iri: &str, face: Option<&str>) -> Request {
    let request = Request::new(Verb::Source, Iri::parse(iri).unwrap());
    match face {
        Some(face) => request.with_arg("as", ArgRef::Inline(face.as_bytes().to_vec())),
        None => request,
    }
}

impl Host {
    fn read(&self, iri: &str, face: Option<&str>, cap: &Capability) -> Result<String, Error> {
        block_on(self.kernel.issue(request(iri, face), cap))
            .map(|r| String::from_utf8(r.bytes).unwrap())
    }

    fn root(&self, iri: &str) -> String {
        self.read(iri, None, &Capability::root()).unwrap()
    }

    fn turtle(&self, iri: &str) -> String {
        self.read(iri, Some("text/turtle"), &Capability::root())
            .unwrap()
    }

    fn cached(&self, iri: &str) -> bool {
        self.kernel
            .is_cached(&request(iri, None), &Capability::root())
    }

    fn call(&self, iri: &str) -> Result<Representation, Error> {
        block_on(self.kernel.issue(request(iri, None), &Capability::root()))
    }

    /// The traffic the pinned contract shows.
    fn drive(&self) {
        self.call("urn:demo:limit:ok").unwrap();
        self.call("urn:demo:limit:ok").unwrap();
        self.call("urn:demo:breaker:down").unwrap_err();
        self.call("urn:demo:breaker:down").unwrap_err(); // trips: threshold 2
        self.call("urn:demo:pool:ok").unwrap();
        assert_eq!(self.call("urn:demo:dr:x").unwrap().bytes, b"backup");
    }
}

/// ★ THE CONTRACT ledger #2173 consumes: every name, class and property, byte for byte.
#[test]
fn the_board_in_turtle() {
    let host = host(true);
    host.drive();
    assert_eq!(
        host.turtle(STATE),
        "\
@prefix ik: <https://ikigai-rs.dev/ns#> .
@prefix thr: <https://ikigai-rs.dev/ns/throttle#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<urn:throttle:state> a thr:StateBoard ;
    thr:governor <urn:throttle:state:breaker> ;
    thr:governor <urn:throttle:state:dr> ;
    thr:governor <urn:throttle:state:limit> ;
    thr:governor <urn:throttle:state:pool> .

<urn:throttle:state:breaker> a thr:CircuitBreaker ;
    thr:name \"breaker\" ;
    thr:governs <urn:iki:space:demo-breaker> ;
    thr:threshold \"2\"^^xsd:nonNegativeInteger ;
    thr:cooldownMillis \"30000\"^^xsd:nonNegativeInteger ;
    thr:key <urn:throttle:state:breaker:key:urn:demo:breaker:down> .

<urn:throttle:state:breaker:key:urn:demo:breaker:down> a thr:Circuit ;
    thr:of <urn:throttle:state:breaker> ;
    thr:target <urn:demo:breaker:down> ;
    thr:failures \"2\"^^xsd:nonNegativeInteger ;
    thr:state thr:Open .

<urn:throttle:state:dr> a thr:Failover ;
    thr:name \"dr\" ;
    thr:candidate <urn:throttle:state:dr:candidate:0> ;
    thr:candidate <urn:throttle:state:dr:candidate:1> ;
    thr:key <urn:throttle:state:dr:key:urn:demo:dr:x> .

<urn:throttle:state:dr:candidate:0> a thr:Candidate ;
    thr:of <urn:throttle:state:dr> ;
    ik:order \"0\"^^xsd:integer ;
    thr:governs <urn:iki:space:demo-primary> .

<urn:throttle:state:dr:candidate:1> a thr:Candidate ;
    thr:of <urn:throttle:state:dr> ;
    ik:order \"1\"^^xsd:integer ;
    thr:governs <urn:iki:space:demo-backup> .

<urn:throttle:state:dr:key:urn:demo:dr:x> a thr:Route ;
    thr:of <urn:throttle:state:dr> ;
    thr:target <urn:demo:dr:x> ;
    thr:lastCandidate <urn:throttle:state:dr:candidate:1> ;
    thr:state thr:Answered .

<urn:throttle:state:limit> a thr:RateLimit ;
    thr:name \"limit\" ;
    thr:governs <urn:iki:space:demo-limit> ;
    thr:key <urn:throttle:state:limit:key:urn:demo:limit:> .

<urn:throttle:state:limit:key:urn:demo:limit:> a thr:Window ;
    thr:of <urn:throttle:state:limit> ;
    thr:prefix \"urn:demo:limit:\" ;
    thr:max \"3\"^^xsd:nonNegativeInteger ;
    thr:windowMillis \"10000\"^^xsd:nonNegativeInteger ;
    thr:hits \"2\"^^xsd:nonNegativeInteger ;
    thr:state thr:Admitting .

<urn:throttle:state:pool> a thr:Throttle ;
    thr:name \"pool\" ;
    thr:governs <urn:iki:space:demo-pool> ;
    thr:key <urn:throttle:state:pool:key:urn:demo:pool:> .

<urn:throttle:state:pool:key:urn:demo:pool:> a thr:Bulkhead ;
    thr:of <urn:throttle:state:pool> ;
    thr:prefix \"urn:demo:pool:\" ;
    thr:max \"2\"^^xsd:nonNegativeInteger ;
    thr:running \"0\"^^xsd:nonNegativeInteger ;
    thr:parked \"0\"^^xsd:nonNegativeInteger ;
    thr:state thr:Admitting .
"
    );
}

#[test]
fn the_board_in_text() {
    let host = host(true);
    host.drive();
    assert_eq!(
        host.root(STATE),
        "\
throttle state: 4 governors visible
breaker: circuit-breaker, governs urn:iki:space:demo-breaker, trips after 2, cooldown 30s
  circuit `urn:demo:breaker:down`: open, 2 consecutive transient failures
dr: failover over 2 spaces
  candidate 0: governs urn:iki:space:demo-primary
  candidate 1: governs urn:iki:space:demo-backup
  route `urn:demo:dr:x`: last call answered at candidate 1
limit: rate-limit, governs urn:iki:space:demo-limit
  window `urn:demo:limit:`: 2 of 3 per 10s, admitting
pool: throttle, governs urn:iki:space:demo-pool
  bulkhead `urn:demo:pool:`: 0 of 2 running, 0 parked
"
    );
}

/// Every name the board mints answers on its own: a governor with its keys, one key, one
/// candidate.
#[test]
fn every_minted_name_is_addressable() {
    let host = host(true);
    host.drive();
    assert_eq!(
        host.root(&governor_iri("limit")),
        "limit: rate-limit, governs urn:iki:space:demo-limit\n  \
         window `urn:demo:limit:`: 2 of 3 per 10s, admitting\n"
    );
    assert_eq!(
        host.root("urn:throttle:state:breaker:key:urn:demo:breaker:down"),
        "circuit `urn:demo:breaker:down`: open, 2 consecutive transient failures\n"
    );
    assert_eq!(
        host.root("urn:throttle:state:dr:candidate:1"),
        "candidate 1: governs urn:iki:space:demo-backup\n"
    );
    let key = host.turtle("urn:throttle:state:limit:key:urn:demo:limit:");
    assert!(
        key.contains("<urn:throttle:state:limit:key:urn:demo:limit:> a thr:Window ;")
            && !key.contains("thr:RateLimit"),
        "one key, alone: {key}"
    );
}

/// Breaker transitions, one per step, each visible on the next read.
#[test]
fn a_circuit_trips_half_opens_and_closes() {
    let host = host(true);
    let key = "urn:throttle:state:breaker:key:urn:demo:breaker:";
    // Nothing has failed: no key, so not found (and closed is what that means).
    assert!(matches!(
        host.read(&format!("{key}ok"), None, &Capability::root()),
        Err(Error::NotFound(_))
    ));
    host.call("urn:demo:breaker:ok").unwrap();
    assert!(matches!(
        host.read(&format!("{key}ok"), None, &Capability::root()),
        Err(Error::NotFound(_))
    ));

    host.call("urn:demo:breaker:down").unwrap_err();
    assert_eq!(
        host.root(&format!("{key}down")),
        "circuit `urn:demo:breaker:down`: closed, 1 consecutive transient failure\n"
    );
    host.call("urn:demo:breaker:down").unwrap_err();
    assert_eq!(
        host.root(&format!("{key}down")),
        "circuit `urn:demo:breaker:down`: open, 2 consecutive transient failures\n"
    );
    // Time alone half-opens it: no call, no cut, and the read is not served stale.
    host.clock.advance(30_000);
    assert_eq!(
        host.root(&format!("{key}down")),
        "circuit `urn:demo:breaker:down`: half-open, 2 consecutive transient failures\n"
    );
}

// ---- authority, and the sensitive-existence rule --------------------------------------

/// A caller with no grant under the family is refused by the kernel's floor, and the
/// refusal is the same for the board, a governor that exists and one that does not: it
/// says nothing about which governors there are.
#[test]
fn an_ungranted_caller_learns_nothing() {
    let host = host(true);
    host.drive();
    let nobody = Capability::scoped(Vec::<String>::new());
    let refusal = |iri: &str| host.read(iri, None, &nobody).unwrap_err();
    for iri in [
        STATE,
        "urn:throttle:state:limit",
        "urn:throttle:state:limit:key:urn:demo:limit:",
        "urn:throttle:state:nothing-here",
    ] {
        let error = refusal(iri);
        assert!(matches!(error, Error::Denied(_)), "{iri}: {error:?}");
        assert!(
            error.to_string().contains(STATE_CAP_FAMILY),
            "{iri}: {error}"
        );
    }
    // The same refusal word for word, but for the name the caller sent.
    assert_eq!(
        refusal("urn:throttle:state:limit")
            .to_string()
            .replace("urn:throttle:state:limit", "{name}"),
        refusal("urn:throttle:state:nothing-here")
            .to_string()
            .replace("urn:throttle:state:nothing-here", "{name}"),
    );
}

/// A caller holding some grants sees exactly those governors; any other is not found,
/// told exactly as a governor that does not exist is.
#[test]
fn a_governor_the_caller_may_not_read_is_not_found() {
    let host = host(true);
    host.drive();
    let limit_only = Capability::scoped([state_cap("limit")]);
    let board = host.read(STATE, None, &limit_only).unwrap();
    assert_eq!(
        board,
        "throttle state: 1 governor visible\n\
         limit: rate-limit, governs urn:iki:space:demo-limit\n  \
         window `urn:demo:limit:`: 2 of 3 per 10s, admitting\n"
    );
    assert!(!board.contains("breaker"));
    let not_found = |iri: &str| host.read(iri, None, &limit_only).unwrap_err();
    for (hidden, missing) in [
        ("urn:throttle:state:breaker", "urn:throttle:state:ghost"),
        (
            "urn:throttle:state:breaker:key:urn:demo:breaker:down",
            "urn:throttle:state:ghost:key:urn:demo:breaker:down",
        ),
        (
            "urn:throttle:state:breaker:key:urn:demo:breaker:down",
            "urn:throttle:state:limit:key:urn:demo:breaker:down",
        ),
    ] {
        assert_eq!(
            not_found(hidden),
            Error::NotFound(format!("no governor state at `{hidden}`"))
        );
        assert_eq!(
            not_found(missing),
            Error::NotFound(format!("no governor state at `{missing}`"))
        );
    }
    // The board's grant list: what a host hands out.
    assert_eq!(host.board.governors(), ["breaker", "dr", "limit", "pool"]);
}

// ---- invalidation ----------------------------------------------------------------------

/// Cacheable once attached: a second read is a hit, a change cuts it, and a read after
/// the cut shows the change.
#[test]
fn cached_until_a_change_cuts_it() {
    let host = host(true);
    let window = "urn:throttle:state:limit:key:urn:demo:limit:";
    assert!(host.root(window).contains("0 of 3"));
    assert!(host.cached(window), "attached: the read is cached");
    assert!(
        host.cached(window),
        "and stays cached while nothing changes"
    );
    // Traffic on ANOTHER governor cuts only that governor's thread.
    host.call("urn:demo:pool:ok").unwrap();
    assert!(
        host.cached(window),
        "another governor's change cuts nothing here"
    );

    host.call("urn:demo:limit:ok").unwrap();
    assert!(
        !host.cached(window),
        "a charged hit cut the window's answer"
    );
    assert!(host.root(window).contains("1 of 3"));
    assert!(host.cached(window));

    // The board hangs from every governor's thread.
    host.root(STATE);
    assert!(host.cached(STATE));
    host.call("urn:demo:dr:x").unwrap();
    assert!(!host.cached(STATE), "a route's change cut the board");
}

/// A change time alone makes (a hit ageing out of the window) is not a cut: the answer
/// carries a deadline instead, and it is not served past it.
#[test]
fn a_window_ageing_out_expires_the_answer() {
    let host = host(true);
    let window = "urn:throttle:state:limit:key:urn:demo:limit:";
    host.call("urn:demo:limit:ok").unwrap();
    let read = block_on(
        host.kernel
            .issue(request(window, None), &Capability::root()),
    )
    .unwrap();
    assert_eq!(
        read.expiry,
        Expiry::At(host.clock.now().plus_millis(10_000))
    );
    assert!(host.cached(window));
    host.clock.advance(10_000);
    assert!(host.root(window).contains("0 of 3"), "aged out, not stale");
}

/// No kernel attached, nothing can cut: every answer is live.
#[test]
fn without_a_kernel_every_answer_is_live() {
    let host = host(false);
    host.drive();
    for iri in [STATE, "urn:throttle:state:limit"] {
        let read = block_on(host.kernel.issue(request(iri, None), &Capability::root())).unwrap();
        assert_eq!(read.expiry, Expiry::Always, "{iri}");
        assert!(!host.cached(iri), "{iri}");
    }
}

/// Work parking on a bulkhead shows as parked, then as running, then as done.
#[test]
fn a_bulkhead_shows_running_and_parked_work() {
    let gate = Arc::new(async_lock::Semaphore::new(0));
    let board = StateBoard::new();
    let held = {
        let gate = Arc::clone(&gate);
        Arc::new(Held(gate))
    };
    let pool = Throttle::new(EndpointSpace::new().bind_arc(Exact::new("urn:slow"), held))
        .limit("urn:slow", 1)
        .observed(&board, "pool")
        .unwrap();
    let spaces: Vec<Arc<dyn Space>> = vec![Arc::new(board.space()), Arc::new(pool)];
    let kernel = Arc::new(Kernel::new(Arc::new(Fallback::new(spaces))));
    board.attach(&kernel);
    let root = Capability::root();
    let key = "urn:throttle:state:pool:key:urn:slow";
    let read = || async {
        let answer = kernel.issue(request(key, None), &root).await.unwrap();
        String::from_utf8(answer.bytes).unwrap()
    };
    let (first, second, seen) = block_on(async {
        let first = kernel.issue(request("urn:slow", None), &root);
        let second = kernel.issue(request("urn:slow", None), &root);
        let watch = async {
            // Both calls have been polled once: one holds the slot, one parked.
            let seen = read().await;
            gate.add_permits(2);
            seen
        };
        futures::join!(first, second, watch)
    });
    assert!(first.is_ok() && second.is_ok());
    assert_eq!(seen, "bulkhead `urn:slow`: 1 of 1 running, 1 parked\n");
    assert_eq!(
        block_on(read()),
        "bulkhead `urn:slow`: 0 of 1 running, 0 parked\n"
    );
}

/// An endpoint that waits for the test's gate.
struct Held(Arc<async_lock::Semaphore>);
#[async_trait::async_trait]
impl ikigai_core::Endpoint for Held {
    async fn invoke(&self, _inv: &ikigai_core::Invocation<'_>) -> Result<Representation, Error> {
        let _go = self.0.acquire().await;
        Ok(Representation::new(text(), "done"))
    }
}

// ---- registration ----------------------------------------------------------------------

#[test]
fn a_governor_name_is_checked() {
    let board = StateBoard::new();
    let bad = |name: &str| RateLimit::new(leaf("x")).observed(&board, name).err();
    assert!(bad("Caps").is_some());
    assert!(bad("has:colon").is_some());
    assert!(bad("").is_some());
    assert!(bad("-lead").is_some());
    assert!(bad("ok-1").is_none());
    let twice = bad("ok-1").expect("a second governor under one name is refused");
    assert!(
        twice.to_string().contains("already observed as `ok-1`"),
        "{twice}"
    );
    let once = RateLimit::new(leaf("y")).observed(&board, "first").unwrap();
    let again = once.observed(&board, "second").err().unwrap();
    assert!(
        again.to_string().contains("already observed as `first`"),
        "{again}"
    );
}

/// A builder that runs after `observed` is seen by the face: the view is refreshed.
#[test]
fn a_limit_added_after_observing_is_shown() {
    let board = StateBoard::new();
    let limit = RateLimit::new(leaf("late"))
        .observed(&board, "late")
        .unwrap()
        .limit("urn:demo:late:", Rate::new(1, Duration::from_secs(1)));
    let spaces: Vec<Arc<dyn Space>> = vec![Arc::new(board.space()), Arc::new(limit)];
    let kernel = Kernel::new(Arc::new(Fallback::new(spaces)));
    let read = block_on(kernel.issue(
        request("urn:throttle:state:late", None),
        &Capability::root(),
    ))
    .unwrap();
    assert!(String::from_utf8(read.bytes)
        .unwrap()
        .contains("window `urn:demo:late:`: 0 of 1 per 1s"));
}

// ---- the overlays stay transparent -----------------------------------------------------

/// Observing a governor changes nothing about the structure it reports.
#[test]
fn an_observed_overlay_still_reports_what_it_encloses() {
    let board = StateBoard::new();
    let bare = leaf("t");
    let observed = CircuitBreaker::new(leaf("t"), 1, Duration::from_secs(1))
        .observed(&board, "t")
        .unwrap();
    assert_eq!(observed.id(), bare.id());
    assert_eq!(observed.topology(), bare.topology());
    let patterns = |s: &dyn Space| -> Vec<String> {
        s.entries()
            .unwrap()
            .into_iter()
            .map(|e| e.pattern)
            .collect()
    };
    assert_eq!(patterns(&observed), patterns(&bare));
}

// ---- the vocabulary: every term the face writes is defined, as it is written -----------
//
// `src/vocabulary.ttl` (`VOCABULARY_TTL`) is the document published at /ns/throttle
// (ledger #2175). These hold it to the face: the face writes no `thr:` term the document
// does not define, every literal it writes has its property's range as its datatype, and
// every subject it writes a property on is typed into that property's domain.

/// The ontology IRI: the document as https://ikigai-rs.dev/ns/throttle serves it.
const ONTOLOGY: &str = "https://ikigai-rs.dev/ns/throttle";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDF_PROPERTY: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property";
const RDFS_CLASS: &str = "http://www.w3.org/2000/01/rdf-schema#Class";
const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
const RDFS_LABEL: &str = "http://www.w3.org/2000/01/rdf-schema#label";
const RDFS_COMMENT: &str = "http://www.w3.org/2000/01/rdf-schema#comment";
const OWL_ONTOLOGY: &str = "http://www.w3.org/2002/07/owl#Ontology";
const OWL_VERSION_INFO: &str = "http://www.w3.org/2002/07/owl#versionInfo";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
/// The one core term the face writes, and the range core's vocabulary gives it.
const IK_ORDER: &str = "https://ikigai-rs.dev/ns#order";
const IK_ORDER_RANGE: &str = "http://www.w3.org/2001/XMLSchema#integer";
/// Defined for a reader and a reasoner, never written by the face: each is the
/// superclass of what the face does write.
const NEVER_WRITTEN: [&str; 3] = ["Governor", "Key", "State"];

fn parse(turtle: &str) -> Vec<Triple> {
    TurtleParser::new()
        .for_slice(turtle.as_bytes())
        .map(|triple| triple.expect("the Turtle parses"))
        .collect()
}

fn subject_iri(triple: &Triple) -> &str {
    match &triple.subject {
        NamedOrBlankNode::NamedNode(node) => node.as_str(),
        other => panic!("no blank nodes, and {other} is one"),
    }
}

fn object_iri(term: &Term) -> Option<&str> {
    match term {
        Term::NamedNode(node) => Some(node.as_str()),
        _ => None,
    }
}

fn thr(local: &str) -> String {
    format!("{}{local}", ikigai_throttle::STATE_NS)
}

/// The vocabulary, read as RDF.
struct Vocabulary(Vec<Triple>);

impl Vocabulary {
    fn load() -> Self {
        Vocabulary(parse(ikigai_throttle::VOCABULARY_TTL))
    }

    fn objects<'a>(&'a self, subject: &'a str, predicate: &'a str) -> Vec<&'a Term> {
        self.0
            .iter()
            .filter(move |t| subject_iri(t) == subject && t.predicate.as_str() == predicate)
            .map(|t| &t.object)
            .collect()
    }

    /// The one IRI `subject` has for `predicate`, if it has one.
    fn iri(&self, subject: &str, predicate: &str) -> Option<String> {
        let objects = self.objects(subject, predicate);
        assert!(objects.len() <= 1, "{subject} has one {predicate} at most");
        objects
            .first()
            .map(|o| object_iri(o).expect("an IRI").to_string())
    }

    fn is_a(&self, subject: &str, class: &str) -> bool {
        self.objects(subject, RDF_TYPE)
            .iter()
            .any(|o| object_iri(o) == Some(class))
    }

    /// A property's one range, which every property states.
    fn range(&self, property: &str) -> String {
        self.iri(property, RDFS_RANGE)
            .unwrap_or_else(|| panic!("{property} states its range"))
    }

    /// `class` is `ancestor` or under it along `rdfs:subClassOf`.
    fn subclass_of(&self, class: &str, ancestor: &str) -> bool {
        class == ancestor
            || self
                .objects(class, RDFS_SUBCLASS_OF)
                .iter()
                .filter_map(|o| object_iri(o))
                .any(|parent| self.subclass_of(parent, ancestor))
    }

    /// Every `thr:` term the document defines.
    fn terms(&self) -> std::collections::BTreeSet<String> {
        self.0
            .iter()
            .map(subject_iri)
            .filter(|s| s.starts_with(ikigai_throttle::STATE_NS))
            .map(str::to_string)
            .collect()
    }
}

/// The deployed /ns/throttle is a copy of `VOCABULARY_TTL`, and `owl:versionInfo` is how
/// one deploy is told apart from the last: devtools' `ns-drift` prints it for the live
/// and the repo side, and the release preflight moves it. It is the crate version, so
/// the release bump must move it. Read twice, because two kinds of reader depend on it:
/// as RDF (exactly one plain-string value on the ontology), and as the one text line the
/// drift scan's regex finds.
#[test]
fn ontology_version_is_the_crate_version() {
    let vocabulary = Vocabulary::load();
    assert!(
        vocabulary.is_a(ONTOLOGY, OWL_ONTOLOGY),
        "{ONTOLOGY} is declared an owl:Ontology"
    );
    let versions = vocabulary.objects(ONTOLOGY, OWL_VERSION_INFO);
    let [Term::Literal(version)] = versions.as_slice() else {
        panic!("the ontology declares exactly one owl:versionInfo literal: {versions:?}");
    };
    assert_eq!(
        version.datatype().as_str(),
        XSD_STRING,
        "a plain string, as core's is"
    );
    assert_eq!(
        version.value(),
        env!("CARGO_PKG_VERSION"),
        "vocabulary.ttl's owl:versionInfo must be bumped with the crate version"
    );

    let lines: Vec<&str> = ikigai_throttle::VOCABULARY_TTL
        .lines()
        .filter(|l| l.trim_start().starts_with("owl:versionInfo"))
        .collect();
    assert_eq!(
        lines,
        [format!(
            "    owl:versionInfo \"{}\" .",
            env!("CARGO_PKG_VERSION")
        )],
        "exactly one owl:versionInfo line, in the form the drift scan and the preflight read"
    );
}

/// Every term is a class, a property or a state, with one label and one comment; every
/// property has one range and at most one domain, each a class or a datatype.
#[test]
fn every_term_is_described() {
    let vocabulary = Vocabulary::load();
    let terms = vocabulary.terms();
    assert!(terms.len() > 30, "the whole vocabulary parsed: {terms:?}");
    for term in std::iter::once(ONTOLOGY.to_string()).chain(terms.iter().cloned()) {
        assert_eq!(
            vocabulary.objects(&term, RDFS_LABEL).len(),
            1,
            "{term}: one label"
        );
        assert_eq!(
            vocabulary.objects(&term, RDFS_COMMENT).len(),
            1,
            "{term}: one comment"
        );
    }
    for term in &terms {
        let kinds = [
            vocabulary.is_a(term, RDFS_CLASS),
            vocabulary.is_a(term, RDF_PROPERTY),
            vocabulary.is_a(term, &thr("State")),
        ];
        assert_eq!(
            kinds.iter().filter(|k| **k).count(),
            1,
            "{term} is exactly one of a class, a property or a state"
        );
        if vocabulary.is_a(term, RDF_PROPERTY) {
            let range = vocabulary.range(term);
            assert!(
                range.starts_with(XSD)
                    || vocabulary.is_a(&range, RDFS_CLASS)
                    || range.starts_with("https://ikigai-rs.dev/ns#")
                    || range == "http://www.w3.org/2000/01/rdf-schema#Resource",
                "{term}'s range {range} is a datatype or a class"
            );
            if let Some(domain) = vocabulary.iri(term, RDFS_DOMAIN) {
                assert!(
                    vocabulary.is_a(&domain, RDFS_CLASS),
                    "{term}'s domain {domain} is a thr: class"
                );
            }
        }
        if let Some(parent) = vocabulary.iri(term, RDFS_SUBCLASS_OF) {
            assert!(
                vocabulary.is_a(&parent, RDFS_CLASS),
                "{term} is under a thr: class"
            );
        }
    }
}

/// Every `thr:` term the face's source can write, read from the source itself, so a
/// branch the drive below does not take (a closed circuit, a failed route) is held too.
fn terms_in_source() -> std::collections::BTreeSet<String> {
    let source = include_str!("../src/state.rs");
    source
        .match_indices("thr:")
        .map(|(at, _)| {
            source[at + 4..]
                .chars()
                .take_while(char::is_ascii_alphanumeric)
                .collect::<String>()
        })
        .filter(|local| !local.is_empty())
        .map(|local| thr(&local))
        .collect()
}

/// ★ The face against its vocabulary (ledger #2175): what the source can write is defined
/// (and nothing is defined that it cannot write, but the three superclasses), and the
/// graph it writes, read from the pinned board in two states, keeps every literal in its
/// property's range and every subject in its property's domain.
#[test]
fn every_term_the_face_writes_is_defined_as_written() {
    let vocabulary = Vocabulary::load();
    let defined = vocabulary.terms();
    let written = terms_in_source();
    let undefined: Vec<&String> = written.difference(&defined).collect();
    assert!(
        undefined.is_empty(),
        "written, never defined: {undefined:?}"
    );
    let unwritten: Vec<&String> = defined
        .difference(&written)
        .filter(|t| !NEVER_WRITTEN.iter().any(|n| **t == thr(n)))
        .collect();
    assert!(
        unwritten.is_empty(),
        "defined, never written: {unwritten:?}"
    );

    // The pinned board, then later: the breaker half-open, the window full.
    let host = host(true);
    host.drive();
    let mut boards = vec![host.turtle(STATE)];
    host.clock.advance(30_000);
    host.call("urn:demo:limit:ok").unwrap();
    host.call("urn:demo:limit:ok").unwrap();
    host.call("urn:demo:limit:ok").unwrap();
    boards.push(host.turtle(STATE));
    assert!(boards[1].contains("thr:HalfOpen") && boards[1].contains("thr:Full"));

    let mut seen = std::collections::BTreeSet::new();
    for board in &boards {
        let graph = parse(board);
        let types = |subject: &str| -> Vec<String> {
            graph
                .iter()
                .filter(|t| subject_iri(t) == subject && t.predicate.as_str() == RDF_TYPE)
                .filter_map(|t| object_iri(&t.object).map(str::to_string))
                .collect()
        };
        for triple in &graph {
            let subject = subject_iri(triple);
            let predicate = triple.predicate.as_str();
            seen.insert(predicate.to_string());
            if predicate == RDF_TYPE {
                let class = object_iri(&triple.object).expect("a class IRI");
                assert!(
                    vocabulary.is_a(class, RDFS_CLASS),
                    "{subject} is typed {class}, a defined class"
                );
                continue;
            }
            if predicate == IK_ORDER {
                let Term::Literal(order) = &triple.object else {
                    panic!("ik:order is a literal");
                };
                assert_eq!(order.datatype().as_str(), IK_ORDER_RANGE, "core's range");
                continue;
            }
            assert!(
                vocabulary.is_a(predicate, RDF_PROPERTY),
                "{predicate} is a defined property"
            );
            let range = vocabulary.range(predicate);
            match &triple.object {
                Term::Literal(literal) => assert_eq!(
                    literal.datatype().as_str(),
                    range,
                    "{subject} {predicate} {literal}: the literal's datatype is the range"
                ),
                Term::NamedNode(object) => {
                    let object = object.as_str();
                    assert!(
                        !range.starts_with(XSD),
                        "{predicate}'s range is the datatype {range}, and it is written an IRI"
                    );
                    if vocabulary.is_a(&range, RDFS_CLASS) {
                        assert!(
                            vocabulary.is_a(object, &range)
                                || types(object)
                                    .iter()
                                    .any(|class| vocabulary.subclass_of(class, &range)),
                            "{subject} {predicate} <{object}>: the object is in the range {range}"
                        );
                    }
                }
                other => panic!("{subject} {predicate} {other}: no blank nodes"),
            }
            if let Some(domain) = vocabulary.iri(predicate, RDFS_DOMAIN) {
                assert!(
                    types(subject)
                        .iter()
                        .any(|class| vocabulary.subclass_of(class, &domain)),
                    "{subject} carries {predicate}, so it is in the domain {domain}"
                );
            }
        }
    }
    // Every property the vocabulary defines was written and checked above.
    let properties: Vec<String> = defined
        .iter()
        .filter(|t| vocabulary.is_a(t, RDF_PROPERTY))
        .cloned()
        .collect();
    let unchecked: Vec<&String> = properties.iter().filter(|p| !seen.contains(*p)).collect();
    assert!(
        unchecked.is_empty(),
        "never written by the drive: {unchecked:?}"
    );
}

// ---- the conformance suite holds the face ----------------------------------------------

/// The module recipe, walked over the board: contracts, ArgSpecs, the declared and
/// enforced capability, a Turtle face that parses with no blank nodes and every term in
/// the shared vocabulary or the registered `thr:` namespace, cacheable once attached. The
/// prefix in the fixture is hostile on purpose: the face must still parse.
#[test]
fn conforms() {
    let board = StateBoard::new();
    let limit = RateLimit::new(EndpointSpace::new())
        .limit(
            "urn:x:\"evil> ; a <urn:y> .",
            Rate::new(1, Duration::from_secs(1)),
        )
        .observed(&board, "limit")
        .unwrap();
    let kernel = Arc::new(Kernel::new(Arc::new(Fallback::new(vec![
        Arc::new(board.space()) as Arc<dyn Space>,
        Arc::new(limit),
    ]))));
    board.attach(&kernel);
    let report = Suite::new()
        .namespace(ikigai_throttle::STATE_NS)
        .cacheable("throttle-state")
        .cacheable("throttle-state-member")
        .fixture(Fixture::new("throttle-state-member", Verb::Source).binding("member", "limit"))
        .host_named_space("ikigai_throttle::StateBoard::space", board.space())
        .run_blocking(&kernel);
    eprintln!("state face: {report}");
    report.assert_clean();
}

// ---- the measurement behind the invalidation choice ------------------------------------

/// Microseconds per call of `f`, best of three runs of `n`.
fn per_call(n: u32, mut f: impl FnMut()) -> f64 {
    (0..3)
        .map(|_| {
            let start = std::time::Instant::now();
            for _ in 0..n {
                f();
            }
            start.elapsed().as_secs_f64() * 1e6 / f64::from(n)
        })
        .fold(f64::INFINITY, f64::min)
}

/// The read cost both ways, and what observing costs the governed path. Not a gate (a
/// timing depends on the machine): run it with
/// `cargo test --release --test state measure -- --ignored --nocapture`.
#[test]
#[ignore = "a measurement, not a check"]
fn measure() {
    let n = 20_000;
    let live = host(false);
    live.drive();
    let cached = host(true);
    cached.drive();
    let board = |h: &Host| {
        h.read(STATE, Some("text/turtle"), &Capability::root())
            .unwrap();
    };
    let key = "urn:throttle:state:limit:key:urn:demo:limit:";
    eprintln!(
        "board read, live (no kernel attached): {:.2} us",
        per_call(n, || board(&live))
    );
    eprintln!(
        "board read, attached, unchanged (cache hit): {:.2} us",
        per_call(n, || board(&cached))
    );
    eprintln!(
        "board read, attached, a change before every read: {:.2} us (read + the call that changed it)",
        per_call(n, || {
            cached.call("urn:demo:pool:ok").unwrap();
            board(&cached);
        })
    );
    eprintln!(
        "  of which the call alone, nobody reading: {:.2} us",
        per_call(n, || {
            cached.call("urn:demo:pool:ok").unwrap();
        })
    );
    eprintln!(
        "key read, live: {:.2} us",
        per_call(n, || {
            live.root(key);
        })
    );
    eprintln!(
        "key read, attached, unchanged: {:.2} us",
        per_call(n, || {
            cached.root(key);
        })
    );

    // What observing costs the governed path. A fresh leaf per row; RateLimit's window is
    // generous so every call is charged and admitted.
    let governed = |observed: bool, attach: bool, read_between: bool| {
        let board = StateBoard::new();
        let mut limit = RateLimit::new(leaf("hot"))
            .limit("urn:demo:hot:", Rate::new(u32::MAX, Duration::from_secs(1)));
        if observed {
            limit = limit.observed(&board, "hot").unwrap();
        }
        let spaces: Vec<Arc<dyn Space>> = vec![Arc::new(board.space()), Arc::new(limit)];
        let kernel = Arc::new(Kernel::new(Arc::new(Fallback::new(spaces))));
        if attach {
            board.attach(&kernel);
        }
        let root = Capability::root();
        per_call(n, || {
            block_on(kernel.issue(request("urn:demo:hot:ok", None), &root)).unwrap();
            if read_between {
                block_on(kernel.issue(request("urn:throttle:state:hot", None), &root)).unwrap();
            }
        })
    };
    eprintln!(
        "governed call, not observed: {:.2} us",
        governed(false, false, false)
    );
    eprintln!(
        "governed call, observed, attached, never read: {:.2} us",
        governed(true, true, false)
    );
    eprintln!(
        "governed call + governor read, observed, live: {:.2} us",
        governed(true, false, true)
    );
    eprintln!(
        "governed call + governor read, observed, attached (a cut per call): {:.2} us",
        governed(true, true, true)
    );
}
