//! **An overlay that encloses one space reports that space** (ledger #978).
//!
//! `Space::topology` is how `urn:kernel:topology`, explain, the diagram and a
//! declared arrangement's harvest see inside a space, and `Space::id` is the name a
//! space claims. Core's default for both is "I say nothing": an opaque, anonymous
//! node. A governor that kept the default made whatever it wrapped invisible: a
//! named space with its doors laid out read back as `ik:OpaqueSpace` the moment a
//! `Timeout` went around it, and a host that needed those doors nameable had to
//! wrap its WHOLE arranged root instead of the one binding it meant to bound.
//!
//! So every single-space overlay forwards all three faces of what it encloses:
//! `topology`, `id` and `entries`. Six near-identical tests, one per overlay, so a
//! failure names the overlay that broke, and the seventh overlay has a template.
//!
//! **Forwarding `id` is a naming claim, and it is true here.** A space's name says
//! "any space named this holds the same doors" (ledger #987), and the cache
//! partitions on it. A governor holds exactly the doors it wraps (the same patterns,
//! answered by the same endpoints, decorated), and what the decoration changes is
//! only ever a refusal (rate-limited, timed out, circuit open), which the kernel
//! never caches. A successful answer through the governor is the answer the bare
//! space gives, so sharing a cache partition with it is sound.
//!
//! `Failover` encloses several spaces, not one, and stays opaque: see the last test.

use std::sync::Arc;
use std::time::Duration;

use ikigai_core::{
    builtins, EndpointSpace, Exact, Iri, Space, SpaceEntry, SpaceKind, Topology, UriTemplate,
};
use ikigai_throttle::{CircuitBreaker, Failover, Rate, RateLimit, Retry, Throttle, Timeout};

fn iri(s: &str) -> Iri {
    Iri::parse(s).unwrap()
}

/// A named leaf with two doors: the shape whose structure a governor must not hide.
fn named() -> EndpointSpace {
    EndpointSpace::new()
        .bind(Exact::new("urn:demo:upper"), builtins::to_upper())
        .bind(
            UriTemplate::parse("urn:demo:echo/{word}").unwrap(),
            builtins::to_upper(),
        )
        .named(iri("urn:iki:space:demo"))
}

fn patterns(entries: Option<Vec<SpaceEntry>>) -> Vec<String> {
    entries
        .expect("the wrapped leaf is enumerable")
        .into_iter()
        .map(|entry| entry.pattern)
        .collect()
}

/// The three faces an enclosing overlay forwards, held to the bare space's.
fn reports_what_it_encloses(name: &str, overlay: &dyn Space) {
    let bare = named();
    let topology: Topology = overlay.topology();
    assert_ne!(
        topology.kind,
        SpaceKind::Opaque,
        "{name}: a governor must not make the space it wraps opaque"
    );
    assert_eq!(
        topology,
        bare.topology(),
        "{name}: the topology is the enclosed space's, doors and name included"
    );
    assert_eq!(
        overlay.id(),
        bare.id(),
        "{name}: the id is the enclosed space's claim, forwarded"
    );
    assert_eq!(
        patterns(overlay.entries()),
        patterns(bare.entries()),
        "{name}: the entries are the enclosed space's"
    );
}

#[test]
fn rate_limit_reports_what_it_encloses() {
    let overlay = RateLimit::new(named()).limit("urn:demo:", Rate::new(1, Duration::from_secs(60)));
    reports_what_it_encloses("RateLimit", &overlay);
}

#[test]
fn retry_reports_what_it_encloses() {
    reports_what_it_encloses("Retry", &Retry::new(named(), 3));
}

#[test]
fn circuit_breaker_reports_what_it_encloses() {
    let overlay = CircuitBreaker::new(named(), 3, Duration::from_secs(1));
    reports_what_it_encloses("CircuitBreaker", &overlay);
}

#[test]
fn timeout_reports_what_it_encloses() {
    reports_what_it_encloses("Timeout", &Timeout::new(named(), Duration::from_secs(1)));
}

#[test]
fn throttle_reports_what_it_encloses() {
    reports_what_it_encloses("Throttle", &Throttle::new(named()).limit("urn:demo:", 2));
}

/// Governors stack, and a stack is still transparent: the forwarding composes.
#[test]
fn a_stack_of_governors_reports_the_innermost_space() {
    let stack = Retry::new(
        CircuitBreaker::new(
            Timeout::new(named(), Duration::from_secs(1)),
            3,
            Duration::from_secs(1),
        ),
        2,
    );
    reports_what_it_encloses("Retry(CircuitBreaker(Timeout))", &stack);
}

/// A governor around an anonymous space claims nothing: forwarding never invents a
/// name, so a host naming a corridor over it never meets a disagreeing claim.
#[test]
fn a_governor_over_an_anonymous_space_claims_no_name() {
    let anonymous = EndpointSpace::new().bind(Exact::new("urn:demo:upper"), builtins::to_upper());
    assert_eq!(Timeout::new(anonymous, Duration::from_secs(1)).id(), None);
}

/// `Failover` encloses SEVERAL spaces and defers which one answers to invoke time.
/// No core `SpaceKind` states that: `Fallback` is first hit at RESOLUTION, so a
/// failover rendered as one would read back (a declaration, a harvest) as an
/// arrangement that never reaches the backup. Opaque, the default, is the honest
/// node; its entries are still every target's, as they always were.
#[test]
fn failover_stays_opaque_and_unnamed_but_enumerates_every_target() {
    let backup = EndpointSpace::new().bind(Exact::new("urn:demo:backup"), builtins::to_upper());
    let failover = Failover::new(vec![
        Arc::new(named()) as Arc<dyn Space>,
        Arc::new(backup) as Arc<dyn Space>,
    ]);
    assert_eq!(failover.topology().kind, SpaceKind::Opaque);
    assert_eq!(failover.id(), None);
    assert_eq!(
        patterns(failover.entries()),
        vec!["urn:demo:upper", "urn:demo:echo/{word}", "urn:demo:backup"]
    );
}
