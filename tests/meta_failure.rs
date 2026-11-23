//! A `Meta` the wrapped endpoint cannot answer must stay an error through every overlay
//! (ikigai-core 0.4.2, `Endpoint::meta_failure`).
//!
//! An endpoint that knows it cannot state its contract (a mount whose peer is down) answers
//! `meta_failure() = Some(error)`, and the kernel answers a `Meta` of it with that error
//! instead of projecting its placeholder `describe()`. The kernel asks the RESOLVED endpoint,
//! which here is an overlay's, so an overlay that does not forward `meta_failure` hides the
//! failure again: its `Meta` answers the placeholder as a success.
//!
//! What Retry does with it: the kernel asks `meta_failure` synchronously, before the
//! endpoint is invoked and before the cache, and a `Meta` never invokes the endpoint, so
//! there is no call for Retry to re-issue and nothing it may wait on (no timer, no run, no
//! retry pool). Retry forwards the failure unchanged, at once, and spends nothing. The
//! failure is never cached, so the next `Meta` asks again and succeeds once the endpoint
//! can state its contract.
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::executor::block_on;
use ikigai_core::{
    ArgSpec, Capability, Description, Endpoint, EndpointSpace, Error, Exact, FixedClock,
    Invocation, Iri, Kernel, MetaRenderer, ReprType, Representation, Request, Result, Space, Verb,
};
use ikigai_throttle::{CircuitBreaker, Failover, Rate, RateLimit, Retry, Throttle, Timeout, Timer};

const REMOTE: &str = "urn:test:remote";

/// A mounted name whose peer may be down. While it is, its `Meta` answers `Unavailable`
/// through `meta_failure`; once it is up, it describes its real contract.
struct Remote {
    down: Arc<AtomicBool>,
    invoked: Arc<AtomicU32>,
}

#[async_trait]
impl Endpoint for Remote {
    async fn invoke(&self, _inv: &Invocation<'_>) -> Result<Representation> {
        self.invoked.fetch_add(1, Ordering::SeqCst);
        Ok(Representation::new(
            ReprType::new("text/plain"),
            b"up".to_vec(),
        ))
    }

    fn describe(&self) -> Description {
        if self.down.load(Ordering::SeqCst) {
            Description::new("remote")
        } else {
            Description::new("remote")
                .verb(Verb::Source)
                .input(ArgSpec::new("q"))
                .output("text/plain")
        }
    }

    fn meta_failure(&self) -> Option<Error> {
        self.down
            .load(Ordering::SeqCst)
            .then(|| Error::Unavailable("the peer is down, so its contract is unknown".into()))
    }
}

struct Peer {
    down: Arc<AtomicBool>,
    invoked: Arc<AtomicU32>,
}

impl Peer {
    fn new() -> Self {
        Peer {
            down: Arc::new(AtomicBool::new(true)),
            invoked: Arc::new(AtomicU32::new(0)),
        }
    }

    fn space(&self) -> EndpointSpace {
        EndpointSpace::new().bind(
            Exact::new(REMOTE),
            Remote {
                down: self.down.clone(),
                invoked: self.invoked.clone(),
            },
        )
    }

    fn recover(&self) {
        self.down.store(false, Ordering::SeqCst);
    }
}

/// A timer that records every wait it is asked for and returns at once.
#[derive(Default)]
struct Recording(AtomicU32);

#[async_trait]
impl Timer for Recording {
    async fn sleep(&self, _delay: Duration) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn clock() -> Arc<FixedClock> {
    Arc::new(FixedClock::at(1_000))
}

fn overlays(peer: &Peer) -> Vec<(&'static str, Arc<dyn Space>)> {
    vec![
        (
            "RateLimit",
            Arc::new(
                RateLimit::new(peer.space())
                    .limit("urn:", Rate::new(100, Duration::from_secs(1)))
                    .with_clock(clock()),
            ),
        ),
        ("Retry", Arc::new(Retry::new(peer.space(), 3))),
        (
            "CircuitBreaker",
            Arc::new(
                CircuitBreaker::new(peer.space(), 3, Duration::from_secs(1)).with_clock(clock()),
            ),
        ),
        (
            "Failover",
            Arc::new(Failover::new(vec![Arc::new(peer.space())])),
        ),
        (
            "Throttle",
            Arc::new(Throttle::new(peer.space()).limit("urn:", 2)),
        ),
        (
            "Timeout",
            Arc::new(Timeout::new(peer.space(), Duration::from_secs(30))),
        ),
        (
            "stack",
            Arc::new(Timeout::new(
                Throttle::new(Retry::new(
                    CircuitBreaker::new(
                        RateLimit::new(Failover::new(vec![Arc::new(peer.space())]))
                            .limit("urn:", Rate::new(100, Duration::from_secs(1)))
                            .with_clock(clock()),
                        3,
                        Duration::from_secs(1),
                    )
                    .with_clock(clock()),
                    3,
                ))
                .limit("urn:", 2),
                Duration::from_secs(30),
            )),
        ),
    ]
}

/// One plain line: the endpoint's id and how many actions its contract declares.
struct Renderer;

impl MetaRenderer for Renderer {
    fn render(&self, description: &Description, _target: &ReprType) -> Result<Representation> {
        Ok(Representation::new(
            ReprType::new("text/plain"),
            format!(
                "{} actions={}",
                description.id,
                description.action_specs().len()
            )
            .into_bytes(),
        ))
    }
}

fn kernel_over(space: Arc<dyn Space>) -> Kernel {
    Kernel::with_meta_renderer(space, Arc::new(Renderer))
}

/// Whether a `Meta` answer describes the real contract, not the placeholder.
fn real(answered: &Representation) -> bool {
    String::from_utf8_lossy(&answered.bytes).contains("actions=1")
}

fn meta(kernel: &Kernel) -> Result<Representation> {
    let request = Request::new(Verb::Meta, Iri::parse(REMOTE).unwrap());
    block_on(kernel.issue(request, &Capability::root()))
}

#[test]
fn the_bare_endpoint_answers_its_meta_failure() {
    let peer = Peer::new();
    let kernel = kernel_over(Arc::new(peer.space()));
    assert!(matches!(meta(&kernel), Err(Error::Unavailable(_))));
}

#[test]
fn every_overlay_answers_the_inner_meta_failure() {
    let mut wrong = Vec::new();
    for (name, _) in overlays(&Peer::new()) {
        let peer = Peer::new();
        let space = overlays(&peer)
            .into_iter()
            .find(|(n, _)| *n == name)
            .unwrap()
            .1;
        let kernel = kernel_over(space);
        match meta(&kernel) {
            Err(Error::Unavailable(message)) => assert_eq!(
                message, "the peer is down, so its contract is unknown",
                "{name}: the inner endpoint's error, unchanged"
            ),
            Ok(answered) => {
                wrong.push(format!(
                    "{name} answered {:?}",
                    String::from_utf8_lossy(&answered.bytes)
                ));
                continue;
            }
            Err(other) => {
                wrong.push(format!("{name} answered {other:?}"));
                continue;
            }
        }
        assert_eq!(
            peer.invoked.load(Ordering::SeqCst),
            0,
            "{name}: a Meta invokes nothing"
        );

        // Not cached: the next Meta asks again, and answers the real contract.
        peer.recover();
        let answered = meta(&kernel)
            .unwrap_or_else(|e| panic!("{name}: the recovered peer's Meta failed: {e}"));
        assert!(
            real(&answered),
            "{name}: the recovered Meta describes the real contract: {:?}",
            String::from_utf8_lossy(&answered.bytes)
        );
    }
    assert!(
        wrong.is_empty(),
        "a Meta of an unreachable peer must answer Unavailable through every overlay: {wrong:#?}"
    );
}

#[test]
fn retry_answers_a_meta_failure_at_once_and_the_next_meta_recovers() {
    let peer = Peer::new();
    let timer = Arc::new(Recording::default());
    let retry = Retry::new(peer.space(), 3)
        .with_clock(clock())
        .with_timer(timer.clone());
    let kernel = kernel_over(Arc::new(retry));

    // The kernel asks `meta_failure` before any endpoint runs: nothing to re-issue and
    // nothing to wait on, so Retry answers the transient failure as it was, at once.
    let error = meta(&kernel).unwrap_err();
    assert!(matches!(error, Error::Unavailable(_)), "{error:?}");
    assert_eq!(error.attempts(), None, "never wrapped in GaveUp");
    assert_eq!(timer.0.load(Ordering::SeqCst), 0, "no backoff was waited");
    assert_eq!(peer.invoked.load(Ordering::SeqCst), 0);

    // Still down: asked again, it fails again; nothing was cached.
    assert!(matches!(meta(&kernel), Err(Error::Unavailable(_))));

    // The peer recovers: the caller's next Meta succeeds.
    peer.recover();
    assert!(real(&meta(&kernel).unwrap()));
    assert_eq!(timer.0.load(Ordering::SeqCst), 0);
}

#[test]
fn failover_answers_the_meta_of_a_backup_that_can_state_its_contract() {
    let primary = Peer::new();
    let backup = Peer::new();
    backup.recover();
    let failover = Failover::new(vec![Arc::new(primary.space()), Arc::new(backup.space())]);
    let kernel = kernel_over(Arc::new(failover));
    let answered = meta(&kernel).expect("the backup states the contract");
    assert!(real(&answered));
}

#[test]
fn failover_answers_the_meta_failure_when_no_candidate_can_state_its_contract() {
    let primary = Peer::new();
    let backup = Peer::new();
    let failover = Failover::new(vec![Arc::new(primary.space()), Arc::new(backup.space())]);
    let kernel = kernel_over(Arc::new(failover));
    assert!(matches!(meta(&kernel), Err(Error::Unavailable(_))));
    backup.recover();
    assert!(real(&meta(&kernel).unwrap()), "the backup came up");
}
