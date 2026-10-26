//! **Every overlay forwards `invoke_body`** (ledger #2263), and each governor has a defined
//! meaning over a streamed body. Before this, every overlay implemented only `invoke`, so
//! under core's default `invoke_body` a one-shot body issued through any of them came out
//! collected into a buffer.
//!
//! - every overlay hands a one-shot body over one-shot, unread;
//! - [`Timeout`] bounds the drain, not only the call;
//! - [`Retry`] and [`Failover`] act only before the body is handed over: a one-shot body
//!   cannot be replayed, so a failure part-way through is the reader's;
//! - [`CircuitBreaker`] counts a mid-stream failure as a failure, and a body read to its
//!   end as a success;
//! - [`Throttle`] holds its slot until the body ends, or is dropped.
//!
//! A buffer or a replayable body is complete when it is handed over, and each governor
//! treats it as it treats a bytes answer.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::channel::oneshot;
use futures::executor::block_on;
use futures::future::{FutureExt, Shared};
use futures::StreamExt;
use ikigai_core::{
    Body, Bytes, Capability, Endpoint, EndpointSpace, Error, Exact, FixedClock, Invocation, Iri,
    Kernel, ReprType, Representation, Request, Space, Verb,
};
use ikigai_throttle::{CircuitBreaker, Failover, Rate, RateLimit, Retry, Throttle, Timeout, Timer};

const TARGET: &str = "urn:stream";

/// What a fixture's `n`th call (from 1) answers.
type Script = dyn Fn(u32) -> Result<Body, Error> + Send + Sync;

/// An endpoint that answers streamed bodies, counting its calls. Its bytes face collects
/// its body face, as a streaming endpoint's does.
struct Streams {
    calls: Arc<AtomicU32>,
    script: Box<Script>,
}

#[async_trait::async_trait]
impl Endpoint for Streams {
    async fn invoke(&self, inv: &Invocation<'_>) -> Result<Representation, Error> {
        let body = self.invoke_body(inv).await?;
        let bytes = inv.collect(body.bytes).await?;
        Ok(Representation::new(
            ReprType::new("text/plain"),
            bytes.to_vec(),
        ))
    }

    async fn invoke_body(&self, _inv: &Invocation<'_>) -> Result<Representation<Body>, Error> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        (self.script)(n).map(|body| Representation::from_body(ReprType::new("text/plain"), body))
    }
}

/// A space binding [`TARGET`] to a fixture, and the fixture's call count.
fn streams(
    script: impl Fn(u32) -> Result<Body, Error> + Send + Sync + 'static,
) -> (EndpointSpace, Arc<AtomicU32>) {
    let calls = Arc::new(AtomicU32::new(0));
    let endpoint = Streams {
        calls: Arc::clone(&calls),
        script: Box::new(script),
    };
    (
        EndpointSpace::new().bind(Exact::new(TARGET), endpoint),
        calls,
    )
}

/// A one-shot body of `chunks`.
fn once(chunks: &[&'static str]) -> Body {
    let chunks: Vec<_> = chunks
        .iter()
        .map(|c| Ok(Bytes::from_static(c.as_bytes())))
        .collect();
    Body::once(futures::stream::iter(chunks))
}

/// A one-shot body of `chunks`, then `error` part-way.
fn failing(chunks: &[&'static str], error: Error) -> Body {
    let chunks: Vec<_> = chunks
        .iter()
        .map(|c| Ok(Bytes::from_static(c.as_bytes())))
        .collect();
    Body::once(futures::stream::iter(chunks).chain(futures::stream::iter([Err(error)])))
}

/// A one-shot body of one chunk that then never ends.
fn stalling(chunk: &'static str) -> Body {
    Body::once(
        futures::stream::iter([Ok(Bytes::from_static(chunk.as_bytes()))])
            .chain(futures::stream::pending()),
    )
}

fn read() -> Request {
    Request::new(Verb::Source, Iri::parse(TARGET).unwrap())
}

fn issue_body(kernel: &Kernel) -> Result<Representation<Body>, Error> {
    block_on(kernel.issue_body(read(), &Capability::root()))
}

/// Read a body to its end: the bytes, or the error it ended with.
fn drain(body: Body) -> Result<Vec<u8>, Error> {
    block_on(async {
        let mut stream = body.into_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            bytes.extend_from_slice(&chunk?);
        }
        Ok(bytes)
    })
}

/// A timer that fires when the test says, and never on its own.
#[derive(Clone)]
struct Trigger {
    fired: Shared<oneshot::Receiver<()>>,
}

impl Trigger {
    fn new() -> (Self, oneshot::Sender<()>) {
        let (fire, fired) = oneshot::channel();
        (
            Trigger {
                fired: fired.shared(),
            },
            fire,
        )
    }
}

#[async_trait::async_trait]
impl Timer for Trigger {
    async fn sleep(&self, _delay: Duration) {
        let _ = self.fired.clone().await;
    }
}

/// A timer that never fires.
struct Never;

#[async_trait::async_trait]
impl Timer for Never {
    async fn sleep(&self, _delay: Duration) {
        futures::future::pending::<()>().await
    }
}

fn kernel(space: impl Space + 'static) -> Kernel {
    Kernel::new(Arc::new(space))
}

#[test]
fn every_overlay_hands_a_one_shot_body_over_one_shot() {
    let leaf = || streams(|_| Ok(once(&["a", "b"]))).0;
    let clock = || Arc::new(FixedClock::at(1_000));
    let overlays: Vec<(&str, Kernel)> = vec![
        ("bare", kernel(leaf())),
        (
            "RateLimit",
            kernel(
                RateLimit::new(leaf())
                    .limit("urn:", Rate::new(10, Duration::from_secs(1)))
                    .with_clock(clock()),
            ),
        ),
        ("Retry", kernel(Retry::new(leaf(), 3))),
        (
            "CircuitBreaker",
            kernel(CircuitBreaker::new(leaf(), 1, Duration::from_secs(1)).with_clock(clock())),
        ),
        (
            "Failover",
            kernel(Failover::new(vec![Arc::new(leaf()), Arc::new(leaf())])),
        ),
        ("Throttle", kernel(Throttle::new(leaf()).limit("urn:", 1))),
        (
            "Timeout",
            kernel(Timeout::new(leaf(), Duration::from_secs(1)).with_timer(Arc::new(Never))),
        ),
    ];
    for (name, kernel) in overlays {
        let body = issue_body(&kernel).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(
            body.bytes.is_one_shot(),
            "{name} collected the body: {:?}",
            body.bytes
        );
        assert_eq!(drain(body.bytes).unwrap(), b"ab", "{name}");
        // The bytes face still collects it, through the same governor.
        let bytes = block_on(kernel.issue(read(), &Capability::root()))
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(bytes.bytes, b"ab", "{name}");
    }
}

#[test]
fn timeout_bounds_the_drain() {
    let (leaf, _) = streams(|_| Ok(stalling("first")));
    let (timer, fire) = Trigger::new();
    let kernel = kernel(Timeout::new(leaf, Duration::from_millis(50)).with_timer(Arc::new(timer)));
    // Polled once, not awaited: a Timeout that drained the body inside the call would
    // never return (this test hung before the fix), and must fail here instead.
    let body = kernel
        .issue_body(read(), &Capability::root())
        .now_or_never()
        .expect("the call returned before its body was read")
        .unwrap();
    let mut stream = body.bytes.into_stream();
    assert_eq!(
        block_on(stream.next()).unwrap().unwrap(),
        &b"first"[..],
        "within budget, a chunk is read"
    );
    fire.send(()).unwrap();
    let elapsed = block_on(stream.next()).expect("an error, not a quiet end");
    assert!(
        matches!(&elapsed, Err(Error::Timeout(detail)) if detail.contains(TARGET)),
        "{elapsed:?}"
    );
    assert!(
        block_on(stream.next()).is_none(),
        "the body ends at the elapse"
    );
}

#[test]
fn timeout_passes_a_drain_that_ends_within_budget() {
    let (leaf, _) = streams(|_| Ok(once(&["x", "y", "z"])));
    let kernel = kernel(Timeout::new(leaf, Duration::from_secs(5)).with_timer(Arc::new(Never)));
    assert_eq!(drain(issue_body(&kernel).unwrap().bytes).unwrap(), b"xyz");
}

#[test]
fn retry_reissues_before_the_body_and_never_after() {
    // A failure before the body is handed over is re-issued.
    let (leaf, calls) = streams(|n| match n {
        1 => Err(Error::Unavailable("busy".into())),
        _ => Ok(once(&["ok"])),
    });
    let kernel = kernel(Retry::new(leaf, 3));
    let body = issue_body(&kernel).unwrap();
    assert!(body.bytes.is_one_shot());
    assert_eq!(drain(body.bytes).unwrap(), b"ok");
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    // A failure part-way through a one-shot body is the reader's: nothing re-issues it.
    let (leaf, calls) = streams(|_| Ok(failing(&["par"], Error::Unavailable("reset".into()))));
    let kernel = self::kernel(Retry::new(leaf, 3));
    let body = issue_body(&kernel).unwrap();
    assert_eq!(
        drain(body.bytes),
        Err(Error::Unavailable("reset".into())),
        "refused, never truncated"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a one-shot body is not replayed"
    );
}

#[test]
fn failover_advances_before_the_body_and_never_after() {
    let (primary, primary_calls) = streams(|_| Err(Error::Unavailable("down".into())));
    let (backup, backup_calls) = streams(|_| Ok(once(&["backup"])));
    let kernel = kernel(Failover::new(vec![Arc::new(primary), Arc::new(backup)]));
    let body = issue_body(&kernel).unwrap();
    assert!(body.bytes.is_one_shot());
    assert_eq!(drain(body.bytes).unwrap(), b"backup");
    assert_eq!(primary_calls.load(Ordering::SeqCst), 1);
    assert_eq!(backup_calls.load(Ordering::SeqCst), 1);

    let (primary, _) = streams(|_| Ok(failing(&["pri"], Error::Unavailable("reset".into()))));
    let (backup, backup_calls) = streams(|_| Ok(once(&["backup"])));
    let kernel = self::kernel(Failover::new(vec![Arc::new(primary), Arc::new(backup)]));
    let body = issue_body(&kernel).unwrap();
    assert_eq!(drain(body.bytes), Err(Error::Unavailable("reset".into())));
    assert_eq!(
        backup_calls.load(Ordering::SeqCst),
        0,
        "a started body is not failed over"
    );
}

#[test]
fn circuit_breaker_counts_a_mid_stream_failure_and_a_clean_end() {
    let (leaf, calls) = streams(|n| match n {
        1 => Ok(failing(&["par"], Error::Unavailable("reset".into()))),
        _ => Ok(once(&["fine"])),
    });
    let clock = Arc::new(FixedClock::at(1_000));
    let breaker = CircuitBreaker::new(leaf, 1, Duration::from_secs(30)).with_clock(clock);
    let kernel = kernel(breaker);

    // Handed over: nothing is counted until the body ends.
    let body = issue_body(&kernel).unwrap();
    assert_eq!(drain(body.bytes), Err(Error::Unavailable("reset".into())));
    // The mid-stream abort tripped the circuit (threshold 1): the next call fails fast.
    let open = issue_body(&kernel).unwrap_err();
    assert_eq!(open.retry_after_millis(), Some(30_000), "{open:?}");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "an open circuit spares the dependency"
    );
}

#[test]
fn circuit_breaker_closes_on_a_body_read_to_its_end() {
    let (leaf, calls) = streams(|n| match n {
        1 => Err(Error::Unavailable("down".into())),
        2 => Ok(once(&["fine"])),
        _ => Err(Error::Unavailable("down".into())),
    });
    let clock = Arc::new(FixedClock::at(1_000));
    // Threshold 2: one failure, then a success must reset the count, or the third trips.
    let kernel = kernel(CircuitBreaker::new(leaf, 2, Duration::from_secs(30)).with_clock(clock));
    assert!(issue_body(&kernel).is_err());
    assert_eq!(drain(issue_body(&kernel).unwrap().bytes).unwrap(), b"fine");
    assert_eq!(
        issue_body(&kernel).unwrap_err(),
        Error::Unavailable("down".into())
    );
    // Had the success not counted, that third failure would have tripped it.
    assert_eq!(
        issue_body(&kernel).unwrap_err(),
        Error::Unavailable("down".into())
    );
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}

#[test]
fn throttle_holds_its_slot_until_the_body_ends() {
    let (leaf, calls) = streams(|_| Ok(once(&["one"])));
    let kernel = kernel(Throttle::new(leaf).limit("urn:", 1));

    let first = issue_body(&kernel).unwrap();
    // The first body is unread: its slot is held, so a second call parks.
    let root = Capability::root();
    let mut second = Box::pin(kernel.issue_body(read(), &root));
    assert!(
        (&mut second).now_or_never().is_none(),
        "a second call ran while the first body was unread"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // Reading the first body to its end frees the slot.
    assert_eq!(drain(first.bytes).unwrap(), b"one");
    let second = block_on(second).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    // Dropping a body unread frees it too.
    drop(second);
    assert!(issue_body(&kernel).is_ok());
}

#[test]
fn a_buffer_is_complete_when_handed_over() {
    // A buffer holds no slot past the call, and the breaker counts it at once.
    let (leaf, _) = streams(|_| Ok(Body::from(b"buf".to_vec())));
    let kernel = kernel(Throttle::new(leaf).limit("urn:", 1));
    let held = issue_body(&kernel).unwrap();
    assert!(!held.bytes.is_one_shot());
    assert!(issue_body(&kernel).is_ok(), "a buffer released its slot");
}
