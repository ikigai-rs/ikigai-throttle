//! Controlled overlapping calls exercise the circuit's generations and probe ownership.
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{channel::oneshot, executor::block_on, FutureExt, StreamExt};
use ikigai_core::{
    Body, Bytes, Capability, Clock, Endpoint, EndpointSpace, Error, Exact, Invocation, Iri, Kernel,
    ReprType, Representation, Request, Time, Verb,
};
use ikigai_throttle::CircuitBreaker;

#[derive(Default)]
struct Manual(AtomicU64);
impl Clock for Manual {
    fn now(&self) -> Time {
        Time::from_millis(self.0.load(Ordering::SeqCst))
    }
}
type Outcome = Result<(), Error>;
struct Scripted {
    steps: Mutex<VecDeque<oneshot::Receiver<Outcome>>>,
    calls: Arc<AtomicUsize>,
    stream_end: Mutex<Option<oneshot::Receiver<Outcome>>>,
}
#[async_trait::async_trait]
impl Endpoint for Scripted {
    async fn invoke(&self, _: &Invocation<'_>) -> Result<Representation, Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let step = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("script exhausted");
        step.await.expect("script sender dropped")?;
        Ok(Representation::new(
            ReprType::new("text/plain"),
            b"ok".to_vec(),
        ))
    }
    async fn invoke_body(&self, inv: &Invocation<'_>) -> Result<Representation<Body>, Error> {
        let answer = self.invoke(inv).await?;
        Ok(answer.map_body(|bytes| {
            if let Some(end) = self.stream_end.lock().unwrap().take() {
                Body::once(futures::stream::once(async move {
                    end.await.expect("stream sender dropped")?;
                    Ok(Bytes::from_static(b"ok"))
                }))
            } else {
                Body::from(bytes)
            }
        }))
    }
}
struct Fixture {
    kernel: Kernel,
    clock: Arc<Manual>,
    calls: Arc<AtomicUsize>,
    send: VecDeque<oneshot::Sender<Outcome>>,
    stream_end: Option<oneshot::Sender<Outcome>>,
}
impl Fixture {
    fn new(steps: usize) -> Self {
        Self::with_streaming(steps, false)
    }
    fn with_streaming(steps: usize, streaming: bool) -> Self {
        let clock = Arc::new(Manual::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let (send, receive) = (0..steps).map(|_| oneshot::channel()).unzip();
        let (stream_send, stream_receive) = oneshot::channel();
        let space = EndpointSpace::new().bind(
            Exact::new("urn:dep"),
            Scripted {
                steps: Mutex::new(receive),
                calls: calls.clone(),
                stream_end: Mutex::new(streaming.then_some(stream_receive)),
            },
        );
        let kernel = Kernel::new(Arc::new(
            CircuitBreaker::new(space, 1, Duration::from_millis(100)).with_clock(clock.clone()),
        ));
        Self {
            kernel,
            clock,
            calls,
            send,
            stream_end: streaming.then_some(stream_send),
        }
    }
    fn next(&mut self) -> oneshot::Sender<Outcome> {
        self.send.pop_front().unwrap()
    }
    fn cool(&self) {
        self.clock.0.fetch_add(100, Ordering::SeqCst);
    }
}
fn down() -> Error {
    Error::Unavailable("down".into())
}
async fn read(kernel: &Kernel, body: bool) -> Outcome {
    let request = Request::new(Verb::Source, Iri::parse("urn:dep").unwrap());
    if body {
        kernel
            .issue_body(request, &Capability::root())
            .await
            .map(|_| ())
    } else {
        kernel.issue(request, &Capability::root()).await.map(|_| ())
    }
}

#[test]
fn half_open_admits_only_one_in_flight_probe_on_either_face() {
    for body in [false, true] {
        let mut f = Fixture::new(3);
        f.next().send(Err(down())).unwrap();
        assert_eq!(block_on(read(&f.kernel, body)), Err(down()));
        f.cool();
        let release = f.next();
        let final_call = f.next();
        let mut probe = Box::pin(read(&f.kernel, body));
        assert!(probe.as_mut().now_or_never().is_none());
        let mut other = Box::pin(read(&f.kernel, body));
        let refused = other.as_mut().now_or_never();
        assert!(
            matches!(refused, Some(Err(Error::RetryAfter { .. }))),
            "second probe was admitted: {refused:?}"
        );
        assert_eq!(f.calls.load(Ordering::SeqCst), 2);
        release.send(Ok(())).unwrap();
        assert_eq!(block_on(probe), Ok(()));
        final_call.send(Ok(())).unwrap();
        assert_eq!(block_on(read(&f.kernel, body)), Ok(()));
    }
}

#[test]
fn a_closed_call_finishing_after_a_trip_cannot_close_the_open_circuit() {
    for body in [false, true] {
        let mut f = Fixture::new(3);
        let release_old = f.next();
        let trip = f.next();
        let after = f.next();
        let mut old = Box::pin(read(&f.kernel, body));
        assert!(old.as_mut().now_or_never().is_none());
        trip.send(Err(down())).unwrap();
        assert_eq!(block_on(read(&f.kernel, body)), Err(down()));
        release_old.send(Ok(())).unwrap();
        assert_eq!(block_on(old), Ok(()));
        after.send(Ok(())).unwrap();
        let result = block_on(read(&f.kernel, body));
        assert!(
            matches!(result, Err(Error::RetryAfter { .. })),
            "stale success closed the circuit: {result:?}"
        );
        assert_eq!(f.calls.load(Ordering::SeqCst), 2);
    }
}

#[test]
fn cancelling_a_probe_releases_admission_without_closing_the_circuit() {
    for body in [false, true] {
        let mut f = Fixture::new(4);
        f.next().send(Err(down())).unwrap();
        assert_eq!(block_on(read(&f.kernel, body)), Err(down()));
        f.cool();
        let cancelled = f.next();
        let next = f.next();
        let mut probe = Box::pin(read(&f.kernel, body));
        assert!(probe.as_mut().now_or_never().is_none());
        drop(probe);
        assert!(cancelled.send(Ok(())).is_err(), "inner call was cancelled");
        next.send(Err(down())).unwrap();
        assert_eq!(block_on(read(&f.kernel, body)), Err(down()));
        assert!(matches!(
            block_on(read(&f.kernel, body)),
            Err(Error::RetryAfter { .. })
        ));
        f.cool();
        f.next().send(Ok(())).unwrap();
        assert_eq!(block_on(read(&f.kernel, body)), Ok(()));
        assert_eq!(f.calls.load(Ordering::SeqCst), 4);
    }
}

#[test]
fn a_nontransient_probe_outcome_releases_admission_without_tripping_again() {
    for error in [
        Error::Denied("no grant".into()),
        Error::InvalidArgument {
            name: "input".into(),
            detail: "bad input".into(),
        },
    ] {
        let mut f = Fixture::new(3);
        f.next().send(Err(down())).unwrap();
        assert_eq!(block_on(read(&f.kernel, false)), Err(down()));
        f.cool();
        f.next().send(Err(error.clone())).unwrap();
        assert_eq!(block_on(read(&f.kernel, false)), Err(error));
        f.next().send(Ok(())).unwrap();
        assert_eq!(block_on(read(&f.kernel, false)), Ok(()));
    }
}

#[test]
fn a_stale_failure_cannot_reopen_a_recovered_circuit() {
    for body in [false, true] {
        let mut f = Fixture::new(4);
        let old_end = f.next();
        let trip = f.next();
        let recovery = f.next();
        let after = f.next();
        let mut old = Box::pin(read(&f.kernel, body));
        assert!(old.as_mut().now_or_never().is_none());
        trip.send(Err(down())).unwrap();
        assert_eq!(block_on(read(&f.kernel, body)), Err(down()));
        f.cool();
        recovery.send(Ok(())).unwrap();
        assert_eq!(block_on(read(&f.kernel, body)), Ok(()));
        old_end.send(Err(down())).unwrap();
        assert_eq!(block_on(old), Err(down()));
        after.send(Ok(())).unwrap();
        assert_eq!(block_on(read(&f.kernel, body)), Ok(()));
        assert_eq!(f.calls.load(Ordering::SeqCst), 4);
    }
}

#[test]
fn a_one_shot_probe_holds_admission_until_its_body_is_dropped() {
    let mut f = Fixture::with_streaming(3, true);
    f.next().send(Err(down())).unwrap();
    assert_eq!(block_on(read(&f.kernel, true)), Err(down()));
    f.cool();
    f.next().send(Ok(())).unwrap();
    let request = Request::new(Verb::Source, Iri::parse("urn:dep").unwrap());
    let probe = block_on(f.kernel.issue_body(request, &Capability::root())).unwrap();
    // A body handed over is still in flight, even after another full cooldown.
    f.cool();
    let mut other = Box::pin(read(&f.kernel, true));
    assert!(matches!(
        other.as_mut().now_or_never(),
        Some(Err(Error::RetryAfter { delay: None, .. }))
    ));
    assert_eq!(f.calls.load(Ordering::SeqCst), 2);
    drop(other);
    drop(probe);
    f.next().send(Err(down())).unwrap();
    assert_eq!(block_on(read(&f.kernel, true)), Err(down()));
    assert!(matches!(
        block_on(read(&f.kernel, true)),
        Err(Error::RetryAfter { .. })
    ));
}

#[test]
fn a_one_shot_probe_decides_health_when_its_stream_ends() {
    for outcome in [Ok(()), Err(down())] {
        let mut f = Fixture::with_streaming(3, true);
        f.next().send(Err(down())).unwrap();
        assert_eq!(block_on(read(&f.kernel, true)), Err(down()));
        f.cool();
        f.next().send(Ok(())).unwrap();
        let request = Request::new(Verb::Source, Iri::parse("urn:dep").unwrap());
        let probe = block_on(f.kernel.issue_body(request, &Capability::root())).unwrap();
        f.stream_end.take().unwrap().send(outcome.clone()).unwrap();
        let chunks: Vec<_> = block_on(probe.bytes.into_stream().collect());
        f.next().send(Ok(())).unwrap();
        let after = block_on(read(&f.kernel, true));
        match outcome {
            Ok(()) => {
                assert!(chunks.iter().all(Result::is_ok));
                assert_eq!(after, Ok(()));
                assert_eq!(f.calls.load(Ordering::SeqCst), 3);
            }
            Err(error) => {
                assert_eq!(chunks, [Err(error)]);
                assert!(
                    matches!(after, Err(Error::RetryAfter { delay: Some(delay), .. }) if delay == Duration::from_millis(100))
                );
                assert_eq!(f.calls.load(Ordering::SeqCst), 2);
            }
        }
    }
}
