//! Audit the documented window boundaries, failure policy, and stacking counts.
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::executor::block_on;
use ikigai_core::{
    Capability, Clock, Endpoint, EndpointSpace, Error, Exact, FixedClock, FnEndpoint, Invocation,
    Iri, Kernel, ReprType, Representation, Request, Resolution, Scope, Space, Time, Verb,
};
use ikigai_throttle::{CircuitBreaker, Failover, Rate, RateLimit, Retry, Timeout, Timer};

#[derive(Default)]
struct Manual(AtomicU64);
impl Clock for Manual {
    fn now(&self) -> Time {
        Time::from_millis(self.0.load(Ordering::SeqCst))
    }
}
fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}
fn ok() -> Representation {
    Representation::new(ReprType::new("text/plain"), b"ok".to_vec())
}
fn issue(kernel: &Kernel, target: &str, verb: Verb, body: bool) -> Result<(), Error> {
    let request = Request::new(verb, Iri::parse(target).unwrap());
    if body {
        block_on(kernel.issue_body(request, &Capability::root())).map(|_| ())
    } else {
        block_on(kernel.issue(request, &Capability::root())).map(|_| ())
    }
}
struct Never;
#[async_trait::async_trait]
impl Timer for Never {
    async fn sleep(&self, _: Duration) {
        futures::future::pending::<()>().await
    }
}
fn retry<S: Space>(space: S) -> Retry<S> {
    Retry::new(space, 3)
        .with_backoff(Duration::ZERO, Duration::ZERO)
        .with_clock(Arc::new(FixedClock::at(0)))
        .with_timer(Arc::new(Never))
}
fn failing(error: Error, calls: Arc<AtomicUsize>) -> EndpointSpace {
    EndpointSpace::new().bind(
        Exact::new("urn:dep"),
        FnEndpoint::new("down", move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(error.clone())
        }),
    )
}

#[test]
fn overlapping_rate_prefixes_have_independent_exact_sliding_windows() {
    for body in [false, true] {
        let clock = Arc::new(Manual::default());
        let mut leaf = EndpointSpace::new();
        for target in ["urn:dep:a", "urn:dep:b", "urn:other"] {
            leaf = leaf.bind(Exact::new(target), FnEndpoint::new("ok", |_| Ok(ok())));
        }
        let meta = Request::new(Verb::Meta, Iri::parse("urn:dep:a").unwrap());
        let Resolution::Hit(original) = leaf.resolve(&meta, &Scope::empty()) else {
            panic!("fixture did not resolve");
        };
        // Short prefix is deliberately registered first.
        let limited = Arc::new(
            RateLimit::new(leaf)
                .with_clock(clock.clone())
                .limit("urn:", Rate::new(1, ms(100)))
                .limit("urn:dep:", Rate::new(2, ms(100))),
        );
        let kernel = Kernel::new(limited.clone());
        assert_eq!(issue(&kernel, "urn:dep:a", Verb::Source, body), Ok(()));
        assert_eq!(issue(&kernel, "urn:other", Verb::Source, body), Ok(()));
        clock.0.store(30, Ordering::SeqCst);
        assert_eq!(issue(&kernel, "urn:dep:b", Verb::Source, body), Ok(()));
        clock.0.store(99, Ordering::SeqCst);
        for target in ["urn:dep:a", "urn:other"] {
            let error = issue(&kernel, target, Verb::Source, body).unwrap_err();
            assert_eq!(error.retry_after_millis(), Some(1));
            assert!(matches!(error, Error::RetryAfter { .. }));
        }
        // Meta neither consumes a slot nor observes the refusal.
        let Resolution::Hit(described) = limited.resolve(&meta, &Scope::empty()) else {
            panic!("Meta was hidden by the limit");
        };
        assert!(Arc::ptr_eq(&original.endpoint, &described.endpoint));
        clock.0.store(100, Ordering::SeqCst);
        assert_eq!(issue(&kernel, "urn:dep:a", Verb::Source, body), Ok(()));
        assert_eq!(issue(&kernel, "urn:other", Verb::Source, body), Ok(()));
        assert_eq!(
            issue(&kernel, "urn:dep:b", Verb::Source, body)
                .unwrap_err()
                .retry_after_millis(),
            Some(30)
        );
        clock.0.store(130, Ordering::SeqCst);
        assert_eq!(issue(&kernel, "urn:dep:b", Verb::Source, body), Ok(()));
    }
}

#[test]
fn caller_errors_are_neither_retried_nor_failed_over_nor_counted_by_the_breaker() {
    for body in [false, true] {
        for error in [
            Error::Denied("no grant".into()),
            Error::InvalidArgument {
                name: "arg".into(),
                detail: "invalid".into(),
            },
            Error::Conflict("taken".into()),
            Error::NotFound("gone".into()),
        ] {
            let calls = Arc::new(AtomicUsize::new(0));
            let backup_calls = Arc::new(AtomicUsize::new(0));
            let primary =
                CircuitBreaker::new(retry(failing(error.clone(), calls.clone())), 1, ms(100))
                    .with_clock(Arc::new(FixedClock::at(0)));
            let failover = Failover::new(vec![
                Arc::new(primary),
                Arc::new(failing(
                    Error::Unavailable("backup".into()),
                    backup_calls.clone(),
                )),
            ]);
            let kernel = Kernel::new(Arc::new(failover));
            for _ in 0..2 {
                assert_eq!(
                    issue(&kernel, "urn:dep", Verb::Source, body),
                    Err(error.clone())
                );
            }
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            assert_eq!(backup_calls.load(Ordering::SeqCst), 0);
        }
    }
}

#[test]
fn failover_tries_each_candidate_once_in_order_and_returns_the_last_error() {
    for body in [false, true] {
        let order = Arc::new(Mutex::new(Vec::new()));
        let spaces = (0..3)
            .map(|n| {
                let order = order.clone();
                Arc::new(EndpointSpace::new().bind(
                    Exact::new("urn:dep"),
                    FnEndpoint::new("down", move |_| {
                        order.lock().unwrap().push(n);
                        Err(Error::Unavailable(format!("candidate {n}")))
                    }),
                )) as Arc<dyn Space>
            })
            .collect();
        let kernel = Kernel::new(Arc::new(Failover::new(spaces)));
        assert_eq!(
            issue(&kernel, "urn:dep", Verb::Source, body),
            Err(Error::Unavailable("candidate 2".into()))
        );
        assert_eq!(*order.lock().unwrap(), [0, 1, 2]);
        order.lock().unwrap().clear();
        assert_eq!(
            issue(&kernel, "urn:dep", Verb::Sink, body),
            Err(Error::Unavailable("candidate 0".into()))
        );
        assert_eq!(*order.lock().unwrap(), [0]);
        // Delete is explicitly documented as idempotent in core and this crate.
        order.lock().unwrap().clear();
        assert_eq!(
            issue(&kernel, "urn:dep", Verb::Delete, body),
            Err(Error::Unavailable("candidate 2".into()))
        );
        assert_eq!(*order.lock().unwrap(), [0, 1, 2]);
    }
}

#[test]
fn retry_inside_and_outside_a_breaker_count_at_their_own_boundary() {
    for body in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let inside = CircuitBreaker::new(
            retry(failing(Error::Unavailable("down".into()), calls.clone())),
            2,
            ms(100),
        )
        .with_clock(Arc::new(FixedClock::at(0)));
        let kernel = Kernel::new(Arc::new(inside));
        for expected_calls in [3, 6] {
            assert_eq!(
                issue(&kernel, "urn:dep", Verb::Source, body)
                    .unwrap_err()
                    .attempts(),
                Some(3)
            );
            assert_eq!(calls.load(Ordering::SeqCst), expected_calls);
        }
        assert!(matches!(
            issue(&kernel, "urn:dep", Verb::Source, body),
            Err(Error::RetryAfter { .. })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 6);

        let calls = Arc::new(AtomicUsize::new(0));
        let outside = retry(
            CircuitBreaker::new(
                failing(Error::Unavailable("down".into()), calls.clone()),
                2,
                ms(100),
            )
            .with_clock(Arc::new(FixedClock::at(0))),
        );
        let kernel = Kernel::new(Arc::new(outside));
        // Two underlying attempts trip; the third meets RetryAfter and cannot wait it.
        assert!(matches!(
            issue(&kernel, "urn:dep", Verb::Source, body),
            Err(Error::RetryAfter { .. })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}

struct Immediate;
#[async_trait::async_trait]
impl Timer for Immediate {
    async fn sleep(&self, _: Duration) {}
}
struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Pending {
    calls: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl Endpoint for Pending {
    async fn invoke(&self, _: &Invocation<'_>) -> Result<Representation, Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let _held = Dropped(self.dropped.clone());
        futures::future::pending().await
    }
}
#[test]
fn timeout_inside_retry_cancels_every_attempt_before_reissuing() {
    for body in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let space = EndpointSpace::new().bind(
            Exact::new("urn:dep"),
            Pending {
                calls: calls.clone(),
                dropped: dropped.clone(),
            },
        );
        let kernel = Kernel::new(Arc::new(retry(
            Timeout::new(space, ms(10)).with_timer(Arc::new(Immediate)),
        )));
        let error = issue(&kernel, "urn:dep", Verb::Source, body).unwrap_err();
        assert_eq!(error.attempts(), Some(3));
        assert!(
            error
                .to_string()
                .ends_with("timeout: `urn:dep` exceeded 10ms"),
            "{error}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(dropped.load(Ordering::SeqCst), 3, "no attempt leaked");
    }
}
