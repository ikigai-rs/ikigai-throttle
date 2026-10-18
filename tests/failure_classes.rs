//! **The overlays decide from the failure's CLASS** (ledger #1546, #200, #972), from core's
//! `Error::class()` plus `Verb::is_idempotent()`, and spend from the run's shared retry pool
//! when the host configured one.
//!
//! Every test here fails on the 0.1.x line, which read `Error::is_transient` alone, kept a
//! private attempt count per layer, refused a rate limit as a permanent `Endpoint` and
//! answered an open circuit with a transient `Unavailable`.
//!
//! Time is a clock the test moves, and the timer a `Retry` waits on moves that same clock,
//! so every wait is exact and costs no real time.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::executor::block_on;
use ikigai_core::{
    cap_retry, Capability, Clock, Endpoint, EndpointSpace, Error, Exact, FailureClass, Invocation,
    Iri, IssueContext, Kernel, ReprType, Representation, Request, Space, Time, Verb,
};
use ikigai_throttle::{CircuitBreaker, Failover, Rate, RateLimit, Retry, Timer};

/// A clock that moves only when the test (or the timer) moves it.
#[derive(Clone, Default)]
struct Manual(Arc<AtomicU64>);

impl Manual {
    fn at(millis: u64) -> Self {
        let clock = Manual::default();
        clock.0.store(millis, Ordering::SeqCst);
        clock
    }
    fn advance(&self, by: Duration) {
        self.0
            .fetch_add(u64::try_from(by.as_millis()).unwrap(), Ordering::SeqCst);
    }
}

impl Clock for Manual {
    fn now(&self) -> Time {
        Time::from_millis(self.0.load(Ordering::SeqCst))
    }
}

/// A timer that "waits" by moving the test's clock, and records every wait.
#[derive(Clone, Default)]
struct Advancing {
    clock: Manual,
    waits: Arc<Mutex<Vec<Duration>>>,
}

impl Advancing {
    fn on(clock: &Manual) -> Self {
        Advancing {
            clock: clock.clone(),
            waits: Arc::default(),
        }
    }
    fn waits(&self) -> Vec<Duration> {
        self.waits.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl Timer for Advancing {
    async fn sleep(&self, delay: Duration) {
        self.waits.lock().unwrap().push(delay);
        self.clock.advance(delay);
    }
}

fn ok() -> Representation {
    Representation::new(ReprType::new("text/plain"), b"ok".to_vec())
}

/// Fails with `error` on its first `fail` invocations, then answers; counts them all.
struct Failing {
    error: Error,
    fail: u32,
    seen: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl Endpoint for Failing {
    async fn invoke(&self, _inv: &Invocation<'_>) -> Result<Representation, Error> {
        let n = self.seen.fetch_add(1, Ordering::SeqCst);
        if n < self.fail {
            Err(self.error.clone())
        } else {
            Ok(ok())
        }
    }
}

/// A space binding `urn:dep` to an endpoint that always fails with `error`, and its counter.
fn failing(error: &Error) -> (EndpointSpace, Arc<AtomicU32>) {
    failing_for(error, u32::MAX)
}

fn failing_for(error: &Error, fail: u32) -> (EndpointSpace, Arc<AtomicU32>) {
    let seen = Arc::new(AtomicU32::new(0));
    let space = EndpointSpace::new().bind_arc(
        Exact::new("urn:dep"),
        Arc::new(Failing {
            error: error.clone(),
            fail,
            seen: seen.clone(),
        }),
    );
    (space, seen)
}

fn request(verb: Verb) -> Request {
    Request::new(verb, Iri::parse("urn:dep").unwrap())
}

fn read(kernel: &Kernel) -> Result<Representation, Error> {
    block_on(kernel.issue(request(Verb::Source), &Capability::root()))
}

/// One representative of every failure class, the class first.
fn one_of_each_class() -> Vec<(FailureClass, Error)> {
    vec![
        (FailureClass::Transient, Error::Unavailable("down".into())),
        (
            FailureClass::RetryAfter,
            Error::RetryAfter {
                delay: Some(Duration::from_millis(40)),
                detail: "rate-limited upstream".into(),
            },
        ),
        (
            FailureClass::OverBudget,
            Error::OverBudget {
                detail: "the query ran past its 5000 ms budget".into(),
            },
        ),
        (FailureClass::Denied, Error::Denied("no grant".into())),
        (
            FailureClass::Refused,
            Error::Refused("origin answered 401".into()),
        ),
        (FailureClass::Permanent, Error::NotFound("gone".into())),
        (FailureClass::Unknown, Error::Endpoint("boom".into())),
    ]
}

/// The table above every test: what each overlay does with each class it receives.
#[test]
fn every_class_is_represented() {
    let classes: Vec<FailureClass> = one_of_each_class()
        .into_iter()
        .map(|(c, e)| {
            assert_eq!(e.class(), c, "{e:?}");
            c
        })
        .collect();
    assert_eq!(
        classes,
        FailureClass::ALL,
        "one row per class, in core's order"
    );
}

/// Retry: a transient failure is re-issued at once, a retry-after one after its delay
/// (it fits the overlay's own bound here), and every other class is returned on the
/// first attempt. A `Sink` is never re-sent, whatever the class.
#[test]
fn retry_decides_from_each_class() {
    for (class, error) in one_of_each_class() {
        let clock = Manual::at(1_000);
        let timer = Advancing::on(&clock);
        let (space, seen) = failing(&error);
        let retry = Retry::new(space, 3)
            .with_timer(Arc::new(timer.clone()))
            .with_max_wait(Duration::from_secs(60));
        let kernel = Kernel::new(Arc::new(retry)).with_clock(Arc::new(clock.clone()));

        let err = read(&kernel).unwrap_err();
        assert_eq!(
            err, error,
            "{class}: the received error comes back unchanged"
        );
        let (attempts, waits) = match class {
            FailureClass::Transient => (3, vec![]),
            FailureClass::RetryAfter => (3, vec![Duration::from_millis(40); 2]),
            _ => (1, vec![]),
        };
        assert_eq!(seen.load(Ordering::SeqCst), attempts, "{class}: attempts");
        assert_eq!(timer.waits(), waits, "{class}: waits");

        let sink = block_on(kernel.issue(request(Verb::Sink), &Capability::root()));
        assert_eq!(sink.unwrap_err(), error, "{class}");
        assert_eq!(
            seen.load(Ordering::SeqCst),
            attempts + 1,
            "{class}: a Sink is invoked exactly once"
        );
    }
}

/// Failover: a transient or retry-after failure moves to the next target (this one is
/// down, or limited); every other class is returned and the backup never touched.
#[test]
fn failover_decides_from_each_class() {
    for (class, error) in one_of_each_class() {
        let (primary, tried) = failing(&error);
        let (backup, served) = failing_for(&Error::Unavailable("unused".into()), 0);
        let kernel = Kernel::new(Arc::new(Failover::new(vec![
            Arc::new(primary) as Arc<dyn Space>,
            Arc::new(backup),
        ])));
        let out = read(&kernel);
        assert_eq!(
            tried.load(Ordering::SeqCst),
            1,
            "{class}: primary tried first"
        );
        match class {
            FailureClass::Transient | FailureClass::RetryAfter => {
                assert!(out.is_ok(), "{class}: the backup serves: {out:?}");
                assert_eq!(served.load(Ordering::SeqCst), 1, "{class}");
            }
            _ => {
                assert_eq!(out.unwrap_err(), error, "{class}: returned unchanged");
                assert_eq!(
                    served.load(Ordering::SeqCst),
                    0,
                    "{class}: backup untouched"
                );
            }
        }
    }
}

/// CircuitBreaker: only a transient failure counts toward tripping. At a threshold of one,
/// a transient failure opens the circuit (the next call is refused without touching the
/// dependency, as a RetryAfter for the cooldown); any other class leaves it closed.
#[test]
fn a_breaker_counts_only_transient_failures() {
    for (class, error) in one_of_each_class() {
        let clock = Manual::at(5_000);
        let (space, seen) = failing(&error);
        let kernel = Kernel::new(Arc::new(
            CircuitBreaker::new(space, 1, Duration::from_millis(1_000))
                .with_clock(Arc::new(clock.clone())),
        ));
        assert_eq!(read(&kernel).unwrap_err(), error, "{class}");
        clock.advance(Duration::from_millis(250));
        let second = read(&kernel).unwrap_err();
        if class == FailureClass::Transient {
            assert_eq!(seen.load(Ordering::SeqCst), 1, "{class}: open, untouched");
            assert!(matches!(second, Error::RetryAfter { .. }), "{second:?}");
            assert_eq!(second.retry_after_millis(), Some(750), "the cooldown left");
        } else {
            assert_eq!(seen.load(Ordering::SeqCst), 2, "{class}: still closed");
            assert_eq!(second, error, "{class}");
        }
    }
}

/// ★ The shared pool: two nested Retry layers of 3 attempts each would make 3 x 3 = 9
/// calls against a dependency that is already failing. Under a run allowed 2 retries,
/// the whole tree makes the first call and 2 re-issues, wherever they happen.
#[test]
fn nested_retry_layers_spend_one_pool_never_multiplying() {
    let nested = |space: EndpointSpace| Retry::new(Retry::new(space, 3), 3);
    let down = Error::Unavailable("down".into());

    // No pool configured: each layer's own attempts, as before.
    let (space, seen) = failing(&down);
    let kernel = Kernel::new(Arc::new(nested(space)));
    assert_eq!(read(&kernel).unwrap_err(), down);
    assert_eq!(
        seen.load(Ordering::SeqCst),
        9,
        "no pool: the layers multiply"
    );

    // A pool of 2 for the whole run.
    let (space, seen) = failing(&down);
    let kernel = Kernel::new(Arc::new(nested(space))).with_retry_budget(0, 2);
    assert_eq!(
        read(&kernel).unwrap_err(),
        down,
        "the received error, unchanged"
    );
    assert_eq!(
        seen.load(Ordering::SeqCst),
        3,
        "one call plus two re-issues in all"
    );

    // A caller whose capability holds one retry grant: one re-issue in the whole tree.
    let (space, seen) = failing(&down);
    let kernel = Kernel::new(Arc::new(nested(space))).with_retry_budget(0, 2);
    let one = Capability::scoped([cap_retry(1)]);
    assert!(block_on(kernel.issue(request(Verb::Source), &one)).is_err());
    assert_eq!(seen.load(Ordering::SeqCst), 2);

    // A caller holding no retry grant gets the host's base, here 0: no re-issue at all,
    // however many attempts the overlays allow.
    let (space, seen) = failing(&down);
    let kernel = Kernel::new(Arc::new(nested(space))).with_retry_budget(0, 2);
    let none = Capability::scoped(["urn:cap:demo:read".to_string()]);
    assert!(block_on(kernel.issue(request(Verb::Source), &none)).is_err());
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "an empty pool re-issues nothing"
    );
}

/// The pool decides a Retry that has attempts left: a recovered dependency is reached only
/// if a retry was left to spend.
#[test]
fn a_spent_pool_returns_the_received_error() {
    let (space, seen) = failing_for(&Error::Timeout("slow".into()), 1);
    let kernel = Kernel::new(Arc::new(Retry::new(space, 5))).with_retry_budget(0, 0);
    assert_eq!(read(&kernel).unwrap_err(), Error::Timeout("slow".into()));
    assert_eq!(seen.load(Ordering::SeqCst), 1);

    let (space, seen) = failing_for(&Error::Timeout("slow".into()), 1);
    let kernel = Kernel::new(Arc::new(Retry::new(space, 5))).with_retry_budget(0, 1);
    assert!(read(&kernel).is_ok(), "one retry was enough");
    assert_eq!(seen.load(Ordering::SeqCst), 2);
}

/// An advance is a re-issue too: Failover over three targets, two down, under a pool of
/// one, reaches the second and returns ITS error rather than spending a retry it has not got.
#[test]
fn failover_advances_spend_from_the_pool() {
    let down = |text: &str| Error::Unavailable(text.into());
    let (first, a) = failing(&down("first"));
    let (second, b) = failing(&down("second"));
    let (third, c) = failing_for(&down("unused"), 0);
    let kernel = Kernel::new(Arc::new(Failover::new(vec![
        Arc::new(first) as Arc<dyn Space>,
        Arc::new(second),
        Arc::new(third),
    ])))
    .with_retry_budget(0, 1);
    assert_eq!(read(&kernel).unwrap_err(), down("second"));
    let counts = [a, b, c].map(|n| n.load(Ordering::SeqCst));
    assert_eq!(
        counts,
        [1, 1, 0],
        "the third target needed a retry the run did not have"
    );
}

/// ★ A RetryAfter is waited only when its delay fits what the run has left. The door set
/// a deadline 100 ms out: a 500 ms delay is returned at once and never waited; a 50 ms
/// delay is waited twice (time left 100, then 50), and a third wait would not fit.
#[test]
fn a_retry_after_longer_than_the_time_left_is_not_waited() {
    let run = |delay_ms: u64, retry: fn(EndpointSpace) -> Retry<EndpointSpace>| {
        let clock = Manual::at(1_000);
        let timer = Advancing::on(&clock);
        let error = Error::RetryAfter {
            delay: Some(Duration::from_millis(delay_ms)),
            detail: "limited".into(),
        };
        let (space, seen) = failing(&error);
        let kernel = Kernel::new(Arc::new(retry(space).with_timer(Arc::new(timer.clone()))))
            .with_clock(Arc::new(clock.clone()));
        let deadline = IssueContext::new().with_not_after(Time::from_millis(1_100));
        let out = block_on(kernel.issue_with_context(
            request(Verb::Source),
            &Capability::root(),
            deadline,
        ));
        assert_eq!(out.unwrap_err(), error, "the received error, unchanged");
        (seen.load(Ordering::SeqCst), timer.waits())
    };
    let plain = |space| Retry::new(space, 5);

    let (seen, waits) = run(500, plain);
    assert_eq!(
        (seen, waits),
        (1, vec![]),
        "500 ms does not fit 100 ms left"
    );

    let (seen, waits) = run(50, plain);
    assert_eq!(seen, 3);
    assert_eq!(
        waits,
        vec![Duration::from_millis(50); 2],
        "100 left, then 50, then 0"
    );

    // An own bound only LOWERS a deadline: 10 ms of own bound refuses a 50 ms wait.
    let bounded = |space| Retry::new(space, 5).with_max_wait(Duration::from_millis(10));
    let (seen, waits) = run(50, bounded);
    assert_eq!((seen, waits), (1, vec![]));
}

/// With no deadline the overlay's own bound decides, and with neither nothing is waited:
/// the caller owns the time. A RetryAfter with no delay ("not now") is never waited.
#[test]
fn without_a_deadline_the_own_bound_decides() {
    let limited = |delay| Error::RetryAfter {
        delay,
        detail: "limited".into(),
    };
    for (bound, delay, attempts) in [
        (None, Some(Duration::from_millis(1)), 1),
        (
            Some(Duration::from_millis(40)),
            Some(Duration::from_millis(40)),
            2,
        ),
        (
            Some(Duration::from_millis(39)),
            Some(Duration::from_millis(40)),
            1,
        ),
        (Some(Duration::from_secs(60)), None, 1),
    ] {
        let clock = Manual::at(0);
        let (space, seen) = failing(&limited(delay));
        let mut retry = Retry::new(space, 2).with_timer(Arc::new(Advancing::on(&clock)));
        if let Some(bound) = bound {
            retry = retry.with_max_wait(bound);
        }
        let kernel = Kernel::new(Arc::new(retry)).with_clock(Arc::new(clock));
        assert_eq!(read(&kernel).unwrap_err(), limited(delay));
        assert_eq!(
            seen.load(Ordering::SeqCst),
            attempts,
            "bound {bound:?}, delay {delay:?}"
        );
    }
}

/// ★ Ledger #972: an over-budget failure is never re-issued, by any overlay, however much
/// budget the run holds: under the same bound a repeat costs the same and fails the same.
#[test]
fn an_over_budget_failure_is_never_reissued() {
    let over = Error::OverBudget {
        detail: "the query ran past its 5000 ms budget".into(),
    };
    let generous = |kernel: Kernel| kernel.with_retry_budget(10, 10);

    let (space, seen) = failing(&over);
    let kernel = generous(Kernel::new(Arc::new(
        Retry::new(space, 10).with_max_wait(Duration::from_secs(60)),
    )));
    assert_eq!(read(&kernel).unwrap_err(), over);
    assert_eq!(seen.load(Ordering::SeqCst), 1, "Retry: once");

    let (primary, tried) = failing(&over);
    let (backup, served) = failing_for(&over, 0);
    let kernel = generous(Kernel::new(Arc::new(Failover::new(vec![
        Arc::new(primary) as Arc<dyn Space>,
        Arc::new(backup),
    ]))));
    assert_eq!(read(&kernel).unwrap_err(), over);
    assert_eq!(
        (tried.load(Ordering::SeqCst), served.load(Ordering::SeqCst)),
        (1, 0),
        "Failover: another target spends the same"
    );

    // The README's stack, under a breaker that would trip on one transient failure.
    let (space, seen) = failing(&over);
    let stack = Retry::new(
        CircuitBreaker::new(Retry::new(space, 10), 1, Duration::from_secs(30)),
        10,
    );
    let kernel = generous(Kernel::new(Arc::new(stack)));
    for _ in 0..3 {
        assert_eq!(read(&kernel).unwrap_err(), over, "never an open circuit");
    }
    assert_eq!(
        seen.load(Ordering::SeqCst),
        3,
        "once per call, never repeated"
    );
}

/// RateLimit answers RetryAfter with the time to the window's next opening: two per
/// 100 ms, hit at 0 and 30; at 40 the oldest hit ages out at 100, so 60 ms to wait.
#[test]
fn a_rate_limit_answers_the_time_to_its_next_opening() {
    let clock = Manual::at(0);
    let (space, _) = failing_for(&Error::Unavailable("unused".into()), 0);
    let kernel = Kernel::new(Arc::new(
        RateLimit::new(space)
            .limit("urn:", Rate::new(2, Duration::from_millis(100)))
            .with_clock(Arc::new(clock.clone())),
    ));
    assert!(read(&kernel).is_ok());
    clock.advance(Duration::from_millis(30));
    assert!(read(&kernel).is_ok());
    clock.advance(Duration::from_millis(10));
    let err = read(&kernel).unwrap_err();
    assert_eq!(err.class(), FailureClass::RetryAfter, "{err:?}");
    assert_eq!(err.retry_after_millis(), Some(60), "{err}");
    assert_eq!(
        err.to_string(),
        "retry after 60 ms: rate-limited: `urn:` is capped at 2 per 100ms"
    );
}

/// A Retry above a RateLimit waits the limit out and gets through: the over-budget
/// stand-in re-checks the window when the waited retry re-invokes it.
#[test]
fn a_retry_waits_out_a_rate_limit_and_gets_through() {
    let clock = Manual::at(0);
    let timer = Advancing::on(&clock);
    let (space, seen) = failing_for(&Error::Unavailable("unused".into()), 0);
    let limited = RateLimit::new(space)
        .limit("urn:", Rate::new(1, Duration::from_millis(100)))
        .with_clock(Arc::new(clock.clone()));
    let retry = Retry::new(limited, 2)
        .with_timer(Arc::new(timer.clone()))
        .with_max_wait(Duration::from_secs(1));
    let kernel = Kernel::new(Arc::new(retry)).with_clock(Arc::new(clock.clone()));
    assert!(read(&kernel).is_ok());
    clock.advance(Duration::from_millis(25));
    assert!(read(&kernel).is_ok(), "waited 75 ms, then admitted");
    assert_eq!(timer.waits(), [Duration::from_millis(75)]);
    assert_eq!(
        seen.load(Ordering::SeqCst),
        2,
        "each admitted call reached the endpoint"
    );
}

/// A Retry above a CircuitBreaker waits the cooldown out instead of spending its attempts
/// on a circuit it cannot close, and the half-open probe gets through.
#[test]
fn a_retry_waits_out_an_open_circuit() {
    let clock = Manual::at(0);
    let timer = Advancing::on(&clock);
    let (space, seen) = failing_for(&Error::Unavailable("blip".into()), 1);
    let breaker = CircuitBreaker::new(space, 1, Duration::from_millis(100))
        .with_clock(Arc::new(clock.clone()));
    let retry = Retry::new(breaker, 3)
        .with_timer(Arc::new(timer.clone()))
        .with_max_wait(Duration::from_secs(1));
    let kernel = Kernel::new(Arc::new(retry)).with_clock(Arc::new(clock));
    assert!(read(&kernel).is_ok(), "blip, open circuit, wait, probe");
    assert_eq!(timer.waits(), [Duration::from_millis(100)]);
    assert_eq!(seen.load(Ordering::SeqCst), 2, "the blip and the probe");
}
