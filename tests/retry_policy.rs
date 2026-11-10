//! Retry timing uses a mock clock and never sleeps in real time.
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::executor::block_on;
use ikigai_core::{
    ActionSpec, ArgRef, ArgSpec, Capability, Clock, Description, Endpoint, EndpointSpace, Error,
    Exact, Invocation, Iri, Kernel, ReprType, Representation, Request, Time, UriTemplate, Verb,
};
use ikigai_throttle::{CircuitBreaker, FullJitter, Jitter, Retry, Timer};

fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}
#[derive(Default)]
struct Manual(AtomicU64);
impl Manual {
    fn advance(&self, by: Duration) {
        self.0.fetch_add(by.as_millis() as u64, Ordering::SeqCst);
    }
}
impl Clock for Manual {
    fn now(&self) -> Time {
        Time::from_millis(self.0.load(Ordering::SeqCst))
    }
}
struct Advancing {
    clock: Arc<Manual>,
    waits: Mutex<Vec<Duration>>,
}
#[async_trait::async_trait]
impl Timer for Advancing {
    async fn sleep(&self, delay: Duration) {
        self.waits.lock().unwrap().push(delay);
        self.clock.advance(delay);
    }
}
#[derive(Default)]
struct Upper(Mutex<Vec<Duration>>);
impl Jitter for Upper {
    fn sample(&self, upper: Duration) -> Duration {
        self.0.lock().unwrap().push(upper);
        upper
    }
}
struct Flaky {
    calls: Arc<AtomicU32>,
    failures: u32,
    clock: Arc<Manual>,
    work: Duration,
    pending: bool,
    error: Error,
    contract: Option<Description>,
}
#[async_trait::async_trait]
impl Endpoint for Flaky {
    async fn invoke(&self, _: &Invocation<'_>) -> Result<Representation, Error> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        self.clock.advance(self.work);
        if self.pending {
            futures::future::pending::<()>().await;
        }
        if call < self.failures {
            Err(self.error.clone())
        } else {
            Ok(Representation::new(
                ReprType::new("text/plain"),
                b"ok".to_vec(),
            ))
        }
    }
    fn describe(&self) -> Description {
        self.contract
            .clone()
            .unwrap_or_else(|| Description::new("flaky"))
    }
}
struct Fixture {
    clock: Arc<Manual>,
    timer: Arc<Advancing>,
    calls: Arc<AtomicU32>,
}
impl Fixture {
    fn new() -> Self {
        let clock = Arc::new(Manual::default());
        Self {
            timer: Arc::new(Advancing {
                clock: clock.clone(),
                waits: Mutex::default(),
            }),
            clock,
            calls: Arc::default(),
        }
    }
    fn space(&self, failures: u32, work: Duration, pending: bool, error: Error) -> EndpointSpace {
        EndpointSpace::new().bind(
            Exact::new("urn:dep"),
            Flaky {
                calls: self.calls.clone(),
                failures,
                clock: self.clock.clone(),
                work,
                pending,
                error,
                contract: None,
            },
        )
    }
    /// A Sink at `target` (an `Exact` IRI or a template) that fails `failures` times,
    /// under `contract`'s declared resend safety.
    fn sink(
        &self,
        target: impl ikigai_core::Grammar + 'static,
        failures: u32,
        contract: ActionSpec,
    ) -> Retry<EndpointSpace> {
        let space = EndpointSpace::new().bind(
            target,
            Flaky {
                calls: self.calls.clone(),
                failures,
                clock: self.clock.clone(),
                work: Duration::ZERO,
                pending: false,
                error: down(),
                contract: Some(Description::new("flaky").action(contract)),
            },
        );
        Retry::new(space, 5)
            .with_clock(self.clock.clone())
            .with_timer(self.timer.clone())
    }
    fn retry(&self, failures: u32, attempts: u32) -> Retry<EndpointSpace> {
        Retry::new(
            self.space(failures, Duration::ZERO, false, down()),
            attempts,
        )
        .with_clock(self.clock.clone())
        .with_timer(self.timer.clone())
    }
    fn kernel(&self, retry: Retry<EndpointSpace>) -> Kernel {
        Kernel::new(Arc::new(retry)).with_clock(self.clock.clone())
    }
    fn waits(&self) -> Vec<Duration> {
        self.timer.waits.lock().unwrap().clone()
    }
    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }
}
fn down() -> Error {
    Error::Unavailable("upstream down".into())
}
fn request(verb: Verb) -> Request {
    Request::new(verb, Iri::parse("urn:dep").unwrap())
}
fn read(kernel: &Kernel, body: bool) -> Result<(), Error> {
    if body {
        block_on(kernel.issue_body(request(Verb::Source), &Capability::root())).map(|_| ())
    } else {
        block_on(kernel.issue(request(Verb::Source), &Capability::root())).map(|_| ())
    }
}
#[test]
fn transient_retries_wait_instead_of_hammering_the_dependency() {
    let f = Fixture::new();
    let kernel = f.kernel(f.retry(2, 3));
    assert!(read(&kernel, false).is_ok());
    assert_eq!(
        f.waits().len(),
        2,
        "each transient retry must use the timer"
    );
}
#[test]
fn backoff_doubles_and_stays_at_the_cap_on_both_faces() {
    for body in [false, true] {
        let f = Fixture::new();
        let jitter = Arc::new(Upper::default());
        let kernel = f.kernel(
            f.retry(4, 5)
                .with_backoff(ms(10), ms(25))
                .with_jitter(jitter.clone()),
        );
        assert!(read(&kernel, body).is_ok());
        assert_eq!(f.waits(), [ms(10), ms(20), ms(25), ms(25)]);
        assert_eq!(*jitter.0.lock().unwrap(), f.waits());
        assert_eq!(f.calls(), 5);
    }
}
#[test]
fn full_jitter_is_repeatable_and_spans_the_inclusive_range() {
    let a = FullJitter::seeded(42);
    let b = FullJitter::seeded(42);
    let mut seen = [false; 11];
    for _ in 0..1000 {
        let delay = a.sample(ms(10));
        assert_eq!(delay, b.sample(ms(10)));
        assert!(delay <= ms(10));
        seen[delay.as_millis() as usize] = true;
    }
    assert!(seen.into_iter().all(|value| value));
    assert_eq!(a.sample(Duration::ZERO), Duration::ZERO);
    assert!(a.sample(Duration::MAX) <= Duration::MAX);
}
#[test]
fn overall_limit_counts_all_waits_and_starts_no_attempt_at_the_deadline() {
    for body in [false, true] {
        let f = Fixture::new();
        let kernel = f.kernel(
            f.retry(u32::MAX, 10)
                .with_backoff(ms(10), ms(100))
                .with_jitter(Arc::new(Upper::default()))
                .with_time_limit(ms(35)),
        );
        let err = read(&kernel, body).unwrap_err();
        assert!(matches!(err, Error::Timeout(_)), "{err:?}");
        assert!(err.to_string().contains("urn:dep"));
        assert_eq!(f.waits(), [ms(10), ms(20), ms(5)]);
        assert_eq!(f.calls(), 3);
        assert_eq!(f.clock.now(), Time::from_millis(35));
    }
}
#[test]
fn overall_limit_includes_work_and_bounds_a_pending_attempt() {
    for body in [false, true] {
        for pending in [false, true] {
            let f = Fixture::new();
            let space = f.space(
                u32::MAX,
                if pending { Duration::ZERO } else { ms(12) },
                pending,
                down(),
            );
            let retry = Retry::new(space, 10)
                .with_clock(f.clock.clone())
                .with_timer(f.timer.clone())
                .with_time_limit(ms(25))
                .with_backoff(ms(10), ms(100))
                .with_jitter(Arc::new(Upper::default()));
            let kernel = f.kernel(retry);
            assert!(matches!(read(&kernel, body), Err(Error::Timeout(_))));
            assert_eq!(f.calls(), if pending { 1 } else { 2 });
            assert_eq!(f.waits(), if pending { vec![ms(25)] } else { vec![ms(10)] });
        }
    }
}
#[test]
fn retry_after_is_a_minimum_not_shortened_by_jitter_or_cap() {
    let f = Fixture::new();
    let space = f.space(
        2,
        Duration::ZERO,
        false,
        Error::RetryAfter {
            delay: Some(ms(15)),
            detail: "limited".into(),
        },
    );
    let retry = Retry::new(space, 3)
        .with_clock(f.clock.clone())
        .with_timer(f.timer.clone())
        .with_max_wait(ms(100))
        .with_backoff(ms(10), ms(20))
        .with_jitter(Arc::new(Upper::default()));
    assert!(read(&f.kernel(retry), false).is_ok());
    assert_eq!(f.waits(), [ms(15), ms(20)]);
}
#[test]
fn a_sink_is_never_repeated_even_if_a_caller_supplies_a_key_without_a_contract() {
    for keyed in [false, true] {
        let f = Fixture::new();
        let kernel = f.kernel(f.retry(u32::MAX, 5));
        let mut req = request(Verb::Sink);
        if keyed {
            req = req.with_arg(
                "idempotency_key",
                ikigai_core::ArgRef::Inline(b"example".to_vec()),
            );
        }
        assert_eq!(
            block_on(kernel.issue(req, &Capability::root())).unwrap_err(),
            down()
        );
        assert_eq!(f.calls(), 1);
        assert!(f.waits().is_empty());
    }
}
#[test]
fn one_shared_breaker_opens_across_hosts_and_recovers_after_cooldown() {
    let f = Fixture::new();
    let other = EndpointSpace::new().bind(
        Exact::new("urn:other"),
        ikigai_core::FnEndpoint::new("healthy", |_| {
            Ok(Representation::new(
                ReprType::new("text/plain"),
                b"ok".to_vec(),
            ))
        }),
    );
    let spaces = ikigai_core::Fallback::new(vec![
        Arc::new(f.space(1, Duration::ZERO, false, down())),
        Arc::new(other),
    ]);
    let breaker = Arc::new(CircuitBreaker::new(spaces, 1, ms(50)).with_clock(f.clock.clone()));
    let mounts = || {
        ikigai_core::Fallback::new(vec![
            Arc::new(ikigai_core::Mount::new("urn:dep", breaker.clone())),
            Arc::new(ikigai_core::Mount::new("urn:other", breaker.clone())),
        ])
    };
    let a = Kernel::new(Arc::new(mounts()));
    let b = Kernel::new(Arc::new(mounts()));
    assert_eq!(read(&a, false).unwrap_err(), down());
    assert!(matches!(read(&b, false), Err(Error::RetryAfter { .. })));
    assert_eq!(f.calls(), 1);
    assert!(
        block_on(b.issue(
            Request::new(Verb::Source, Iri::parse("urn:other").unwrap()),
            &Capability::root()
        ))
        .is_ok(),
        "another target has its own circuit"
    );
    f.clock.advance(ms(50));
    assert!(read(&b, false).is_ok());
    assert!(read(&a, false).is_ok());
    assert_eq!(f.calls(), 3);
}

#[test]
fn a_late_ready_success_does_not_escape_the_overall_limit() {
    let f = Fixture::new();
    let space = f.space(0, ms(30), false, down());
    let retry = Retry::new(space, 3)
        .with_clock(f.clock.clone())
        .with_timer(f.timer.clone())
        .with_time_limit(ms(25));
    assert!(matches!(
        read(&f.kernel(retry), false),
        Err(Error::Timeout(_))
    ));
    assert_eq!(f.calls(), 1);
    assert!(f.waits().is_empty());
}

#[test]
fn an_initial_delay_above_the_maximum_is_clamped() {
    let f = Fixture::new();
    let kernel = f.kernel(
        f.retry(1, 2)
            .with_backoff(ms(100), ms(5))
            .with_jitter(Arc::new(Upper::default())),
    );
    assert!(read(&kernel, false).is_ok());
    assert_eq!(f.waits(), [ms(5)]);
}

fn keyed_sink() -> ActionSpec {
    ActionSpec::new(Verb::Sink)
        .input(ArgSpec::new("key").optional())
        .resend_safe_with_key("key")
}

fn sink(kernel: &Kernel, key: Option<&[u8]>) -> Result<(), Error> {
    let mut req = request(Verb::Sink);
    if let Some(key) = key {
        req = req.with_arg("key", ArgRef::Inline(key.to_vec()));
    }
    block_on(kernel.issue(req, &Capability::root())).map(|_| ())
}

#[test]
fn a_sink_declared_safe_under_a_key_is_repeated_when_the_call_carries_it() {
    let f = Fixture::new();
    let kernel = f.kernel(f.sink(Exact::new("urn:dep"), 2, keyed_sink()));
    assert!(sink(&kernel, Some(b"order-7")).is_ok());
    assert_eq!(
        f.calls(),
        3,
        "two transient failures, then the keyed resend lands"
    );
    assert_eq!(f.waits().len(), 2, "each resend waits its backoff");
}

#[test]
fn a_keyed_sink_without_its_key_is_never_repeated() {
    for key in [None, Some(&b""[..]), Some(&b"  "[..])] {
        let f = Fixture::new();
        let kernel = f.kernel(f.sink(Exact::new("urn:dep"), 2, keyed_sink()));
        assert_eq!(sink(&kernel, key).unwrap_err(), down(), "key {key:?}");
        assert_eq!(f.calls(), 1, "key {key:?}");
        assert!(f.waits().is_empty());
    }
}

#[test]
fn a_sink_declared_safe_outright_is_repeated() {
    let f = Fixture::new();
    let kernel = f.kernel(f.sink(
        Exact::new("urn:dep"),
        2,
        ActionSpec::new(Verb::Sink).resend_safe(),
    ));
    assert!(sink(&kernel, None).is_ok());
    assert_eq!(f.calls(), 3);
}

#[test]
fn a_sink_that_declares_nothing_is_never_repeated() {
    let f = Fixture::new();
    let kernel = f.kernel(f.sink(Exact::new("urn:dep"), 2, ActionSpec::new(Verb::Sink)));
    assert_eq!(sink(&kernel, Some(b"order-7")).unwrap_err(), down());
    assert_eq!(f.calls(), 1);
}

#[test]
fn a_permanent_failure_of_a_safe_sink_is_not_repeated() {
    let f = Fixture::new();
    let space = EndpointSpace::new().bind(
        Exact::new("urn:dep"),
        Flaky {
            calls: f.calls.clone(),
            failures: 2,
            clock: f.clock.clone(),
            work: Duration::ZERO,
            pending: false,
            error: Error::Endpoint("bad".into()),
            contract: Some(
                Description::new("flaky").action(ActionSpec::new(Verb::Sink).resend_safe()),
            ),
        },
    );
    let retry = Retry::new(space, 5)
        .with_clock(f.clock.clone())
        .with_timer(f.timer.clone());
    assert!(sink(&f.kernel(retry), None).is_err());
    assert_eq!(f.calls(), 1);
}

#[test]
fn a_key_captured_from_the_target_counts_as_given() {
    let contract = ActionSpec::new(Verb::Sink)
        .input(ArgSpec::new("key").binding())
        .resend_safe_with_key("key");
    let f = Fixture::new();
    let kernel = f.kernel(f.sink(UriTemplate::parse("urn:orders/{key}").unwrap(), 2, contract));
    let req = Request::new(Verb::Sink, Iri::parse("urn:orders/order-7").unwrap());
    assert!(block_on(kernel.issue(req, &Capability::root())).is_ok());
    assert_eq!(f.calls(), 3);
}
