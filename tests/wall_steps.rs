//! A wall-clock step moves neither a rate window's end nor a breaker cooldown's
//! (ledger #4302, core's time design). Each governor measures on a `MonotonicClock`
//! over a fake host source the test advances; the kernel's wall `Clock` steps forward
//! and backward beside it. On 0.7.0, where the governors read an epoch `Clock`, a host
//! wall clock stepping forward expired the window early and stepping backward held
//! the cooldown open past its elapsed deadline.

use futures::executor::block_on;
use ikigai_core::{
    Capability, Clock, EndpointSpace, Error, Exact, FnEndpoint, Iri, Kernel, MonotonicClock,
    ReprType, Representation, Request, Time, Verb,
};
use ikigai_throttle::{CircuitBreaker, Rate, RateLimit, StateBoard};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// A counter in milliseconds, shared by a fake host source and a fake wall.
#[derive(Clone)]
struct Counter(Arc<AtomicU64>);
impl Counter {
    fn at(millis: u64) -> Self {
        Counter(Arc::new(AtomicU64::new(millis)))
    }
    fn set(&self, millis: u64) {
        self.0.store(millis, Ordering::SeqCst);
    }
    /// Read as a host's elapsed source, the shape a wasm host adapts.
    fn host(&self) -> MonotonicClock {
        let ticks = Arc::clone(&self.0);
        MonotonicClock::from_host(move || Duration::from_millis(ticks.load(Ordering::SeqCst)))
    }
}
/// Read as an epoch wall clock, which NTP may step either way.
impl Clock for Counter {
    fn now(&self) -> Time {
        Time::from_millis(self.0.load(Ordering::SeqCst))
    }
}

fn read(kernel: &Kernel, target: &str) -> Result<Representation, Error> {
    block_on(kernel.issue(
        Request::new(Verb::Source, Iri::parse(target).unwrap()),
        &Capability::root(),
    ))
}

fn text(kernel: &Kernel, target: &str) -> String {
    String::from_utf8(read(kernel, target).unwrap().bytes).unwrap()
}

#[test]
fn a_wall_step_does_not_move_a_rate_windows_end() {
    let elapsed = Counter::at(0);
    let wall = Counter::at(1_700_000_000_000);
    let ok = FnEndpoint::new("x", |_| {
        Ok(Representation::new(
            ReprType::new("text/plain"),
            b"ok".to_vec(),
        ))
    });
    let board = StateBoard::new();
    let limit = RateLimit::new(EndpointSpace::new().bind(Exact::new("urn:x"), ok))
        .limit("urn:x", Rate::new(1, Duration::from_secs(10)))
        .with_clock(elapsed.host())
        .observed(&board, "limit")
        .unwrap();
    let spaces: Vec<Arc<dyn ikigai_core::Space>> = vec![Arc::new(board.space()), Arc::new(limit)];
    let kernel = Arc::new(
        Kernel::new(Arc::new(ikigai_core::Fallback::new(spaces)))
            .with_clock(Arc::new(wall.clone())),
    );
    board.attach(&kernel);
    let face = "urn:throttle:state:limit:key:urn:x";

    assert!(read(&kernel, "urn:x").is_ok());
    // A forward step of an hour, one real second: still one hit, still full.
    wall.set(1_700_000_000_000 + 3_600_000);
    elapsed.set(1_000);
    let limited = read(&kernel, "urn:x").unwrap_err();
    assert_eq!(limited.retry_after_millis(), Some(9_000), "{limited}");
    assert!(
        text(&kernel, face).contains("1 of 1 per 10s, full"),
        "{}",
        text(&kernel, face)
    );
    // A backward step of a day, nine more real seconds: the window's end is unmoved.
    wall.set(1_700_000_000_000 - 86_400_000);
    elapsed.set(9_999);
    assert_eq!(
        read(&kernel, "urn:x").unwrap_err().retry_after_millis(),
        Some(1)
    );
    elapsed.set(10_000);
    assert!(
        text(&kernel, face).contains("0 of 1 per 10s, admitting"),
        "{}",
        text(&kernel, face)
    );
    assert!(
        read(&kernel, "urn:x").is_ok(),
        "opens exactly a window after the hit"
    );
}

#[test]
fn a_wall_step_does_not_move_a_breaker_cooldowns_end() {
    let elapsed = Counter::at(0);
    let wall = Counter::at(1_700_000_000_000);
    let down = FnEndpoint::new("down", |_| Err(Error::Unavailable("down".into())));
    let board = StateBoard::new();
    let breaker = CircuitBreaker::new(
        EndpointSpace::new().bind(Exact::new("urn:dep"), down),
        1,
        Duration::from_secs(30),
    )
    .with_clock(elapsed.host())
    .observed(&board, "breaker")
    .unwrap();
    let spaces: Vec<Arc<dyn ikigai_core::Space>> = vec![Arc::new(board.space()), Arc::new(breaker)];
    let kernel = Arc::new(
        Kernel::new(Arc::new(ikigai_core::Fallback::new(spaces)))
            .with_clock(Arc::new(wall.clone())),
    );
    board.attach(&kernel);
    let face = "urn:throttle:state:breaker:key:urn:dep";

    assert_eq!(
        read(&kernel, "urn:dep").unwrap_err(),
        Error::Unavailable("down".into())
    );
    // Forward a day, one real second: still open, 29 s left.
    wall.set(1_700_000_000_000 + 86_400_000);
    elapsed.set(1_000);
    let open = read(&kernel, "urn:dep").unwrap_err();
    assert_eq!(open.retry_after_millis(), Some(29_000), "{open}");
    assert!(
        text(&kernel, face).contains(": open,"),
        "{}",
        text(&kernel, face)
    );
    // Backward an hour, the cooldown elapsed: half-open, and the probe goes through.
    wall.set(1_700_000_000_000 - 3_600_000);
    elapsed.set(30_000);
    assert!(
        text(&kernel, face).contains(": half-open,"),
        "{}",
        text(&kernel, face)
    );
    assert_eq!(
        read(&kernel, "urn:dep").unwrap_err(),
        Error::Unavailable("down".into()),
        "the cooldown ended when its elapsed time did, whatever the wall says"
    );
}
