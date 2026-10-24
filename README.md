# ikigai-throttle

**Reliability interception overlays** for [ikigai](https://github.com/ikigai-rs).

An *interception overlay* is a [`Space`] that wraps another `Space`, adding
cross-cutting behaviour to every resolution that flows through it without the
wrapped space knowing. It's the substrate's composition primitive turned to
reliability: the same shape you'd reach for as middleware, but as a resource
resolver you stack in front of anything — a leaf space, a `Fallback`, a remote
mount, or another overlay.

These are, in effect, **Michael Nygard's *Release It!* stability patterns as
resolver decorators** — Circuit Breaker, Timeouts, Bulkhead — expressed once and
composable everywhere.

## The family

| overlay | what it does |
| --- | --- |
| **`RateLimit`** | Refuse resolutions over a per-URI-prefix rate with `RetryAfter`, the time until the window opens (external politeness — a published rate you must not exceed). |
| **`Retry`** | Re-issue on a *transient* failure, or after a *retry-after* delay that fits the run, up to N times and within the run's retry budget — but only for an *idempotent* verb (a `Sink` is never blindly re-sent). |
| **`CircuitBreaker`** | Count consecutive transient failures per target; **trip open** after a threshold and **fail fast** with `RetryAfter` for the cooldown left (without touching the dependency), then **half-open** and probe to recover. |
| **`Failover`** | Try an ordered list `[primary, backup, …]`, advancing on a transient or retry-after failure of an idempotent verb. |
| **`Timeout`** | Bound an invocation; if the budget elapses, drop the work and return a transient timeout. |
| **`Throttle`** | Cap *concurrency* per prefix and **park** the excess until a slot frees (backpressure, never an error) — Nygard's Bulkhead. |

Three things every re-issuing overlay reads, all from core (none of them is a table
of this crate's):

- **the verb** — `Verb::is_idempotent`: Source / Exists / Meta / Delete are
  idempotent, so `Retry` and `Failover` may re-issue them; a non-idempotent `Sink` is
  never re-sent (that needs an idempotency key, not a blind retry);
- **the failure's class** — `Error::class()`, which says who may repeat a failure
  and when:

  | class received | `Retry` | `Failover` | `CircuitBreaker` |
  | --- | --- | --- | --- |
  | transient (`Timeout`, `Unavailable`) | re-issue | advance to the next target | counts toward tripping |
  | retry-after (`RetryAfter`) | wait the delay and re-issue, only when it fits | advance (another target is not limited) | not counted |
  | over-budget, denied, refused, permanent, unknown | returned unchanged | returned unchanged | not counted |

  A retry-after delay *fits* when it is at most the run's time left (a door set a
  deadline: `Invocation::time_left`), lowered by `Retry::with_max_wait` when that is
  set; with no deadline, `with_max_wait` alone is the bound, and unset it waits for
  nothing. The wait is on a `Timer` (`Retry::with_timer`; futures-timer by default
  on a native target, none on wasm32, where every `RetryAfter` is returned to the
  caller). An over-budget failure is never repeated by anything (under the same
  bound it costs the same and fails the same);
- **the run's retry budget** — when the host configured one
  (`Kernel::with_retry_budget`, ceilings granted as `urn:cap:retry:<n>`), every
  re-issue and every failover advance spends from ONE pool shared by the whole
  resolution tree, so nested `Retry` layers cannot multiply their attempts; with the
  pool spent, an overlay returns the error it received. `attempts` still caps each
  `Retry` layer. A kernel with no retry budget keeps each overlay's own `attempts`.

`Timeout` stays a transient `Timeout`: it cannot tell a wait on something slow (a
repeat may beat it) from an expensive computation (a repeat costs the same), so only
the work itself can say `OverBudget`.

## Transparent to identity

An overlay decorates an endpoint; it does not become a different resource. So
every one of these forwards what the resolution underneath reported — its
bindings **and** its [`Resolved::canonical`], the name a rewriting space actually
resolved under. That is what lets an `Alias` be composed *below* a governor:

```rust
// The `urn:store:` -> `urn:iki:store:` migration, rate-limited.
let space = RateLimit::new(Alias::new(migration, backing))
    .limit("urn:", Rate::new(100, Duration::from_secs(60)));
```

The kernel adopts the reported name before it derives the cache id, fires the
golden-thread cut and evaluates the capability floor, so the logical and the
backing name stay **one resource**: one cache entry, one thread — a `Sink`
through either invalidates the other. An overlay that rebuilt the resolution
instead of forwarding it would silently split them back into two names that
merely agree until one is written; `tests/canonical.rs` holds every overlay in
the family to it, and `just gates` will not let a seventh join without one.

`Failover` is the exception that has to be stated: it keeps *all* its targets and
defers the choice of which one answers to invoke — after the cache key is
derived. It reports a canonical only when **every** target agrees on one (mirrors
of a resource are that resource), and reports none when they disagree, rather
than speaking for a target that may never serve. For the same reason each target
is invoked on the variables **its own** grammar captured: candidates bound under
different patterns (`urn:x/{id}` beside `urn:x/{name}`) each receive the
arguments they declare, never the primary's.

## One identity per resource

A governor wraps the endpoint it resolved, and it keeps **one wrapper per inner
endpoint**: a governed resource resolves to the same endpoint `Arc` every time,
the over-budget stand-in included. The kernel memoizes each endpoint's capability
floor by that identity, so a governed read is described once, not on every
request; a fresh wrapper per resolution missed the memo every time. Measured on a
module-shaped cached read (a description with three `ArgSpec`s, release build):
`Timeout` 1257 ns before, 418 ns after; `Retry(Timeout)` 1380 ns before, 436 ns
after; the bare space 380 ns. A rebound endpoint always gets a new wrapper, and the
table is bounded and swept, so a space that builds a fresh endpoint per resolution
cannot grow it or keep dropped endpoints alive. `tests/identity.rs` pins it.

## Transparent to structure

The same holds for what a space says about itself. An overlay that encloses one
space reports that space's `topology()`, `id()` and `entries()`, so
`urn:kernel:topology`, explain, the diagram and a declared arrangement's harvest
see through a governor to the doors it guards, and a host can put a `Timeout`
around the one binding it means to bound instead of around its whole root. Stacks
compose: `Retry(CircuitBreaker(Timeout(space)))` reports `space`.

Forwarding the name is a claim (*any space with this name holds the same doors*),
and it is true here: a governor holds exactly the doors it wraps, and what it adds
is only ever a refusal (rate-limited, timed out, circuit open), which the kernel
never caches. A successful answer through a governor is the bare space's answer.
`tests/topology.rs` holds every overlay to it.

`Failover` is again the exception, and reports an opaque node with no name: it
encloses several spaces and chooses the answering one at invoke time, which no
core space kind states. Calling it a `Fallback` (first hit at *resolution*) would
read back as an arrangement that never reaches the backup.

## Composing them

They nest, and the nesting *is* a resilience policy:

```rust
use ikigai_throttle::{CircuitBreaker, Failover, Retry, Timeout};
use std::time::Duration;
use std::sync::Arc;

// Bound each attempt, ride out blips on the primary, give up on it once it's a
// corpse, and fail over to a backup — every layer reading the same failure class.
let primary = CircuitBreaker::new(
    Retry::new(Timeout::new(primary_space, Duration::from_secs(2)), 3),
    5,                          // trip after 5 consecutive transient failures
    Duration::from_secs(30),    // stay open 30s, then probe
);
let resilient = Failover::new(vec![Arc::new(primary), Arc::new(backup_space)]);
// Kernel::new(Arc::new(resilient))
```

`Retry` rides out a blip on the primary; `CircuitBreaker` gives up on it once
it's dead and fails fast; that trip-open is the instant trigger for `Failover` to
move to the backup.

## Observable state

What the governors hold is readable as resources, in a space of its own that the
host mounts **beside** the governed one, so the overlays stay transparent:

```rust
use ikigai_throttle::{CircuitBreaker, StateBoard};

let board = StateBoard::new();
let guarded = CircuitBreaker::new(inner, 5, Duration::from_secs(30))
    .observed(&board, "upstream")?;           // urn:throttle:state:upstream
let root = Fallback::new(vec![Arc::new(board.space()), Arc::new(guarded)]);
let kernel = Arc::new(Kernel::new(Arc::new(root)));
board.attach(&kernel);                        // answers become cacheable
```

| name | what it answers |
| --- | --- |
| `urn:throttle:state` | every governor the caller may read |
| `urn:throttle:state:{governor}` | one governor and its keys |
| `urn:throttle:state:{governor}:key:{key}` | one prefix's window or bulkhead, one target's circuit or failover route |
| `urn:throttle:state:{governor}:candidate:{n}` | a failover's `n`th space |

Each answers `text/plain` and `as=text/turtle` (the `thr:` vocabulary,
`https://ikigai-rs.dev/ns/throttle#`, skolemized, no blank nodes). A governor names
the space it governs by the `id()` it forwards (`thr:governs`), which is the IRI
`urn:kernel:topology` gives that node, so a diagram joins the state to the
arrangement it already draws. `RateLimit` shows each prefix's hits in the window,
`CircuitBreaker` each failed target's circuit (closed, open, half-open) and its
failure count, `Throttle` the work running and parked per prefix, `Failover` the
candidate each target's last call ended at. `Retry` and `Timeout` hold state for one
call only and have no face.

Reading a governor needs `urn:cap:throttle:state:{governor}`; every name declares the
family `urn:cap:throttle:state:*`. A caller with no grant under it is refused by the
kernel's floor the same way for every name, registered or not, and a governor the
caller may not read is `NotFound` exactly as one that does not exist: this is the
information the rate-limit floor once leaked (ledger #14), so its existence is not
disclosed either.

Without `attach` every answer is live. With it, an answer is cacheable and hangs from
the governor's thread `urn:throttle:governor:{governor}`, which the governor cuts on
each change its face shows; a change time alone makes (a hit ageing out, a cooldown
running out) sets the answer's deadline instead, so give the governors the kernel's
clock. A governor cuts only when its face was read since the last cut, so observing
costs an unread governor one atomic swap per change. Measured on one machine
(release, `cargo test --release --test state measure -- --ignored --nocapture`): a
four-governor board read costs about 15 µs live and 0.8 µs from the cache; a governed
call costs the same observed or not (0.8 µs); a cut costs about 0.2 µs.

## RateLimit

```rust
use ikigai_throttle::{RateLimit, Rate};
use std::time::Duration;

let space = RateLimit::new(inner)
    .limit("urn:system:exec", Rate::new(3, Duration::from_secs(10)))
    .limit("urn:httpGet",     Rate::new(30, Duration::from_secs(60)));
// Kernel::new(Arc::new(space))
```

Not to be confused with core's `Limit`. That is a structural carve-out: it removes
a family of names from a space, so they resolve as `Unresolved`, whatever the
traffic. `RateLimit` is a rate governor: the names stay bound and resolvable, and a
caller over the budget is refused for now, with a hint of when to come back.

Longest-prefix wins; an unmatched target is never rate-limited; `Meta`
(self-description) is exempt — an agent must always be able to read what it may or
may not invoke. The overlay is transparent to enumeration, so the catalog/manifold
sees the wrapped bindings unchanged.

Over budget, a resolution lands on a stand-in that refuses on invoke with
`Error::RetryAfter`, whose delay is the time until the window next opens (since
0.2.0; it was a permanent `Error::Endpoint` with the delay only in its text). An HTTP
edge answers it with 429 and `Retry-After`; a `Retry` above waits it out when the
delay fits the run, never re-issuing straight into the limit. A `Rate::new(0, …)`
prefix never opens, so its refusal carries no delay. The stand-in re-checks the
window when it is invoked, so a waited retry that finds the window open is admitted
(and charged). The stand-in **describes itself as
the wrapped endpoint**, so the kernel's capability floor holds whatever the budget
says: a caller with no grant for a gated resource is still `Denied`, and never
learns the prefix is limited. Two consequences of the limit acting at resolution,
which the kernel runs before its cache lookup: a representation already in the
cache is served to an over-budget caller (the resource the limit protects is not
touched), and a cache hit is charged against the window.

`cargo run --example throttle-demo` watches a runaway loop hit the wall:

```
budget: 3 exec calls / 10s

  call 1: (ran)
  call 2: (ran)
  call 3: (ran)
  call 4: BLOCKED — retry after 10000 ms: rate-limited: `urn:system:exec` is capped at 3 per 10s
  call 5: BLOCKED — retry after 10000 ms: rate-limited: `urn:system:exec` is capped at 3 per 10s
```

The motivating use is a standing server (a dev server, a background dreamer, a
red-team agent) where a runaway or buggy agent must not hammer `urn:system:exec`
or a remote API through the substrate.

## Conformance

Passes [`ikigai-conformance`](https://github.com/ikigai-rs/ikigai-conformance).
This crate binds no endpoint of its own, so `tests/conformance.rs` walks a small
conforming fixture space bare, through each overlay alone, and through the stack
above, and requires every report to be clean with the same shape — then pins by
hand what the walk cannot see: the catalog and every description through an
overlay are the wrapped space's verbatim; a threaded read is cached under the
wrapped endpoint's thread and cut by a `Sink` through the overlay, a live read stays
live, a pure read caches with no thread; every typed `Error` an endpoint raises
comes out of every overlay unchanged (`Retry` re-issues a transient one `attempts`
times and returns the last); each overlay's own refusal is typed as its semantics
say (open circuit: `RetryAfter` for the cooldown left; elapsed budget: transient
`Timeout`; rate limit: `RetryAfter` until the window opens) and none of them is ever
cached. `tests/failure_classes.rs` holds every failure class against `Retry`,
`Failover` and `CircuitBreaker`, the shared retry pool across nested layers, a
retry-after delay longer than the run's time left (not waited), and an over-budget
failure (never re-issued, ledger #972). `REQUIRED` runs on the `Sink` too, and a negative control (endpoints that
ignore a required input or refuse its absence untyped) is found through every
overlay exactly as it is bare.

## Notes

- **Time comes from a core `Clock`.** `RateLimit` (its window) and
  `CircuitBreaker` (its cooldown) read the clock given with `with_clock`, and
  default to a monotonic clock on a native target. Hand them the clock you gave
  your kernel, or a test clock that moves only when the test moves it. Time is
  read in whole milliseconds. Never a temporal corridor's pinned clock: a window
  measured on a stopped clock never slides.
- **wasm32-unknown-unknown:** every overlay but `Timeout` builds there (CI lints
  the lib on wasm32). There is no default clock on wasm32, so the host installs
  one, as it does for its kernel; a `RateLimit` or `CircuitBreaker` without one
  refuses what it governs with an error naming `with_clock`, and never panics.
  `Timeout` is native only: it needs a timer that wakes a future, which a clock
  is not, and futures-timer's native backend panics on wasm32.
- `Timeout` bounds *genuinely-async* work. A purely **synchronous blocking** call
  inside an invoke never yields to the executor, so a single-threaded runtime
  can't fire the timer; that hang is fixed at the transport (a socket read
  timeout), complementary to this overlay.
- Still to come: logging, egress-filtering, and load-balancing overlays. Same
  shape, every one.

## License

MIT OR Apache-2.0.
