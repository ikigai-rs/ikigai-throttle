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
| **`RateLimit`** | Reject resolutions over a per-URI-prefix rate (external politeness — a published rate you must not exceed). |
| **`Retry`** | Re-issue on a *transient* failure, up to N times — but only for an *idempotent* verb (a `Sink` is never blindly re-sent). |
| **`CircuitBreaker`** | Count consecutive transient failures per target; **trip open** after a threshold and **fail fast** for a cooldown (without touching the dependency), then **half-open** and probe to recover. |
| **`Failover`** | Try an ordered list `[primary, backup, …]`, advancing on a transient, idempotent failure. |
| **`Timeout`** | Bound an invocation; if the budget elapses, drop the work and return a transient timeout. |
| **`Throttle`** | Cap *concurrency* per prefix and **park** the excess until a slot frees (backpressure, never an error) — Nygard's Bulkhead. |

Two things every overlay reads:

- **the verb** — Source / Exists / Meta / Delete are idempotent, so `Retry` and
  `Failover` may re-issue them; a non-idempotent `Sink` is never re-sent (that
  needs an idempotency key, not a blind retry);
- **`Error::is_transient`** — timeouts and unavailability are worth retrying;
  a denial, a not-found, or a bad argument is permanent and returns immediately.

## Composing them

They nest, and the nesting *is* a resilience policy:

```rust
use ikigai_throttle::{CircuitBreaker, Failover, Retry, Timeout};
use std::time::Duration;
use std::sync::Arc;

// Bound each attempt, ride out blips on the primary, give up on it once it's a
// corpse, and fail over to a backup — every layer reading the same transient/
// permanent distinction.
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

## RateLimit

```rust
use ikigai_throttle::{RateLimit, Rate};
use std::time::Duration;

let space = RateLimit::new(inner)
    .limit("urn:system:exec", Rate::new(3, Duration::from_secs(10)))
    .limit("urn:httpGet",     Rate::new(30, Duration::from_secs(60)));
// Kernel::new(Arc::new(space))
```

Longest-prefix wins; an unmatched target is never rate-limited; `Meta`
(self-description) is exempt — an agent must always be able to read what it may or
may not invoke. The overlay is transparent to enumeration, so the catalog/manifold
sees the wrapped bindings unchanged.

`cargo run --example throttle-demo` watches a runaway loop hit the wall:

```
budget: 3 exec calls / 10s

  call 1: (ran)
  call 2: (ran)
  call 3: (ran)
  call 4: BLOCKED — rate-limited: `urn:system:exec` is capped at 3/10s — retry after 10s
```

The motivating use is a standing server (a dev server, a background dreamer, a
red-team agent) where a runaway or buggy agent must not hammer `urn:system:exec`
or a remote API through the substrate.

## Notes

- Native crate — `RateLimit`/`CircuitBreaker` keep a sliding window of `Instant`s;
  a wasm face would inject a clock (a later refinement).
- `Timeout` bounds *genuinely-async* work. A purely **synchronous blocking** call
  inside an invoke never yields to the executor, so a single-threaded runtime
  can't fire the timer; that hang is fixed at the transport (a socket read
  timeout), complementary to this overlay.
- Still to come: logging, egress-filtering, and load-balancing overlays. Same
  shape, every one.

## License

MIT OR Apache-2.0.
