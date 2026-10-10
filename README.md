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

Not to be confused with core's `Limit`. That is a structural carve-out: it removes
a family of names from a space, so they resolve as `Unresolved`, whatever the
traffic. `RateLimit` is a rate governor: the names stay bound and resolvable, and a
caller over the budget is refused for now, with a hint of when to come back.

Longest-prefix wins; an unmatched target is never rate-limited; `Meta`
(self-description) is exempt — an agent must always be able to read what it may or
may not invoke. The overlay is transparent to enumeration, so the catalog/manifold
sees the wrapped bindings unchanged.

Over budget, a resolution lands on a stand-in that refuses on invoke with a
permanent `Error::Endpoint` carrying the retry hint (a transient error would have a
`Retry` above re-issue straight into the limit). The stand-in **describes itself as
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
  call 4: BLOCKED — rate-limited: `urn:system:exec` is capped at 3/10s — retry after 10s
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
say (open circuit: transient `Unavailable`; elapsed budget: transient `Timeout`;
rate limit: permanent `Endpoint` with a retry hint) and none of them is ever
cached.

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
