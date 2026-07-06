# ikigai-throttle

Rate-limiting as an **interception overlay** for [ikigai](https://github.com/ikigai-rs).

`Throttle` wraps *any* [`Space`] and caps how often resources under a URI prefix
may resolve. Over budget, a request resolves not to its real endpoint but to a
**throttled endpoint** that returns an honest `throttled — retry after N` error
on invoke. Everything else passes straight through.

```rust
use ikigai_throttle::{Throttle, Rate};
use std::time::Duration;

let space = Throttle::new(inner)
    .limit("urn:system:exec", Rate::new(3, Duration::from_secs(10)))
    .limit("urn:httpGet",     Rate::new(30, Duration::from_secs(60)));
// Kernel::new(Arc::new(space))
```

Longest-prefix wins; an unmatched target is never throttled; `Meta`
(self-description) is exempt — an agent must always be able to read what it may
or may not invoke. The overlay is transparent to enumeration, so the
catalog/manifold sees the wrapped bindings unchanged.

## The interception primitive

This is the **first instance** of ikigai's interception overlay — the same
Space-decorator shape will carry logging, egress filtering, and load-balancing.
The motivating use is a standing server (a dev server, a background dreamer, a
red-team agent) where a runaway or buggy agent must not hammer `urn:system:exec`
or a remote API through the substrate.

`cargo run --example throttle-demo` watches a runaway loop hit the wall:

```
budget: 3 exec calls / 10s

  call 1: (ran)
  call 2: (ran)
  call 3: (ran)
  call 4: BLOCKED — throttled: `urn:system:exec` is capped at 3/10s — retry after 10s
```

Native crate (it keeps a sliding window of `Instant`s); a wasm face would inject
a clock — a later refinement.

## License
MIT OR Apache-2.0.
