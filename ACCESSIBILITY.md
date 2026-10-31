# Accessibility

What `ikigai-throttle` emits that people read, the rules that output follows, how it is tested,
and how to report a barrier. It is not a conformance claim. Adapted from the
[ACCESSIBILITY.md template](https://github.com/mgifford/ACCESSIBILITY.md) (MIT).

| Field | Value |
|---|---|
| Target | WCAG 2.2 Level AA, for any page that presents this output; this repository writes text only (no HTML, SVG or color) |
| Status | Evaluation in progress. No conformance claim. |
| Owner | the maintainers of this repository |
| Report a barrier | [a GitHub issue](https://github.com/ikigai-rs/ikigai-throttle/issues/new?labels=accessibility) with the `accessibility` label; anything security-sensitive through [private vulnerability reporting](https://github.com/ikigai-rs/ikigai-throttle/security/advisories/new) |
| Last reviewed | 2026-10-31 |

## What it emits that people read

This is a library of overlays a host stacks around its spaces.

- **The state face** (`src/state.rs`): `urn:throttle:state` and the names under it answer
  `text/plain` by default, one line per governor and an indented line per key, for example
  `breaker: circuit-breaker, governs urn:iki:space:x, trips after 2, cooldown 30s` and
  ``circuit `urn:x:down`: open, 2 consecutive transient failures``. `as=text/turtle` answers the
  same state as RDF in the `thr:` vocabulary.
- **Refusals** the overlays answer in place of the governed call (`src/lib.rs`,
  `src/timeout.rs`): ``rate-limited: `urn:x:` is capped at 3 per 10s`` and ``circuit open for
  `urn:x` `` (both `RetryAfter`, whose display core prefixes with the delay), a timeout
  (`` `urn:x` exceeded 500ms ``), `no failover target resolved the request`, and, when a host
  gave an overlay no clock or timer, a sentence naming the builder that installs one
  (`install one with RateLimit::with_clock`).
- **Refusals from the state face**: an unknown face (`as=`), a governor name that is not
  allowed, or one already observed, each an `InvalidArgument` naming the argument with a
  sentence; a governor that does not exist or that the caller may not read is the same
  `NotFound` (`no governor state at …`).
- **Terminal text** from the example only, `examples/throttle-demo.rs`: plain `println!` lines,
  no color.

Not this repository's: how a host shows any of this, and the display of core's error kinds.

## The rules that output follows

Verified by reading the source:

- Every state is a word in the text face (`open`, `half-open`, `closed`, `admitting`, `full`,
  `answered`, `failed`) and a term in the Turtle face; a count is a number with its noun
  (`2 consecutive transient failures`, singular for one).
- A refusal names what it is about (the prefix, the target, the governor name or the argument)
  and, where the host can fix it, how (`install one with …::with_clock`, the accepted faces).
- No color, no terminal control sequences and no markup are written.

## How it is tested

- `tests/state.rs`: `the_board_in_text` pins the whole text face byte for byte, and
  `a_circuit_trips_half_opens_and_closes` pins the `half-open` line; `a_governor_name_is_checked`
  covers the name refusal; `an_ungranted_caller_learns_nothing` and
  `a_governor_the_caller_may_not_read_is_not_found` pin that the refusals disclose nothing.
- `tests/failure_classes.rs` pins the rate-limit refusal's full display
  (`retry after 60 ms: rate-limited: …`); `tests/conformance.rs` and the unit tests in
  `src/lib.rs` check that refusals say `rate-limited`, `circuit open` or `timeout`.

These test wording for correctness; none was written as an accessibility test.
**No automated accessibility checker, browser or assistive technology has been used on this
output.**

## Known limitations

Found by reading the source on 2026-10-31:

- **Nesting is shown by indentation alone.** In the text face a key belongs to the governor
  above it only because its line starts with two spaces; a screen reader usually does not
  announce leading spaces, so the grouping is carried by order.
- **Durations are abbreviated** (`30s`, `500ms`), which a screen reader may read as a number and
  a letter.
- **The bulkhead line does not say its state in a word.** A window line ends `admitting` or
  `full`; a bulkhead line (`0 of 2 running, 0 parked`) leaves `full` to be worked out, although
  the Turtle face states it.
- A timeout says only that the budget was exceeded, not that the failure is transient and may be
  retried.
- Every message is English only.

## Contributing

Read this file before changing any output listed above: the state face or a refusal. Keep every
state a word, keep meaning in words, and name what a refusal is about. A change to the text face
updates `the_board_in_text` on purpose. Say in the pull request what was tested, with what, and
what was not.

## Reporting a barrier

[Open an issue](https://github.com/ikigai-rs/ikigai-throttle/issues/new?labels=accessibility)
with the `accessibility` label (the link sets it when GitHub lets you add labels; otherwise a
maintainer adds it): what you were doing, what happened, and the host and any assistive
technology you used, if you are comfortable sharing them. Anything security-sensitive goes
through [private vulnerability reporting](https://github.com/ikigai-rs/ikigai-throttle/security/advisories/new).
**We acknowledge a report within 7 days**, and promise nothing further.

## Change history

| Date | Change |
|---|---|
| 2026-10-31 | First version. |
