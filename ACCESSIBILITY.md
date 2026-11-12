# Accessibility

What `ikigai-throttle` emits that people read, the rules that output follows, how it is tested,
and how to report a barrier. It is not a conformance claim. Adapted from the
[ACCESSIBILITY.md template](https://github.com/mgifford/ACCESSIBILITY.md) (MIT).

| Field | Value |
|---|---|
| Target | WCAG 2.2 Level AA, for any page that presents this output; this repository emits text and RDF (no HTML, SVG or color) |
| Status | Evaluation in progress. No conformance claim. |
| Owner | the maintainers of this repository |
| Report a barrier | [a GitHub issue](https://github.com/ikigai-rs/ikigai-throttle/issues/new?labels=accessibility) with the `accessibility` label; anything security-sensitive through [private vulnerability reporting](https://github.com/ikigai-rs/ikigai-throttle/security/advisories/new) |
| Last reviewed | 2026-11-12 |

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
  (`install one with RateLimit::with_clock`). Retry also refuses a missing clock or
  timer, naming `Retry::with_clock` and `Retry::with_timer`. When Retry gives up it
  answers core's `GaveUp`, whose display core writes (`gave up after 3 attempts over
  30 ms: unavailable: upstream down`, singular for one attempt) and which carries the
  last error's message; when the overall limit cut a call short before any error, that
  message names the target (`` retry: `urn:x` exceeded its overall time limit ``).
- **Refusals from the state face**: an unknown face (`as=`), a governor name that is not
  allowed, or one already observed, each an `InvalidArgument` naming the argument with a
  sentence; a governor that does not exist or that the caller may not read is the same
  `NotFound` (`no governor state at …`).
- **Terminal text** from the example only, `examples/throttle-demo.rs`: plain `println!` lines,
  no color.

Not this repository's: how a host shows any of this, and the display of core's error kinds.

## The rules that output follows

Output rules, with current results and exceptions in the table below:

- Circuit, window and route states are words in the text face (`open`, `half-open`,
  `closed`, `admitting`, `full`, `answered`, `failed`) and terms in the Turtle face; a count is a number with its noun
  (`2 consecutive transient failures`, singular for one).
- Rate-limit, circuit-open, timeout and state-face argument refusals name what they are
  about (the prefix, target, governor name or argument). Missing-facility refusals name the
  builder that installs it (`install one with …::with_clock`); an unknown face lists the
  accepted faces. The generic failover refusal names no target, and Retry may carry an
  upstream error message.
- Built-in text messages use no color, terminal control sequences or markup. Caller-provided
  strings and upstream error details are not covered by that statement.

## How it is tested

Every named check has a result: Pass, Fail or Blocked, with evidence. A Pass covers only
that check, not overall accessibility. The checks below were run or reviewed on 2026-11-12
against main commit `7812f4799dba44773079774b1c527bd458a498d5`, before this documentation
change. Commands used Cargo 1.99.0 (5f94df478), rustc 1.99.0 (b940084d7), on Linux.
`cargo test --all-targets` passed (one ignored measurement, not an accessibility check).
The tests check wording and behavior; none was written as an accessibility evaluation.
CI repeats the Rust tests, but does not repeat this source review or the example run.

| Check | Result | Evidence |
|---|---|---|
| Text state face, counts and circuit state words | Pass | `cargo test --all-targets`: `tests/state.rs` tests `the_board_in_text` and `a_circuit_trips_half_opens_and_closes` pin text, singular/plural failure counts and half-open state. Covers fixture output, not how a host presents it. |
| Turtle state terms and vocabulary | Pass | Same command: `tests/state.rs` tests `the_board_in_turtle`, `every_term_is_described` and `every_term_the_face_writes_is_defined_as_written` check RDF and vocabulary. Does not test a rendered host interface. |
| Governor-name and nondisclosing state refusals | Pass | Same command: `tests/state.rs` tests `a_governor_name_is_checked`, `an_ungranted_caller_learns_nothing` and `a_governor_the_caller_may_not_read_is_not_found`. Checks fixture errors, not assistive technology usability. |
| Rate-limit, circuit-open and timeout refusals | Pass | Same command: `tests/failure_classes.rs`, `tests/conformance.rs` and unit tests in `src/lib.rs` check typed refusals and wording, including the rate-limit full display. Does not establish clarity for every reader. |
| Retry final failure, overall-limit target and missing timing facility guidance | Pass | Same command: `tests/retry_policy.rs` and unit tests in `src/lib.rs` and `src/timeout.rs` check typed `GaveUp`, attempt count, elapsed time, full display, target naming and builder guidance. Injected clocks/timers, no browser. |
| Half-open concurrent-probe refusal | Pass | Same command: `tests/breaker_concurrency.rs` checks typed `RetryAfter` with unknown delay (`None`) while a probe runs. Does not test its announcement. |
| State-face argument guidance and built-in text without color or terminal controls | Pass | Source review of `src/state.rs` (`observe`, `face_of`, `absent`, text serialization), message construction in `src/lib.rs` and `src/timeout.rs`, and `examples/throttle-demo.rs` on the main commit above. Built-in text only; caller strings and upstream errors can contain other content. This is a current manual review, not a CI accessibility gate. |
| Example terminal output | Pass | `cargo run --example throttle-demo`, same toolchain and date: three successes and two refusals with target, rate and delay in words/numbers, no color or redraw. Captured text only; no terminal or assistive technology usability evaluation. |
| Text grouping independent of indentation | Fail | Current source review of `src/state.rs::text` and the pinned `the_board_in_text` fixture: key lines belong to the governor above by order and two-space indentation; they do not repeat the governor name. No screen reader was used. |
| Expanded duration wording | Fail | Current source review and `the_board_in_text` fixture use `30s`; timeout construction in `src/timeout.rs` uses `ms`. Pronunciation has not been tested. |
| Explicit bulkhead state word | Fail | `tests/state.rs::a_bulkhead_shows_running_and_parked_work` and `src/state.rs::key_node` show counts without an explicit admitting/full word in text; Turtle has the state term. |
| Timeout text explains transient retryability | Fail | Current review of `src/timeout.rs::elapsed`: text only says the target exceeded its budget; the transient failure class is typed, not explained in the message. |
| Language coverage beyond English | Fail | Current source review of the output constructors above: built-in messages are English only. No localization evaluation was performed. |
| Automated accessibility checker (axe or equivalent) | Blocked | No checker has been run on this output; no rendered host interface or browser checker is configured here. Rust tests do not replace this check. |
| Browser rendering, zoom, reflow and contrast | Blocked | No browser evaluation was performed; this library provides text and RDF, and no host page was supplied for evaluation. |
| Manual keyboard | Blocked | No person performed a keyboard pass; no interactive host interface was available for this review. |
| Screen reader | Blocked | No screen reader was used; no screen reader session was available for this review. |
| Screen magnifier | Blocked | No magnifier was used; no magnifier session was available for this review. |
| Speech input | Blocked | No speech-input software was used; no speech-input session was available for this review. |
| Switch access | Blocked | No switch-access software was used; no switch-access session was available for this review. |
| Braille display | Blocked | No braille display was used; no braille hardware was available for this review. |

## Known limitations

Rechecked against the main commit above on 2026-11-12 by source review, not assistive technology:

- **Nesting is shown by indentation alone.** In the text face a key belongs to the governor
  above it only because its line starts with two spaces; leading spaces may not
  be announced by a screen reader, so the grouping is carried by order. This has not been
  tested with a screen reader here.
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
updates `the_board_in_text` on purpose. In the pull request, give every check a `Check | Result | Evidence`
table row with Pass, Fail or Blocked, the test or command and tool version, or why it could
not run. Include unrun assistive technology checks as Blocked. Historical runs that CI does
not repeat stay Blocked with their date until rerun; do not treat them as a current Pass.

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
| 2026-11-10 | Document retry overall-limit and missing-facility refusals. |
| 2026-11-11 | Retry's final failure is core's typed `GaveUp`. |
| 2026-11-12 | Record current checks as Pass, Fail or Blocked, with evidence and coverage limits; include concurrent-probe refusals. |
