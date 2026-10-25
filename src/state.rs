//! **The governors' observable state, as resources** (ledger #198).
//!
//! A governor keeps live state: [`RateLimit`](crate::RateLimit)'s per-prefix windows,
//! [`CircuitBreaker`](crate::CircuitBreaker)'s per-target circuits,
//! [`Throttle`](crate::Throttle)'s running and parked work, and the candidate that last
//! served each target through a [`Failover`](crate::Failover). This module serves that
//! state as resources, so a caller (a diagram, an operator, a test) can read it.
//!
//! [`Retry`](crate::Retry) and [`Timeout`](crate::Timeout) have nothing to show and no
//! face: every attempt count and deadline they hold lives for one call, and the retry
//! budget belongs to the run ([`Invocation::retry_budget`]), not to the overlay.
//!
//! # A separate space, never the overlay's own doors
//!
//! Every overlay stays transparent to identity, enumeration and topology (ledger #978,
//! `tests/topology.rs`): a governed space reports the doors it wraps and nothing more.
//! So the state is not a door on the overlay. A host makes a [`StateBoard`], names each
//! governor it wants observed onto it (`observed(&board, "name")`), and mounts
//! [`StateBoard::space`] BESIDE the governed space.
//!
//! # Names
//!
//! - `urn:throttle:state`: the board, every governor the caller may read.
//! - `urn:throttle:state:{governor}`: one governor and its keys. The name is the one the
//!   host gave `observed`, lowercase ASCII letters, digits and `-`.
//! - `urn:throttle:state:{governor}:key:{key}`: one key, in the governor's own terms: a
//!   [`RateLimit`](crate::RateLimit) or [`Throttle`](crate::Throttle) prefix, a
//!   [`CircuitBreaker`](crate::CircuitBreaker) or [`Failover`](crate::Failover) target.
//!   `{key}` is the key spelled as a name ([`encode_name`]).
//! - `urn:throttle:state:{governor}:candidate:{n}`: a [`Failover`](crate::Failover)'s
//!   `n`th space, from 0.
//!
//! Each answers `as=text/plain` (the default) and `as=text/turtle`. The Turtle names each
//! governed space by the [`Space::id`](ikigai_core::Space::id) the overlay forwards
//! (`thr:governs`), which is the IRI `urn:kernel:topology` gives the same node, so the
//! state joins to the arrangement with no change to topology. An anonymous space has no
//! such name, and its governor says nothing under `thr:governs`.
//!
//! # Authority, and why an ungranted caller learns nothing
//!
//! Reading a governor needs `urn:cap:throttle:state:{governor}` ([`state_cap`]); every
//! name here declares the family form [`STATE_CAP_FAMILY`], which the kernel's floor
//! enforces before the face runs. The state is what the rate-limit floor leak once told an
//! ungranted caller (ledger #14), so its EXISTENCE is sensitive too:
//!
//! - A caller holding no grant under the family is refused `Denied` by the floor for
//!   every name under `urn:throttle:state`, registered or not, so the refusal says
//!   nothing about which governors exist.
//! - A caller holding some grants reads the board as the governors it may read, and a
//!   governor or key it may not read is `NotFound` with the same message as one that does
//!   not exist. Root reads everything.
//!
//! # Invalidation
//!
//! With no cutter, every answer is live (uncacheable), which is always correct. Once the
//! host gives the board its kernel ([`StateBoard::attach`]), an answer is CACHEABLE and
//! hangs from each governor's thread, named after the governor
//! (`urn:throttle:governor:{governor}`, [`governor_thread`]). A governor cuts it on every change its face shows:
//! a hit charged to a window, a failure counted or a circuit tripped or closed, work
//! starting, parking or finishing, a failover ending at another candidate. Changes that
//! time alone makes (a hit ageing out of a window, a cooldown running out) cut nothing;
//! the answer's deadline is the soonest of them, measured on the governor's clock and set
//! on the kernel's, so give the governors the kernel's clock. A governor cuts only when
//! something has read its face since its last cut, so an unread board costs the governed
//! path one atomic swap per change.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, RwLock, Weak};
use std::time::Duration;

use ikigai_core::{
    encode_name, escape_iri_fragment, ArgRef, ArgSpec, Capability, Description, Endpoint,
    EndpointSpace, Error, Exact, Invocation, Iri, Kernel, ReprType, Representation, UriTemplate,
    Verb,
};

use crate::span;

/// The board: every governor observed on a [`StateBoard`], as the caller may read them.
pub const STATE: &str = "urn:throttle:state";

/// What every name under [`STATE`] declares: some grant under
/// `urn:cap:throttle:state:` (the parameterized family form). Which governors a caller
/// reads is then decided by [`state_cap`], one grant per governor.
pub const STATE_CAP_FAMILY: &str = "urn:cap:throttle:state:*";

/// The vocabulary of the Turtle face, `thr:`: its own published vocabulary, defined in
/// [`VOCABULARY_TTL`](crate::VOCABULARY_TTL) and served at
/// `https://ikigai-rs.dev/ns/throttle`, separate from core's `/ns` (ledger #2175).
pub const STATE_NS: &str = "https://ikigai-rs.dev/ns/throttle#";

/// The grant that reads one governor: `urn:cap:throttle:state:{governor}`.
pub fn state_cap(governor: &str) -> String {
    format!("urn:cap:throttle:state:{governor}")
}

/// The name of one governor's state: `urn:throttle:state:{governor}`.
pub fn governor_iri(governor: &str) -> String {
    format!("{STATE}:{governor}")
}

/// The golden thread a governor cuts on every change its face shows, and every answer
/// about it hangs from: `urn:throttle:governor:{governor}`. Named for the governor, not
/// for any one name of its face, since the board, the governor and each key all read the
/// same state. A host that wants to hear changes listens on `urn:throttle:governor:`.
pub fn governor_thread(governor: &str) -> String {
    format!("urn:throttle:governor:{governor}")
}

/// The longest governor name accepted.
const NAME_MAX: usize = 64;

/// A function that cuts a golden thread: the host's way back into its kernel.
type Cutter = Arc<dyn Fn(&str) + Send + Sync>;

/// Where governors are observed, and the space that serves what they hold.
///
/// ```
/// use std::{sync::Arc, time::Duration};
/// use futures::executor::block_on;
/// use ikigai_core::{Capability, EndpointSpace, Exact, Fallback, FnEndpoint, Iri, Kernel,
///     ReprType, Representation, Request, Space, Verb};
/// use ikigai_throttle::{Rate, RateLimit, StateBoard};
///
/// let ok = FnEndpoint::new("x", |_| Ok(Representation::new(ReprType::new("text/plain"), "ok")));
/// let board = StateBoard::new();
/// let governed = RateLimit::new(EndpointSpace::new().bind(Exact::new("urn:demo:x"), ok))
///     .limit("urn:demo:", Rate::new(3, Duration::from_secs(10)))
///     .observed(&board, "demo")
///     .unwrap();
/// // The state is a space of its own, mounted BESIDE the governed one.
/// let root: Vec<Arc<dyn Space>> = vec![Arc::new(board.space()), Arc::new(governed)];
/// let kernel = Arc::new(Kernel::new(Arc::new(Fallback::new(root))));
/// board.attach(&kernel);
///
/// let root = Capability::root();
/// let read = |iri: &str| block_on(kernel.issue(Request::new(Verb::Source, Iri::parse(iri).unwrap()), &root));
/// read("urn:demo:x").unwrap();
/// let state = read("urn:throttle:state:demo:key:urn:demo:").unwrap();
/// assert_eq!(
///     String::from_utf8(state.bytes).unwrap(),
///     "window `urn:demo:`: 1 of 3 per 10s, admitting\n",
/// );
/// ```
#[derive(Clone, Default)]
pub struct StateBoard {
    board: Arc<Board>,
}

#[derive(Default)]
struct Board {
    governors: RwLock<BTreeMap<String, Registration>>,
    cutter: RwLock<Option<Cutter>>,
}

#[derive(Clone)]
struct Registration {
    view: Arc<dyn Observed>,
    watch: Arc<Watch>,
}

impl Board {
    fn cutter(&self) -> Option<Cutter> {
        self.cutter
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn cut(&self, thread: &str) {
        if let Some(cut) = self.cutter() {
            cut(thread);
        }
    }

    /// The governors `capability` may read, by name.
    fn visible(&self, capability: &Capability) -> Vec<(String, Registration)> {
        self.governors
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|(name, _)| capability.allows(&state_cap(name)))
            .map(|(name, registration)| (name.clone(), registration.clone()))
            .collect()
    }
}

impl StateBoard {
    /// An empty board; observe governors onto it with their `observed` builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// The space that serves this board: `urn:throttle:state` and every name under it.
    /// Mount it beside the governed space. Anonymous: name it (`.named(..)`) if the host
    /// needs it nameable; a board is the host's instance, not a module's fixed space.
    pub fn space(&self) -> EndpointSpace {
        EndpointSpace::new()
            .bind(
                Exact::new(STATE),
                StateFace {
                    board: Arc::clone(&self.board),
                    member: false,
                },
            )
            .bind(
                UriTemplate::parse(format!("{STATE}:{{member}}"))
                    .expect("a literal prefix and one variable"),
                StateFace {
                    board: Arc::clone(&self.board),
                    member: true,
                },
            )
    }

    /// Cut this board's threads through `kernel`, so its answers can be cached. Holds the
    /// kernel weakly: the kernel holds the board (through the space) and a strong handle
    /// back would keep both alive forever.
    pub fn attach(&self, kernel: &Arc<Kernel>) {
        let kernel = Arc::downgrade(kernel);
        self.cut_with(move |thread| {
            if let Some(kernel) = kernel.upgrade() {
                kernel.cut(thread);
            }
        });
    }

    /// Cut this board's threads with `cut` (a host whose kernel is held some other way).
    /// From now on every answer is cacheable; see the module docs.
    pub fn cut_with(&self, cut: impl Fn(&str) + Send + Sync + 'static) {
        *self
            .board
            .cutter
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::new(cut));
    }

    /// The names of the governors observed here, in order: what a host grants
    /// ([`state_cap`]) to let a caller read them.
    pub fn governors(&self) -> Vec<String> {
        self.board
            .governors
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .keys()
            .cloned()
            .collect()
    }
}

/// What a governor knows about its own state, read for the face. Implemented beside each
/// governor, over the same shared state its endpoints change.
pub(crate) trait Observed: Send + Sync {
    fn read(&self) -> Reading;
}

/// One governor's state at one instant.
pub(crate) struct Reading {
    pub(crate) kind: Kind,
    pub(crate) keys: Vec<Key>,
    /// The soonest change time alone will make (a hit ageing out, a cooldown running
    /// out), on the governor's clock. `None` when nothing will change until a call does.
    pub(crate) next_change: Option<Duration>,
}

/// Which governor, and what is fixed about it.
pub(crate) enum Kind {
    RateLimit {
        governs: Option<Iri>,
    },
    CircuitBreaker {
        governs: Option<Iri>,
        threshold: u32,
        cooldown: Duration,
    },
    Throttle {
        governs: Option<Iri>,
    },
    /// Each candidate space's forwarded name, in order.
    Failover {
        candidates: Vec<Option<Iri>>,
    },
}

/// One key a governor holds state for.
pub(crate) enum Key {
    Window {
        prefix: String,
        max: u32,
        window: Duration,
        hits: u32,
    },
    Circuit {
        target: String,
        failures: u32,
        circuit: Circuit,
    },
    Bulkhead {
        prefix: String,
        max: usize,
        running: usize,
        parked: usize,
    },
    /// The candidate the last call to `target` ended at, and whether it answered.
    Route {
        target: String,
        candidate: usize,
        answered: bool,
    },
}

/// A circuit's state.
pub(crate) enum Circuit {
    Closed,
    Open,
    /// The cooldown has run out: the next call is a probe.
    HalfOpen,
}

/// The handle a governor changes its state through. Empty until the governor is
/// observed; then it knows its name, its board, and whether anything has read its face
/// since it last cut.
#[derive(Default)]
pub(crate) struct Watch {
    slot: OnceLock<Watcher>,
}

struct Watcher {
    thread: String,
    /// Set by every read of the face, cleared by the cut it causes.
    armed: AtomicBool,
    board: Weak<Board>,
    name: String,
}

impl Watch {
    pub(crate) fn new() -> Arc<Self> {
        Arc::default()
    }

    /// Whether a host has observed this governor (so a change is worth recording).
    pub(crate) fn is_observed(&self) -> bool {
        self.slot.get().is_some()
    }

    /// The governor's state just changed: cut its thread, if anything read the face since
    /// the last cut. Call it AFTER the change and with no lock held, so a read that
    /// starts after the cut sees the change, and the cut takes the kernel's cache lock
    /// with none of this crate's held.
    ///
    /// Why skipping is safe: a cached answer was filed by a read, every read arms, and
    /// the kernel refuses to file an answer computed before a cut that ran during it. So
    /// with the flag clear there is no answer to invalidate; and a read that arms after
    /// this swap reads the state after this change (SeqCst on both sides).
    pub(crate) fn changed(&self) {
        let Some(watcher) = self.slot.get() else {
            return;
        };
        if watcher.armed.swap(false, Ordering::SeqCst) {
            if let Some(board) = watcher.board.upgrade() {
                board.cut(&watcher.thread);
            }
        }
    }

    fn arm(&self) {
        if let Some(watcher) = self.slot.get() {
            watcher.armed.store(true, Ordering::SeqCst);
        }
    }

    /// The governor's configuration changed after it was observed (a builder ran): put
    /// the new view on the board under the same name.
    pub(crate) fn refresh(&self, view: Arc<dyn Observed>) {
        let Some(watcher) = self.slot.get() else {
            return;
        };
        let Some(board) = watcher.board.upgrade() else {
            return;
        };
        if let Some(registration) = board
            .governors
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get_mut(&watcher.name)
        {
            registration.view = view;
        }
        board.cut(&watcher.thread);
    }
}

/// A governor name: lowercase ASCII letters, digits and `-`, starting with a letter or a
/// digit, at most [`NAME_MAX`] long. One path segment with no `:`, so every name under
/// [`STATE`] says which governor it belongs to.
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= NAME_MAX
        && !name.starts_with('-')
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// Put a governor on `board` as `name`. Refused for a bad name, a name already on the
/// board, or a governor already observed.
pub(crate) fn observe(
    board: &StateBoard,
    name: &str,
    watch: &Arc<Watch>,
    view: Arc<dyn Observed>,
) -> Result<(), Error> {
    let refuse = |detail: String| Error::InvalidArgument {
        name: "name".to_string(),
        detail,
    };
    if !valid_name(name) {
        return Err(refuse(format!(
            "`{name}` is not a governor name: lowercase letters, digits and `-`, \
             at most {NAME_MAX}, not starting with `-`"
        )));
    }
    {
        let mut governors = board
            .board
            .governors
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if governors.contains_key(name) {
            return Err(refuse(format!(
                "a governor is already observed as `{name}` on this board"
            )));
        }
        let watcher = Watcher {
            thread: governor_thread(name),
            armed: AtomicBool::new(false),
            board: Arc::downgrade(&board.board),
            name: name.to_string(),
        };
        if let Err(watcher) = watch.slot.set(watcher) {
            let _ = watcher;
            let already = watch.slot.get().map_or("", |w| w.name.as_str());
            return Err(refuse(format!(
                "this governor is already observed as `{already}`"
            )));
        }
        governors.insert(
            name.to_string(),
            Registration {
                view,
                watch: Arc::clone(watch),
            },
        );
    }
    // A new member of the board, and a name that answered NotFound until now.
    board.board.cut(STATE);
    board.board.cut(&governor_thread(name));
    Ok(())
}

// -----------------------------------------------------------------------------------------
// The face
// -----------------------------------------------------------------------------------------

const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
const TEXT: &str = "text/plain;charset=utf-8";
const TURTLE: &str = "text/turtle";

/// The endpoint behind every name under [`STATE`]: the board itself, or one member.
struct StateFace {
    board: Arc<Board>,
    member: bool,
}

#[derive(Clone, Copy, PartialEq)]
enum Face {
    Text,
    Turtle,
}

fn face_of(inv: &Invocation<'_>) -> Result<Face, Error> {
    let face = match inv.request.args.get("as") {
        Some(ArgRef::Inline(bytes)) => std::str::from_utf8(bytes).unwrap_or("").trim(),
        _ => "text/plain",
    };
    match face {
        "text/plain" | "" => Ok(Face::Text),
        "text/turtle" => Ok(Face::Turtle),
        other => Err(Error::InvalidArgument {
            name: "as".to_string(),
            detail: format!(
                "`{other}` is not a face of `{}` (text/plain, text/turtle)",
                inv.request.target.as_str()
            ),
        }),
    }
}

/// The one answer for a member the caller may not read and for one that does not exist.
fn absent(target: &str) -> Error {
    Error::NotFound(format!("no governor state at `{target}`"))
}

#[async_trait::async_trait]
impl Endpoint for StateFace {
    async fn invoke(&self, inv: &Invocation<'_>) -> Result<Representation, Error> {
        let face = face_of(inv)?;
        let target = inv.request.target.as_str();
        let visible = self.board.visible(inv.capability);

        // Which governors this answer reads: all the caller may see, or the one a member
        // name belongs to.
        let (chosen, governor): (Vec<(String, Registration)>, Option<String>) = if self.member {
            let rest = target
                .strip_prefix(STATE)
                .and_then(|rest| rest.strip_prefix(':'))
                .unwrap_or("");
            let name = rest.split(':').next().unwrap_or("").to_string();
            if !valid_name(&name) {
                return Err(absent(target));
            }
            // Hidden or missing, the answer is the same, and so is what it hangs from:
            // the governor's thread, cut when it is observed or changes.
            let Some(found) = visible.into_iter().find(|(n, _)| *n == name) else {
                inv.depends_on(governor_thread(&name));
                return Err(absent(target));
            };
            (vec![found], Some(name))
        } else {
            (visible, None)
        };

        // Arm before reading, so a change after this point cuts what this read files.
        for (_, registration) in &chosen {
            registration.watch.arm();
        }
        let readings: Vec<(String, Reading)> = chosen
            .iter()
            .map(|(name, registration)| (name.clone(), registration.view.read()))
            .collect();

        let mut nodes = Vec::new();
        if governor.is_none() {
            nodes.push(board_node(&readings));
        }
        for (name, reading) in &readings {
            nodes.extend(governor_nodes(name, reading));
        }
        if let Some(name) = &governor {
            let selected = select(nodes, target);
            if selected.is_empty() {
                inv.depends_on(governor_thread(name));
                return Err(absent(target));
            }
            nodes = selected;
        }

        let representation = match face {
            Face::Text => Representation::new(
                ReprType::new("text/plain").with_param("charset", "utf-8"),
                text(&nodes).into_bytes(),
            ),
            Face::Turtle => Representation::new(
                ReprType::new("text/turtle").with_param("charset", "utf-8"),
                turtle(&nodes).into_bytes(),
            ),
        };
        Ok(self.expiry(inv, &readings, representation))
    }

    fn name(&self) -> &str {
        if self.member {
            "throttle-state-member"
        } else {
            "throttle-state"
        }
    }

    fn describe(&self) -> Description {
        let face = ArgSpec::new("as")
            .summary("response face")
            .class(XSD_STRING)
            .one_of(["text/plain", "text/turtle"])
            .default_value("text/plain");
        let description = if self.member {
            Description::new("throttle-state-member")
                .title("One governor's state, or one key's")
                .summary(
                    "One governor observed on the host's state board and its keys \
                     (urn:throttle:state:{governor}), or one key (…:key:{key}) or failover \
                     candidate (…:candidate:{n}). Needs urn:cap:throttle:state:{governor}; a \
                     governor the caller may not read is not found, exactly as one that does \
                     not exist.",
                )
                .input(
                    ArgSpec::new("member")
                        .summary("the governor, and optionally its key or candidate")
                        .class(XSD_STRING)
                        .binding(),
                )
        } else {
            Description::new("throttle-state")
                .title("Governor state")
                .summary(
                    "Every governor observed on the host's state board that the caller may \
                     read (urn:cap:throttle:state:{governor} each): rate windows, circuits, \
                     bulkheads and failover routes, each governor naming the space it \
                     governs. Cacheable once the host attached its kernel, cut on every \
                     change.",
                )
        };
        description
            .verb(Verb::Source)
            .input(face)
            .requires(STATE_CAP_FAMILY)
            .output(TEXT)
            .output(TURTLE)
    }
}

impl StateFace {
    /// Cacheable with a cutter, until the soonest change time alone makes; live without.
    fn expiry(
        &self,
        inv: &Invocation<'_>,
        readings: &[(String, Reading)],
        representation: Representation,
    ) -> Representation {
        if self.board.cutter().is_none() {
            return representation;
        }
        let mut representation = representation;
        for (name, _) in readings {
            representation = representation.depends_on(governor_thread(name));
        }
        let soonest = readings.iter().filter_map(|(_, r)| r.next_change).min();
        match soonest {
            None => representation.cacheable(),
            Some(left) => match inv.now() {
                Some(now) if !left.is_zero() => {
                    representation.cacheable_until(now.plus_millis(crate::millis(left)))
                }
                // Due now, or no kernel time to set a deadline on: stay live.
                _ => representation,
            },
        }
    }
}

// -----------------------------------------------------------------------------------------
// The graph both faces render
// -----------------------------------------------------------------------------------------

/// One subject of the state graph, and its text line.
struct Node {
    iri: String,
    class: &'static str,
    props: Vec<(&'static str, Obj)>,
    text: String,
    /// Which governor's node this is (its IRI), for a member read of the governor.
    governor: Option<String>,
}

/// An object in the state graph. IRIs are IRI-escaped; strings keep their raw value
/// until [`ikigai_core::turtle_literal`] writes them as Turtle literals.
enum Obj {
    Iri(String),
    Str(String),
    Count(u64),
    Order(u64),
    Term(&'static str),
}

fn board_node(readings: &[(String, Reading)]) -> Node {
    Node {
        iri: STATE.to_string(),
        class: "thr:StateBoard",
        props: readings
            .iter()
            .map(|(name, _)| ("thr:governor", Obj::Iri(governor_iri(name))))
            .collect(),
        text: format!(
            "throttle state: {} governor{} visible",
            readings.len(),
            if readings.len() == 1 { "" } else { "s" }
        ),
        governor: None,
    }
}

/// The name of one key of `governor`.
fn key_iri(governor: &str, key: &str) -> String {
    format!(
        "{}:key:{}",
        governor_iri(governor),
        escape_iri_fragment(&encode_name(key))
    )
}

fn governs(props: &mut Vec<(&'static str, Obj)>, id: &Option<Iri>) -> String {
    match id {
        Some(id) => {
            props.push((
                "thr:governs",
                Obj::Iri(escape_iri_fragment(id.as_str()).into_owned()),
            ));
            format!("governs {}", id.as_str())
        }
        None => "governs an anonymous space".to_string(),
    }
}

/// A governor's node, then its keys (and a failover's candidates).
fn governor_nodes(name: &str, reading: &Reading) -> Vec<Node> {
    let iri = governor_iri(name);
    let mut props = vec![("thr:name", Obj::Str(name.to_string()))];
    let mut children = Vec::new();
    let (class, line) = match &reading.kind {
        Kind::RateLimit { governs: id } => (
            "thr:RateLimit",
            format!("{name}: rate-limit, {}", governs(&mut props, id)),
        ),
        Kind::Throttle { governs: id } => (
            "thr:Throttle",
            format!("{name}: throttle, {}", governs(&mut props, id)),
        ),
        Kind::CircuitBreaker {
            governs: id,
            threshold,
            cooldown,
        } => {
            let line = format!(
                "{name}: circuit-breaker, {}, trips after {threshold}, cooldown {}",
                governs(&mut props, id),
                span(*cooldown)
            );
            props.push(("thr:threshold", Obj::Count(u64::from(*threshold))));
            props.push(("thr:cooldownMillis", Obj::Count(crate::millis(*cooldown))));
            ("thr:CircuitBreaker", line)
        }
        Kind::Failover { candidates } => {
            for (n, id) in candidates.iter().enumerate() {
                let cell = format!("{iri}:candidate:{n}");
                props.push(("thr:candidate", Obj::Iri(cell.clone())));
                let mut cell_props = vec![
                    ("thr:of", Obj::Iri(iri.clone())),
                    ("ik:order", Obj::Order(n as u64)),
                ];
                let said = governs(&mut cell_props, id);
                children.push(Node {
                    iri: cell,
                    class: "thr:Candidate",
                    props: cell_props,
                    text: format!("  candidate {n}: {said}"),
                    governor: Some(iri.clone()),
                });
            }
            (
                "thr:Failover",
                format!("{name}: failover over {} spaces", candidates.len()),
            )
        }
    };
    for key in &reading.keys {
        let node = key_node(name, &iri, key);
        props.push(("thr:key", Obj::Iri(node.iri.clone())));
        children.push(node);
    }
    let mut nodes = vec![Node {
        iri: iri.clone(),
        class,
        props,
        text: line,
        governor: Some(iri),
    }];
    nodes.extend(children);
    nodes
}

fn key_node(name: &str, governor: &str, key: &Key) -> Node {
    let of = ("thr:of", Obj::Iri(governor.to_string()));
    let target_iri = |t: &str| Obj::Iri(escape_iri_fragment(t).into_owned());
    let prefix_str = |p: &str| Obj::Str(p.to_string());
    let (iri, class, props, text) = match key {
        Key::Window {
            prefix,
            max,
            window,
            hits,
        } => {
            let full = hits >= max;
            (
                key_iri(name, prefix),
                "thr:Window",
                vec![
                    of,
                    ("thr:prefix", prefix_str(prefix)),
                    ("thr:max", Obj::Count(u64::from(*max))),
                    ("thr:windowMillis", Obj::Count(crate::millis(*window))),
                    ("thr:hits", Obj::Count(u64::from(*hits))),
                    (
                        "thr:state",
                        Obj::Term(if full { "thr:Full" } else { "thr:Admitting" }),
                    ),
                ],
                format!(
                    "  window `{prefix}`: {hits} of {max} per {}, {}",
                    span(*window),
                    if full { "full" } else { "admitting" }
                ),
            )
        }
        Key::Circuit {
            target,
            failures,
            circuit,
        } => {
            let (term, word) = match circuit {
                Circuit::Closed => ("thr:Closed", "closed"),
                Circuit::Open => ("thr:Open", "open"),
                Circuit::HalfOpen => ("thr:HalfOpen", "half-open"),
            };
            (
                key_iri(name, target),
                "thr:Circuit",
                vec![
                    of,
                    ("thr:target", target_iri(target)),
                    ("thr:failures", Obj::Count(u64::from(*failures))),
                    ("thr:state", Obj::Term(term)),
                ],
                format!(
                    "  circuit `{target}`: {word}, {failures} consecutive transient failure{}",
                    if *failures == 1 { "" } else { "s" }
                ),
            )
        }
        Key::Bulkhead {
            prefix,
            max,
            running,
            parked,
        } => {
            let full = running >= max;
            (
                key_iri(name, prefix),
                "thr:Bulkhead",
                vec![
                    of,
                    ("thr:prefix", prefix_str(prefix)),
                    ("thr:max", Obj::Count(*max as u64)),
                    ("thr:running", Obj::Count(*running as u64)),
                    ("thr:parked", Obj::Count(*parked as u64)),
                    (
                        "thr:state",
                        Obj::Term(if full { "thr:Full" } else { "thr:Admitting" }),
                    ),
                ],
                format!("  bulkhead `{prefix}`: {running} of {max} running, {parked} parked"),
            )
        }
        Key::Route {
            target,
            candidate,
            answered,
        } => (
            key_iri(name, target),
            "thr:Route",
            vec![
                of,
                ("thr:target", target_iri(target)),
                (
                    "thr:lastCandidate",
                    Obj::Iri(format!("{governor}:candidate:{candidate}")),
                ),
                (
                    "thr:state",
                    Obj::Term(if *answered {
                        "thr:Answered"
                    } else {
                        "thr:Failed"
                    }),
                ),
            ],
            format!(
                "  route `{target}`: last call {} at candidate {candidate}",
                if *answered { "answered" } else { "failed" }
            ),
        ),
    };
    Node {
        iri,
        class,
        props,
        text,
        governor: Some(governor.to_string()),
    }
}

/// What a member read answers: a governor's IRI selects the governor and everything
/// under it; any other IRI selects that one node. Empty when nothing has the name.
fn select(nodes: Vec<Node>, target: &str) -> Vec<Node> {
    let is_governor = nodes
        .iter()
        .any(|node| node.iri == target && node.governor.as_deref() == Some(target));
    nodes
        .into_iter()
        .filter(|node| {
            if is_governor {
                node.governor.as_deref() == Some(target)
            } else {
                node.iri == target
            }
        })
        .collect()
}

fn text(nodes: &[Node]) -> String {
    let mut out = String::new();
    let alone = nodes.len() == 1;
    for node in nodes {
        // A single key read alone is not indented under a governor it was read without.
        out += if alone {
            node.text.trim_start()
        } else {
            &node.text
        };
        out.push('\n');
    }
    out
}

fn turtle(nodes: &[Node]) -> String {
    let mut out = format!(
        "@prefix ik: <https://ikigai-rs.dev/ns#> .\n\
         @prefix thr: <{STATE_NS}> .\n\
         @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
    );
    for node in nodes {
        out += &format!("\n<{}> a {}", node.iri, node.class);
        for (predicate, object) in &node.props {
            let object = match object {
                Obj::Iri(iri) => format!("<{iri}>"),
                Obj::Str(s) => ikigai_core::turtle_literal(s),
                Obj::Count(n) => format!("\"{n}\"^^xsd:nonNegativeInteger"),
                Obj::Order(n) => format!("\"{n}\"^^xsd:integer"),
                Obj::Term(term) => (*term).to_string(),
            };
            out += &format!(" ;\n    {predicate} {object}");
        }
        out += " .\n";
    }
    out
}
