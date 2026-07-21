//! Split coordination: the seam that lets several pipeline instances divide
//! one broker-less source's work without duplicates.
//!
//! Brokered sources bring their own coordination (a Kafka consumer group
//! assigns partitions across processes). Broker-less sources — object-store
//! backfills, database range scans, file tails — have no group protocol:
//! two processes pointed at the same input each see all of it. This module
//! closes that gap with a leader-assigned model:
//!
//! - A source-provided [`SplitPlanner`] enumerates the work as weighted
//!   **splits** (an object-store backfill: object lists bin-packed by
//!   bytes; a database source: balanced id ranges). The planner runs only
//!   on the fleet's elected leader — workers receive split descriptors and
//!   never re-enumerate.
//! - The leader also computes a **desired assignment** and publishes it
//!   per instance. Every worker holds a [`SplitCoordinator`] that leases
//!   the splits it was assigned, heartbeats them in the background, and
//!   cooperatively drains any it is no longer assigned. A dead owner's
//!   leases expire and its work is reassigned.
//! - Progress commits are **fenced**: a commit from an instance that no
//!   longer owns the split is rejected and writes nothing, so committed
//!   progress can only replay, never regress.
//!
//! `etl-core` owns only the seam — the two traits and their handshake
//! types — plus the reusable source-side driver
//! ([`CoordinationDriver`](driver::CoordinationDriver)). Concrete backends
//! live in backend crates (the NATS JetStream KV backend in
//! `etl-coordination`); a source receives its coordinator at
//! pipeline-assembly time, mirroring the framing seam
//! ([`RecordFramer`](crate::framing::RecordFramer)).
//!
//! # Delivery contract
//!
//! Coordination preserves at-least-once, nothing stronger. Ownership
//! revocations and takeovers may briefly overlap — a taken-over split's uncommitted tail is
//! replayed by the new owner, and a zombie may emit records after its
//! lease was seized. Both produce **duplicates, never loss**: the
//! correctness boundary is [`SplitCoordinator::commit`], a fenced durable
//! write that a backend must reject once the caller no longer owns the
//! split ([`CoordinationErrorKind::Fenced`] — and a fenced write must
//! write nothing).
//!
//! A split that repeatedly kills its owners (or is explicitly
//! [`fail`](SplitCoordinator::fail)ed) is **quarantined** after a bounded
//! number of attempts rather than crashing workers forever. Quarantined
//! splits stay visible and block [`CoordinationEvent::AllComplete`]: a
//! bounded job whose planned data went unprocessed finishes as
//! [`CoordinationEvent::Stalled`], never as a false success.
//!
//! # Threading
//!
//! Like [`Source`](crate::source::Source), a coordinator is driven
//! synchronously from the pipeline's controller thread. Implementations
//! own their I/O (typically a background task on the runtime handle they
//! were built with), must bound every call, and must never rely on being
//! polled to keep a lease alive — renewal runs in the background so
//! backpressure cannot cost ownership.

use crate::error::ErrorClass;
use std::fmt;

pub mod driver;

/// Maximum length of a [`SplitId`] in bytes.
pub const SPLIT_ID_MAX_LEN: usize = 128;

/// Deterministic identity of one split, minted by the planner.
///
/// Stable across replans of unchanged work — the property that makes
/// replanning idempotent (create-if-absent in the store) and progress
/// resumable across owners. Validated at construction so key-encoding
/// problems surface as [`Fatal`](CoordinationErrorKind::Fatal) here, not
/// as opaque backend write failures: 1–128 bytes of `[A-Za-z0-9_-]`
/// (`.` is reserved as the store's key-hierarchy separator).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SplitId(String);

impl SplitId {
    /// Validate and wrap a split id.
    ///
    /// # Errors
    ///
    /// [`Fatal`](CoordinationErrorKind::Fatal) when empty, longer than
    /// [`SPLIT_ID_MAX_LEN`], or containing anything outside
    /// `[A-Za-z0-9_-]`.
    pub fn new(id: impl Into<String>) -> Result<SplitId, CoordinationError> {
        let id = id.into();
        if id.is_empty() || id.len() > SPLIT_ID_MAX_LEN {
            return Err(CoordinationError::new(
                CoordinationErrorKind::Fatal,
                format!(
                    "split id must be 1..={SPLIT_ID_MAX_LEN} bytes, got {} ({id:?})",
                    id.len()
                ),
            ));
        }
        if let Some(bad) = id
            .chars()
            .find(|c| !(c.is_ascii_alphanumeric() || *c == '_' || *c == '-'))
        {
            return Err(CoordinationError::new(
                CoordinationErrorKind::Fatal,
                format!("split id may only contain [A-Za-z0-9_-], got {bad:?} in {id:?}"),
            ));
        }
        Ok(SplitId(id))
    }

    /// The id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SplitId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Monotonic fencing token for one split's lease, minted by the backend.
/// Strictly increases across ownership changes of that split; a write
/// presented under a superseded epoch is rejected.
///
/// Distinct from the checkpointer's *assignment* epoch (a per-process
/// counter over [`SourceEvent::LanesAssigned`](crate::source::SourceEvent)
/// cycles) and from the planner's *generation* (which orders leaders): a
/// `LeaseEpoch` orders owners of one split **across processes**.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeaseEpoch(pub u64);

/// One unit of leasable work, as the planner enumerated it.
///
/// The descriptor is carried verbatim to whichever worker gains the split,
/// so workers never re-enumerate the input (one LIST/scan per plan, on the
/// leader). It is opaque to the framework and to backends; keep it small —
/// backends enforce a size cap (the NATS backend: a fixed 512 KiB per
/// stored value). Descriptors are written once at planning and never
/// rewritten by commits, so their size never taxes the commit path.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SplitSpec {
    /// Deterministic identity, stable across replans of unchanged work.
    pub id: SplitId,
    /// Source-defined payload: everything a worker needs to process the
    /// split (an object-store source: member keys with etags and sizes; a
    /// database source: an id range).
    pub descriptor: Vec<u8>,
    /// Relative cost hint (bytes, rows) — **the balance objective**.
    ///
    /// This is load, not sort order. Under the work-stealing balancer this
    /// field only ordered claims and balance was on split *count*; a
    /// planner that populated it as a ranking key rather than a cost will
    /// now skew the leader's distribution.
    ///
    /// The
    /// leader distributes summed weight, not split count, so a planner
    /// that emits wildly uneven splits (an object-store planner gives any
    /// object at or above its packing target a split to itself) still
    /// balances correctly. A planner that leaves every weight at the
    /// default degrades to count-balancing, which is the right behaviour
    /// when splits really are uniform. `0` is treated as `1`.
    pub weight: u64,
}

impl SplitSpec {
    /// A split with the default weight of 1.
    #[must_use]
    pub fn new(id: SplitId, descriptor: Vec<u8>) -> SplitSpec {
        SplitSpec {
            id,
            descriptor,
            weight: 1,
        }
    }

    /// Set the relative cost hint.
    #[must_use]
    pub fn with_weight(mut self, weight: u64) -> SplitSpec {
        self.weight = weight;
        self
    }
}

/// A split as the planner submits it: the spec plus optional seed
/// progress. Seeds are first-writer-wins — a seed never overwrites an
/// existing record — which is the migration path for work that already has
/// a pre-coordination checkpoint. A brand-new source plans without seeds.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct PlannedSplit {
    /// The split itself.
    pub spec: SplitSpec,
    /// Durable progress to seed if (and only if) the split has none.
    pub seed: Option<SplitProgress>,
}

impl PlannedSplit {
    /// A split with no seed progress.
    #[must_use]
    pub fn new(spec: SplitSpec) -> PlannedSplit {
        PlannedSplit { spec, seed: None }
    }

    /// Attach seed progress (first writer wins).
    #[must_use]
    pub fn with_seed(mut self, seed: SplitProgress) -> PlannedSplit {
        self.seed = Some(seed);
        self
    }
}

/// Whether a plan's enumeration can still grow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanFinality {
    /// More work may appear; the leader re-runs the planner on its replan
    /// interval and [`CoordinationEvent::AllComplete`] never fires.
    Open,
    /// The enumeration is complete and final. Once every split is
    /// completed the job reports [`CoordinationEvent::AllComplete`] (or
    /// [`CoordinationEvent::Stalled`] if any were quarantined).
    Final,
}

/// One planner run's output.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SplitPlan {
    /// The enumerated work. On a replan, splits already planned are
    /// deduplicated by id (create-if-absent) — emitting them again is a
    /// cheap no-op, which is what makes replanning idempotent.
    pub splits: Vec<PlannedSplit>,
    /// Whether this enumeration is complete.
    pub finality: PlanFinality,
    /// Opaque planner cursor persisted in the plan record and handed back
    /// on the next run via [`PlanContext::planner_state`] (e.g. an
    /// object-store listing's start-after key). `None` keeps the previous
    /// cursor.
    pub planner_state: Option<Vec<u8>>,
}

impl SplitPlan {
    /// A plan with no cursor update.
    #[must_use]
    pub fn new(splits: Vec<PlannedSplit>, finality: PlanFinality) -> SplitPlan {
        SplitPlan {
            splits,
            finality,
            planner_state: None,
        }
    }

    /// Persist a planner cursor for the next run.
    #[must_use]
    pub fn with_planner_state(mut self, state: Vec<u8>) -> SplitPlan {
        self.planner_state = Some(state);
        self
    }
}

/// What the backend hands the planner on each run.
#[derive(Debug)]
#[non_exhaustive]
pub struct PlanContext<'a> {
    /// The cursor persisted by the previous run's
    /// [`SplitPlan::planner_state`]; `None` on the first ever plan.
    pub planner_state: Option<&'a [u8]>,
    /// The current plan generation: bumped on every leader failover, so a
    /// planner can tell a fresh leadership from a continuation.
    pub generation: u64,
}

impl<'a> PlanContext<'a> {
    /// Build a context (backends construct this; sources only read it).
    #[must_use]
    pub fn new(planner_state: Option<&'a [u8]>, generation: u64) -> PlanContext<'a> {
        PlanContext {
            planner_state,
            generation,
        }
    }
}

/// One split's durable progress, written through the fenced
/// [`commit`](SplitCoordinator::commit) and handed to the next owner in
/// [`CoordinationEvent::Gained`].
///
/// The `state` payload is opaque to the framework **and** to backends —
/// arbitrary bytes, no encoding constraint. The source owns its schema;
/// keeping it opaque is what keeps connector types out of `etl-core`'s
/// public API.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SplitProgress {
    /// Committable watermark: one past the last acknowledged record, in
    /// the source's own offset encoding. Backends enforce that it never
    /// decreases across a split's committed history.
    pub watermark: i64,
    /// Source-defined opaque resume state.
    pub state: Vec<u8>,
    /// Bounded jobs: this split is fully delivered and committed.
    /// Terminal — a backend never re-offers a completed split.
    pub completed: bool,
}

impl SplitProgress {
    /// In-progress split state (`completed: false`).
    #[must_use]
    pub fn new(watermark: i64, state: Vec<u8>) -> SplitProgress {
        SplitProgress {
            watermark,
            state,
            completed: false,
        }
    }

    /// Terminal split state: fully delivered and committed.
    #[must_use]
    pub fn completed(watermark: i64, state: Vec<u8>) -> SplitProgress {
        SplitProgress {
            watermark,
            state,
            completed: true,
        }
    }
}

/// Ownership or job-state change surfaced by [`SplitCoordinator::poll`].
///
/// Events for one split are ordered: a split is `Gained` before it can be
/// `Lost`, and a re-`Gained` split always carries a higher [`LeaseEpoch`]
/// than the tenancy it replaces.
#[derive(Debug)]
#[non_exhaustive]
pub enum CoordinationEvent {
    /// This instance now holds the lease for the split and must start (or
    /// resume) processing it.
    Gained {
        /// The split now owned, descriptor included — the gaining worker
        /// never saw the planner run.
        split: SplitSpec,
        /// Fencing token for this tenancy.
        epoch: LeaseEpoch,
        /// The last fenced-committed progress to resume from. `None` for
        /// a split that has never committed.
        progress: Option<SplitProgress>,
    },
    /// The lease was lost — seized by a peer after expiry, stolen for
    /// balance, or self-fenced after renewals could not reach the backend.
    /// The source must stop the split promptly and must not commit it
    /// again (a late commit is rejected as
    /// [`CoordinationErrorKind::Fenced`] regardless — this event is the
    /// cooperative fast path).
    Lost {
        /// The split no longer owned.
        split: SplitId,
    },
    /// The leader has stopped assigning this split to this instance, and
    /// wants it back. The owner should stop intake at a safe boundary,
    /// chase the split's tail to a final fenced commit, and release it —
    /// the next owner then resumes from a point covering everything this
    /// one emitted, so the transfer replays nothing.
    ///
    /// **The split is leaving either way.** Unlike the peer request this
    /// replaced, a revocation is a decision rather than a proposal: a
    /// source that declines — through the driver's
    /// [`SplitSource::begin_revoke`](driver::SplitSource::begin_revoke),
    /// which defaults to declining — or that does not finish inside
    /// `drain_deadline` has the release forced instead, and its
    /// uncommitted tail replays under the next owner. Declining is
    /// therefore still *safe*; it is just the expensive way to comply.
    ///
    /// Idempotent: the event may be re-emitted for a split already
    /// draining, and a revocation for a split this instance does not hold
    /// is a silent no-op.
    RevokeRequested {
        /// The split to give up.
        split: SplitId,
    },
    /// The split exhausted its delivery attempts (repeated owner deaths or
    /// explicit [`fail`](SplitCoordinator::fail) reports) and was parked.
    /// It will not be re-offered; it stays visible in the store and in the
    /// `etl_coordination_splits_quarantined` gauge, and it blocks
    /// [`AllComplete`](CoordinationEvent::AllComplete).
    Quarantined {
        /// The parked split.
        split: SplitId,
        /// Delivery attempts consumed.
        attempts: u32,
    },
    /// Final plan and every split committed `completed`. Bounded sources
    /// translate this into
    /// [`SourceEvent::Drained`](crate::source::SourceEvent::Drained). An
    /// instance that owns no splits must keep polling until this arrives —
    /// it is the standby that covers an owner dying at the finish line.
    AllComplete,
    /// Final plan, nothing left runnable or running, but quarantined
    /// splits remain: the job cannot finish cleanly. Surfaced to every
    /// instance exactly where `AllComplete` would have been. The source
    /// decides whether this is fatal (the default in the driver) or a
    /// drain-with-warning.
    Stalled {
        /// Splits that completed.
        completed: u64,
        /// Splits parked in quarantine.
        quarantined: u64,
    },
}

/// Why a coordination operation failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CoordinationErrorKind {
    /// The fenced write lost: this instance no longer owns the split and
    /// **nothing was written**. Not a pipeline error — callers intercept
    /// it and treat the split as lost (the matching
    /// [`CoordinationEvent::Lost`] follows from
    /// [`poll`](SplitCoordinator::poll)).
    Fenced,
    /// Transient backend failure; the operation may succeed if retried on
    /// the caller's next tick. Backends keep renewing owned leases through
    /// caller-visible retryable failures.
    Retryable,
    /// Unrecoverable: backend misconfiguration, a store without the
    /// required atomic primitives, a corrupt or incompatible record, a
    /// diverging job fingerprint. The pipeline must stop.
    Fatal,
}

/// Error from a coordination backend.
#[derive(Debug, thiserror::Error)]
#[error("coordination error ({kind:?}): {reason}")]
#[non_exhaustive]
pub struct CoordinationError {
    /// How the caller must react; see [`CoordinationErrorKind`].
    pub kind: CoordinationErrorKind,
    /// Human-readable cause.
    pub reason: String,
}

impl CoordinationError {
    /// Build an error. Backends live outside this crate, so construction
    /// goes through this constructor (the struct is `#[non_exhaustive]`).
    pub fn new(kind: CoordinationErrorKind, reason: impl Into<String>) -> CoordinationError {
        CoordinationError {
            kind,
            reason: reason.into(),
        }
    }

    /// Map to the framework error taxonomy. [`Fenced`]
    /// (CoordinationErrorKind::Fenced) maps to [`Fatal`](ErrorClass::Fatal)
    /// only as a backstop — callers are expected to intercept it before
    /// classification and handle the split loss instead of failing.
    #[must_use]
    pub fn class(&self) -> ErrorClass {
        match self.kind {
            CoordinationErrorKind::Retryable => ErrorClass::Retryable,
            CoordinationErrorKind::Fenced | CoordinationErrorKind::Fatal => ErrorClass::Fatal,
        }
    }
}

/// Source-provided work enumerator, run only on the fleet's elected
/// leader — at job start and again on the replan interval while the plan
/// is [`Open`](PlanFinality::Open).
///
/// Every worker presents an equivalent planner at
/// [`SplitCoordinator::start`]; whichever instance holds leadership uses
/// its own copy. `plan` may block on real I/O (an object-store LIST, an
/// index query) — backends call it off their async loop.
pub trait SplitPlanner: Send {
    /// Cheap, deterministic job identity, derived from *configuration*,
    /// never from the enumeration itself. Every worker joining the job
    /// must present a byte-equal fingerprint or be rejected as
    /// [`Fatal`](CoordinationErrorKind::Fatal) — divergent configurations
    /// can never interpret the same split table two ways.
    fn fingerprint(&self) -> String;

    /// Enumerate the current work. Idempotency contract: unchanged work
    /// yields the same [`SplitId`]s, so re-emitting already-planned splits
    /// is a cheap store-side no-op.
    fn plan(&mut self, ctx: PlanContext<'_>) -> Result<SplitPlan, CoordinationError>;
}

/// Per-worker coordination handle: leases splits toward a bounded working
/// set, surfaces ownership changes, and owns the fenced progress commit.
///
/// Dyn-compatible; sources hold a `Box<dyn SplitCoordinator>` injected at
/// assembly time (typically via the
/// [`CoordinationDriver`](driver::CoordinationDriver) rather than
/// directly). Driven from the controller thread — implementations do their
/// I/O elsewhere and bound every call.
///
/// ```
/// use etl_core::coordination::{
///     ControlWaker, CoordinationError, CoordinationEvent, PlanContext, PlanFinality,
///     PlannedSplit, SplitCoordinator, SplitId, SplitPlan, SplitPlanner, SplitProgress,
///     SplitSpec, LeaseEpoch,
/// };
/// use std::collections::BTreeMap;
/// use std::time::Duration;
///
/// /// A trivial single-instance backend: plans immediately, grants every
/// /// split to the one worker, keeps progress in memory. Real backends
/// /// persist through fenced CAS writes; this shape is only the seam's
/// /// contract in miniature.
/// #[derive(Default)]
/// struct LocalCoordinator {
///     pending: Vec<CoordinationEvent>,
///     committed: BTreeMap<SplitId, SplitProgress>,
///     total: usize,
/// }
///
/// impl SplitCoordinator for LocalCoordinator {
///     fn start(&mut self, mut planner: Box<dyn SplitPlanner>) -> Result<(), CoordinationError> {
///         let plan = planner.plan(PlanContext::new(None, 1))?;
///         assert!(matches!(plan.finality, PlanFinality::Final));
///         self.total = plan.splits.len();
///         for planned in plan.splits {
///             let progress = planned.seed.clone();
///             self.pending.push(CoordinationEvent::Gained {
///                 split: planned.spec,
///                 epoch: LeaseEpoch(1),
///                 progress,
///             });
///         }
///         Ok(())
///     }
///
///     fn set_waker(&mut self, _w: ControlWaker) {}
///
///     fn poll(&mut self) -> Result<Vec<CoordinationEvent>, CoordinationError> {
///         let mut events = std::mem::take(&mut self.pending);
///         if self.total > 0
///             && self.committed.len() == self.total
///             && self.committed.values().all(|p| p.completed)
///         {
///             self.total = 0; // fire AllComplete once
///             events.push(CoordinationEvent::AllComplete);
///         }
///         Ok(events)
///     }
///
///     fn commit(&mut self, s: &SplitId, p: &SplitProgress) -> Result<(), CoordinationError> {
///         self.committed.insert(s.clone(), p.clone());
///         Ok(())
///     }
///
///     fn fail(&mut self, _s: &SplitId, _r: &str) -> Result<(), CoordinationError> {
///         Ok(())
///     }
///
///     fn release(&mut self, _s: &[SplitId]) -> Result<(), CoordinationError> {
///         Ok(())
///     }
/// }
///
/// struct TwoSplits;
/// impl SplitPlanner for TwoSplits {
///     fn fingerprint(&self) -> String {
///         "example:v1".into()
///     }
///     fn plan(&mut self, _ctx: PlanContext<'_>) -> Result<SplitPlan, CoordinationError> {
///         let splits = ["a", "b"]
///             .into_iter()
///             .map(|id| {
///                 Ok(PlannedSplit::new(SplitSpec::new(
///                     SplitId::new(id)?,
///                     format!("range:{id}").into_bytes(),
///                 )))
///             })
///             .collect::<Result<_, CoordinationError>>()?;
///         Ok(SplitPlan::new(splits, PlanFinality::Final))
///     }
/// }
///
/// let mut c: Box<dyn SplitCoordinator> = Box::new(LocalCoordinator::default());
/// c.start(Box::new(TwoSplits)).unwrap();
/// let gained = c.poll().unwrap();
/// assert_eq!(gained.len(), 2);
/// for id in ["a", "b"] {
///     let id = SplitId::new(id).unwrap();
///     c.commit(&id, &SplitProgress::completed(10, vec![])).unwrap();
/// }
/// assert!(matches!(
///     c.poll().unwrap().last(),
///     Some(CoordinationEvent::AllComplete)
/// ));
/// ```
pub trait SplitCoordinator: Send {
    /// Join the job: verify the fingerprint, hand over this worker's
    /// planner (used only if and while this instance is elected leader),
    /// and start the backend's claim and renewal machinery. Called exactly
    /// once, before any other method.
    fn start(&mut self, planner: Box<dyn SplitPlanner>) -> Result<(), CoordinationError>;

    /// Hand the backend the handle it signals when it has events to
    /// deliver. Called once, before [`start`](SplitCoordinator::start).
    ///
    /// The control-plane wait lives in
    /// [`CoordinationDriver`](driver::CoordinationDriver), not here,
    /// because completions arrive from two directions the backend cannot
    /// see between them: the backend's own machinery, and the *lanes*
    /// reaching end-of-input on pipeline threads. A backend that parks
    /// internally cannot be woken by the second, which is why
    /// [`poll`](SplitCoordinator::poll) does not block. Signal this waker
    /// whenever a later `poll` would return something.
    fn set_waker(&mut self, waker: ControlWaker);

    /// Ownership and job-state changes since the last call. **Must not
    /// block** — return whatever is pending, including nothing. The driver
    /// parks on the [`ControlWaker`] instead.
    fn poll(&mut self) -> Result<Vec<CoordinationEvent>, CoordinationError>;

    /// Fenced durable commit of one owned split's progress. `Ok` means
    /// durable. [`Fenced`](CoordinationErrorKind::Fenced) means the split
    /// is no longer owned and **nothing was written** — stop the split,
    /// never retry the write. [`Retryable`](CoordinationErrorKind::Retryable)
    /// leaves the previous committed state authoritative; re-committing
    /// the merged progress on the next tick is idempotent.
    ///
    /// A commit on a split this instance no longer holds — including one
    /// it already committed `completed` — returns `Fenced` without a
    /// following [`Lost`](CoordinationEvent::Lost) event: `Lost` marks an
    /// involuntary end of a live tenancy, and there is none. (The
    /// [`CoordinationDriver`](driver::CoordinationDriver) never issues
    /// such commits; hand-rolled callers must tolerate the error.)
    fn commit(
        &mut self,
        split: &SplitId,
        progress: &SplitProgress,
    ) -> Result<(), CoordinationError>;

    /// Report an owned split as unprocessable *by this tenancy*: consumes
    /// one delivery attempt and releases it for another worker to retry.
    /// At the backend's attempt cap the split is quarantined instead
    /// ([`CoordinationEvent::Quarantined`]). Use for poison input; a
    /// transient local problem is better handled by
    /// [`release`](SplitCoordinator::release), which consumes nothing.
    fn fail(&mut self, split: &SplitId, reason: &str) -> Result<(), CoordinationError>;

    /// Voluntarily hand back owned splits (shutdown, scale-down) so peers
    /// claim them without waiting out a lease. Consumes no delivery
    /// attempts. Best-effort and idempotent; splits not released simply
    /// expire.
    fn release(&mut self, splits: &[SplitId]) -> Result<(), CoordinationError>;

    /// Release splits given up through a cooperative revocation — the owner
    /// has drained each split, committed its tail, and is now handing it
    /// back. Semantically a [`release`](SplitCoordinator::release)
    /// (attempt-free, best-effort, idempotent), but distinguished so a
    /// revocation-aware backend can record the drain as having completed
    /// and never mistake a single-split revocation for a departure from the
    /// fleet.
    ///
    /// Defaulted to [`release`](SplitCoordinator::release) so existing
    /// backends keep working — the hand-back then reads as an ordinary one
    /// — and so the trait stays dyn-compatible. A backend that implements
    /// the revocation protocol overrides it.
    fn release_drained(&mut self, splits: &[SplitId]) -> Result<(), CoordinationError> {
        self.release(splits)
    }

    /// Decline a [`RevokeRequested`](CoordinationEvent::RevokeRequested)
    /// the embedder cannot serve — the source refused to stop the split's
    /// intake, or the split is not in a drainable state.
    ///
    /// **A decline does not keep the split.** It reports that the *clean*
    /// path is unavailable, so the backend stops waiting and takes the
    /// expensive one immediately instead of holding the rebalance open
    /// until its deadline; the split still leaves, and its uncommitted tail
    /// replays under the next owner. Best-effort and idempotent; declining
    /// a split that was never revoked is a no-op.
    ///
    /// **Backend obligation.** A backend that emits `RevokeRequested` must
    /// guarantee the split leaves whatever the source does: force the
    /// release on a decline, and bound a drain that never finishes with a
    /// deadline of its own. Without that, one uncooperative source pins the
    /// fleet's rebalancing open forever. (`etl-coordination` spells this
    /// deadline `drain_deadline`.)
    ///
    /// Defaulted to a no-op so existing backends keep working and the
    /// trait stays dyn-compatible.
    fn decline_revoke(&mut self, _split: &SplitId) -> Result<(), CoordinationError> {
        Ok(())
    }
}

/// Wakes a coordinated source's control-plane wait.
///
/// Cheap to clone and safe to signal from any thread — including a
/// pipeline thread on the data path, because [`wake`](ControlWaker::wake)
/// never blocks. The channel behind it holds a single slot, so a burst of
/// signals collapses into one wakeup, and a signal that lands while the
/// driver is between its check and its park is buffered rather than lost.
///
/// Signal it for anything the driver would otherwise only notice between
/// waits: a backend with events ready, a lane reaching end-of-input, a
/// lane reporting poison.
#[derive(Clone, Debug)]
pub struct ControlWaker(crossbeam_channel::Sender<()>);

impl ControlWaker {
    /// A waker attached to nothing: [`wake`](ControlWaker::wake) is a
    /// no-op. For unit tests that construct a lane without a driver, and
    /// for sources that have no control-plane park to interrupt.
    #[must_use]
    pub fn inert() -> ControlWaker {
        let (tx, rx) = crossbeam_channel::bounded(1);
        drop(rx);
        ControlWaker(tx)
    }

    /// Wake the driver if it is parked, or make its next park return
    /// immediately. Never blocks.
    pub fn wake(&self) {
        // Full slot means a wakeup is already pending — nothing to add.
        let _ = self.0.try_send(());
    }
}

/// The waker and the parking half the driver owns.
pub(crate) fn control_channel() -> (ControlWaker, crossbeam_channel::Receiver<()>) {
    let (tx, rx) = crossbeam_channel::bounded(1);
    (ControlWaker(tx), rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopCoordinator;

    impl SplitCoordinator for NoopCoordinator {
        fn start(&mut self, _planner: Box<dyn SplitPlanner>) -> Result<(), CoordinationError> {
            Ok(())
        }

        fn set_waker(&mut self, _waker: ControlWaker) {}

        fn poll(&mut self) -> Result<Vec<CoordinationEvent>, CoordinationError> {
            Ok(vec![])
        }

        fn commit(
            &mut self,
            split: &SplitId,
            _progress: &SplitProgress,
        ) -> Result<(), CoordinationError> {
            Err(CoordinationError::new(
                CoordinationErrorKind::Fenced,
                format!("split {split} is owned by a peer at epoch 2"),
            ))
        }

        fn fail(&mut self, _split: &SplitId, _reason: &str) -> Result<(), CoordinationError> {
            Ok(())
        }

        fn release(&mut self, _splits: &[SplitId]) -> Result<(), CoordinationError> {
            Ok(())
        }
    }

    struct NoopPlanner;

    impl SplitPlanner for NoopPlanner {
        fn fingerprint(&self) -> String {
            "noop:v1".into()
        }

        fn plan(&mut self, ctx: PlanContext<'_>) -> Result<SplitPlan, CoordinationError> {
            assert!(ctx.planner_state.is_none());
            Ok(SplitPlan::new(vec![], PlanFinality::Final))
        }
    }

    #[test]
    fn split_coordinator_is_object_safe() {
        // Compiles only if both traits are dyn-compatible (the seam's contract).
        let mut c: Box<dyn SplitCoordinator> = Box::new(NoopCoordinator);
        c.start(Box::new(NoopPlanner)).unwrap();
        assert!(c.poll().unwrap().is_empty());
        c.release(&[SplitId::new("s-0").unwrap()]).unwrap();
        // The defaulted revocation release delegates to `release`, staying
        // dyn-compatible and callable through the trait object.
        c.release_drained(&[SplitId::new("s-0").unwrap()]).unwrap();
    }

    #[test]
    fn split_ids_validate_charset_and_length() {
        assert_eq!(
            SplitId::new("rows-000000-000125").unwrap().as_str(),
            "rows-000000-000125"
        );
        assert_eq!(SplitId::new("A_z9").unwrap().to_string(), "A_z9");
        for bad in [
            "",
            "a.b",
            "a b",
            "a/b",
            "å",
            &"x".repeat(SPLIT_ID_MAX_LEN + 1),
        ] {
            let err = SplitId::new(bad).unwrap_err();
            assert_eq!(err.kind, CoordinationErrorKind::Fatal, "{bad:?}");
        }
        assert!(SplitId::new("x".repeat(SPLIT_ID_MAX_LEN)).is_ok());
    }

    #[test]
    fn error_kinds_map_to_the_framework_taxonomy() {
        let fenced = CoordinationError::new(CoordinationErrorKind::Fenced, "seized");
        assert_eq!(fenced.class(), ErrorClass::Fatal, "unintercepted backstop");
        assert_eq!(
            CoordinationError::new(CoordinationErrorKind::Retryable, "timeout").class(),
            ErrorClass::Retryable
        );
        assert_eq!(
            CoordinationError::new(CoordinationErrorKind::Fatal, "no CAS").class(),
            ErrorClass::Fatal
        );
        assert!(fenced.to_string().contains("seized"));
    }

    #[test]
    fn builders_cover_the_non_exhaustive_structs() {
        let spec = SplitSpec::new(SplitId::new("s").unwrap(), b"d".to_vec());
        assert_eq!(spec.weight, 1, "default weight");
        assert_eq!(spec.clone().with_weight(64 << 20).weight, 64 << 20);

        let planned = PlannedSplit::new(spec.clone());
        assert!(planned.seed.is_none());
        let seeded = planned.with_seed(SplitProgress::new(7, b"state".to_vec()));
        assert_eq!(seeded.seed.as_ref().unwrap().watermark, 7);

        let plan = SplitPlan::new(vec![seeded], PlanFinality::Open);
        assert!(plan.planner_state.is_none());
        assert_eq!(
            plan.with_planner_state(b"cursor".to_vec())
                .planner_state
                .as_deref(),
            Some(b"cursor".as_slice())
        );

        let running = SplitProgress::new(7, vec![]);
        assert!(!running.completed);
        assert!(SplitProgress::completed(7, vec![]).completed);

        let ctx = PlanContext::new(Some(b"cursor"), 3);
        assert_eq!(ctx.generation, 3);
    }

    #[test]
    fn lease_epochs_order_across_owners() {
        assert!(LeaseEpoch(2) > LeaseEpoch(1));
    }
}
