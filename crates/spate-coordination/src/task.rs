//! The single-writer background task: one per process owns **all** store
//! I/O, so every local decision is serialized against every local write.
//!
//! The loop is watch-driven. Lease deletions and record changes arrive as
//! push events, so claims and revocations run immediately on the deltas. A
//! periodic reconcile listing is the missed-event backstop, and a jittered
//! heartbeat tick renews every owned lease at a third of the TTL. The
//! planner runs on the blocking pool and is awaited as a **select arm**,
//! never inline, so a slow enumeration cannot stall renewals.
//!
//! On a store whose watch is polled, the durable watch covers only the
//! assignment records, the plan record and the `verdict` marker. Each
//! worker reads the records of splits it was assigned and has not seen, and
//! the leader re-reads, every poll interval, each assigned split that shows
//! no lease; only the leader reconciles, over the split records. A worker
//! that reports the job terminal writes the marker, and a worker that sees
//! it lists the split records and judges, whatever its own view covers.
//!
//! Correctness recap (see `protocol.rs` for the pure rules): the durable
//! progress record's CAS revision is the only fence; lease keys are
//! liveness. A zombie's commit that lands *before* a takeover CAS is legal
//! (it was still the owner; progress is monotone) and is adopted by the
//! claimant on its CAS retry, which reduces replay.

use crate::config::CoordinationConfig;
use crate::error::{retryable, store_error};
use crate::leader::{PlanRun, SeedEvent, SeedRun, SeedSteps};
use crate::protocol::{self, SplitState};
use crate::records::{self, AssignmentVal, LeaderVal, LeaseVal, PlanRecord, SplitSpecRecord};
use crate::store::{
    CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchMode,
};
use futures_util::FutureExt as _;
use futures_util::StreamExt as _;
use futures_util::future::BoxFuture;
use spate_core::clock::tokio::Clock;
use spate_core::coordination::ControlWaker;
use spate_core::coordination::{
    CoordinationError, CoordinationErrorKind, CoordinationEvent, SplitId, SplitPlanner,
    SplitProgress,
};
use spate_core::metrics::CoordinationMetrics;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::mpsc as std_mpsc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

mod assignment;
mod claim;
mod command;
mod depart;
mod reconcile;
mod renew;
mod revoke;
mod startup;
mod terminal;
mod watch;

/// Control-thread → task requests. Replies go over a rendezvous-sized
/// std channel so the controller's bounded `recv_timeout` needs no
/// polling loop.
pub(crate) enum Command {
    Commit {
        split: SplitId,
        progress: SplitProgress,
        reply: std_mpsc::SyncSender<Result<(), CoordinationError>>,
    },
    Fail {
        split: SplitId,
        reason: String,
        reply: std_mpsc::SyncSender<Result<(), CoordinationError>>,
    },
    Release {
        splits: Vec<SplitId>,
        /// Whether this release is a departure from the fleet rather than a
        /// revocation hand-back. Only a departure that empties the working
        /// set retires this worker; a revocation of the last split keeps it
        /// in the fleet. Shutdown sends `Depart`.
        departure: bool,
        reply: std_mpsc::SyncSender<Result<(), CoordinationError>>,
    },
    /// Leave the job for good, retrying each write until `deadline`. The
    /// task stops once it has run it.
    Depart {
        deadline: std::time::Instant,
        reply: std_mpsc::SyncSender<DepartReply>,
    },
    /// The source cannot stop this split at a safe boundary. A revocation
    /// is a decision, so the split still goes back, by the forced,
    /// replaying route rather than the clean one.
    DeclineRevoke {
        split: SplitId,
        reply: std_mpsc::SyncSender<Result<(), CoordinationError>>,
    },
}

impl Command {
    /// Answer without running it.
    fn refuse(self, error: CoordinationError) {
        match self {
            Command::Commit { reply, .. }
            | Command::Fail { reply, .. }
            | Command::Release { reply, .. }
            | Command::DeclineRevoke { reply, .. } => {
                let _ = reply.try_send(Err(error));
            }
            Command::Depart { reply, .. } => {
                let _ = reply.try_send(DepartReply::Refused(error));
            }
        }
    }
}

/// The task's answer to a `Depart`.
pub(crate) enum DepartReply {
    /// The task ran the departure and writes nothing more. `unreleased`
    /// names each split, with its epoch, that it may have left behind.
    Ran {
        result: Result<(), CoordinationError>,
        unreleased: Vec<(SplitId, u64)>,
    },
    /// The task could not take the command yet.
    Refused(CoordinationError),
}

/// What a departure could not do: its first fatal error, the operations the
/// store did not confirm, and the splits it may have left behind.
#[derive(Default)]
struct Shortfall {
    fatal: Option<CoordinationError>,
    undone: Vec<String>,
    unreleased: Vec<(SplitId, u64)>,
}

impl Shortfall {
    fn note(&mut self, what: String, e: &StoreError) {
        match e {
            StoreError::Fatal(_) => {
                self.fatal.get_or_insert_with(|| store_error(&what, e));
            }
            StoreError::Retryable(_) => self.undone.push(what),
        }
    }

    fn fatal(&mut self, e: CoordinationError) {
        self.fatal.get_or_insert(e);
    }

    fn into_reply(self) -> DepartReply {
        let result = match self.fatal {
            Some(e) => Err(e),
            None if self.undone.is_empty() => Ok(()),
            None => Err(retryable(format!(
                "departure unconfirmed by the store before its deadline: {}",
                self.undone.join("; ")
            ))),
        };
        DepartReply::Ran {
            result,
            unreleased: self.unreleased,
        }
    }
}

/// Task → control-thread notifications.
pub(crate) enum TaskEvent {
    Coordination(CoordinationEvent),
    /// The task hit a fatal error and stopped; every later call fails.
    Failed(CoordinationErrorKind, String),
}

/// One split this worker holds. The authoritative record lives once, in
/// `splits`. This carries only what the view cannot: the lease revision
/// to CAS renewals against, the self-fence clock, and the record's attempts
/// at the start of the tenancy.
struct OwnedSplit {
    lease_rev: Revision,
    /// Last successful lease write, for renewal cadence and the
    /// starvation self-fence.
    last_ok_write: Instant,
    /// The record's delivery attempts when this tenancy began; a same-epoch
    /// owner-cleared record with more is this tenancy's own failure report.
    attempts: u32,
    /// Start of the first renewal since the last confirmed one that failed
    /// with an error; it may have applied.
    unconfirmed_since: Option<Instant>,
}

/// One listing's result.
pub(crate) type Listed = Result<Vec<Entry>, StoreError>;

/// A listing read beside the task loop.
pub(crate) type Listing = BoxFuture<'static, Listed>;

/// Point reads of durable keys beside the task loop, each with its result.
pub(crate) type Reads = BoxFuture<'static, Vec<(String, Result<Option<Entry>, StoreError>)>>;

/// Point reads one read run keeps in flight at once.
const READ_CONCURRENCY: usize = 64;

/// A reconcile whose listings are in flight, and the view's revisions when
/// they began.
pub(crate) struct ReconcileRun {
    listings: BoxFuture<'static, (Listed, Listed)>,
    started: Instant,
    leader: Option<Revision>,
    presence: BTreeMap<String, Revision>,
    leases: BTreeMap<String, Revision>,
    assignments: BTreeMap<String, Revision>,
}

/// What the view learned while a reconcile listing was in flight.
#[derive(Default)]
struct SinceListing {
    /// Ephemeral keys deleted from the view.
    leases: BTreeSet<String>,
    /// Durable keys deleted from the view.
    records: BTreeSet<String>,
    /// The lease view was rebuilt from a newer snapshot.
    leases_rebuilt: bool,
}

/// How one release attempt ended. The caller needs the distinction to
/// avoid reporting a tenancy end twice: a fenced release has already been
/// announced by [`Task::drop_owned`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReleaseOutcome {
    /// The owner-clear landed; this worker gave the split up.
    Released,
    /// The CAS lost: a peer had already taken the split.
    Fenced,
    /// The write failed or its outcome could not be read back; the lease
    /// key was dropped best-effort.
    WriteFailed,
    /// Not held (already released, lost, or completed), or already ended by
    /// this worker's own failure report.
    Missing,
}

/// One split this worker is draining away because the leader stopped
/// assigning it. The drain is cooperative (stop intake at a safe
/// boundary, chase the tail to a final fenced commit, release), so it
/// replays nothing. The deadline stops a wedged drain from pinning a
/// rebalance open forever.
struct Revoking {
    /// When the revocation was requested: the drain deadline's anchor and
    /// the `drain` phase of `spate_coordination_drain_duration_seconds`.
    /// Drawn from the injected `Clock`, so both readers must be too —
    /// measuring it with real `.elapsed()` would compare two timelines and
    /// saturate to zero under a test clock running ahead of wall time.
    started: Instant,
    /// The last commit this worker landed for the split, same clock. Only
    /// a *cancelled* entry is judged against it: a live revocation holds a
    /// rebalance open and gets one absolute deadline, whereas a cancelled
    /// one has no rebalance waiting on it and is bounded instead by going
    /// quiet.
    last_progress: Instant,
    /// The leader took this revocation back, but the drain it started is
    /// still out there. The entry outlives the revocation to bound that
    /// drain. A source cannot be asked to resume intake it has already
    /// stopped, so a drain that never finishes strands the split with
    /// nothing reading it. Already counted
    /// [`RevocationOutcome::Cancelled`](spate_core::metrics::RevocationOutcome::Cancelled);
    /// it owes no second outcome.
    cancelled: bool,
}

pub(crate) struct Task<S: CoordinationStore + Clone> {
    pub(crate) store: S,
    pub(crate) config: CoordinationConfig,
    /// Time source for every deadline in the control loop, covering lease
    /// expiry and the starvation self-fence, the heartbeat/reconcile/replan
    /// cadence, the grace window, the drain deadline, and the renewal
    /// cadence gate. `SystemClock` in production; in tests an injected
    /// clock the test advances, so no transition fires on scheduler jitter.
    /// Anything anchored to it must also be *read* through it. Commands are
    /// served on a clock-independent arm, so a test can drive a coordinator
    /// whose clock is not moving.
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) fingerprint: String,
    pub(crate) fp: u64,
    pub(crate) instance: String,
    pub(crate) nonce: String,
    pub(crate) seed: u64,
    pub(crate) planner: Option<Box<dyn SplitPlanner>>,
    pub(crate) metrics: Option<CoordinationMetrics>,
    pub(crate) commands: mpsc::Receiver<Command>,
    pub(crate) events: std_mpsc::Sender<TaskEvent>,
    /// Signalled after every event pushed to `events`: the driver parks on
    /// it rather than inside `SplitCoordinator::poll`, so an event that is
    /// only queued is an event the driver has not been told about.
    pub(crate) waker: Option<ControlWaker>,

    // Observed store state.
    pub(crate) splits: BTreeMap<String, SplitState>,
    /// Live workers by presence key, with the revision last seen. The
    /// revision orders deletes against puts (stale echoes are ignored).
    pub(crate) presence: BTreeMap<String, Revision>,
    /// Each live member's own lane budget, as it advertised on its presence
    /// key. Kept beside `presence` rather than inside it because only the
    /// leader reads it, and only to feed `desired_assignment`.
    member_caps: BTreeMap<String, u32>,
    /// Every observed `assign.{instance}` record with the revision last
    /// seen. The leader CASes against these revisions to publish, and skips
    /// the write entirely when the desired assignment already matches what
    /// is stored, so a steady-state fleet writes nothing at all. Workers
    /// only ever read their own entry.
    assignments: BTreeMap<String, (AssignmentVal, Revision)>,
    pub(crate) plan: Option<(PlanRecord, Revision)>,
    pub(crate) plan_rev_seen: u64,
    leader_observed: Option<(LeaderVal, Revision)>,

    // Incremental status tallies over `splits`, maintained by
    // `upsert_progress`.
    completed_count: u64,
    quarantined_count: u64,
    runnable_count: u64,

    // Local state.
    owned: BTreeMap<String, OwnedSplit>,
    /// Leases a release left in place, because its read-back answered from
    /// before this worker's latest renewal or its delete at the read revision
    /// failed, by split id with the released tenancy's epoch; each heartbeat
    /// retries until the key is gone or no longer this tenancy's.
    owed_leases: BTreeMap<String, u64>,
    /// Lease observations whose durable record has not arrived yet
    /// (snapshot ordering, watch races): attached when the record shows
    /// up, so a held split can never be misread as expired.
    pending_leases: BTreeMap<String, (LeaseVal, Revision)>,
    /// Spec records observed before their progress record (snapshots may
    /// deliver the two in either order); attached on progress arrival.
    pending_specs: BTreeMap<String, SplitSpecRecord>,
    pub(crate) leadership: Option<Revision>,
    pub(crate) plan_now: bool,
    /// A split may need parking while this worker sits at its lane budget.
    /// `reconcile_assignment` skips its whole-map scan once there is no
    /// claim slot open, so this flag lets a quarantine decision through.
    /// Without it a bounded job with a poison split idles instead of
    /// reaching `Stalled`.
    quarantine_scan: bool,
    /// Set when this worker leaves the fleet. It must not claim, lead or
    /// renew its presence again. A claim re-takes its own hand-backs, and a
    /// renewed presence key has the leader assign it work it never takes.
    parting: bool,
    /// Set once a `Depart` has been answered; the loop stops on it.
    stopping: bool,
    terminal_reported: bool,
    round: u64,
    /// The splits this worker has been told to hold. Empty and
    /// `assignment_seen == false` means the leader has not spoken yet.
    /// That is a different state from "hold nothing"; see
    /// [`Task::reconcile_assignment`].
    assigned: BTreeSet<String>,
    /// Whether any assignment record for this instance has ever been
    /// observed. Absence of an instruction and an instruction to hold
    /// nothing are different states, and conflating them would make a
    /// worker release everything during a leader gap.
    assignment_seen: bool,
    /// Highest assignment generation observed for this instance. A record
    /// stamped below it is a deposed leader's late write and is ignored.
    assign_generation: u64,
    /// Splits currently draining away because the leader stopped assigning
    /// them, keyed by split id.
    revoking: BTreeMap<String, Revoking>,
    /// Splits whose acquisition this worker is still waiting on, with when
    /// they were assigned. This is the input to
    /// `spate_coordination_assignment_latency_seconds`.
    awaiting: BTreeMap<String, Instant>,
    /// Set when the leader's assignment inputs moved (membership, split
    /// status, specs, or a grace window elapsing). `desired_assignment` is
    /// a full recompute over every split, and `step` runs on every watch
    /// event, so recomputing unconditionally made a commit-heavy fleet pay
    /// an O(members x splits) scan per commit. Cleared by the publish.
    assign_dirty: bool,
    /// Leader side only: instances whose presence key vanished, and when.
    /// Splits that still name them as owner are withheld from assignment
    /// until `rebalance_delay` elapses, so a pod that crashed and comes back
    /// reclaims its own work. Cleared the moment the instance reappears.
    departed: BTreeMap<String, Instant>,
    /// The peers last reported by `observe_membership`; `None` until it has
    /// run at all. An empty set instead means a worker that has looked and
    /// is alone. Membership is logged from a diff of this, not from the
    /// presence-key events: `try_rewatch` rebuilds `presence` from a
    /// snapshot, so the events say the whole fleet arrived while the set
    /// says nothing moved.
    reported_members: Option<BTreeSet<String>>,
    /// Leader side only: the membership the last announced assignment was
    /// computed over. A publish whose member set matches it did not follow
    /// a fleet change, so whatever it rewrote came from splits completing.
    announced_members: BTreeSet<String>,
    /// While a reconcile listing is in flight, what the view learned since
    /// it began that the listing cannot know.
    since_listing: Option<SinceListing>,
    /// The terminal verdict wants an authoritative listing.
    terminal_due: bool,
    /// `Some(interval)` on a store whose watches are polled. Every branch
    /// that differs between the two modes reads this field.
    pub(crate) polled: Option<Duration>,
    /// Durable keys a point read found absent or could not read, skipped
    /// until the next refresh tick.
    parked_reads: BTreeSet<String>,
    /// A refresh tick fired: the next read run retries parked keys and
    /// re-reads the leader's unleased assigned splits.
    refresh_due: bool,
    /// A leader on a polled store lists split and spec records before its
    /// first plan run and publish; this is due until a listing lands.
    pub(crate) catch_up_due: bool,
    /// False from a polled election win until that listing lands.
    pub(crate) caught_up: bool,
    /// A `verdict` marker was seen: the view may be partial, so the
    /// verdict listing runs without the local gate.
    verdict_seen: bool,
    /// The verdict marker may start a listing; spent by each listing it
    /// starts and restored on each refresh tick.
    verdict_listing_allowed: bool,
    /// This worker wrote the verdict marker, or saw one.
    verdict_written: bool,
    #[cfg(feature = "testing")]
    probe: Arc<crate::loop_probe::LoopProbe>,
}

impl<S: CoordinationStore + Clone> Task<S> {
    #[expect(clippy::too_many_arguments, reason = "assembled once, by the handle")]
    pub(crate) fn new(
        store: S,
        config: CoordinationConfig,
        clock: Arc<dyn Clock>,
        fingerprint: String,
        instance: String,
        nonce: String,
        planner: Box<dyn SplitPlanner>,
        metrics: Option<CoordinationMetrics>,
        commands: mpsc::Receiver<Command>,
        events: std_mpsc::Sender<TaskEvent>,
        waker: Option<ControlWaker>,
    ) -> Task<S> {
        let seed = protocol::stable_hash_str(0, &format!("{instance}/{nonce}"));
        let fp = records::fingerprint_hash(&fingerprint);
        let polled = match store.watch_mode() {
            WatchMode::Push => None,
            WatchMode::Polled { interval } => Some(interval),
        };
        Task {
            store,
            waker,
            config,
            clock,
            fingerprint,
            fp,
            instance,
            nonce,
            seed,
            planner: Some(planner),
            metrics,
            commands,
            events,
            splits: BTreeMap::new(),
            presence: BTreeMap::new(),
            member_caps: BTreeMap::new(),
            assignments: BTreeMap::new(),
            plan: None,
            plan_rev_seen: 0,
            leader_observed: None,
            completed_count: 0,
            quarantined_count: 0,
            runnable_count: 0,
            owned: BTreeMap::new(),
            owed_leases: BTreeMap::new(),
            pending_leases: BTreeMap::new(),
            pending_specs: BTreeMap::new(),
            leadership: None,
            plan_now: false,
            quarantine_scan: false,
            parting: false,
            stopping: false,
            terminal_reported: false,
            round: 0,
            assigned: BTreeSet::new(),
            assignment_seen: false,
            assign_generation: 0,
            revoking: BTreeMap::new(),
            awaiting: BTreeMap::new(),
            assign_dirty: true,
            departed: BTreeMap::new(),
            reported_members: None,
            announced_members: BTreeSet::new(),
            since_listing: None,
            terminal_due: false,
            polled,
            parked_reads: BTreeSet::new(),
            refresh_due: false,
            catch_up_due: false,
            caught_up: true,
            verdict_seen: false,
            verdict_listing_allowed: false,
            verdict_written: false,
            #[cfg(feature = "testing")]
            probe: Arc::default(),
        }
    }

    /// Report this task's loop through `probe`.
    #[cfg(feature = "testing")]
    pub(crate) fn with_probe(mut self, probe: Arc<crate::loop_probe::LoopProbe>) -> Task<S> {
        self.probe = probe;
        self
    }

    #[cfg(feature = "testing")]
    fn probe_applied(&self, ks: Keyspace, revision: Revision) {
        self.probe.applied(ks, revision);
    }

    #[cfg(not(feature = "testing"))]
    fn probe_applied(&self, _: Keyspace, _: Revision) {}

    #[cfg(feature = "testing")]
    fn probe_loop_top(&self, next_timer: Instant, planning: bool) {
        self.probe.at_loop_top(next_timer, planning);
    }

    #[cfg(not(feature = "testing"))]
    fn probe_loop_top(&self, _: Instant, _: bool) {}

    /// Run to completion (fatal error or handle drop).
    pub(crate) async fn run(mut self) {
        if let Err(e) = self.run_inner().await {
            tracing::error!(error = %e, "coordination task stopped");
            let _ = self
                .events
                .send(TaskEvent::Failed(e.kind, e.reason.clone()));
            if let Some(w) = &self.waker {
                w.wake();
            }
        }
    }

    // The heavyweight handlers below are `Box::pin`ned at their await
    // sites. Inlining them into the select arms builds a future large
    // enough to overflow a debug-build worker stack over a real store.
    async fn run_inner(&mut self) -> Result<(), CoordinationError> {
        Box::pin(self.startup()).await?;
        // Armed before the first step, so an election in that step does not
        // delay the first renewal of presence and the leader key.
        let mut heartbeat = self.clock.now() + self.next_heartbeat();

        let mut lease_watch = Box::pin(self.rewatch(Keyspace::Ephemeral)).await?;
        let mut state_watch = Box::pin(self.rewatch(Keyspace::Durable)).await?;
        Box::pin(self.step()).await?;

        // Store reads that can outlast a lease run beside the loop, so a
        // slow listing never holds up a renewal.
        let mut planning: Option<PlanRun> = None;
        let mut seeding: Option<SeedRun> = None;
        let mut seed_steps = SeedSteps::default();
        let mut reconciling: Option<ReconcileRun> = None;
        let mut terminal: Option<Listing> = None;
        let mut reads: Option<Reads> = None;
        let mut catch_up: Option<Listing> = None;

        // The first reconcile lands anywhere in the first interval, so a
        // fleet started together does not list together.
        let mut reconcile =
            self.clock.now() + protocol::spread(self.seed, self.config.reconcile_interval);
        let mut replan = self.clock.now() + self.config.replan_interval;
        let mut refresh = self.polled.map(|interval| self.clock.now() + interval);

        loop {
            if let Some(run) = &seeding
                && !self.leads(run)
            {
                run.depose();
            }
            if planning.is_none() && seeding.is_none() {
                planning = self.maybe_start_plan()?;
            }
            if terminal.is_none()
                && std::mem::take(&mut self.terminal_due)
                && !self.terminal_reported
            {
                let store = self.store.clone();
                terminal = Some(
                    async move { store.list(Keyspace::Durable, records::SPLIT_PREFIX).await }
                        .boxed(),
                );
            }
            if catch_up.is_none() && std::mem::take(&mut self.catch_up_due) {
                let store = self.store.clone();
                catch_up = Some(
                    async move {
                        let mut entries =
                            store.list(Keyspace::Durable, records::SPLIT_PREFIX).await?;
                        entries.extend(store.list(Keyspace::Durable, records::SPEC_PREFIX).await?);
                        Ok(entries)
                    }
                    .boxed(),
                );
            }
            if reads.is_none() {
                let wanted = self.wanted_reads();
                if !wanted.is_empty() {
                    reads = Some(self.start_reads(wanted));
                }
            }
            let busy = planning.is_some()
                || seeding.is_some()
                || reconciling.is_some()
                || terminal.is_some()
                || reads.is_some()
                || catch_up.is_some();
            let next_timer = heartbeat.min(reconcile).min(replan);
            self.probe_loop_top(refresh.map_or(next_timer, |r| r.min(next_timer)), busy);
            tokio::select! {
                command = self.commands.recv() => {
                    let Some(command) = command else {
                        // Handle dropped: unreleased leases expire and peers take over.
                        return Ok(());
                    };
                    Box::pin(self.handle_command(command)).await?;
                    // The control thread is waiting on these replies.
                    while !self.stopping
                        && let Ok(command) = self.commands.try_recv()
                    {
                        Box::pin(self.handle_command(command)).await?;
                    }
                    if self.stopping {
                        return Ok(());
                    }
                    Box::pin(self.step()).await?;
                }
                event = lease_watch.next() => {
                    match event {
                        Some(Ok(event)) => {
                            if let WatchEvent::Put(Entry { revision, .. })
                            | WatchEvent::Delete { revision, .. } = &event
                            {
                                self.probe_applied(Keyspace::Ephemeral, *revision);
                            }
                            self.apply_lease_event(event)?;
                        }
                        Some(Err(e)) => {
                            tracing::warn!(error = %e, "lease watch broke; re-watching");
                            lease_watch = Box::pin(self.rewatch(Keyspace::Ephemeral)).await?;
                        }
                        None => lease_watch = Box::pin(self.rewatch(Keyspace::Ephemeral)).await?,
                    }
                    Box::pin(self.step()).await?;
                }
                event = state_watch.next() => {
                    match event {
                        Some(Ok(event)) => {
                            if let WatchEvent::Put(Entry { revision, .. })
                            | WatchEvent::Delete { revision, .. } = &event
                            {
                                self.probe_applied(Keyspace::Durable, *revision);
                            }
                            self.apply_state_event(event)?;
                        }
                        Some(Err(e)) => {
                            tracing::warn!(error = %e, "state watch broke; re-watching");
                            state_watch = Box::pin(self.rewatch(Keyspace::Durable)).await?;
                        }
                        None => state_watch = Box::pin(self.rewatch(Keyspace::Durable)).await?,
                    }
                    Box::pin(self.step()).await?;
                }
                () = self.clock.sleep_until(heartbeat) => {
                    self.round += 1;
                    Box::pin(self.heartbeat()).await?;
                    heartbeat = self.clock.now() + self.next_heartbeat();
                    Box::pin(self.step()).await?;
                }
                () = self.clock.sleep_until(reconcile) => {
                    if reconciling.is_none() {
                        reconciling = self.start_reconcile();
                    }
                    reconcile = self.clock.now() + self.next_reconcile();
                }
                listed = async { (&mut reconciling.as_mut().expect("guarded by is_some").listings).await },
                    if reconciling.is_some() =>
                {
                    let run = reconciling.take().expect("selected arm requires it");
                    self.finish_reconcile(&run, listed)?;
                    Box::pin(self.step()).await?;
                }
                listed = async { terminal.as_mut().expect("guarded by is_some").await },
                    if terminal.is_some() =>
                {
                    terminal = None;
                    self.finish_terminal(listed)?;
                    Box::pin(self.step()).await?;
                }
                read = async { reads.as_mut().expect("guarded by is_some").await },
                    if reads.is_some() =>
                {
                    reads = None;
                    self.finish_reads(read)?;
                    Box::pin(self.step()).await?;
                }
                listed = async { catch_up.as_mut().expect("guarded by is_some").await },
                    if catch_up.is_some() =>
                {
                    catch_up = None;
                    self.finish_catch_up(listed)?;
                    Box::pin(self.step()).await?;
                }
                () = self.clock.sleep_until(refresh.unwrap_or(heartbeat)), if refresh.is_some() => {
                    self.refresh_due = true;
                    self.verdict_listing_allowed = self.verdict_seen;
                    self.catch_up_due |= catch_up.is_none()
                        && self.leadership.is_some()
                        && !self.caught_up;
                    refresh = self.polled.map(|interval| self.clock.now() + interval);
                    Box::pin(self.step()).await?;
                }
                () = self.clock.sleep_until(replan) => {
                    if self.leadership.is_some() && self.plan_is_open() {
                        self.plan_now = true;
                    }
                    replan = self.clock.now() + self.config.replan_interval;
                    Box::pin(self.step()).await?;
                }
                joined = async { (&mut planning.as_mut().expect("guarded by is_some").handle).await },
                    if planning.is_some() =>
                {
                    let run = planning.take().expect("selected arm requires it");
                    seeding = self.land_plan(joined, run)?;
                    Box::pin(self.step()).await?;
                }
                event = async { seeding.as_mut().expect("guarded by is_some").next().await },
                    if seeding.is_some() =>
                {
                    match event {
                        SeedEvent::Wins(wins) => {
                            self.fold_seeded(wins)?;
                            if seed_steps.folded(self.clock.now()) {
                                Box::pin(self.step()).await?;
                            }
                        }
                        SeedEvent::Done(seeded, wins) => {
                            self.fold_seeded(wins)?;
                            seed_steps = SeedSteps::default();
                            let run = seeding.take().expect("selected arm requires it");
                            Box::pin(self.finish_plan(run, seeded)).await?;
                            Box::pin(self.step()).await?;
                        }
                    }
                }
                () = self.clock.sleep_until(seed_steps.due().unwrap_or(heartbeat)),
                    if seed_steps.due().is_some() =>
                {
                    seed_steps.fired(self.clock.now());
                    Box::pin(self.step()).await?;
                }
            }
        }
    }

    fn next_heartbeat(&self) -> Duration {
        protocol::jitter(self.seed, self.round, self.config.renew_interval())
    }

    /// Reconcile ticks are jittered like heartbeats, on their own key.
    fn next_reconcile(&self) -> Duration {
        protocol::jitter(
            self.seed.rotate_left(32),
            self.round,
            self.config.reconcile_interval,
        )
    }

    pub(crate) fn plan_is_open(&self) -> bool {
        self.plan
            .as_ref()
            .is_none_or(|(p, _)| p.finality == records::PlanFinalityRepr::Open)
    }

    pub(crate) fn emit(&self, event: CoordinationEvent) {
        // The handle side is unbounded; a send fails only when the handle
        // is gone, and the command channel closure stops the loop then.
        let _ = self.events.send(TaskEvent::Coordination(event));
        if let Some(w) = &self.waker {
            w.wake();
        }
    }

    async fn step(&mut self) -> Result<(), CoordinationError> {
        self.prune_departed();
        self.observe_membership();
        if self.terminal_reported {
            if self.polled.is_some() && !self.verdict_written {
                self.write_verdict().await?;
            }
            self.update_gauges();
            return Ok(());
        }
        if self.parting {
            // Leaving the fleet: observe only. The released work belongs to the others.
            self.check_terminal();
            self.update_gauges();
            return Ok(());
        }
        if self.leader_observed.is_none() && self.leadership.is_none() {
            self.try_elect().await?;
        }
        // Decide before reconciling: this worker acts on its own fresh
        // assignment in the same step.
        if self.leadership.is_some() {
            self.publish_assignments().await?;
        }
        self.reconcile_assignment().await?;
        self.service_revocations().await?;
        self.check_terminal();
        self.update_gauges();
        Ok(())
    }

    /// Report membership transitions observed since the last step.
    ///
    /// The lines come from a diff of the presence set, not from the
    /// presence-key events: `try_rewatch` rebuilds `presence` from a
    /// snapshot, so the events say the whole fleet arrived at every watch
    /// reconnect while the set says nothing moved. A worker running alone
    /// logs nothing, and one that starts into a running fleet says so once
    /// rather than once per peer.
    fn observe_membership(&mut self) {
        // This runs on every step, which is every watch event.
        let peers = || self.presence.keys().filter(|i| **i != self.instance);
        if let Some(reported) = &self.reported_members
            && peers().eq(reported.iter())
        {
            return;
        }
        let members: BTreeSet<String> = peers().cloned().collect();
        let live = self.live_workers();
        match &self.reported_members {
            None => {
                if !members.is_empty() {
                    let found = members
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>()
                        .join(", ");
                    tracing::info!(live, peers = %found, "joined a fleet already running");
                }
            }
            Some(previous) => {
                for instance in members.difference(previous) {
                    tracing::info!(instance = %instance, live, "peer joined");
                }
                for instance in previous.difference(&members) {
                    tracing::info!(instance = %instance, live, "peer left");
                }
            }
        }
        self.reported_members = Some(members);
    }

    /// Fleet size as this worker reports it; once parting, it counts itself
    /// only while its own presence key is in its view.
    fn live_workers(&self) -> usize {
        protocol::live_workers(
            &self.presence,
            (!self.parting).then_some(self.instance.as_str()),
        )
    }

    fn update_gauges(&self) {
        self.metrics(|m| {
            m.set_splits_owned(self.owned.len());
            m.set_splits_completed(usize::try_from(self.completed_count).unwrap_or(usize::MAX));
            m.set_splits_quarantined(usize::try_from(self.quarantined_count).unwrap_or(usize::MAX));
            m.set_live_workers(self.live_workers());
            m.set_leader(self.leadership.is_some());
            m.set_idle(self.owned.is_empty());
            m.set_splits_draining(self.revoking.len());
        });
    }

    pub(crate) fn metrics(&self, f: impl FnOnce(&CoordinationMetrics)) {
        if let Some(m) = &self.metrics {
            f(m);
        }
    }
}
