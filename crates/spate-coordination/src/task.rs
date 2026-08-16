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
//! Correctness recap (see `protocol.rs` for the pure rules): the durable
//! progress record's CAS revision is the only fence; lease keys are
//! liveness. A zombie's commit that lands *before* a takeover CAS is legal
//! (it was still the owner; progress is monotone) and is adopted by the
//! claimant on its CAS retry, which reduces replay.

use crate::clock::Clock;
use crate::config::CoordinationConfig;
use crate::error::{fatal, store_error};
use crate::leader::PlanRun;
use crate::protocol::{self, ClaimAction, ClaimKind, SplitState};
use crate::records::{
    self, AssignmentVal, LeaderVal, LeaseVal, PlanRecord, SplitProgressRecord, SplitSpecRecord,
    SplitStatus, WorkerVal,
};
use crate::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, WatchEvent, WatchStream,
};
use futures_util::StreamExt as _;
use spate_core::coordination::ControlWaker;
use spate_core::coordination::{
    CoordinationError, CoordinationErrorKind, CoordinationEvent, LeaseEpoch, SplitId, SplitPlanner,
    SplitProgress,
};
use spate_core::metrics::{
    AcquireReason, CoordinationMetrics, RevocationOutcome, SplitLossReason, WriteOutcome,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::mpsc as std_mpsc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

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
        /// Whether this release is a departure from the fleet (shutdown,
        /// scale-down) rather than a revocation hand-back. Only a
        /// departure that empties the working set retires this worker; a
        /// revocation of the last split keeps it in the fleet.
        departure: bool,
        reply: std_mpsc::SyncSender<Result<(), CoordinationError>>,
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
    fn reply_channel(&self) -> &std_mpsc::SyncSender<Result<(), CoordinationError>> {
        match self {
            Command::Commit { reply, .. }
            | Command::Fail { reply, .. }
            | Command::Release { reply, .. }
            | Command::DeclineRevoke { reply, .. } => reply,
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
/// to CAS renewals against and the self-fence clock.
struct OwnedSplit {
    lease_rev: Revision,
    /// Last successful lease write, for renewal cadence and the
    /// starvation self-fence.
    last_ok_write: Instant,
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
    /// The write failed; the lease key was dropped best-effort.
    WriteFailed,
    /// Not held (already released, lost, or completed).
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
    /// [`RevocationOutcome::Cancelled`]; it owes no second outcome.
    cancelled: bool,
}

pub(crate) struct Task<S: CoordinationStore> {
    pub(crate) store: S,
    pub(crate) config: CoordinationConfig,
    /// Time source for every deadline in the control loop, covering lease
    /// expiry and the starvation self-fence, the heartbeat/reconcile/replan
    /// cadence, the grace window, the drain deadline, and the renewal
    /// cadence gate. `SystemClock` in production; in tests an injected
    /// clock the test advances, so no transition fires on scheduler jitter.
    /// Anything anchored to it must also be *read* through it.
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
    /// Set when this worker released its last split. The worker is leaving
    /// the fleet, so it must not claim or lead again; otherwise it
    /// re-claims its own hand-backs.
    parting: bool,
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
    /// Their splits are withheld from assignment until `rebalance_delay`
    /// elapses, so a pod bounce reclaims its own work instead of the fleet
    /// churning around it. Cleared the moment the instance reappears.
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
}

impl<S: CoordinationStore> Task<S> {
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
            pending_leases: BTreeMap::new(),
            pending_specs: BTreeMap::new(),
            leadership: None,
            plan_now: false,
            quarantine_scan: false,
            parting: false,
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
        }
    }

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

        let mut lease_watch = Box::pin(self.rewatch(Keyspace::Ephemeral)).await?;
        let mut state_watch = Box::pin(self.rewatch(Keyspace::Durable)).await?;
        Box::pin(self.step()).await?;

        let mut planning: Option<PlanRun> = None;

        let mut heartbeat = self.clock.now() + self.next_heartbeat();
        let mut reconcile = self.clock.now() + self.config.reconcile_interval;
        let mut replan = self.clock.now() + self.config.replan_interval;

        loop {
            if planning.is_none() {
                planning = self.maybe_start_plan()?;
            }
            tokio::select! {
                command = self.commands.recv() => {
                    let Some(command) = command else {
                        // Handle dropped: unreleased leases expire and peers take over.
                        return Ok(());
                    };
                    Box::pin(self.handle_command(command)).await?;
                    // The control thread is waiting on these replies.
                    while let Ok(command) = self.commands.try_recv() {
                        Box::pin(self.handle_command(command)).await?;
                    }
                    Box::pin(self.step()).await?;
                }
                event = lease_watch.next() => {
                    match event {
                        Some(Ok(event)) => self.apply_lease_event(event)?,
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
                        Some(Ok(event)) => self.apply_state_event(event)?,
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
                    Box::pin(self.reconcile()).await?;
                    reconcile = self.clock.now() + self.config.reconcile_interval;
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
                    Box::pin(self.finish_plan(joined, run)).await?;
                    Box::pin(self.step()).await?;
                }
            }
        }
    }

    fn next_heartbeat(&self) -> Duration {
        protocol::jitter(self.seed, self.round, self.config.renew_interval())
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

    // ------------------------------------------------------------------
    // Startup.

    async fn startup(&mut self) -> Result<(), CoordinationError> {
        self.budgeted("store probe", Self::probe).await?;
        self.budgeted("joining the job", Self::join_job).await?;
        self.budgeted("announcing presence", Self::announce).await?;
        Ok(())
    }

    /// Startup-budgeted retry: capped exponential backoff, fatal after
    /// the configured attempts. Steady-state operations are NOT budgeted —
    /// they retry on later ticks and escalate through lease expiry.
    async fn budgeted<F>(&mut self, what: &str, op: F) -> Result<(), CoordinationError>
    where
        F: AsyncFn(&mut Self) -> Result<(), CoordinationError>,
    {
        let mut delay = Duration::from_millis(200);
        for attempt in 1..=self.config.startup_max_attempts {
            match op(self).await {
                Ok(()) => return Ok(()),
                Err(e) if e.kind == CoordinationErrorKind::Retryable => {
                    tracing::warn!(attempt, error = %e, "{what} failed; retrying");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(5));
                }
                Err(e) => return Err(e),
            }
        }
        Err(fatal(format!(
            "{what} did not succeed within {} attempts",
            self.config.startup_max_attempts
        )))
    }

    /// Verify the store's conditional semantics before trusting fencing
    /// to them: create wins, duplicate create loses, stale update loses,
    /// guarded delete works. All four are checked in both keyspaces.
    async fn probe(&mut self) -> Result<(), CoordinationError> {
        for ks in [Keyspace::Durable, Keyspace::Ephemeral] {
            let key = records::probe_key(&self.instance);
            let ctx = "store probe";
            let rev = match self
                .store
                .create(ks, &key, b"probe".to_vec())
                .await
                .map_err(|e| store_error(ctx, &e))?
            {
                CasOutcome::Won(rev) => rev,
                CasOutcome::Lost => {
                    // Leftover from a crashed run: clear and re-probe.
                    let _ = self
                        .store
                        .delete(ks, &key, None)
                        .await
                        .map_err(|e| store_error(ctx, &e))?;
                    return Err(crate::error::retryable(
                        "probe key existed (crashed predecessor?); cleared, retrying",
                    ));
                }
            };
            if self
                .store
                .create(ks, &key, b"dup".to_vec())
                .await
                .map_err(|e| store_error(ctx, &e))?
                != CasOutcome::Lost
            {
                return Err(fatal(
                    "store accepted a duplicate create: create-if-absent is not enforced; \
                     this store cannot host coordination",
                ));
            }
            let rev2 = match self
                .store
                .update(ks, &key, b"update".to_vec(), rev)
                .await
                .map_err(|e| store_error(ctx, &e))?
            {
                CasOutcome::Won(rev2) => rev2,
                CasOutcome::Lost => {
                    return Err(fatal(
                        "store rejected a matched-revision update: CAS is broken; this \
                         store cannot host coordination",
                    ));
                }
            };
            if self
                .store
                .update(ks, &key, b"stale".to_vec(), rev)
                .await
                .map_err(|e| store_error(ctx, &e))?
                != CasOutcome::Lost
            {
                return Err(fatal(
                    "store accepted a stale-revision update: compare-and-swap is not \
                     enforced; fencing would corrupt silently; this store cannot host \
                     coordination",
                ));
            }
            let _ = self
                .store
                .delete(ks, &key, Some(rev2))
                .await
                .map_err(|e| store_error(ctx, &e))?;
        }
        Ok(())
    }

    /// Read or create the plan record; the fingerprint check rejects a
    /// divergently-configured worker before it can touch anything.
    async fn join_job(&mut self) -> Result<(), CoordinationError> {
        let ctx = "reading the plan record";
        if let Some(entry) = self
            .store
            .get(Keyspace::Durable, records::PLAN_KEY)
            .await
            .map_err(|e| store_error(ctx, &e))?
        {
            let plan = PlanRecord::parse(&entry.value, &self.fingerprint)?;
            self.plan = Some((plan, entry.revision));
            return Ok(());
        }
        let fresh = PlanRecord::new(self.fingerprint.clone());
        match self
            .store
            .create(Keyspace::Durable, records::PLAN_KEY, fresh.encode())
            .await
            .map_err(|e| store_error("creating the plan record", &e))?
        {
            CasOutcome::Won(rev) => {
                self.plan = Some((fresh, rev));
                Ok(())
            }
            CasOutcome::Lost => Err(crate::error::retryable(
                "lost the plan-creation race; re-reading",
            )),
        }
    }

    /// This worker's presence value. It advertises the lane budget so the
    /// leader balances against each member's own `max_in_flight` rather
    /// than assuming the fleet is homogeneous.
    fn worker_val(&self) -> WorkerVal {
        WorkerVal {
            schema: records::SCHEMA,
            nonce: self.nonce.clone(),
            max_in_flight: self.config.max_in_flight,
        }
    }

    /// Write the worker presence key (taking over a dead predecessor's).
    async fn announce(&mut self) -> Result<(), CoordinationError> {
        let key = records::worker_key(&self.instance);
        let val = records::encode_val(&self.worker_val());
        let ctx = "announcing presence";
        match self
            .store
            .create(Keyspace::Ephemeral, &key, val.clone())
            .await
            .map_err(|e| store_error(ctx, &e))?
        {
            CasOutcome::Won(_) => Ok(()),
            CasOutcome::Lost => {
                // A presence key under our id: a predecessor not yet
                // expired, or a live twin that lease fencing catches.
                let entry = self
                    .store
                    .get(Keyspace::Ephemeral, &key)
                    .await
                    .map_err(|e| store_error(ctx, &e))?;
                match entry {
                    None => Err(crate::error::retryable("presence key vanished; retrying")),
                    Some(entry) => {
                        match self
                            .store
                            .update(Keyspace::Ephemeral, &key, val, entry.revision)
                            .await
                            .map_err(|e| store_error(ctx, &e))?
                        {
                            CasOutcome::Won(_) => Ok(()),
                            CasOutcome::Lost => {
                                Err(crate::error::retryable("presence key contended; retrying"))
                            }
                        }
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Watch plumbing.

    /// (Re-)establish a watch: drain its snapshot into a rebuilt view,
    /// return the live tail. Unbudgeted; retries until the store answers.
    /// While it retries, queued commands are refused as Retryable so the
    /// controller's bounded waits fail fast instead of backing up behind
    /// an unreachable store and wedging the control thread.
    async fn rewatch(&mut self, ks: Keyspace) -> Result<WatchStream, CoordinationError> {
        loop {
            match self.try_rewatch(ks).await {
                Ok(stream) => return Ok(stream),
                Err(e) if e.kind == CoordinationErrorKind::Retryable => {
                    tracing::warn!(error = %e, "watch establishment failed; retrying");
                    while let Ok(command) = self.commands.try_recv() {
                        let reason = format!(
                            "store unreachable while re-establishing watches: {}",
                            e.reason
                        );
                        let _ = command.reply_channel().try_send(Err(CoordinationError::new(
                            CoordinationErrorKind::Retryable,
                            reason,
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn try_rewatch(&mut self, ks: Keyspace) -> Result<WatchStream, CoordinationError> {
        let mut stream = self
            .store
            .watch(ks, "")
            .await
            .map_err(|e| store_error("establishing watch", &e))?;
        let mut snapshot = Vec::new();
        loop {
            match stream.next().await {
                Some(Ok(WatchEvent::SnapshotDone)) => break,
                Some(Ok(WatchEvent::Put(entry))) => snapshot.push(entry),
                Some(Ok(WatchEvent::Delete { .. })) => {}
                Some(Err(e)) => return Err(store_error("watch snapshot", &e)),
                None => {
                    return Err(crate::error::retryable(
                        "watch stream ended during snapshot",
                    ));
                }
            }
        }
        // The snapshot is authoritative for its keyspace: rebuild.
        match ks {
            Keyspace::Ephemeral => {
                self.presence.clear();
                self.member_caps.clear();
                self.assign_dirty = true;
                self.leader_observed = None;
                self.pending_leases.clear();
                for state in self.splits.values_mut() {
                    state.lease = None;
                }
                for entry in snapshot {
                    self.apply_lease_put(&entry)?;
                }
            }
            Keyspace::Durable => {
                for entry in snapshot {
                    self.apply_state_put(&entry)?;
                }
            }
        }
        Ok(stream)
    }

    fn apply_lease_event(&mut self, event: WatchEvent) -> Result<(), CoordinationError> {
        match event {
            WatchEvent::Put(entry) => self.apply_lease_put(&entry),
            WatchEvent::Delete { key, revision } => {
                self.apply_lease_delete(&key, Some(revision));
                Ok(())
            }
            WatchEvent::SnapshotDone => Ok(()),
        }
    }

    fn apply_lease_put(&mut self, entry: &Entry) -> Result<(), CoordinationError> {
        if entry.key == records::LEADER_KEY {
            let leader: LeaderVal = records::parse_val(&entry.key, &entry.value)?;
            if self.leadership.is_some() && leader.nonce != self.nonce {
                // Deposed: someone else won the key after our lease
                // lapsed. The generation fence rejects our plan writes.
                tracing::warn!(new_leader = %leader.owner, "leadership lost");
                self.leadership = None;
                self.metrics(|m| m.set_leader(false));
            }
            self.leader_observed = Some((leader, entry.revision));
            return Ok(());
        }
        if let Some(instance) = records::parse_worker_key(&entry.key) {
            if self
                .presence
                .insert(instance.to_string(), entry.revision)
                .is_none()
            {
                self.assign_dirty = true; // membership grew
            }
            // An unreadable presence value costs balance, never safety:
            // the member keeps the leader's own budget.
            match records::parse_val::<WorkerVal>(&entry.key, &entry.value) {
                Ok(worker) if worker.max_in_flight > 0 => {
                    if self
                        .member_caps
                        .insert(instance.to_string(), worker.max_in_flight)
                        != Some(worker.max_in_flight)
                    {
                        self.assign_dirty = true;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(key = %entry.key, error = %e, "unreadable presence value");
                }
            }
            return Ok(());
        }
        if let Some(id) = records::parse_split_key(&entry.key) {
            // A put at or below the revision the view already holds is a
            // stale echo and must not be applied.
            if let Some(state) = self.splits.get(id)
                && state
                    .lease
                    .as_ref()
                    .is_some_and(|(_, rev)| *rev >= entry.revision)
            {
                return Ok(());
            }
            if self
                .pending_leases
                .get(id)
                .is_some_and(|(_, rev)| *rev >= entry.revision)
            {
                return Ok(());
            }
            let lease: LeaseVal = records::parse_val(&entry.key, &entry.value)?;
            if self.owned.contains_key(id) {
                // Someone rewrote the lease of a split we hold. Our own
                // heartbeat echoes match our nonce; anything else fenced us.
                if lease.nonce != self.nonce {
                    if lease.owner == self.instance {
                        return Err(fatal(format!(
                            "two live workers share instance_id {:?} (foreign nonce on our \
                             lease for split {id}); instance ids must be unique per live \
                             worker — use the pod name, not a constant",
                            self.instance
                        )));
                    }
                    tracing::warn!(split = %id, thief = %lease.owner, "lease taken; split lost");
                    self.drop_owned(id, SplitLossReason::Fenced);
                }
            }
            match self.splits.get_mut(id) {
                Some(state) => {
                    if state
                        .lease
                        .as_ref()
                        .is_none_or(|(l, _)| l.owner != lease.owner)
                    {
                        self.assign_dirty = true;
                    }
                    state.lease = Some((lease, entry.revision));
                }
                None => {
                    self.pending_leases
                        .insert(id.to_string(), (lease, entry.revision));
                }
            }
        }
        Ok(())
    }

    /// Apply a lease-key deletion. `revision` is the deletion's own
    /// revision when it came from a watch (used to discard stale echoes
    /// of deletes the key has since been rewritten past); `None` means
    /// authoritative absence from a reconcile listing.
    fn apply_lease_delete(&mut self, key: &str, revision: Option<Revision>) {
        let newer_than = |current: Revision| revision.is_none_or(|rev| rev > current);
        if key == records::LEADER_KEY {
            if !self
                .leader_observed
                .as_ref()
                .is_none_or(|(_, rev)| newer_than(*rev))
            {
                return; // stale echo: the key was rewritten after this delete
            }
            self.leader_observed = None;
            if let Some(rev) = self.leadership
                && newer_than(rev)
            {
                tracing::warn!("leadership lease expired");
                self.leadership = None;
                self.metrics(|m| m.set_leader(false));
            }
            return;
        }
        if let Some(instance) = records::parse_worker_key(key) {
            if self.presence.get(instance).copied().is_none_or(newer_than) {
                self.presence.remove(instance);
                self.member_caps.remove(instance);
                self.assign_dirty = true;
                // Start this instance's grace window. Every worker tracks
                // it: leadership can move between the departure and the
                // next publish.
                if instance != self.instance {
                    let now = self.clock.now();
                    self.departed.entry(instance.to_string()).or_insert(now);
                }
            }
            return;
        }
        if let Some(id) = records::parse_split_key(key) {
            if let Some(owned) = self.owned.get(id) {
                if !newer_than(owned.lease_rev) {
                    return; // stale echo of a delete our claim already replaced
                }
                // Heartbeats have been failing for a full TTL.
                self.drop_owned(id, SplitLossReason::Starved);
            }
            // No revocation bookkeeping here. Lease deletes also happen
            // for fails, completions, departures, and expiry; the
            // leader's `assign.{instance}` record starts and ends one.
            if let Some(state) = self.splits.get_mut(id)
                && state.lease.as_ref().is_none_or(|(_, rev)| newer_than(*rev))
            {
                if state.lease.is_some() {
                    self.assign_dirty = true;
                }
                state.lease = None;
                // An expired owner may sit at the attempts cap: give the
                // next pass a chance to park it even at target.
                if state.progress.status == SplitStatus::Runnable
                    && state.progress.attempts + 1 >= self.config.max_attempts
                {
                    self.quarantine_scan = true;
                }
            }
            if self
                .pending_leases
                .get(id)
                .is_some_and(|(_, rev)| newer_than(*rev))
            {
                self.pending_leases.remove(id);
            }
        }
    }

    fn apply_state_event(&mut self, event: WatchEvent) -> Result<(), CoordinationError> {
        match event {
            WatchEvent::Put(entry) => self.apply_state_put(&entry),
            WatchEvent::Delete { key, .. } => {
                // The protocol deletes two durable keys: assignment
                // records for departed instances, and startup probe keys.
                if let Some(instance) = records::parse_assign_key(&key) {
                    self.assignments.remove(instance);
                    if instance == self.instance {
                        // An absent record means "nothing has been
                        // decided", never "release everything".
                        self.assignment_seen = false;
                        self.assigned.clear();
                        self.awaiting.clear();
                    }
                } else if let Some(instance) = records::parse_probe_key(&key) {
                    tracing::debug!(instance = %instance, "startup probe key cleared");
                } else {
                    tracing::warn!(
                        key = %key,
                        "durable record deleted externally; reconcile treats it as absent"
                    );
                }
                Ok(())
            }
            WatchEvent::SnapshotDone => Ok(()),
        }
    }

    pub(crate) fn apply_state_put(&mut self, entry: &Entry) -> Result<(), CoordinationError> {
        if let Some(instance) = records::parse_assign_key(&entry.key) {
            // Stale-echo guard, as on every other watched key.
            if self
                .assignments
                .get(instance)
                .is_some_and(|(_, rev)| *rev >= entry.revision)
            {
                return Ok(());
            }
            // An unreadable assignment carries no ownership: ignoring it
            // costs balance and this worker keeps what it holds.
            let val: AssignmentVal = match records::parse_val(&entry.key, &entry.value) {
                Ok(val) => val,
                Err(e) => {
                    tracing::warn!(key = %entry.key, error = %e, "unreadable assignment ignored");
                    return Ok(());
                }
            };
            self.apply_assignment(instance, val, entry.revision);
            return Ok(());
        }
        if entry.key == records::PLAN_KEY {
            if entry.revision.0 <= self.plan_rev_seen {
                return Ok(());
            }
            self.plan_rev_seen = entry.revision.0;
            let plan = PlanRecord::parse(&entry.value, &self.fingerprint)?;
            if self.leadership.is_some()
                && let Some((current, _)) = &self.plan
                && plan.generation > current.generation
            {
                tracing::warn!(
                    generation = plan.generation,
                    "deposed by a newer plan generation"
                );
                self.leadership = None;
                self.metrics(|m| m.set_leader(false));
            }
            self.plan = Some((plan, entry.revision));
            return Ok(());
        }
        if let Some(id) = records::parse_spec_key(&entry.key) {
            let record = SplitSpecRecord::parse(&entry.key, &entry.value, self.fp)?;
            self.attach_spec(id, record);
            return Ok(());
        }
        if let Some(id) = records::parse_split_key(&entry.key) {
            let record = SplitProgressRecord::parse(&entry.key, &entry.value, self.fp)?;
            self.upsert_progress(id, record, entry.revision)?;
        }
        Ok(())
    }

    /// Attach an observed spec record (immutable, so re-deliveries are
    /// echoes) to its split, or buffer it until the progress record lands.
    pub(crate) fn attach_spec(&mut self, id: &str, record: SplitSpecRecord) {
        match self.splits.get_mut(id) {
            Some(state) => {
                if state.spec.is_none() {
                    state.spec = Some(record);
                    // A split becomes assignable once its spec is observed.
                    self.assign_dirty = true;
                }
            }
            None => {
                self.pending_specs.entry(id.to_string()).or_insert(record);
            }
        }
    }

    /// Fold a progress record into the view, from a watch event, a
    /// reconcile listing, or our own successful write. This is the ONLY
    /// place progress state changes: it keeps the status tallies exact,
    /// fences our ownership when a peer's higher epoch arrives, and emits
    /// the `Quarantined` transition exactly once.
    pub(crate) fn upsert_progress(
        &mut self,
        id: &str,
        record: SplitProgressRecord,
        rev: Revision,
    ) -> Result<(), CoordinationError> {
        let (previous_status, current_epoch) = match self.splits.get(id) {
            Some(state) => {
                if state.progress_rev >= rev {
                    return Ok(()); // stale, or our own echoed write
                }
                if state.progress.owner != record.owner {
                    // An owner-clear moves the sticky pass.
                    self.assign_dirty = true;
                }
                (Some(state.progress.status), Some(state.progress.epoch))
            }
            None => (None, None),
        };
        // A foreign `owner` on a split we are awaiting is the normal
        // mid-revocation state, and the wait is what `awaiting` times.
        // Clearing the timer here reads every assignment latency as ~0;
        // it is retired on a claim, or when the leader stops assigning.
        if let Some(current_epoch) = current_epoch
            && self.owned.contains_key(id)
            && record.epoch > current_epoch
        {
            // A claimant CASed the record past our tenancy.
            self.drop_owned(id, SplitLossReason::Fenced);
        }
        if previous_status != Some(record.status) {
            match previous_status {
                Some(SplitStatus::Runnable) => self.runnable_count -= 1,
                Some(SplitStatus::Completed) => self.completed_count -= 1,
                Some(SplitStatus::Quarantined) => self.quarantined_count -= 1,
                None => {}
            }
            match record.status {
                SplitStatus::Runnable => self.runnable_count += 1,
                SplitStatus::Completed => self.completed_count += 1,
                SplitStatus::Quarantined => self.quarantined_count += 1,
            }
            if record.status == SplitStatus::Quarantined {
                self.metrics(|m| m.quarantined());
                self.emit(CoordinationEvent::Quarantined {
                    split: SplitId::new(id.to_string())?,
                    attempts: record.attempts,
                });
            }
        }
        // A runnable split whose next takeover would hit the cap needs a
        // quarantine decision, even at this worker's working-set target.
        if record.status == SplitStatus::Runnable && record.attempts + 1 >= self.config.max_attempts
        {
            self.quarantine_scan = true;
        }
        if previous_status != Some(record.status) || previous_status.is_none() {
            self.assign_dirty = true; // the assignable pool moved
        }
        match self.splits.get_mut(id) {
            Some(state) => {
                state.progress = record;
                state.progress_rev = rev;
            }
            None => {
                // A lease or spec observed before its progress record attaches now.
                let lease = self.pending_leases.remove(id);
                let spec = self.pending_specs.remove(id);
                self.splits.insert(
                    id.to_string(),
                    SplitState {
                        progress: record,
                        progress_rev: rev,
                        spec,
                        lease,
                    },
                );
            }
        }
        Ok(())
    }

    /// The reconcile backstop: authoritative listings of both keyspaces,
    /// applied like fresh snapshots (a key we believe live but absent
    /// from the listing is treated as deleted). Watches whose streams
    /// died silently get re-established by their select arms; nothing to
    /// do for them here.
    async fn reconcile(&mut self) -> Result<(), CoordinationError> {
        let started = Instant::now();
        let leases = match self.store.list(Keyspace::Ephemeral, "").await {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!(error = %e, "reconcile listing failed; next tick retries");
                return Ok(());
            }
        };
        let live: std::collections::BTreeSet<&str> =
            leases.iter().map(|e| e.key.as_str()).collect();
        if self.leader_observed.is_some() && !live.contains(records::LEADER_KEY) {
            self.apply_lease_delete(records::LEADER_KEY, None);
        }
        let gone_workers: Vec<String> = self
            .presence
            .keys()
            .filter(|i| !live.contains(records::worker_key(i).as_str()))
            .cloned()
            .collect();
        for instance in gone_workers {
            self.apply_lease_delete(&records::worker_key(&instance), None);
        }
        let gone_leases: Vec<String> = self
            .splits
            .iter()
            .filter(|(id, s)| {
                s.lease.is_some() && !live.contains(records::split_key_str(id).as_str())
            })
            .map(|(id, _)| records::split_key_str(id))
            .collect();
        for key in gone_leases {
            self.apply_lease_delete(&key, None);
        }
        for entry in &leases {
            self.apply_lease_put(entry)?;
        }
        match self.store.list(Keyspace::Durable, "").await {
            Ok(entries) => {
                // Assignment records are the one durable key this
                // protocol deletes, and watch snapshots drop delete
                // markers. A missed deletion leaves a cached revision
                // whose instance never receives another assignment.
                let live: std::collections::BTreeSet<&str> =
                    entries.iter().map(|e| e.key.as_str()).collect();
                let gone: Vec<String> = self
                    .assignments
                    .keys()
                    .filter(|i| !live.contains(records::assign_key(i).as_str()))
                    .cloned()
                    .collect();
                for instance in gone {
                    self.assignments.remove(&instance);
                    self.assign_dirty = true;
                    if instance == self.instance {
                        // An absent record means "nothing has been decided".
                        self.assignment_seen = false;
                        self.assigned.clear();
                        self.awaiting.clear();
                    }
                }
                for entry in &entries {
                    self.apply_state_put(entry)?;
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "durable reconcile listing failed; next tick retries");
            }
        }
        // A departed instance's grace window elapses on time, not on an event.
        self.assign_dirty = true;
        self.metrics(|m| m.reconcile(started.elapsed()));
        Ok(())
    }

    // ------------------------------------------------------------------
    // The engine step: election → planning → claims → revocations →
    // terminal.

    async fn step(&mut self) -> Result<(), CoordinationError> {
        self.prune_departed();
        self.observe_membership();
        if self.terminal_reported {
            self.update_gauges();
            return Ok(());
        }
        if self.parting {
            // Leaving the fleet: observe only. The released work belongs to the others.
            self.check_terminal().await?;
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
        self.check_terminal().await?;
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
        let live = protocol::live_workers(&self.presence, &self.instance);
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

    fn update_gauges(&self) {
        self.metrics(|m| {
            m.set_splits_owned(self.owned.len());
            m.set_splits_completed(usize::try_from(self.completed_count).unwrap_or(usize::MAX));
            m.set_splits_quarantined(usize::try_from(self.quarantined_count).unwrap_or(usize::MAX));
            m.set_live_workers(protocol::live_workers(&self.presence, &self.instance));
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

    /// Move this worker toward the assignment it was given: claim what is
    /// named and not held, drain away what is held and not named, and cancel
    /// the drain of anything the leader has named again.
    ///
    /// **Absence of an assignment is not an instruction to hold nothing.**
    /// Until a record for this instance has been observed the worker keeps
    /// what it has and claims nothing new, so a leader gap costs
    /// rebalancing but never work. Once a record exists its omissions are
    /// meaningful, and a split missing from it is one to give up.
    ///
    /// Quarantine decisions run regardless of any of that: a fleet that has
    /// been told to hold nothing must still be able to reach the `Stalled`
    /// verdict, or a bounded job with a poison split would idle instead of
    /// finishing.
    async fn reconcile_assignment(&mut self) -> Result<(), CoordinationError> {
        let quarantine_scan = std::mem::take(&mut self.quarantine_scan);
        let cap = self.config.max_in_flight as usize;
        // Scanning every split is the expensive half of a step. At the
        // lane budget the only reason to scan is a quarantine decision.
        if self.owned.len() < cap || quarantine_scan {
            let candidates = protocol::claim_candidates(
                &self.splits,
                |id| self.owned.contains_key(id),
                &self.instance,
                self.config.max_attempts,
            );
            for (id, action) in candidates {
                match action {
                    ClaimAction::Quarantine(kind) => self.try_quarantine(&id, kind).await?,
                    ClaimAction::Claim(kind) => {
                        // A claimable split that was not assigned to us
                        // belongs to another worker, or to the queue.
                        if !self.assigned.contains(&id) || self.owned.len() >= cap {
                            continue;
                        }
                        self.try_claim(&id, kind).await?;
                    }
                }
            }
        }
        // The leader can take a revocation back: `desired_assignment` is
        // sticky on the current owner, and a draining split still holds
        // its lease. This ends the revocation, not the drain, so the
        // entry stays, flagged and re-anchored on a slower bound. A
        // source that already stopped intake keeps draining
        // (`SplitSource` has no seam to resume it), and a drain that
        // never finishes strands the split; `service_revocations` bounds
        // it.
        let restored: Vec<String> = self
            .revoking
            .iter()
            .filter(|(id, r)| {
                !r.cancelled && self.assigned.contains(*id) && self.owned.contains_key(*id)
            })
            .map(|(id, _)| id.clone())
            .collect();
        let now = self.clock.now();
        for id in restored {
            tracing::debug!(split = %id, "revocation cancelled: the leader assigned it back");
            if let Some(entry) = self.revoking.get_mut(&id) {
                entry.cancelled = true;
                // Re-anchor: the absolute clock stops here and the
                // silence clock starts.
                entry.last_progress = now;
            }
            self.metrics(|m| m.revocation(RevocationOutcome::Cancelled));
        }
        if !self.assignment_seen {
            return Ok(());
        }
        // A cancelled entry is a drain, not a revocation, so the leader
        // dropping the split again must re-request.
        let stale: Vec<String> = self
            .owned
            .keys()
            .filter(|id| {
                !self.assigned.contains(*id) && self.revoking.get(*id).is_none_or(|r| r.cancelled)
            })
            .cloned()
            .collect();
        for id in stale {
            self.begin_revoke(&id);
        }
        Ok(())
    }

    /// Leader side: compute the desired assignment and publish the records
    /// that changed.
    ///
    /// Gated on `assign_dirty`, which every input to the decision sets:
    /// membership, split status, ownership, spec arrival, and the reconcile
    /// tick (a grace window elapsing is time-based rather than
    /// event-driven). [`protocol::desired_assignment`] is a fixpoint, so
    /// recomputing on a clean fleet would publish nothing; it is skipped
    /// anyway because the recompute itself is an O(members x splits) scan
    /// and `step` runs on every watch event, which on a commit-heavy fleet
    /// is every commit.
    ///
    /// Publishing is best-effort per instance. A failed or lost write
    /// leaves that instance on its previous assignment, which is stale but
    /// never unsafe, and the next step retries. There is no barrier and no
    /// acknowledgment protocol: the leader learns that a revocation
    /// completed by watching the split's lease disappear.
    async fn publish_assignments(&mut self) -> Result<(), CoordinationError> {
        if !std::mem::take(&mut self.assign_dirty) {
            return Ok(());
        }
        let generation = match &self.plan {
            Some((plan, _)) => plan.generation,
            None => return Ok(()), // nothing planned yet: nothing to assign
        };
        let members: BTreeSet<String> = self.presence.keys().cloned().collect();
        if members.is_empty() {
            return Ok(());
        }
        let reserved = self.reserved_splits();
        // The tie-break seed is the job fingerprint, NOT `self.seed`,
        // which mixes in a per-run nonce: a leader-specific seed makes
        // every failover re-break every tie and churn the fleet.
        let desired = protocol::desired_assignment(
            &members,
            &self.splits,
            &reserved,
            &self.member_caps,
            self.config.max_in_flight,
            self.fp,
        );
        // Where the last published assignment put each split, read
        // before the writes consume `desired`. This is the leader's own
        // record of what it decided; ownership cannot answer it, since a
        // graceful release clears `owner` before dropping presence.
        //
        // Two passes, in this order: a departed instance's record
        // outlives it and is the only evidence of where its splits were
        // assigned, while a live member's record has to win when both
        // name the same split.
        let mut previous: BTreeMap<&str, &str> = BTreeMap::new();
        for pass in [false, true] {
            for (instance, (val, _)) in &self.assignments {
                if pass != members.contains(instance.as_str()) {
                    continue;
                }
                for id in &val.splits {
                    previous.insert(id.as_str(), instance.as_str());
                }
            }
        }
        // A split named for the first time is work being handed out, not
        // a move. This counts what was *published*, not what landed, so a
        // write that fails below has its move counted again on each
        // publish until a later one rewrites its record.
        let moved = desired
            .iter()
            .flat_map(|(instance, splits)| {
                splits
                    .iter()
                    .map(move |id| (id.as_str(), instance.as_str()))
            })
            .filter(|(id, instance)| previous.get(id).is_some_and(|prev| prev != instance))
            .count();
        let mut published = 0usize;
        for (instance, splits) in desired {
            let current = self.assignments.get(&instance);
            if current.is_some_and(|(val, _)| val.splits == splits && val.generation == generation)
            {
                continue; // unchanged: the common case, and free
            }
            let val = AssignmentVal {
                schema: records::SCHEMA,
                generation,
                splits,
            };
            let key = records::assign_key(&instance);
            let bytes = records::encode_val(&val);
            let outcome = match current {
                Some((_, rev)) => {
                    self.store
                        .update(Keyspace::Durable, &key, bytes, *rev)
                        .await
                }
                None => self.store.create(Keyspace::Durable, &key, bytes).await,
            };
            match outcome {
                // Adopt our own write rather than just recording it: the
                // watch echo arrives at the revision just stored, which
                // the stale-echo guard drops.
                Ok(CasOutcome::Won(rev)) => {
                    published += 1;
                    self.apply_assignment(&instance, val, rev);
                }
                // Someone else wrote it, or the key is gone and the
                // cached revision is a ghost. Drop the entry: the next
                // attempt creates.
                Ok(CasOutcome::Lost) => {
                    self.assignments.remove(&instance);
                    self.assign_dirty = true;
                }
                Err(e) => {
                    tracing::debug!(%instance, error = %e, "assignment publish failed; retrying");
                }
            }
        }
        if published > 0 {
            // One line per rebalance. Both arms are needed: a member
            // joining a fleet whose lanes are full moves nothing, and a
            // grace window expiring moves splits with the fleet the same
            // size. Neither admits a split completing.
            if members != self.announced_members || moved > 0 {
                tracing::info!(
                    members = members.len(),
                    moved,
                    generation,
                    "assignment published"
                );
            }
            self.announced_members = members;
        }
        // Drop assignments for instances gone past their grace window.
        let stale: Vec<String> = self
            .assignments
            .keys()
            .filter(|i| !self.presence.contains_key(*i) && !self.departed.contains_key(*i))
            .cloned()
            .collect();
        for instance in stale {
            let key = records::assign_key(&instance);
            if let Some((_, rev)) = self.assignments.get(&instance)
                && matches!(
                    self.store.delete(Keyspace::Durable, &key, Some(*rev)).await,
                    Ok(CasOutcome::Won(_))
                )
            {
                self.assignments.remove(&instance);
            }
        }
        Ok(())
    }

    /// Force the next leader step to recompute and republish.
    pub(crate) fn mark_assignment_dirty(&mut self) {
        self.assign_dirty = true;
    }

    /// Expire the grace windows of departed instances.
    ///
    /// Runs on **every** worker's step, not just the leader's: every worker
    /// records departures (leadership can move between a departure and the
    /// next publish), so every worker has to expire them too, or a process
    /// that is never elected accumulates one entry per historical peer for
    /// its whole life.
    ///
    /// A zero delay short-circuits to "withhold nothing" rather than
    /// falling through the same code path with a zero comparison. That is
    /// deliberate and it is the point of this function's shape: a zero that
    /// flows through a general delay path as just another value is how
    /// "withhold nothing" turns into "withhold indefinitely". Making it a
    /// case of its own means that bug is not expressible here.
    fn prune_departed(&mut self) {
        // A returning instance cancels its own grace window immediately.
        self.departed.retain(|i, _| !self.presence.contains_key(i));
        if self.config.rebalance_delay.is_zero() {
            self.departed.clear();
            return;
        }
        let delay = self.config.rebalance_delay;
        let now = self.clock.now();
        self.departed
            .retain(|_, since| now.duration_since(*since) < delay);
    }

    /// Splits withheld from assignment because their owner departed less
    /// than `rebalance_delay` ago.
    fn reserved_splits(&mut self) -> BTreeSet<String> {
        self.prune_departed();
        if self.config.rebalance_delay.is_zero() || self.departed.is_empty() {
            return BTreeSet::new();
        }
        self.splits
            .iter()
            .filter(|(_, state)| {
                state
                    .progress
                    .owner
                    .as_deref()
                    .is_some_and(|o| self.departed.contains_key(o))
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Adopt an `assign.{instance}` record seen on the durable watch.
    fn apply_assignment(&mut self, instance: &str, val: AssignmentVal, rev: Revision) {
        if instance == self.instance {
            // A deposed leader's late write must not walk us backwards.
            if val.generation < self.assign_generation {
                tracing::debug!(
                    generation = val.generation,
                    seen = self.assign_generation,
                    "ignoring an assignment from a superseded generation"
                );
                return;
            }
            self.assign_generation = val.generation;
            let now: BTreeSet<String> = val.splits.iter().cloned().collect();
            // Start the acquisition clock for newly-named splits, and
            // stop it for anything no longer expected. Not for one
            // already held: that reports a drain as a reassignment.
            for id in now.difference(&self.assigned) {
                if self.owned.contains_key(id) {
                    continue;
                }
                self.awaiting.insert(id.clone(), Instant::now());
            }
            self.awaiting.retain(|id, _| now.contains(id));
            self.assigned = now;
            self.assignment_seen = true;
        }
        self.assignments.insert(instance.to_string(), (val, rev));
    }

    /// Ask the source to give a split up gracefully. Idempotent: the
    /// driver treats a repeated request for a split it is already draining
    /// as a no-op, so re-emitting on a later step is harmless.
    fn begin_revoke(&mut self, id: &str) {
        let Ok(split) = SplitId::new(id.to_string()) else {
            return;
        };
        let now = self.clock.now();
        match self.revoking.get_mut(id) {
            // Re-revoking a drain this worker had cancelled. The drain
            // never stopped, so `started` still anchors the deadline and
            // one already past it is forced on the spot.
            Some(entry) => entry.cancelled = false,
            None => {
                self.revoking.insert(
                    id.to_string(),
                    Revoking {
                        started: now,
                        last_progress: now,
                        cancelled: false,
                    },
                );
                // Only a drain that is starting says so; the arm above
                // would put two starts against one finish.
                tracing::debug!(split = %id, "drain started");
            }
        }
        // The denominator of the revocation lifecycle, counted once per
        // revocation: `requested - drained - forced - cancelled` is the
        // revocations in flight, which is *not* `splits_draining`.
        self.metrics(|m| m.revocation(RevocationOutcome::Requested));
        self.emit(CoordinationEvent::RevokeRequested { split });
    }

    /// Enforce the drain deadline, against two different clocks.
    ///
    /// A cooperative revocation that completes releases through
    /// [`Task::release_splits`] and clears itself from `revoking`. What is
    /// left is bounded one of two ways, because the deadline is protecting
    /// two different things:
    ///
    /// - **A live revocation** holds a rebalance open, so it gets one
    ///   absolute `drain_deadline` from the request. Past it the drain
    ///   declined or is too slow to wait for, and it is forced.
    /// - **A cancelled revocation** ([`Task::reconcile_assignment`] takes
    ///   the decision back, and runs first in every step) has no rebalance
    ///   waiting on it, so slowness costs nothing and is not forced. What
    ///   it still owes is *liveness*: the source stopped intake at a safe
    ///   boundary and cannot be asked to resume, so a drain that never
    ///   finishes leaves the split owned, leased, assigned, and unread for
    ///   the life of the process, and a bounded job that contains it can
    ///   never complete. It is therefore bounded by
    ///   silence: `drain_deadline` with no commit landing at all, which for
    ///   a live drain cannot happen (its tail acks as it flushes) and for a
    ///   wedged one always does.
    ///
    /// Forcing is a *release*, not an abandonment: the owner field is
    /// cleared so the next claimant sees `Released` rather than `Expired`
    /// and spends no delivery attempt. Being revoked is not poison
    /// evidence. A stalled cancelled drain is
    /// re-claimed by this same worker (the leader still names it here), so
    /// forcing it costs one lane teardown and a bounded replay, and gets a
    /// reading split back.
    async fn service_revocations(&mut self) -> Result<(), CoordinationError> {
        // An entry whose split left `owned` by a route that did not
        // settle it (a terminal commit, or an explicit `fail`) is still a
        // revocation that ended. Count it, or the in-flight count drifts.
        let orphans: Vec<String> = self
            .revoking
            .keys()
            .filter(|id| !self.owned.contains_key(*id))
            .cloned()
            .collect();
        for id in orphans {
            self.settle_revocation(&id, RevocationOutcome::Forced);
        }
        let deadline = self.config.drain_deadline;
        let now = self.clock.now();
        let overdue: Vec<(String, bool)> = self
            .revoking
            .iter()
            .filter(|(_, r)| {
                let anchor = if r.cancelled {
                    r.last_progress
                } else {
                    r.started
                };
                now.duration_since(anchor) >= deadline
            })
            .map(|(id, r)| (id.clone(), r.cancelled))
            .collect();
        for (id, cancelled) in overdue {
            if cancelled {
                tracing::warn!(
                    split = %id,
                    ?deadline,
                    "a cancelled revocation's drain has committed nothing for a full drain \
                     deadline; releasing the split so it can be re-claimed and read again \
                     (its uncommitted tail replays)"
                );
            } else {
                tracing::warn!(
                    split = %id,
                    ?deadline,
                    "drain deadline exceeded; forcing the revocation (its uncommitted tail replays)"
                );
            }
            self.force_revocation(&id).await?;
        }
        Ok(())
    }

    /// Retire one `revoking` entry under a terminal outcome, exactly once.
    /// A no-op for a split that is not being revoked, which is what lets
    /// every path that can end a tenancy call it unconditionally.
    ///
    /// A **cancelled** entry retires silently. Its revocation already
    /// terminated under [`RevocationOutcome::Cancelled`] and the entry
    /// outlived it only as a watchdog over the drain; counting a second
    /// outcome here would break `requested = drained + forced + cancelled`.
    /// The drain that finishes after a cancellation is therefore invisible
    /// to both the counter and the duration histogram. By then it is a
    /// split going nowhere, not a revocation ending.
    fn settle_revocation(&mut self, id: &str, outcome: RevocationOutcome) {
        let Some(entry) = self.revoking.remove(id) else {
            return;
        };
        if entry.cancelled {
            return;
        }
        let drained_for = self.clock.now().duration_since(entry.started);
        self.metrics(|m| {
            m.revocation(outcome);
            // A forced release measures `drain_deadline`, not draining.
            if outcome == RevocationOutcome::Drained {
                m.drain_duration(drained_for);
            }
        });
        tracing::debug!(
            split = %id,
            ?outcome,
            drained_for_ms = drained_for.as_millis(),
            "drain finished"
        );
    }

    /// End a revocation the expensive way: give the split back without
    /// waiting for the drain, so its uncommitted tail replays under the
    /// next owner.
    ///
    /// Also the exit from a cancelled revocation's stalled drain, where
    /// "the next owner" is usually this worker again. The split is still
    /// assigned here, so it is re-claimed with a fresh lane and starts
    /// reading. That path counts no `Forced` (the revocation ended as
    /// `Cancelled`) and reports the release and its `Lost`.
    ///
    /// The release runs before the fence. The release CAS needs the lease
    /// revision, which lives in `owned`, and a fence would take it away.
    /// The loss is reported exactly once: if the release lost its CAS a
    /// peer ended this tenancy, `drop_owned` already counted `fenced` and
    /// emitted `Lost`, and adding a `revoked` on top would count one
    /// tenancy end twice under two different reasons.
    async fn force_revocation(&mut self, id: &str) -> Result<(), CoordinationError> {
        self.settle_revocation(id, RevocationOutcome::Forced);
        let Ok(split) = SplitId::new(id.to_string()) else {
            return Ok(());
        };
        if self.release_one(&split).await? == ReleaseOutcome::Fenced {
            return Ok(()); // `drop_owned` already reported it
        }
        self.metrics(|m| m.lost(SplitLossReason::Revoked));
        self.emit(CoordinationEvent::Lost { split });
        Ok(())
    }

    /// The two-key claim: lease first (create, or CAS-update for a fast
    /// reclaim), then the progress-record CAS that transfers ownership.
    async fn try_claim(&mut self, id: &str, mut kind: ClaimKind) -> Result<(), CoordinationError> {
        // A cooperative release is two writes on two watch streams, and a
        // fast claimant can see the lease vanish before the owner-clear
        // arrives, misreading a completed revocation as a death takeover
        // and burning a delivery attempt. One durable read settles it:
        // an already-cleared owner means Released.
        if kind == ClaimKind::Expired {
            let key = records::split_key_str(id);
            if let Ok(Some(entry)) = self.store.get(Keyspace::Durable, &key).await
                && let Ok(record) = SplitProgressRecord::parse(&key, &entry.value, self.fp)
            {
                // Honor the downgrade only from a read at least as fresh
                // as the view: a lagging replica's stale owner-`None`
                // lets a poison split cycle past its quarantine cap.
                let fresh = self
                    .splits
                    .get(id)
                    .is_none_or(|state| entry.revision >= state.progress_rev);
                if record.owner.is_none() && fresh {
                    kind = ClaimKind::Released;
                }
                self.upsert_progress(id, record, entry.revision)?;
            }
        }
        let Some(state) = self.splits.get(id) else {
            return Ok(());
        };
        let next_epoch = state.progress.epoch + 1;
        let lease_key = records::split_key_str(id);
        let lease_val = records::encode_val(&LeaseVal {
            schema: records::SCHEMA,
            owner: self.instance.clone(),
            nonce: self.nonce.clone(),
            epoch: next_epoch,
        });
        let started = Instant::now();
        let lease_outcome = match (kind, &state.lease) {
            (ClaimKind::Reclaim, Some((_, rev))) => {
                self.store
                    .update(Keyspace::Ephemeral, &lease_key, lease_val, *rev)
                    .await
            }
            _ => {
                self.store
                    .create(Keyspace::Ephemeral, &lease_key, lease_val)
                    .await
            }
        };
        let lease_rev = match lease_outcome {
            Ok(CasOutcome::Won(rev)) => rev,
            Ok(CasOutcome::Lost) => return Ok(()), // a peer won; watch updates the view
            Err(e) => {
                tracing::warn!(split = %id, error = %e, "lease write failed; next tick retries");
                self.metrics(|m| m.write(WriteOutcome::Error, started.elapsed()));
                return Ok(());
            }
        };
        let reason = match kind {
            ClaimKind::Create => AcquireReason::Create,
            // A split whose previous owner cleared the owner field left
            // cleanly, so this claim replays nothing. Expired never
            // reaches here; the guard above downgraded it.
            ClaimKind::Released => AcquireReason::Reassigned,
            ClaimKind::Reclaim => AcquireReason::Reclaimed,
            ClaimKind::Expired => AcquireReason::Expired,
        };
        self.record_claim(id, kind, reason, next_epoch, lease_rev, started)
            .await?;
        // Assignment-to-acquisition. Observed on the write that
        // transferred ownership, so a rising median means slow reassignments
        // are *succeeding*, not that they got slower.
        if self.owned.contains_key(id)
            && let Some(since) = self.awaiting.remove(id)
        {
            self.metrics(|m| m.assignment_latency(since.elapsed()));
        }
        Ok(())
    }

    /// The progress-record CAS after a won lease. On a lost CAS, adopt a
    /// zombie's late commit (legal, since it was still the owner) and retry
    /// once. The acquisition metric counts here, on the write that
    /// transfers ownership, under the caller's reason.
    async fn record_claim(
        &mut self,
        id: &str,
        kind: ClaimKind,
        reason: AcquireReason,
        next_epoch: u64,
        lease_rev: Revision,
        started: Instant,
    ) -> Result<(), CoordinationError> {
        let key = records::split_key_str(id);
        for attempt in 0..2 {
            let Some(state) = self.splits.get(id) else {
                self.release_lease_key(id, lease_rev).await;
                return Ok(());
            };
            let Some(spec_record) = &state.spec else {
                // The spec put is in flight; the source needs the descriptor.
                self.release_lease_key(id, lease_rev).await;
                return Ok(());
            };
            let split = spec_record.spec()?;
            let mut record = state.progress.clone();
            record.epoch = next_epoch;
            record.owner = Some(self.instance.clone());
            record.attempts += u32::from(kind.consumes_attempt() && attempt == 0);
            record.written_at_ms = records::now_ms();
            let expected = state.progress_rev;
            let outcome = self
                .store
                .update(Keyspace::Durable, &key, record.encode(), expected)
                .await;
            match outcome {
                Ok(CasOutcome::Won(rev)) => {
                    self.metrics(|m| {
                        m.write(WriteOutcome::Ok, started.elapsed());
                        m.acquired(reason);
                    });
                    tracing::debug!(split = %id, ?reason, epoch = next_epoch, "split claimed");
                    let progress = record.progress()?;
                    self.record_own_write(id, record, rev, lease_rev)?;
                    self.emit(CoordinationEvent::Gained {
                        split,
                        epoch: LeaseEpoch(next_epoch),
                        progress,
                    });
                    return Ok(());
                }
                Ok(CasOutcome::Lost) => {
                    self.metrics(|m| m.write(WriteOutcome::Conflict, started.elapsed()));
                    // Refresh and decide: a zombie's late commit is
                    // adopted (retry); a terminal record means walk away.
                    match self.store.get(Keyspace::Durable, &key).await {
                        Ok(Some(entry)) => {
                            self.apply_state_put(&entry)?;
                            let fresh = &self.splits[id];
                            if fresh.progress.status != SplitStatus::Runnable
                                || fresh.progress.epoch >= next_epoch
                            {
                                // Terminal, or another claimant beat us
                                // between our lease write and record CAS.
                                self.release_lease_key(id, lease_rev).await;
                                return Ok(());
                            }
                            // else: adopted progress; retry the CAS once.
                        }
                        Ok(None) | Err(_) => {
                            self.release_lease_key(id, lease_rev).await;
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(split = %id, error = %e, "claim record write failed");
                    self.metrics(|m| m.write(WriteOutcome::Error, started.elapsed()));
                    self.release_lease_key(id, lease_rev).await;
                    return Ok(());
                }
            }
        }
        self.release_lease_key(id, lease_rev).await;
        Ok(())
    }

    /// Park an out-of-attempts split: the record CAS applies the same
    /// fence a claim would (epoch bump), so the dead owner's zombie can
    /// never write again.
    async fn try_quarantine(&mut self, id: &str, kind: ClaimKind) -> Result<(), CoordinationError> {
        let Some(state) = self.splits.get(id) else {
            return Ok(());
        };
        let mut record = state.progress.clone();
        record.epoch += 1;
        record.status = SplitStatus::Quarantined;
        record.owner = None;
        record.attempts += u32::from(kind.consumes_attempt());
        record.written_at_ms = records::now_ms();
        let attempts = record.attempts;
        let key = records::split_key_str(id);
        let expected = state.progress_rev;
        let started = Instant::now();
        match self
            .store
            .update(Keyspace::Durable, &key, record.encode(), expected)
            .await
        {
            Ok(CasOutcome::Won(rev)) => {
                self.metrics(|m| m.write(WriteOutcome::Ok, started.elapsed()));
                tracing::warn!(split = %id, attempts, "split quarantined: out of delivery attempts");
                // upsert emits the Quarantined event and counts the metric
                // on the status transition.
                self.upsert_progress(id, record, rev)?;
                // Clear the dead owner's stale lease key so listings stay tidy.
                if kind == ClaimKind::Reclaim
                    && let Some(state) = self.splits.get(id)
                    && let Some((_, lease_rev)) = state.lease
                {
                    self.release_lease_key(id, lease_rev).await;
                }
                Ok(())
            }
            Ok(CasOutcome::Lost) => {
                self.metrics(|m| m.write(WriteOutcome::Conflict, started.elapsed()));
                Ok(()) // someone else moved it; the view will refresh
            }
            Err(e) => {
                tracing::warn!(split = %id, error = %e, "quarantine write failed; next tick retries");
                self.metrics(|m| m.write(WriteOutcome::Error, started.elapsed()));
                // Re-arm the scan: `reconcile_assignment` takes the flag
                // before deciding whether to scan, so a worker at its
                // lane budget never re-derives this candidate and the
                // bounded job hangs instead of stalling.
                self.quarantine_scan = true;
                Ok(())
            }
        }
    }

    /// Best-effort removal of a lease key we hold (guarded by revision).
    async fn release_lease_key(&mut self, id: &str, lease_rev: Revision) {
        let key = records::split_key_str(id);
        if let Err(e) = self
            .store
            .delete(Keyspace::Ephemeral, &key, Some(lease_rev))
            .await
        {
            tracing::debug!(split = %id, error = %e, "lease cleanup failed; it will expire");
        }
        if let Some(state) = self.splits.get_mut(id)
            && state
                .lease
                .as_ref()
                .is_some_and(|(_, rev)| *rev == lease_rev)
        {
            state.lease = None;
        }
    }

    /// Fold our own successful claim into the view so later decisions see
    /// it (the watch echo arrives with a revision we already know and is
    /// skipped).
    fn record_own_write(
        &mut self,
        id: &str,
        record: SplitProgressRecord,
        rev: Revision,
        lease_rev: Revision,
    ) -> Result<(), CoordinationError> {
        let epoch = record.epoch;
        self.upsert_progress(id, record, rev)?;
        if let Some(state) = self.splits.get_mut(id) {
            state.lease = Some((
                LeaseVal {
                    schema: records::SCHEMA,
                    owner: self.instance.clone(),
                    nonce: self.nonce.clone(),
                    epoch,
                },
                lease_rev,
            ));
        }
        self.owned.insert(
            id.to_string(),
            OwnedSplit {
                lease_rev,
                last_ok_write: self.clock.now(),
            },
        );
        Ok(())
    }

    /// Stop tracking an owned split and tell the source.
    fn drop_owned(&mut self, id: &str, reason: SplitLossReason) {
        if self.owned.remove(id).is_none() {
            return;
        }
        // A split fenced or starved mid-drain ends that revocation the
        // expensive way: whatever was uncommitted replays.
        self.settle_revocation(id, RevocationOutcome::Forced);
        self.metrics(|m| m.lost(reason));
        if let Ok(split) = SplitId::new(id.to_string()) {
            self.emit(CoordinationEvent::Lost { split });
        }
    }

    // ------------------------------------------------------------------
    // Heartbeat.

    async fn heartbeat(&mut self) -> Result<(), CoordinationError> {
        // Presence first: membership must outlive lease hiccups.
        self.renew_presence().await;
        if self.leadership.is_some() {
            self.renew_leadership().await?;
        }
        let ids: Vec<String> = self.owned.keys().cloned().collect();
        for id in ids {
            self.renew_split(&id).await?;
        }
        // Starvation self-fence: any owned split without a successful
        // write for a full lease is dropped. Reads `clock`, the source
        // that stamps `last_ok_write`, so a test fences only what it
        // advances the clock past, and must step by no more than a
        // renew-interval while the worker is alive, or it expires the
        // lease and the renewal that would have saved it at once.
        let now = self.clock.now();
        let starved: Vec<String> = self
            .owned
            .iter()
            .filter(|(_, o)| now.duration_since(o.last_ok_write) >= self.config.lease_duration)
            .map(|(id, _)| id.clone())
            .collect();
        for id in starved {
            tracing::warn!(split = %id, "self-fencing: no successful lease write for a full lease");
            self.drop_owned(&id, SplitLossReason::Starved);
        }
        Ok(())
    }

    async fn renew_presence(&mut self) {
        let key = records::worker_key(&self.instance);
        let val = records::encode_val(&self.worker_val());
        // Failures are tolerated: presence tunes fair-share, and
        // correctness does not depend on it.
        match self.store.get(Keyspace::Ephemeral, &key).await {
            Ok(Some(entry)) => {
                let _ = self
                    .store
                    .update(Keyspace::Ephemeral, &key, val, entry.revision)
                    .await;
            }
            Ok(None) => {
                let _ = self.store.create(Keyspace::Ephemeral, &key, val).await;
            }
            Err(e) => tracing::debug!(error = %e, "presence renewal failed; next beat retries"),
        }
    }

    async fn renew_leadership(&mut self) -> Result<(), CoordinationError> {
        let Some(rev) = self.leadership else {
            return Ok(());
        };
        let generation = self.plan.as_ref().map_or(0, |(p, _)| p.generation);
        let val = records::encode_val(&LeaderVal {
            schema: records::SCHEMA,
            owner: self.instance.clone(),
            nonce: self.nonce.clone(),
            generation,
        });
        match self
            .store
            .update(Keyspace::Ephemeral, records::LEADER_KEY, val, rev)
            .await
        {
            Ok(CasOutcome::Won(new_rev)) => {
                self.leadership = Some(new_rev);
                Ok(())
            }
            Ok(CasOutcome::Lost) => {
                tracing::warn!("leadership renewal fenced; demoting");
                self.leadership = None;
                self.metrics(|m| m.set_leader(false));
                Ok(())
            }
            Err(e) => {
                tracing::warn!(error = %e, "leadership renewal failed; next beat retries");
                Ok(())
            }
        }
    }

    async fn renew_split(&mut self, id: &str) -> Result<(), CoordinationError> {
        let Some(owned) = self.owned.get(id) else {
            return Ok(());
        };
        // Cadence gate: skip if we renewed within the last interval. Read
        // from `clock`, the same source that stamps `last_ok_write`.
        // Under a frozen test clock nothing renews until the test
        // advances; `TestClock::advance_stepped` gives a live worker its
        // renewal inside every step.
        if self.clock.now().duration_since(owned.last_ok_write) < self.config.renew_interval() {
            return Ok(());
        }
        let lease_rev = owned.lease_rev;
        let epoch = self
            .splits
            .get(id)
            .map(|s| s.progress.epoch)
            .unwrap_or_default();
        let val = records::encode_val(&LeaseVal {
            schema: records::SCHEMA,
            owner: self.instance.clone(),
            nonce: self.nonce.clone(),
            epoch,
        });
        let key = records::split_key_str(id);
        let started = Instant::now();
        match self
            .store
            .update(Keyspace::Ephemeral, &key, val, lease_rev)
            .await
        {
            Ok(CasOutcome::Won(rev)) => {
                if let Some(owned) = self.owned.get_mut(id) {
                    owned.lease_rev = rev;
                    owned.last_ok_write = self.clock.now();
                }
                if let Some(state) = self.splits.get_mut(id)
                    && let Some((_, lease_rev)) = &mut state.lease
                {
                    *lease_rev = rev;
                }
                Ok(())
            }
            Ok(CasOutcome::Lost) => {
                self.metrics(|m| m.write(WriteOutcome::Conflict, started.elapsed()));
                match self.store.get(Keyspace::Ephemeral, &key).await {
                    Ok(Some(entry)) => {
                        let lease: LeaseVal = records::parse_val(&key, &entry.value)?;
                        if lease.owner == self.instance && lease.nonce == self.nonce {
                            // Maybe-landed: a previous renewal reported
                            // an error but wrote. Adopt its revision;
                            // dropping costs a delivery attempt.
                            if let Some(owned) = self.owned.get_mut(id) {
                                owned.lease_rev = entry.revision;
                                owned.last_ok_write = self.clock.now();
                            }
                            if let Some(state) = self.splits.get_mut(id) {
                                state.lease = Some((lease, entry.revision));
                            }
                            return Ok(());
                        }
                        if lease.owner == self.instance && lease.nonce != self.nonce {
                            return Err(fatal(format!(
                                "two live workers share instance_id {:?} (foreign nonce on \
                                 our lease for split {id}); instance ids must be unique per \
                                 live worker — use the pod name, not a constant",
                                self.instance
                            )));
                        }
                        // A thief CASed our lease: surrender.
                        self.drop_owned(id, SplitLossReason::Fenced);
                        Ok(())
                    }
                    Ok(None) => {
                        self.drop_owned(id, SplitLossReason::Starved);
                        Ok(())
                    }
                    Err(_) => {
                        // Cannot tell; the next beat decides.
                        Ok(())
                    }
                }
            }
            Err(e) => {
                tracing::warn!(split = %id, error = %e, "lease renewal failed; next beat retries");
                Ok(())
            }
        }
    }

    // ------------------------------------------------------------------
    // Commands.

    async fn handle_command(&mut self, command: Command) -> Result<(), CoordinationError> {
        match command {
            Command::Commit {
                split,
                progress,
                reply,
            } => {
                let result = self.commit(&split, &progress).await;
                let fatal_error = result
                    .as_ref()
                    .err()
                    .filter(|e| e.kind == CoordinationErrorKind::Fatal)
                    .map(|e| fatal(e.reason.clone()));
                let _ = reply.try_send(result);
                if let Some(e) = fatal_error {
                    return Err(e);
                }
                Ok(())
            }
            Command::Fail {
                split,
                reason,
                reply,
            } => {
                let result = self.fail_split(&split, &reason).await;
                let _ = reply.try_send(result);
                Ok(())
            }
            Command::Release {
                splits,
                departure,
                reply,
            } => {
                let result = self.release_splits(&splits, departure).await;
                let _ = reply.try_send(result);
                Ok(())
            }
            Command::DeclineRevoke { split, reply } => {
                let id = split.as_str().to_string();
                // A decline says the source never stopped intake, so
                // there is no drain. A live revocation is forced; under a
                // cancelled one the entry is dropped, watchdog and all.
                let cancelled = self.revoking.get(&id).is_some_and(|r| r.cancelled);
                let result = if cancelled {
                    self.revoking.remove(&id);
                    Ok(())
                } else if self.revoking.contains_key(&id) {
                    self.force_revocation(&id).await
                } else {
                    Ok(()) // never offered, or already settled
                };
                let _ = reply.try_send(result);
                Ok(())
            }
        }
    }

    /// The fenced commit: one CAS on the durable progress record. The
    /// record is small by schema (the descriptor lives in the immutable
    /// spec record), so commit cost is independent of descriptor size. A
    /// lost CAS means a peer owns the split. Nothing was written, the
    /// caller gets `Fenced`, and the `Lost` event follows.
    async fn commit(
        &mut self,
        split: &SplitId,
        progress: &SplitProgress,
    ) -> Result<(), CoordinationError> {
        let id = split.as_str();
        if !self.owned.contains_key(id) {
            return Err(CoordinationError::new(
                CoordinationErrorKind::Fenced,
                format!("split {split} is not held by this worker; nothing was written"),
            ));
        }
        let state = self.splits.get(id).expect("owned splits are in the view");
        let owned_epoch = state.progress.epoch;
        if let Some(previous) = state.progress.watermark
            && progress.watermark < previous
        {
            return Err(fatal(format!(
                "split {split}: watermark would regress {previous} -> {} — this is a \
                 source bug (watermarks are one past the last acknowledged record and \
                 never move backwards)",
                progress.watermark
            )));
        }
        let mut record = state.progress.clone();
        record.watermark = Some(progress.watermark);
        record.state = Some(records::b64_encode(&progress.state));
        record.completed = progress.completed;
        if progress.completed {
            record.status = SplitStatus::Completed;
        }
        record.written_at_ms = records::now_ms();
        let expected = state.progress_rev;
        let key = records::split_key_str(id);
        let started = Instant::now();
        match self
            .store
            .update(Keyspace::Durable, &key, record.encode(), expected)
            .await
        {
            Ok(CasOutcome::Won(rev)) => {
                self.metrics(|m| m.write(WriteOutcome::Ok, started.elapsed()));
                // A landed commit is the only liveness signal a draining
                // split gives the task, and what a cancelled revocation's
                // watchdog is armed against.
                let now = self.clock.now();
                if let Some(entry) = self.revoking.get_mut(id) {
                    entry.last_progress = now;
                }
                self.upsert_progress(id, record, rev)?;
                if progress.completed {
                    self.finish_completed(id).await;
                }
                Ok(())
            }
            Ok(CasOutcome::Lost) => {
                self.metrics(|m| m.write(WriteOutcome::Conflict, started.elapsed()));
                // Maybe-landed hazard: if the winning write is OUR OWN,
                // adopt it instead of reporting a false fence.
                if let Ok(Some(entry)) = self.store.get(Keyspace::Durable, &key).await {
                    let fresh = SplitProgressRecord::parse(&key, &entry.value, self.fp)?;
                    if fresh.owner.as_deref() == Some(self.instance.as_str())
                        && fresh.epoch == owned_epoch
                        && fresh.watermark == Some(progress.watermark)
                        && fresh.completed == progress.completed
                    {
                        self.upsert_progress(id, fresh, entry.revision)?;
                        if progress.completed {
                            self.finish_completed(id).await;
                        }
                        return Ok(());
                    }
                    self.upsert_progress(id, fresh, entry.revision)?;
                }
                self.drop_owned(id, SplitLossReason::Fenced);
                Err(CoordinationError::new(
                    CoordinationErrorKind::Fenced,
                    format!("split {split} is owned by a peer; nothing was written"),
                ))
            }
            Err(e) => {
                self.metrics(|m| m.write(WriteOutcome::Error, started.elapsed()));
                Err(store_error(&format!("committing split {split}"), &e))
            }
        }
    }

    /// Terminal commit bookkeeping: hand the lease back, stop tracking.
    async fn finish_completed(&mut self, id: &str) {
        let lease_rev = self.owned.get(id).map(|o| o.lease_rev);
        self.owned.remove(id);
        // A split that finishes mid-revocation ends it: its tail is
        // committed and nothing replays.
        self.settle_revocation(id, RevocationOutcome::Drained);
        if let Some(lease_rev) = lease_rev {
            self.release_lease_key(id, lease_rev).await;
        }
    }

    /// Explicit failure report: consumes an attempt, ends this tenancy
    /// gracefully-for-the-lease but non-gracefully for the attempt
    /// accounting, and quarantines at the cap.
    async fn fail_split(&mut self, split: &SplitId, reason: &str) -> Result<(), CoordinationError> {
        let id = split.as_str();
        let Some(owned) = self.owned.get(id) else {
            return Err(CoordinationError::new(
                CoordinationErrorKind::Fenced,
                format!("split {split} is not held by this worker"),
            ));
        };
        let lease_rev = owned.lease_rev;
        let state = self.splits.get(id).expect("owned splits are in the view");
        self.metrics(|m| m.failed());
        let mut record = state.progress.clone();
        record.attempts += 1;
        record.owner = None;
        let quarantining = record.attempts >= self.config.max_attempts;
        if quarantining {
            record.status = SplitStatus::Quarantined;
            record.epoch += 1;
        }
        record.written_at_ms = records::now_ms();
        let attempts = record.attempts;
        tracing::warn!(split = %id, reason, attempts, quarantining, "split failed by the source");
        let key = records::split_key_str(id);
        let expected = state.progress_rev;
        match self
            .store
            .update(Keyspace::Durable, &key, record.encode(), expected)
            .await
        {
            Ok(CasOutcome::Won(rev)) => {
                // This tenancy ends by request: no Lost event, no loss
                // metric. Remove from `owned` before folding the write so
                // the epoch bump cannot read as a peer's fence.
                self.owned.remove(id);
                // A failure mid-revocation leaves an uncommitted tail to replay.
                self.settle_revocation(id, RevocationOutcome::Forced);
                self.upsert_progress(id, record, rev)?;
                self.release_lease_key(id, lease_rev).await;
                Ok(())
            }
            Ok(CasOutcome::Lost) => {
                self.drop_owned(id, SplitLossReason::Fenced);
                Err(CoordinationError::new(
                    CoordinationErrorKind::Fenced,
                    format!("split {split} is owned by a peer"),
                ))
            }
            Err(e) => Err(store_error(&format!("failing split {split}"), &e)),
        }
    }

    /// Graceful hand-back: clear `owner` on the record (so the next claim
    /// consumes no attempt), then drop the lease key (so peers claim
    /// instantly instead of after the TTL).
    ///
    /// `departure` distinguishes a shutdown/scale-down release, which
    /// retires this worker once its working set empties, from a
    /// revocation's hand-back, which never leaves the fleet even when it
    /// gives up the last split.
    async fn release_splits(
        &mut self,
        splits: &[SplitId],
        departure: bool,
    ) -> Result<(), CoordinationError> {
        let mut released = 0u64;
        for split in splits {
            if self.release_one(split).await? == ReleaseOutcome::Released {
                released += 1;
            }
        }
        self.metrics(|m| m.released(released));
        if !departure {
            return Ok(());
        }
        // Releasing the last held split is how a worker leaves the fleet.
        // Stop claiming (or the releaser re-claims its own hand-backs),
        // hand leadership back, drop the presence key. The gate is on
        // what was ASKED, not on what the store acknowledged: a departing
        // worker that keeps claiming strands splits when the process exits.
        if self.owned.is_empty() && !splits.is_empty() {
            self.parting = true;
            self.demote().await;
            let key = records::worker_key(&self.instance);
            let _ = self.store.delete(Keyspace::Ephemeral, &key, None).await;
            self.presence.remove(&self.instance);
        }
        Ok(())
    }

    /// Release one held split, reporting how the tenancy ended so
    /// the caller can decide whether it still owes the source a `Lost`.
    ///
    /// A split under revocation settles here whichever command drove the
    /// release; a shutdown that happens to release a draining split still
    /// ended that revocation. Scoping the count to `revoking` keeps a bulk
    /// hand-back from reading as a fleet of revocations, so no `departure`
    /// flag is needed to tell them apart.
    async fn release_one(&mut self, split: &SplitId) -> Result<ReleaseOutcome, CoordinationError> {
        let id = split.as_str();
        let Some(owned) = self.owned.get(id) else {
            return Ok(ReleaseOutcome::Missing); // released/lost/completed
        };
        let lease_rev = owned.lease_rev;
        let Some(state) = self.splits.get(id) else {
            return Ok(ReleaseOutcome::Missing);
        };
        let mut record = state.progress.clone();
        record.owner = None;
        record.written_at_ms = records::now_ms();
        let expected = state.progress_rev;
        let key = records::split_key_str(id);
        match self
            .store
            .update(Keyspace::Durable, &key, record.encode(), expected)
            .await
        {
            Ok(CasOutcome::Won(rev)) => {
                self.owned.remove(id);
                self.upsert_progress(id, record, rev)?;
                self.release_lease_key(id, lease_rev).await;
                // The cooperative outcome: the tail is committed and the
                // owner cleared, so the next owner replays nothing.
                self.settle_revocation(id, RevocationOutcome::Drained);
                Ok(ReleaseOutcome::Released)
            }
            Ok(CasOutcome::Lost) => {
                // Fenced: the split is someone else's problem now, which
                // is what a release wanted. `drop_owned` settles it.
                self.drop_owned(id, SplitLossReason::Fenced);
                Ok(ReleaseOutcome::Fenced)
            }
            Err(e) => {
                tracing::warn!(split = %id, error = %e, "release write failed; lease will expire");
                // Still drop the lease key best-effort: the attempt
                // accounting is conservative (counts as non-graceful).
                self.owned.remove(id);
                self.release_lease_key(id, lease_rev).await;
                self.settle_revocation(id, RevocationOutcome::Forced);
                Ok(ReleaseOutcome::WriteFailed)
            }
        }
    }

    // ------------------------------------------------------------------
    // Terminal detection.

    /// Decide whether the job is over, and how.
    ///
    /// The verdict latches forever and a wrong `AllComplete` reports an
    /// incomplete backfill as a success, so correctness comes before
    /// promptness here:
    ///
    /// - **Quarantine blocks completion explicitly.** The invariant is not
    ///   left to fall out of `completed == total` arithmetic; one slipped
    ///   tally would otherwise dress unprocessed data up as a green exit.
    /// - **The verdict is rendered against an authoritative listing**, not
    ///   against this worker's watch-fed view and not against
    ///   `plan.planned`. `planned` is only a lower bound: `finish_plan`
    ///   seeds split records *before* it recounts and publishes, and both
    ///   publish-failure paths leave the seeded records behind, so a
    ///   `Final` plan, which never replans, can name fewer splits than
    ///   the store holds. Judging a *subset* that happens to be
    ///   all-complete is how a quarantined split goes unseen.
    ///
    /// The listing costs one store round trip and is gated behind a local
    /// pre-check, so it runs essentially once per job.
    async fn check_terminal(&mut self) -> Result<(), CoordinationError> {
        if self.terminal_reported {
            return Ok(());
        }
        let Some((plan, _)) = &self.plan else {
            return Ok(());
        };
        if plan.finality != records::PlanFinalityRepr::Final {
            return Ok(());
        }
        let planned = plan.planned;
        // Cheap gate: pay for the listing only once this worker's view
        // covers what the plan promised and looks terminal. `planned` is
        // a lower bound, so this is `<`, not `!=`.
        let local = self.splits.len() as u64;
        if local < planned || self.completed_count + self.quarantined_count != local {
            return Ok(());
        }

        // Authoritative recount. Applying the entries is idempotent, so
        // this doubles as catch-up for a view missing records.
        let entries = match self
            .store
            .list(Keyspace::Durable, records::SPLIT_PREFIX)
            .await
        {
            Ok(entries) => entries,
            Err(e) => {
                // Refusing to judge is the safe direction.
                tracing::warn!(error = %e, "terminal listing failed; deferring the verdict");
                return Ok(());
            }
        };
        for entry in &entries {
            self.apply_state_put(entry)?;
        }

        let total = self.splits.len() as u64;
        if total != entries.len() as u64 {
            // The listing and the folded view disagree on cardinality.
            // Judge nothing this tick.
            return Ok(());
        }
        let (completed, quarantined) = (self.completed_count, self.quarantined_count);
        if completed == total && quarantined == 0 {
            self.terminal_reported = true;
            self.emit(CoordinationEvent::AllComplete);
        } else if completed + quarantined == total {
            self.terminal_reported = true;
            self.emit(CoordinationEvent::Stalled {
                completed,
                quarantined,
            });
        }
        Ok(())
    }
}
