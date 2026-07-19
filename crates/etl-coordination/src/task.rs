//! The single-writer background task: one per process owns **all** store
//! I/O, so every local decision is serialized against every local write.
//!
//! The loop is watch-driven — lease deletions and record changes arrive as
//! push events, claims and steals run immediately on the deltas — with a
//! periodic reconcile listing as the missed-event backstop and a jittered
//! heartbeat tick renewing every owned lease at a third of the TTL. The
//! planner runs on the blocking pool and is awaited as a **select arm**,
//! never inline, so a slow enumeration cannot stall renewals.
//!
//! Correctness recap (see `protocol.rs` for the pure rules): the durable
//! progress record's CAS revision is the only fence; lease keys are
//! liveness. A zombie's commit that lands *before* a takeover CAS is legal
//! (it was still the owner; progress is monotone) and is adopted by the
//! claimant on its CAS retry — less replay, not a violation.

use crate::clock::Clock;
use crate::config::CoordinationConfig;
use crate::error::{fatal, store_error};
use crate::leader::PlanRun;
use crate::protocol::{self, ClaimAction, ClaimKind, SplitState};
use crate::records::{
    self, HandoffVal, LeaderVal, LeaseVal, PlanRecord, SplitProgressRecord, SplitSpecRecord,
    SplitStatus, WorkerVal,
};
use crate::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, WatchEvent, WatchStream,
};
use etl_core::coordination::ControlWaker;
use etl_core::coordination::{
    CoordinationError, CoordinationErrorKind, CoordinationEvent, LeaseEpoch, SplitId, SplitPlanner,
    SplitProgress,
};
use etl_core::metrics::{
    AcquireReason, CoordinationMetrics, HandoffOutcome, SplitLossReason, WriteOutcome,
};
use futures_util::StreamExt as _;
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
        /// scale-down) rather than a cooperative handoff grant. Only a
        /// departure that empties the working set retires this worker; a
        /// handoff grant of the last split keeps it in the fleet.
        departure: bool,
        reply: std_mpsc::SyncSender<Result<(), CoordinationError>>,
    },
    /// The embedder cannot serve a `HandoffRequested` it was offered:
    /// free the grant slot and exclude the split from re-offer for a
    /// round budget, so the victim can offer a different one.
    DeclineHandoff {
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
            | Command::DeclineHandoff { reply, .. } => reply,
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
/// `splits` — this carries only what the view cannot: the lease revision
/// to CAS renewals against and the self-fence clock.
struct OwnedSplit {
    lease_rev: Revision,
    /// Last successful lease write, for renewal cadence and the
    /// starvation self-fence.
    last_ok_write: Instant,
}

/// This worker's one outstanding cooperative-handoff request. A worker
/// under its working-set target with nothing claimable writes a
/// `handoff.{victim}` key asking the victim to drain and release a split,
/// instead of stealing it (which replays the uncommitted tail). One
/// request at a time; the round counter is the sole timeout.
struct HandoffOut {
    /// Instance this worker is requesting a split from.
    victim: String,
    /// Round the request was FIRST opened. Preserved across key
    /// recreations (a TTL blip must not reset the fallback clock).
    since_round: u64,
    /// Round the key was last refreshed — the once-per-round gate.
    refreshed_round: u64,
    /// Whether the fallback [`HandoffOutcome::Timeout`] has been counted
    /// (once) for this request.
    timed_out: bool,
}

pub(crate) struct Task<S: CoordinationStore> {
    pub(crate) store: S,
    pub(crate) config: CoordinationConfig,
    /// Time source for the starvation self-fence. `SystemClock` in
    /// production; frozen in tests so scheduler jitter cannot spuriously
    /// expire a held lease. Renewal cadence still uses real `.elapsed()`.
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
    /// Live workers by presence key, with the revision last seen — the
    /// revision orders deletes against puts (stale echoes are ignored).
    pub(crate) presence: BTreeMap<String, Revision>,
    /// Observed cooperative-handoff request keys, by victim instance (the
    /// `{victim}` in `handoff.{victim}`), with the revision last seen. A
    /// requester CASes its own key against this revision to refresh it; a
    /// victim reads `handoff_requests[self.instance]` to see who is asking.
    handoff_requests: BTreeMap<String, (HandoffVal, Revision)>,
    pub(crate) plan: Option<(PlanRecord, Revision)>,
    pub(crate) plan_rev_seen: u64,
    leader_observed: Option<(LeaderVal, Revision)>,

    // Incremental status tallies over `splits` — every progress mutation
    // flows through `upsert_progress`, so the per-event path never
    // rescans the whole map.
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
    /// A split may need parking while this worker sits at its working-set
    /// target; this flag lets the next claim pass run the quarantine
    /// decisions even with no claim slot open.
    quarantine_scan: bool,
    /// Set when this worker released its last split: it is leaving the
    /// fleet, so it must not claim, steal, or lead again — otherwise the
    /// releaser instantly re-claims its own hand-backs.
    parting: bool,
    terminal_reported: bool,
    round: u64,
    /// This worker's one outstanding handoff request (requester side).
    handoff_out: Option<HandoffOut>,
    /// A split this worker is currently draining to grant away (victim
    /// side): the split id and the round the grant began. At most one at a
    /// time — one drain per victim.
    handoff_granting: Option<(String, u64)>,
    /// Splits about to be released to us by the victim we requested —
    /// marked from the victim's explicit `granted` annotation on our
    /// request key (never inferred from lease traffic) so the ensuing
    /// claim is labelled [`AcquireReason::Handoff`] rather than a plain
    /// `Released`, and so a vanished request key can be told apart from a
    /// TTL blip (granted ⇒ served ⇒ never re-created).
    handoff_incoming: BTreeSet<String>,
    /// Victim side: splits the embedder declined to drain, with the round
    /// of the decline. Excluded from grants for one `handoff_rounds`
    /// budget — most declines are transient (a lane not yet open), so the
    /// victim offers its other splits first and retries later.
    handoff_declined: BTreeMap<String, u64>,
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
            handoff_requests: BTreeMap::new(),
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
            handoff_out: None,
            handoff_granting: None,
            handoff_incoming: BTreeSet::new(),
            handoff_declined: BTreeMap::new(),
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
    // sites: each select arm would otherwise embed its own copy of the
    // handler's whole future (step alone reaches through claims into the
    // store client), and the resulting mega-machine has overflowed a
    // debug-build worker stack over the real NATS client. These run at
    // control-plane cadence; the allocation is noise.
    async fn run_inner(&mut self) -> Result<(), CoordinationError> {
        Box::pin(self.startup()).await?;

        let mut lease_watch = Box::pin(self.rewatch(Keyspace::Ephemeral)).await?;
        let mut state_watch = Box::pin(self.rewatch(Keyspace::Durable)).await?;
        Box::pin(self.step()).await?;

        // The planner runs on the blocking pool and is joined by a select
        // arm below — a plan taking longer than the lease must not stop
        // renewals, watch processing, or command service.
        let mut planning: Option<PlanRun> = None;

        let mut heartbeat = Instant::now() + self.next_heartbeat();
        let mut reconcile = Instant::now() + self.config.reconcile_interval;
        let mut replan = Instant::now() + self.config.replan_interval;

        loop {
            if planning.is_none() {
                planning = self.maybe_start_plan()?;
            }
            tokio::select! {
                command = self.commands.recv() => {
                    let Some(command) = command else {
                        // Handle dropped: stop quietly; unreleased leases
                        // expire and peers take over.
                        return Ok(());
                    };
                    Box::pin(self.handle_command(command)).await?;
                    // Serve queued commands before anything else — the
                    // control thread is waiting on replies.
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
                () = tokio::time::sleep_until(heartbeat) => {
                    self.round += 1;
                    Box::pin(self.heartbeat()).await?;
                    heartbeat = Instant::now() + self.next_heartbeat();
                    Box::pin(self.step()).await?;
                }
                () = tokio::time::sleep_until(reconcile) => {
                    Box::pin(self.reconcile()).await?;
                    reconcile = Instant::now() + self.config.reconcile_interval;
                    Box::pin(self.step()).await?;
                }
                () = tokio::time::sleep_until(replan) => {
                    if self.leadership.is_some() && self.plan_is_open() {
                        self.plan_now = true;
                    }
                    replan = Instant::now() + self.config.replan_interval;
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
    /// guarded delete works — in both keyspaces.
    async fn probe(&mut self) -> Result<(), CoordinationError> {
        for ks in [Keyspace::Durable, Keyspace::Ephemeral] {
            let key = format!("_probe.{}", self.instance);
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

    /// Write the worker presence key (taking over a dead predecessor's).
    async fn announce(&mut self) -> Result<(), CoordinationError> {
        let key = records::worker_key(&self.instance);
        let val = records::encode_val(&WorkerVal {
            schema: records::SCHEMA,
            nonce: self.nonce.clone(),
        });
        let ctx = "announcing presence";
        match self
            .store
            .create(Keyspace::Ephemeral, &key, val.clone())
            .await
            .map_err(|e| store_error(ctx, &e))?
        {
            CasOutcome::Won(_) => Ok(()),
            CasOutcome::Lost => {
                // A presence key already exists under our id: a not-yet-
                // expired predecessor (fine, take it over) or a live twin
                // (caught by lease fencing + nonce checks the moment it
                // matters; presence alone cannot distinguish them).
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
    /// return the live tail. Unbudgeted — retries until the store answers.
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
                self.handoff_requests.clear();
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
                // lapsed. Demote quietly; the generation fence rejects
                // any in-flight plan write of ours.
                tracing::warn!(new_leader = %leader.owner, "leadership lost");
                self.leadership = None;
                self.metrics(|m| m.set_leader(false));
            }
            self.leader_observed = Some((leader, entry.revision));
            return Ok(());
        }
        if let Some(instance) = records::parse_worker_key(&entry.key) {
            self.presence.insert(instance.to_string(), entry.revision);
            return Ok(());
        }
        if let Some(victim) = records::parse_handoff_key(&entry.key) {
            // Same stale-echo guard as the worker branch: a put at or below
            // the revision the view already holds is a delayed broadcast.
            if self
                .handoff_requests
                .get(victim)
                .is_some_and(|(_, rev)| *rev >= entry.revision)
            {
                return Ok(());
            }
            // Unlike lease values, a corrupt request is not worth killing
            // the fleet over: this key is liveness-only (its own contract —
            // losing it costs a rebalance opportunity, never correctness),
            // so ignore it and let it expire. A key naming US as requester
            // under a foreign nonce is a restarted predecessor's leftover,
            // not a live twin: also non-fatal, swept by the next pursue
            // pass.
            let val: HandoffVal = match records::parse_val(&entry.key, &entry.value) {
                Ok(val) => val,
                Err(e) => {
                    tracing::warn!(key = %entry.key, error = %e, "unreadable handoff request ignored");
                    return Ok(());
                }
            };
            // The victim's grant annotation on OUR live request is the
            // grant's attribution: exactly this split is incoming. Never
            // inferred from lease traffic — the victim's unrelated lease
            // deletes (fail, completion, departure) must not be mistaken
            // for a grant.
            if val.requester == self.instance
                && val.nonce == self.nonce
                && self
                    .handoff_out
                    .as_ref()
                    .is_some_and(|out| out.victim == victim)
                && let Some(granted) = &val.granted
            {
                self.handoff_incoming.clear();
                self.handoff_incoming.insert(granted.clone());
            }
            self.handoff_requests
                .insert(victim.to_string(), (val, entry.revision));
            return Ok(());
        }
        if let Some(id) = records::parse_split_key(&entry.key) {
            // Puts and deletes for one key are ordered only through their
            // revisions: a put at or below what the view already holds is
            // a stale echo (delayed broadcast, reconcile overlap) and must
            // not be applied — least of all to fence ourselves with it.
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
                    state.lease = Some((lease, entry.revision));
                }
                // The record has not arrived yet (snapshot ordering):
                // buffer the lease so the split cannot be misread as
                // unleased (and falsely expired) when the record lands.
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
                // Our own leadership lease expired under us.
                tracing::warn!("leadership lease expired");
                self.leadership = None;
                self.metrics(|m| m.set_leader(false));
            }
            return;
        }
        if let Some(instance) = records::parse_worker_key(key) {
            if self.presence.get(instance).copied().is_none_or(newer_than) {
                self.presence.remove(instance);
            }
            return;
        }
        if let Some(victim) = records::parse_handoff_key(key) {
            // A handoff request key vanished (victim granted, requester
            // withdrew, or TTL expiry). Drop the observation; the requester
            // side self-heals through the round counter. Deliberately does
            // NOT clear `handoff_granting` — a victim mid-drain keeps
            // draining, and its own release bookkeeping closes the grant.
            if self
                .handoff_requests
                .get(victim)
                .is_none_or(|(_, rev)| newer_than(*rev))
            {
                self.handoff_requests.remove(victim);
            }
            return;
        }
        if let Some(id) = records::parse_split_key(key) {
            if let Some(owned) = self.owned.get(id) {
                if !newer_than(owned.lease_rev) {
                    return; // stale echo of a delete our claim already replaced
                }
                // Our lease expired while we believed we held the split:
                // heartbeats have been failing for a full TTL.
                self.drop_owned(id, SplitLossReason::Starved);
            }
            // Deliberately no handoff bookkeeping here: a victim's lease
            // deletes also happen for fails, completions, departures, and
            // expiry, so a grant is never inferred from lease traffic — it
            // arrives as the explicit `granted` annotation on our request
            // key instead.
            if let Some(state) = self.splits.get_mut(id)
                && state.lease.as_ref().is_none_or(|(_, rev)| newer_than(*rev))
            {
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
            // Durable keys are never deleted by this protocol; a delete
            // means external interference and the reconcile pass treats
            // missing records the same way.
            WatchEvent::Delete { key, .. } => {
                tracing::warn!(key = %key, "durable record deleted externally");
                Ok(())
            }
            WatchEvent::SnapshotDone => Ok(()),
        }
    }

    pub(crate) fn apply_state_put(&mut self, entry: &Entry) -> Result<(), CoordinationError> {
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
                // A successor bumped the generation: we are deposed.
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

    /// Attach an observed spec record (immutable — re-deliveries are
    /// echoes) to its split, or buffer it until the progress record lands.
    pub(crate) fn attach_spec(&mut self, id: &str, record: SplitSpecRecord) {
        match self.splits.get_mut(id) {
            Some(state) => {
                if state.spec.is_none() {
                    state.spec = Some(record);
                }
            }
            None => {
                self.pending_specs.entry(id.to_string()).or_insert(record);
            }
        }
    }

    /// Fold a progress record into the view — from a watch event, a
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
                (Some(state.progress.status), Some(state.progress.epoch))
            }
            None => (None, None),
        };
        // A granted split observed under an owner that is neither us nor
        // the draining victim means a peer won the claim race for it: the
        // request was served all the same, so close the cycle — recreating
        // the request would make the victim drain a second split for a
        // move that already happened. (The victim as owner is the drain's
        // own mid-flight commits; with the cycle already closed, any
        // foreign owner — the victim re-claiming its hand-back included —
        // just retires the stale marker.)
        if self.handoff_incoming.contains(id)
            && let Some(owner) = record.owner.as_deref()
            && owner != self.instance
        {
            let in_drain = self
                .handoff_out
                .as_ref()
                .is_some_and(|out| out.victim == owner);
            if !in_drain {
                self.handoff_incoming.remove(id);
                self.handoff_out = None;
            }
        }
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
        // quarantine decision — which must run even when this worker sits
        // at its working-set target.
        if record.status == SplitStatus::Runnable && record.attempts + 1 >= self.config.max_attempts
        {
            self.quarantine_scan = true;
        }
        match self.splits.get_mut(id) {
            Some(state) => {
                state.progress = record;
                state.progress_rev = rev;
            }
            None => {
                // A lease or spec observed before its progress record
                // (snapshot ordering) attaches now — a held split must
                // never look unleased.
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
        let gone_handoffs: Vec<String> = self
            .handoff_requests
            .keys()
            .filter(|v| !live.contains(records::handoff_key(v).as_str()))
            .cloned()
            .collect();
        for victim in gone_handoffs {
            self.apply_lease_delete(&records::handoff_key(&victim), None);
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
                for entry in &entries {
                    self.apply_state_put(entry)?;
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "durable reconcile listing failed; next tick retries");
            }
        }
        self.metrics(|m| m.reconcile(started.elapsed()));
        Ok(())
    }

    // ------------------------------------------------------------------
    // The engine step: election → planning → claims → steal → terminal.

    async fn step(&mut self) -> Result<(), CoordinationError> {
        if self.terminal_reported {
            self.update_gauges();
            return Ok(());
        }
        if self.parting {
            // Leaving the fleet: observe only. No claims, no steals, no
            // leadership — the released work belongs to the others now.
            self.check_terminal().await?;
            self.update_gauges();
            return Ok(());
        }
        if self.leader_observed.is_none() && self.leadership.is_none() {
            self.try_elect().await?;
        }
        self.claim_pass().await?;
        // Victim side of the cooperative handoff: gated, like the claim
        // pass, on not leaving and not already terminal.
        self.service_handoff().await;
        self.check_terminal().await?;
        self.update_gauges();
        Ok(())
    }

    fn update_gauges(&self) {
        self.metrics(|m| {
            m.set_splits_owned(self.owned.len());
            m.set_splits_completed(usize::try_from(self.completed_count).unwrap_or(usize::MAX));
            m.set_splits_quarantined(usize::try_from(self.quarantined_count).unwrap_or(usize::MAX));
            m.set_live_workers(protocol::live_workers(&self.presence, &self.instance));
            m.set_leader(self.leadership.is_some());
            m.set_idle(self.owned.is_empty());
        });
    }

    pub(crate) fn metrics(&self, f: impl FnOnce(&CoordinationMetrics)) {
        if let Some(m) = &self.metrics {
            f(m);
        }
    }

    async fn claim_pass(&mut self) -> Result<(), CoordinationError> {
        let workers = protocol::live_workers(&self.presence, &self.instance);
        let incomplete = usize::try_from(self.runnable_count).unwrap_or(usize::MAX);
        let target = protocol::target(incomplete, workers, self.config.max_in_flight);
        let quarantine_scan = std::mem::take(&mut self.quarantine_scan);
        if self.owned.len() >= target && !quarantine_scan {
            // At the working-set target: any request we were pursuing is no
            // longer wanted — drop it so a victim slot frees for a worker
            // that still needs work.
            self.withdraw_handoff().await;
            return Ok(());
        }
        let candidates = protocol::claim_candidates(
            &self.splits,
            |id| self.owned.contains_key(id),
            &self.instance,
            self.config.max_attempts,
            incomplete,
            workers,
            self.seed,
        );
        let mut had_claim_candidates = false;
        for (id, action) in candidates {
            match action {
                // Parking an out-of-attempts split is never gated on the
                // working-set target: a fleet of full workers must still
                // reach the Stalled verdict.
                ClaimAction::Quarantine(kind) => self.try_quarantine(&id, kind).await?,
                ClaimAction::Claim(kind) => {
                    if self.owned.len() >= target {
                        continue;
                    }
                    had_claim_candidates = true;
                    self.try_claim(&id, kind).await?;
                }
            }
        }
        if !had_claim_candidates && self.owned.len() < target {
            // Nothing unclaimed remains: pursue a cooperative handoff (or
            // fall back to a steal) rather than replaying an owner's tail.
            self.pursue_handoff_or_steal().await?;
        } else if had_claim_candidates && self.handoff_incoming.is_empty() {
            // Claimable work exists again, and no grant is in flight for
            // us: the request's "nothing claimable" precondition lapsed,
            // so withdraw rather than letting the key silently TTL out
            // while this branch skips its refresh. (With a grant in
            // flight the request is being served — its ack cleans up.)
            self.withdraw_handoff().await;
        }
        Ok(())
    }

    /// Requester side of the cooperative handoff, reached only under the
    /// working-set target with nothing claimable. Either advance the one
    /// outstanding request — refresh it, retire it as unjustified, or fall
    /// back to a fenced steal once the round budget elapses — or, coin-gated
    /// like the steal it replaces, open a fresh request against the
    /// most-loaded peer.
    async fn pursue_handoff_or_steal(&mut self) -> Result<(), CoordinationError> {
        let own = self.owned.len();
        let Some(out) = &self.handoff_out else {
            // A restarted predecessor's request keys (our stable id, a
            // foreign nonce) would otherwise sit until TTL, blocking the
            // victims they name and inviting ghost grants nobody claims.
            let stale: Vec<(String, Revision)> = self
                .handoff_requests
                .iter()
                .filter(|(_, (val, _))| val.requester == self.instance && val.nonce != self.nonce)
                .map(|(victim, (_, rev))| (victim.clone(), *rev))
                .collect();
            for (victim, rev) in stale {
                let key = records::handoff_key(&victim);
                let _ = self
                    .store
                    .delete(Keyspace::Ephemeral, &key, Some(rev))
                    .await;
                self.handoff_requests.remove(&victim);
            }
            // No request outstanding. Skip roughly half the opportunities
            // to soften herding when several under-target workers race for
            // the same victim (the steal gate, unchanged).
            if protocol::stable_hash(self.seed, self.round) & 1 == 0
                && let Some(victim) = protocol::handoff_victim(
                    &self.splits,
                    |v| self.handoff_requests.contains_key(v),
                    &self.instance,
                    own,
                    self.seed,
                )
            {
                self.open_handoff(&victim).await;
            }
            return Ok(());
        };
        let victim = out.victim.clone();
        let since_round = out.since_round;
        let refreshed_round = out.refreshed_round;
        let timed_out = out.timed_out;
        // The imbalance that justified the request may have evaporated.
        if !protocol::handoff_justified(&self.splits, &victim, &self.instance, own) {
            self.withdraw_handoff().await;
            return Ok(());
        }
        // Refresh the key once per round whether or not the budget has
        // elapsed: the fallback deliberately keeps the request up while
        // steals lose, so a revived victim can still grant first — but a
        // key left to TTL out mid-fallback would silently withdraw the
        // request instead.
        if refreshed_round < self.round {
            self.refresh_handoff(&victim).await;
            if self.handoff_out.is_none() {
                return Ok(()); // superseded by a foreign requester
            }
        }
        if protocol::handoff_fallback_due(since_round, self.round, self.config.handoff_rounds) {
            // The victim is dead or stuck: fall back to today's replaying
            // steal.
            if !timed_out {
                if let Some(out) = &mut self.handoff_out {
                    out.timed_out = true;
                }
                self.metrics(|m| m.handoff(HandoffOutcome::Timeout));
            }
            if protocol::stable_hash(self.seed, self.round) & 1 == 0 {
                // Prefer the split the victim already annotated as granted:
                // its drain is partly done, and stealing a *different* one
                // would take two splits off the victim for one imbalance
                // unit. Falls through to the normal pick when the granted
                // split is not (yet) steal-eligible.
                let granted = self.handoff_incoming.iter().next().and_then(|g| {
                    let state = self.splits.get(g)?;
                    let lease_foreign = state
                        .lease
                        .as_ref()
                        .is_some_and(|(lease, _)| lease.owner != self.instance);
                    (state.progress.status == SplitStatus::Runnable
                        && state.spec.is_some()
                        && state.progress.watermark.is_some()
                        && lease_foreign)
                        .then(|| g.clone())
                });
                if let Some(victim_split) = granted.or_else(|| {
                    protocol::steal_candidate(&self.splits, &self.instance, own, self.seed)
                }) {
                    self.try_steal(&victim_split).await?;
                }
            }
        }
        Ok(())
    }

    /// Open a new handoff request: create-if-absent of `handoff.{victim}`.
    /// The create is the fairness arbiter — only one requester per victim
    /// wins, so at most one drain runs per victim at a time.
    async fn open_handoff(&mut self, victim: &str) {
        let key = records::handoff_key(victim);
        let val = HandoffVal {
            schema: records::SCHEMA,
            requester: self.instance.clone(),
            nonce: self.nonce.clone(),
            granted: None,
        };
        match self
            .store
            .create(Keyspace::Ephemeral, &key, records::encode_val(&val))
            .await
        {
            Ok(CasOutcome::Won(rev)) => {
                self.handoff_requests.insert(victim.to_string(), (val, rev));
                self.handoff_out = Some(HandoffOut {
                    victim: victim.to_string(),
                    since_round: self.round,
                    refreshed_round: self.round,
                    timed_out: false,
                });
                self.metrics(|m| m.handoff(HandoffOutcome::Requested));
            }
            // Another requester already holds this victim's slot: leave it.
            Ok(CasOutcome::Lost) => {}
            Err(e) => {
                tracing::warn!(victim = %victim, error = %e, "handoff request write failed");
            }
        }
    }

    /// Refresh the outstanding request's key: CAS-update it against the
    /// revision we observed, carrying the victim's `granted` annotation
    /// through unchanged (a refresh must never un-grant a grant). A key
    /// absent from the view splits two ways: with NO grant annotated it
    /// is a TTL blip — recreate it, `since_round` preserved by the caller
    /// so the fallback clock never resets; with a grant annotated the
    /// request was SERVED and ack-deleted — close the cycle instead of
    /// resurrecting it, or the victim drains a second split for a move
    /// that already happened. A foreign requester in the view means the
    /// per-victim slot was taken over: stand down WITHOUT deleting their
    /// key.
    async fn refresh_handoff(&mut self, victim: &str) {
        let key = records::handoff_key(victim);
        let observed = self.handoff_requests.get(victim).cloned();
        let val = HandoffVal {
            schema: records::SCHEMA,
            requester: self.instance.clone(),
            nonce: self.nonce.clone(),
            granted: observed
                .as_ref()
                .and_then(|(existing, _)| existing.granted.clone()),
        };
        let bytes = records::encode_val(&val);
        let outcome = match observed {
            Some((existing, _)) if existing.requester != self.instance => {
                // Superseded: the slot is a foreign requester's now.
                self.handoff_out = None;
                self.handoff_incoming.clear();
                return;
            }
            Some((_, rev)) => {
                self.store
                    .update(Keyspace::Ephemeral, &key, bytes, rev)
                    .await
            }
            None if !self.handoff_incoming.is_empty() => {
                // Served: a grant was annotated and the key is gone — that
                // is the victim's ack, not a blip. The marker stays until
                // the granted split's claim resolves.
                self.handoff_out = None;
                return;
            }
            None => self.store.create(Keyspace::Ephemeral, &key, bytes).await,
        };
        match outcome {
            Ok(CasOutcome::Won(rev)) => {
                self.handoff_requests.insert(victim.to_string(), (val, rev));
                if let Some(out) = &mut self.handoff_out {
                    out.refreshed_round = self.round;
                }
            }
            Ok(CasOutcome::Lost) => {
                // The key moved (or vanished) without the watch having told
                // us — e.g. the victim's annotation raced this refresh.
                // Watch losses are legal and reconcile may be a full
                // interval away, so re-sync from the store now: absent →
                // served (grant annotated) or blip (recreate, fallback
                // clock preserved); a foreign requester → stand down;
                // ours → adopt the newer revision (grant annotation
                // included, via the normal put path on the next watch
                // echo or here directly).
                match self.store.get(Keyspace::Ephemeral, &key).await {
                    Ok(None) if !self.handoff_incoming.is_empty() => {
                        self.handoff_out = None;
                    }
                    Ok(None) => {
                        if let Ok(CasOutcome::Won(rev)) = self
                            .store
                            .create(Keyspace::Ephemeral, &key, records::encode_val(&val))
                            .await
                        {
                            self.handoff_requests.insert(victim.to_string(), (val, rev));
                            if let Some(out) = &mut self.handoff_out {
                                out.refreshed_round = self.round;
                            }
                        }
                    }
                    Ok(Some(entry)) => {
                        if let Ok(existing) = records::parse_val::<HandoffVal>(&key, &entry.value) {
                            if existing.requester != self.instance {
                                self.handoff_out = None;
                                self.handoff_incoming.clear();
                            } else {
                                if existing.nonce == self.nonce
                                    && self
                                        .handoff_out
                                        .as_ref()
                                        .is_some_and(|out| out.victim == victim)
                                    && let Some(granted) = &existing.granted
                                {
                                    self.handoff_incoming.clear();
                                    self.handoff_incoming.insert(granted.clone());
                                }
                                if let Some(out) = &mut self.handoff_out {
                                    out.refreshed_round = self.round;
                                }
                            }
                            self.handoff_requests
                                .insert(victim.to_string(), (existing, entry.revision));
                        }
                    }
                    Err(e) => {
                        tracing::debug!(victim = %victim, error = %e, "handoff refresh re-sync failed; next round retries");
                    }
                }
            }
            Err(e) => {
                tracing::debug!(victim = %victim, error = %e, "handoff refresh failed; it will expire");
            }
        }
    }

    /// Drop this worker's outstanding request (if any): guarded-delete the
    /// key so a victim slot frees for another worker, and forget both the
    /// local intent and any splits we were expecting from the victim.
    async fn withdraw_handoff(&mut self) {
        self.handoff_incoming.clear();
        let Some(out) = self.handoff_out.take() else {
            return;
        };
        // Only delete a key we still own (matching requester): a foreign
        // takeover of the slot must survive our withdrawal.
        if let Some((existing, rev)) = self.handoff_requests.get(&out.victim)
            && existing.requester == self.instance
        {
            let rev = *rev;
            let key = records::handoff_key(&out.victim);
            if let Err(e) = self
                .store
                .delete(Keyspace::Ephemeral, &key, Some(rev))
                .await
            {
                tracing::debug!(victim = %out.victim, error = %e, "handoff withdraw failed; it will expire");
            }
            self.handoff_requests.remove(&out.victim);
        }
    }

    /// Victim side of the cooperative handoff: honour a request naming this
    /// worker by draining one of its splits and granting it. Pure-local
    /// plus an emit — the actual drain and release run through the driver's
    /// [`SplitCoordinator::release_handoff`](etl_core::coordination::SplitCoordinator::release_handoff).
    async fn service_handoff(&mut self) {
        if let Some((split, since)) = self.handoff_granting.clone() {
            // A grant is already in flight; hold until it resolves, but
            // self-heal if the world moved on under us.
            if !self.owned.contains_key(&split) {
                // Released, lost, or completed: the grant is done.
                self.handoff_granting = None;
            } else if !self.handoff_requests.contains_key(&self.instance)
                && self.round.saturating_sub(since) >= u64::from(self.config.handoff_rounds)
            {
                // The request that prompted the grant is gone and the drain
                // never completed (the source declined, say): re-enable so a
                // later request can be serviced. A re-emitted
                // `HandoffRequested` is driver-idempotent, so this is safe.
                self.handoff_granting = None;
            }
            return;
        }
        // Is a peer asking us? (A key naming us as requester would be our
        // own predecessor's — never grant against that.)
        let Some((val, request_rev)) = self.handoff_requests.get(&self.instance).cloned() else {
            return;
        };
        if val.requester == self.instance {
            return;
        }
        // Splits the embedder recently declined sit out one round budget;
        // prune the cool-down as it lapses so the map cannot grow.
        let budget = u64::from(self.config.handoff_rounds);
        let round = self.round;
        self.handoff_declined
            .retain(|_, since| round.saturating_sub(*since) < budget);
        let declined = std::mem::take(&mut self.handoff_declined);
        let picked = protocol::handoff_grant(
            &self.splits,
            |id| self.owned.contains_key(id) && !declined.contains_key(id),
            &val.requester,
            &self.instance,
            self.seed,
        );
        self.handoff_declined = declined;
        let Some(split) = picked else {
            return;
        };
        let Ok(split_id) = SplitId::new(split.clone()) else {
            return;
        };
        // Annotate the grant on the request key BEFORE draining: the
        // annotation is the grant's attribution — the requester marks
        // exactly this split incoming, and can tell a served request's
        // deleted key apart from a TTL blip. A lost CAS means the view is
        // stale (the requester refreshed under us); the next step retries
        // against the fresh revision.
        let annotated = HandoffVal {
            granted: Some(split.clone()),
            ..val
        };
        let key = records::handoff_key(&self.instance);
        match self
            .store
            .update(
                Keyspace::Ephemeral,
                &key,
                records::encode_val(&annotated),
                request_rev,
            )
            .await
        {
            Ok(CasOutcome::Won(rev)) => {
                self.handoff_requests
                    .insert(self.instance.clone(), (annotated, rev));
                self.handoff_granting = Some((split, self.round));
                self.emit(CoordinationEvent::HandoffRequested { split: split_id });
            }
            Ok(CasOutcome::Lost) => {}
            Err(e) => {
                tracing::debug!(error = %e, "grant annotation failed; next step retries");
            }
        }
    }

    /// The two-key claim: lease first (create, or CAS-update for a fast
    /// reclaim), then the progress-record CAS that actually transfers
    /// ownership.
    async fn try_claim(&mut self, id: &str, mut kind: ClaimKind) -> Result<(), CoordinationError> {
        // A graceful release is two writes on two watch streams — the
        // durable owner-clear, then the ephemeral lease delete — and a
        // fast claimant can see the lease vanish before the owner-clear
        // arrives, misreading a consented grant as a death takeover (an
        // attempt burned, and the handoff cycle left open). For a split
        // our own handoff request marked incoming, one durable read
        // settles it: an already-cleared owner means Released.
        if kind == ClaimKind::Expired && self.handoff_incoming.contains(id) {
            let key = records::split_key_str(id);
            if let Ok(Some(entry)) = self.store.get(Keyspace::Durable, &key).await
                && let Ok(record) = SplitProgressRecord::parse(&key, &entry.value, self.fp)
            {
                // Honor the downgrade only from a read at least as fresh
                // as the view: a lagging replica's stale owner-`None`
                // (from before the current owner claimed) must not turn a
                // genuine expiry takeover attempt-free, or a poison split
                // could cycle past its quarantine cap.
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
            // A released split our victim's grant annotation marked
            // incoming is a consent-first, replay-free transfer, not a
            // plain reclaim of abandoned work. (Expired never reaches
            // this arm.) The marker is only consumed below, once the
            // record CAS actually wins.
            ClaimKind::Released if self.handoff_incoming.contains(id) => AcquireReason::Handoff,
            ClaimKind::Released => AcquireReason::Released,
            ClaimKind::Reclaim => AcquireReason::Reclaimed,
            ClaimKind::Expired => AcquireReason::Expired,
        };
        self.record_claim(id, kind, reason, next_epoch, lease_rev, started)
            .await?;
        if self.owned.contains_key(id) && self.handoff_incoming.remove(id) {
            // The grant is served and won. Close the cycle so the next
            // request starts a fresh fallback clock — the victim's ack
            // deleted the request key, which this side cannot tell apart
            // from a TTL blip, and the blip rule preserves `since_round`
            // across recreates. A LOST record CAS leaves marker and cycle
            // open instead: the winner's record arriving through
            // `upsert_progress` settles it.
            self.handoff_out = None;
        }
        Ok(())
    }

    /// The progress-record CAS after a won lease. On a lost CAS, adopt a
    /// zombie's late commit (legal — it was still the owner) and retry
    /// once. The acquisition metric counts here, on the write that
    /// actually transfers ownership, under the caller's reason.
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
                // Cannot hand the source a split without its descriptor;
                // the spec put is in flight (leader writes it first).
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

    /// Steal: CAS the victim's lease key (their next heartbeat loses →
    /// they observe Lost), then the record CAS that transfers ownership.
    async fn try_steal(&mut self, id: &str) -> Result<(), CoordinationError> {
        let Some(state) = self.splits.get(id) else {
            return Ok(());
        };
        let Some((_, lease_rev)) = state.lease else {
            return Ok(()); // freed in the meantime: normal claim next step
        };
        let next_epoch = state.progress.epoch + 1;
        let lease_val = records::encode_val(&LeaseVal {
            schema: records::SCHEMA,
            owner: self.instance.clone(),
            nonce: self.nonce.clone(),
            epoch: next_epoch,
        });
        let started = Instant::now();
        let key = records::split_key_str(id);
        match self
            .store
            .update(Keyspace::Ephemeral, &key, lease_val, lease_rev)
            .await
        {
            // Steals bypass the attempt increment (a steal is not death
            // evidence), so run the shared record-CAS with a kind that
            // does not consume attempts — but count the acquisition as
            // what it is. Nothing is counted until the record CAS wins:
            // the lease write alone transfers nothing.
            Ok(CasOutcome::Won(new_lease_rev)) => {
                self.record_claim(
                    id,
                    ClaimKind::Released,
                    AcquireReason::Stolen,
                    next_epoch,
                    new_lease_rev,
                    started,
                )
                .await
            }
            Ok(CasOutcome::Lost) => Ok(()),
            Err(e) => {
                tracing::warn!(split = %id, error = %e, "steal lease write failed");
                Ok(())
            }
        }
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
                // A stale lease key from the dead owner (Reclaim kind) is
                // best-effort cleared so listings stay tidy.
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
                // Re-arm the scan, or "next tick retries" is a lie:
                // `claim_pass` takes the flag before its target check, so a
                // worker already at target never re-derives this candidate.
                // The split would sit `Runnable` at the attempts cap
                // forever, counting toward neither tally, and the bounded
                // job would hang instead of stalling.
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
        // A split fenced or starved out from under us while we were draining
        // it for a grant aborts that handoff (the fallback steal fencing the
        // victim mid-drain lands here). The victim-side release bookkeeping
        // owns the Granted/Aborted count on the graceful path, so this is
        // the only place a lost drain is counted.
        if self.handoff_granting.as_ref().is_some_and(|(s, _)| s == id) {
            self.handoff_granting = None;
            self.metrics(|m| m.handoff(HandoffOutcome::Aborted));
        }
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
        // write for a full lease is dropped before peers must fight our
        // zombie for it. Reads `clock` (not real time) so a frozen test
        // clock cannot self-fence a lease the scheduler merely stalled;
        // `last_ok_write` is stamped from the same clock, so the difference
        // is 0 while frozen. Production `SystemClock` is real wall time.
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
        let val = records::encode_val(&WorkerVal {
            schema: records::SCHEMA,
            nonce: self.nonce.clone(),
        });
        // Presence renewal is contention-free in steady state: read the
        // current revision and CAS. Failures are tolerated — presence
        // only tunes fair-share; correctness never depends on it.
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
        // Cadence gate: skip if we renewed within the last interval. This
        // MUST stay real `.elapsed()`, not `clock.now()`. `last_ok_write` is
        // stamped from `clock`; under a frozen test clock `clock.now() -
        // last_ok_write` is always 0, which would gate out every renewal and
        // never exercise the fault. `.elapsed()` measures real wall time, so
        // renewals still fire on cadence (identical to `Instant::now()` under
        // the production `SystemClock`).
        if owned.last_ok_write.elapsed() < self.config.renew_interval() {
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
                            // Maybe-landed: a previous renewal reported an
                            // error but actually wrote. The lease is still
                            // ours — adopt its revision instead of dropping
                            // a split we hold (that drop would cost a
                            // delivery attempt on the reclaim).
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
                        // The lease expired under us.
                        self.drop_owned(id, SplitLossReason::Starved);
                        Ok(())
                    }
                    Err(_) => {
                        // Cannot tell; the next beat (or the starvation
                        // self-fence) decides.
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
            Command::DeclineHandoff { split, reply } => {
                let id = split.as_str();
                if self
                    .handoff_granting
                    .as_ref()
                    .is_some_and(|(granting, _)| granting == id)
                {
                    self.handoff_granting = None;
                }
                // Exclude the split from re-offer for one round budget:
                // most declines are transient (a lane not yet open), so a
                // brief cool-down lets the victim offer its other splits
                // first and retry this one later.
                self.handoff_declined.insert(id.to_string(), self.round);
                let _ = reply.try_send(Ok(()));
                Ok(())
            }
        }
    }

    /// The fenced commit: one CAS on the durable progress record — small
    /// by schema (the descriptor lives in the immutable spec record), so
    /// commit cost is independent of descriptor size. A lost CAS means a
    /// peer owns the split — nothing was written, the caller gets
    /// `Fenced`, and the `Lost` event follows.
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
                self.upsert_progress(id, record, rev)?;
                if progress.completed {
                    self.finish_completed(id).await;
                }
                Ok(())
            }
            Ok(CasOutcome::Lost) => {
                self.metrics(|m| m.write(WriteOutcome::Conflict, started.elapsed()));
                // Maybe-landed hazard: if the winning write is OUR OWN
                // (a previous timed-out attempt that actually landed),
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
                // metric — remove from `owned` before folding the write so
                // the epoch bump cannot read as a peer's fence.
                self.owned.remove(id);
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
    /// `departure` distinguishes a shutdown/scale-down release — which
    /// retires this worker once its working set empties — from a
    /// cooperative-handoff grant, which never leaves the fleet even when it
    /// hands off the last split. On the handoff path each granted split is
    /// counted [`HandoffOutcome::Granted`] (or `Aborted` if its CAS lost),
    /// and the request-slot key `handoff.{self}` is deleted afterward as
    /// the grant's ack to the fleet.
    async fn release_splits(
        &mut self,
        splits: &[SplitId],
        departure: bool,
    ) -> Result<(), CoordinationError> {
        let mut released = 0u64;
        for split in splits {
            let id = split.as_str();
            let Some(owned) = self.owned.get(id) else {
                continue; // already gone: released/lost/completed
            };
            let lease_rev = owned.lease_rev;
            let Some(state) = self.splits.get(id) else {
                continue;
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
                    released += 1;
                    // `Granted` counts the annotated grant alone — a
                    // multi-split non-departure batch must not inflate it,
                    // and a drain finishing for a request that no longer
                    // exists is still one granted move, not several.
                    if !departure && self.handoff_granting.as_ref().is_some_and(|(s, _)| s == id) {
                        self.metrics(|m| m.handoff(HandoffOutcome::Granted));
                        self.handoff_granting = None;
                    }
                }
                Ok(CasOutcome::Lost) => {
                    // Fenced during shutdown: the split is someone else's
                    // problem now, which is exactly what release wanted. On
                    // the handoff path `drop_owned` counts the Aborted and
                    // clears the grant, so nothing more to do here.
                    self.drop_owned(id, SplitLossReason::Fenced);
                }
                Err(e) => {
                    tracing::warn!(split = %id, error = %e, "release write failed; lease will expire");
                    // Still drop the lease key best-effort: the attempt
                    // accounting is conservative (counts as non-graceful).
                    self.owned.remove(id);
                    self.release_lease_key(id, lease_rev).await;
                    if !departure && self.handoff_granting.as_ref().is_some_and(|(s, _)| s == id) {
                        self.metrics(|m| m.handoff(HandoffOutcome::Aborted));
                        self.handoff_granting = None;
                    }
                }
            }
        }
        self.metrics(|m| m.released(released));
        if !departure {
            // The grant's ack to the fleet: delete our request-slot key so
            // the requester's outstanding request retires and the victim
            // slot frees for a future drain. Guarded on the revision we
            // observed AND on this batch actually releasing something — a
            // late release for a split already fenced away must not
            // destroy a fresh request (possibly another requester's) that
            // arrived since.
            if released > 0
                && let Some((_, rev)) = self.handoff_requests.get(&self.instance)
            {
                let rev = *rev;
                let key = records::handoff_key(&self.instance);
                let _ = self
                    .store
                    .delete(Keyspace::Ephemeral, &key, Some(rev))
                    .await;
                self.handoff_requests.remove(&self.instance);
            }
            return Ok(());
        }
        // Releasing the last held split is how a worker leaves the fleet
        // (shutdown, scale-down). Make the departure real: stop claiming
        // (or the releaser instantly re-claims its own hand-backs), hand
        // leadership back, and drop the presence key so the remaining
        // workers' fair share widens now instead of after the TTL. The
        // gate is on what was ASKED, not on what the store acknowledged —
        // a release whose last split was concurrently stolen (or whose
        // write failed) is still a departure, and a departing worker that
        // keeps claiming strands splits when the process exits.
        if self.owned.is_empty() && !splits.is_empty() {
            self.parting = true;
            self.demote().await;
            let key = records::worker_key(&self.instance);
            let _ = self.store.delete(Keyspace::Ephemeral, &key, None).await;
            self.presence.remove(&self.instance);
            // A departing worker abandons any request it was pursuing.
            self.withdraw_handoff().await;
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Terminal detection.

    /// Decide whether the job is over, and how.
    ///
    /// Two properties matter more than promptness here, because the verdict
    /// latches forever and a wrong `AllComplete` reports an incomplete
    /// backfill as a success:
    ///
    /// - **Quarantine blocks completion explicitly.** The invariant is not
    ///   left to fall out of `completed == total` arithmetic; one slipped
    ///   tally would otherwise dress unprocessed data up as a green exit.
    /// - **The verdict is rendered against an authoritative listing**, not
    ///   against this worker's watch-fed view and not against
    ///   `plan.planned`. `planned` is only a lower bound: `finish_plan`
    ///   seeds split records *before* it recounts and publishes, and both
    ///   publish-failure paths leave the seeded records behind, so a
    ///   `Final` plan — which never replans — can name fewer splits than
    ///   the store actually holds. Judging a *subset* that happens to be
    ///   all-complete is exactly how a quarantined split goes unseen.
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
        // Cheap gate: only pay for the listing once this worker's own view
        // both covers what the plan promised and looks terminal. `planned`
        // is a lower bound (see above), so this is `<`, not `!=` — an
        // undercounting plan record must not freeze the verdict forever.
        let local = self.splits.len() as u64;
        if local < planned || self.completed_count + self.quarantined_count != local {
            return Ok(());
        }

        // Authoritative recount. Applying the entries is idempotent —
        // `upsert_progress` drops anything at or behind a revision it has
        // already folded — so this doubles as the catch-up for a view that
        // was missing records.
        let entries = match self
            .store
            .list(Keyspace::Durable, records::SPLIT_PREFIX)
            .await
        {
            Ok(entries) => entries,
            Err(e) => {
                // Refusing to judge is the safe direction: the job keeps
                // running and re-judges on a later tick.
                tracing::warn!(error = %e, "terminal listing failed; deferring the verdict");
                return Ok(());
            }
        };
        for entry in &entries {
            self.apply_state_put(entry)?;
        }

        let total = self.splits.len() as u64;
        if total != entries.len() as u64 {
            // The listing and the folded view disagree on cardinality —
            // records outside `split.` prefixing, or a concurrent seed.
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
