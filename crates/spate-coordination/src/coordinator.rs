//! The synchronous coordinator handle sources embed.
//!
//! One background task per process owns all store I/O (see `task.rs`);
//! this handle drives it over channels from the pipeline's controller
//! thread. Commands (commit/fail/release) get bounded blocking replies;
//! events arrive on an unbounded queue the controller drains via `poll`.
//! Nothing here blocks on the tokio runtime. The sync side uses plain
//! `std::sync::mpsc` receives, so a wedged runtime cannot deadlock the
//! controller.

use crate::config::CoordinationConfig;
use crate::error::fatal;
use crate::records::{self, LeaseVal, SplitProgressRecord};
use crate::store::metered::Metered;
use crate::store::{CoordinationStore, Keyspace, WatchMode};
use crate::task::{Command, DepartReply, Task, TaskEvent};
use spate_core::clock::tokio::{Clock, SystemClock};
use spate_core::coordination::ControlWaker;
use spate_core::coordination::{
    CoordinationError, CoordinationErrorKind, CoordinationEvent, LeaseEpoch, SplitCoordinator,
    SplitId, SplitPlanner, SplitProgress,
};
use spate_core::metrics::CoordinationMetrics;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::mpsc as std_mpsc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Command-queue depth; the controller thread sends one command at a time,
/// so anything above a handful only covers bursts around shutdown.
const COMMAND_DEPTH: usize = 64;

/// Pause before re-sending a `Depart` the task refused.
const DEPART_RETRY: Duration = Duration::from_millis(20);

/// A [`SplitCoordinator`] over any [`CoordinationStore`].
///
/// Built with a multi-thread runtime handle (the background task and the
/// store's I/O live there; a current-thread runtime would deadlock the
/// blocking replies and is rejected at construction).
///
/// After the background task processes every split in a nonempty
/// [`release`](SplitCoordinator::release) request, an empty owned set
/// permanently retires this worker from the fleet and stops it claiming work.
/// Split, role and membership hand-back is best-effort; errors or deferred
/// commands can prevent completion, and success does not guarantee store
/// deletion.
pub struct StoreCoordinator<S: CoordinationStore + Clone> {
    store: S,
    config: CoordinationConfig,
    clock: Arc<dyn Clock>,
    io: tokio::runtime::Handle,
    metrics: Option<CoordinationMetrics>,
    instance: String,
    nonce: String,
    running: Option<Running>,
    failed: Option<(CoordinationErrorKind, String)>,
    /// Set by the driver before `start`; handed to the task so every
    /// queued event also wakes the driver's park.
    waker: Option<ControlWaker>,
    #[cfg(feature = "testing")]
    probe: Arc<crate::loop_probe::LoopProbe>,
}

struct Running {
    commands: mpsc::Sender<Command>,
    events: std_mpsc::Receiver<TaskEvent>,
    task: tokio::task::JoinHandle<()>,
    /// Splits observed Gained (with their tenancy epoch) minus
    /// Lost/completed. This is the release set the direct teardown
    /// fallback works from. The epoch pins the tenancy: a direct release must
    /// never clear a record a same-named restart has since reclaimed.
    held: BTreeMap<String, u64>,
}

impl<S: CoordinationStore + Clone> std::fmt::Debug for StoreCoordinator<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreCoordinator")
            .field("instance", &self.instance)
            .field("started", &self.running.is_some())
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl<S: CoordinationStore + Clone> StoreCoordinator<S> {
    /// Wrap a store. `io` must be a multi-thread runtime handle.
    ///
    /// # Errors
    ///
    /// Fatal on invalid configuration, a current-thread runtime, a store
    /// whose lease TTL diverges from `config.lease_duration` or whose
    /// `op_timeout` diverges from `config.op_timeout`, or a polled store
    /// whose interval is zero or not below it.
    pub fn new(
        store: S,
        config: CoordinationConfig,
        io: tokio::runtime::Handle,
        metrics: Option<CoordinationMetrics>,
    ) -> Result<StoreCoordinator<S>, CoordinationError> {
        StoreCoordinator::with_clock(store, config, io, metrics, Arc::new(SystemClock))
    }

    /// Like [`new`](StoreCoordinator::new) but drives the starvation
    /// self-fence from an injected [`Clock`]. A frozen clock makes fencing
    /// deterministic under CI scheduler jitter. Pass the same clock to the
    /// store so its lease expiry stays coherent.
    ///
    /// # Errors
    ///
    /// Fatal on invalid configuration, a current-thread runtime, a store
    /// whose lease TTL diverges from `config.lease_duration` or whose
    /// `op_timeout` diverges from `config.op_timeout`, or a polled store
    /// whose interval is zero or not below it.
    #[doc(hidden)]
    pub fn with_clock(
        store: S,
        config: CoordinationConfig,
        io: tokio::runtime::Handle,
        metrics: Option<CoordinationMetrics>,
        clock: Arc<dyn Clock>,
    ) -> Result<StoreCoordinator<S>, CoordinationError> {
        config.validate()?;
        if io.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
            return Err(fatal(
                "coordination needs a multi-thread tokio runtime: the coordinator's \
                 background task and the controller's blocking replies cannot share one \
                 thread",
            ));
        }
        // Renewal cadence comes from the config, expiry from the store's
        // TTL: built from different values they silently churn leases
        // (renewals pace against the wrong deadline). Fail fast instead.
        let store_ttl = store.lease_ttl();
        if store_ttl != config.lease_duration {
            return Err(fatal(format!(
                "the store's lease TTL ({store_ttl:?}) does not match \
                 coordination.lease_duration ({:?}): both must be built from the same \
                 value — construct the store with the config's lease_duration",
                config.lease_duration
            )));
        }
        if let Some(store_timeout) = store.op_timeout()
            && store_timeout != config.op_timeout
        {
            return Err(fatal(format!(
                "the store sizes its timeouts from an op_timeout of {store_timeout:?}, which \
                 differs from coordination.op_timeout ({:?}): construct the store with the \
                 config's op_timeout",
                config.op_timeout
            )));
        }
        if let WatchMode::Polled { interval } = store.watch_mode()
            && (interval.is_zero() || interval >= config.lease_duration)
        {
            return Err(fatal(format!(
                "the store polls its watches every {interval:?}, which must be above zero \
                 and below coordination.lease_duration ({:?})",
                config.lease_duration
            )));
        }
        let instance = match &config.instance_id {
            Some(id) => id.clone(),
            None => format!("spate-{}", uuid::Uuid::new_v4().simple()),
        };
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        Ok(StoreCoordinator {
            store,
            config,
            clock,
            io,
            metrics,
            instance,
            nonce,
            running: None,
            failed: None,
            waker: None,
            #[cfg(feature = "testing")]
            probe: Arc::default(),
        })
    }

    /// What this worker's control loop last reported.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    #[must_use]
    pub fn loop_probe(&self) -> Arc<crate::loop_probe::LoopProbe> {
        Arc::clone(&self.probe)
    }

    /// This worker's (stable or generated) instance id.
    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance
    }

    fn check_failed(&self) -> Result<(), CoordinationError> {
        match &self.failed {
            Some((kind, reason)) => Err(CoordinationError::new(*kind, reason.clone())),
            None => Ok(()),
        }
    }

    fn fail_from(&mut self, kind: CoordinationErrorKind, reason: &str) -> CoordinationError {
        self.failed = Some((kind, reason.to_string()));
        CoordinationError::new(kind, reason.to_string())
    }

    /// Drops `split` from the held set when `result` ends its tenancy.
    fn note_commit(
        &mut self,
        split: &SplitId,
        progress: &SplitProgress,
        result: &Result<(), CoordinationError>,
    ) {
        if let Some(running) = self.running.as_mut() {
            match result {
                Ok(()) if progress.completed => {
                    running.held.remove(split.as_str());
                }
                Err(e) if e.kind == CoordinationErrorKind::Fenced => {
                    running.held.remove(split.as_str());
                }
                _ => {}
            }
        }
    }

    fn command(
        &mut self,
        build: impl FnOnce(std_mpsc::SyncSender<Result<(), CoordinationError>>) -> Command,
    ) -> Result<(), CoordinationError> {
        let budget = self.config.op_timeout * 3;
        self.send_until(Instant::now() + budget, budget, build)?
    }

    /// Send a command and wait for its reply until `deadline_at`. `Err`
    /// means no reply came; a timeout reports `budget` as the wait it
    /// exceeded.
    fn send_until<R>(
        &mut self,
        deadline_at: Instant,
        budget: Duration,
        build: impl FnOnce(std_mpsc::SyncSender<R>) -> Command,
    ) -> Result<R, CoordinationError> {
        self.check_failed()?;
        let Some(running) = &self.running else {
            return Err(fatal("coordinator used before start"));
        };
        let commands = running.commands.clone();
        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        // Enqueue with the same deadline as the reply: a full queue means
        // the task is backed up behind an unreachable store, and an
        // unbounded send here would wedge the controller thread for good.
        let mut command = build(reply_tx);
        loop {
            match commands.try_send(command) {
                Ok(()) => break,
                Err(mpsc::error::TrySendError::Full(returned)) => {
                    if Instant::now() >= deadline_at {
                        return Err(CoordinationError::new(
                            CoordinationErrorKind::Retryable,
                            "coordination command queue is full; the store may be slow or \
                             unreachable",
                        ));
                    }
                    command = returned;
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return Err(self.drain_failure());
                }
            }
        }
        // The reply is awaited synchronously with a deadline; a wedged
        // store surfaces as Retryable, not a hung pipeline.
        let remaining = deadline_at.saturating_duration_since(Instant::now());
        match reply_rx.recv_timeout(remaining) {
            Ok(reply) => Ok(reply),
            Err(std_mpsc::RecvTimeoutError::Timeout) => Err(CoordinationError::new(
                CoordinationErrorKind::Retryable,
                format!(
                    "coordination command timed out after {budget:?}; the store may be slow or \
                     unreachable"
                ),
            )),
            Err(std_mpsc::RecvTimeoutError::Disconnected) => Err(self.drain_failure()),
        }
    }

    /// The task died: pull its parting Failed event (if any) so the
    /// caller gets the real cause rather than a broken-channel error.
    fn drain_failure(&mut self) -> CoordinationError {
        if let Some(running) = &self.running {
            while let Ok(event) = running.events.try_recv() {
                if let TaskEvent::Failed(kind, reason) = event {
                    self.failed = Some((kind, reason));
                }
            }
        }
        match &self.failed {
            Some((kind, reason)) => CoordinationError::new(*kind, reason.clone()),
            None => self.fail_from(
                CoordinationErrorKind::Fatal,
                "coordination task stopped unexpectedly",
            ),
        }
    }

    /// Track held splits (and their tenancy epochs) from the event stream
    /// (for the teardown fallback) while passing events through.
    fn observe(held: &mut BTreeMap<String, u64>, event: &CoordinationEvent) {
        match event {
            CoordinationEvent::Gained { split, epoch, .. } => {
                held.insert(split.id.as_str().to_string(), epoch.0);
            }
            CoordinationEvent::Lost { split } | CoordinationEvent::Quarantined { split, .. } => {
                held.remove(split.as_str());
            }
            CoordinationEvent::AllComplete | CoordinationEvent::Stalled { .. } => {}
            // Non-exhaustive upstream enum: future events cannot affect
            // the held-set bookkeeping this crate defined against.
            _ => {}
        }
    }

    /// Whether the background task can still service commands (its
    /// command channel is open).
    fn task_alive(&self) -> bool {
        self.running
            .as_ref()
            .is_some_and(|r| !r.commands.is_closed())
    }

    /// Direct-store release for teardown paths where the background task
    /// (or its runtime) is already gone: a private current-thread runtime
    /// runs guarded owner-clears and lease deletes within `budget` on the
    /// coordinator's clock. Best-effort; anything it cannot reach expires.
    fn release_direct(&self, splits: &[(SplitId, u64)], budget: Duration) {
        // IO too: a store client may open connections on this runtime.
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            return;
        };
        let store = self.store.clone();
        let instance = self.instance.clone();
        let nonce = self.nonce.clone();
        let clock = self.clock.clone();
        let ids: Vec<(String, u64)> = splits
            .iter()
            .map(|(s, epoch)| (s.as_str().to_string(), *epoch))
            .collect();
        let finished = runtime.block_on(async move {
            let release = async {
                for (id, epoch) in ids {
                    let key = records::split_key_str(&id);
                    // Record: clear the owner before deleting the lease, so
                    // a claim that follows the delete reads no owner and
                    // consumes no attempt. The epoch guards against a
                    // same-named restart that reclaimed the split. Parsed
                    // leniently (no fingerprint at hand); the CAS on the
                    // read revision is still safe.
                    if let Ok(Some(entry)) = store.get(Keyspace::Durable, &key).await
                        && let Ok(mut record) =
                            serde_json::from_slice::<SplitProgressRecord>(&entry.value)
                        && record.owner.as_deref() == Some(instance.as_str())
                        && record.epoch == epoch
                    {
                        record.owner = None;
                        record.written_at_ms = records::now_ms();
                        let _ = store
                            .update(Keyspace::Durable, &key, record.encode(), entry.revision)
                            .await;
                    }
                    // Lease: delete only if it is really ours.
                    if let Ok(Some(entry)) = store.get(Keyspace::Ephemeral, &key).await
                        && let Ok(lease) = serde_json::from_slice::<LeaseVal>(&entry.value)
                        && lease.owner == instance
                        && lease.nonce == nonce
                    {
                        let _ = store
                            .delete(Keyspace::Ephemeral, &key, Some(entry.revision))
                            .await;
                    }
                }
            };
            // Built inside `block_on`: the system clock's sleep needs this
            // runtime's timer.
            let expiry = clock.sleep_until(clock.now() + budget);
            tokio::select! {
                () = release => true,
                () = expiry => false,
            }
        });
        if !finished {
            tracing::warn!("direct release ran out of time; remaining leases will expire");
        }
    }
}

impl<S: CoordinationStore + Clone> SplitCoordinator for StoreCoordinator<S> {
    fn start(&mut self, planner: Box<dyn SplitPlanner>) -> Result<(), CoordinationError> {
        self.check_failed()?;
        if self.running.is_some() {
            return Err(fatal("SplitCoordinator::start called twice"));
        }
        let fingerprint = planner.fingerprint();
        if fingerprint.is_empty() {
            return Err(fatal("the planner fingerprint must not be empty"));
        }
        let (command_tx, command_rx) = mpsc::channel(COMMAND_DEPTH);
        let (event_tx, event_rx) = std_mpsc::channel();
        let metrics = self.metrics.take();
        if let Some(m) = &metrics {
            self.store.attach_metrics(m);
        }
        // The decorator applies the per-op deadline and the store-op
        // latency histograms to every primitive in one place.
        let store = Metered::new(self.store.clone(), self.config.op_timeout, metrics.clone());
        let task = Task::new(
            store,
            self.config.clone(),
            self.clock.clone(),
            fingerprint,
            self.instance.clone(),
            self.nonce.clone(),
            planner,
            metrics,
            command_rx,
            event_tx,
            self.waker.clone(),
        );
        #[cfg(feature = "testing")]
        let task = task.with_probe(Arc::clone(&self.probe));
        let join = self.io.spawn(task.run());
        self.running = Some(Running {
            commands: command_tx,
            events: event_rx,
            task: join,
            held: BTreeMap::new(),
        });
        Ok(())
    }

    fn set_waker(&mut self, waker: ControlWaker) {
        self.waker = Some(waker);
    }

    fn poll(&mut self) -> Result<Vec<CoordinationEvent>, CoordinationError> {
        self.check_failed()?;
        let Some(running) = self.running.as_mut() else {
            return Err(fatal("coordinator polled before start"));
        };
        let mut out = Vec::new();
        let mut failure = None;
        loop {
            match running.events.try_recv() {
                Ok(TaskEvent::Coordination(event)) => {
                    Self::observe(&mut running.held, &event);
                    out.push(event);
                }
                Ok(TaskEvent::Failed(kind, reason)) => {
                    failure = Some((kind, reason));
                    break;
                }
                // Nothing queued. This call never blocks. The driver owns
                // the wait and the task wakes it when it enqueues.
                Err(std_mpsc::TryRecvError::Empty) => break,
                Err(std_mpsc::TryRecvError::Disconnected) => {
                    if out.is_empty() {
                        return Err(self.drain_failure());
                    }
                    break;
                }
            }
        }
        if let Some((kind, reason)) = failure {
            self.failed = Some((kind, reason.clone()));
            if out.is_empty() {
                return Err(CoordinationError::new(kind, reason));
            }
        }
        Ok(out)
    }

    fn commit(
        &mut self,
        split: &SplitId,
        progress: &SplitProgress,
    ) -> Result<(), CoordinationError> {
        let result = self.command(|reply| Command::Commit {
            split: split.clone(),
            progress: progress.clone(),
            reply,
        });
        self.note_commit(split, progress, &result);
        result
    }

    /// Sends the commits in order under one `op_timeout` for the batch.
    /// An entry reached after the budget is spent is answered `Retryable`
    /// without being sent.
    fn commit_final(
        &mut self,
        commits: &[(SplitId, SplitProgress)],
    ) -> Vec<Result<(), CoordinationError>> {
        let budget = self.config.op_timeout;
        let deadline = Instant::now() + budget;
        let mut results = Vec::with_capacity(commits.len());
        for (split, progress) in commits {
            if Instant::now() >= deadline {
                results.push(Err(CoordinationError::new(
                    CoordinationErrorKind::Retryable,
                    format!(
                        "final commit budget of {budget:?} spent before this split; nothing \
                         was sent"
                    ),
                )));
                continue;
            }
            let result = self
                .send_until(deadline, budget, |reply| Command::Commit {
                    split: split.clone(),
                    progress: progress.clone(),
                    reply,
                })
                .and_then(|reply| reply);
            self.note_commit(split, progress, &result);
            results.push(result);
        }
        results
    }

    fn fail(
        &mut self,
        split: &SplitId,
        epoch: LeaseEpoch,
        reason: &str,
    ) -> Result<(), CoordinationError> {
        let result = self.command(|reply| Command::Fail {
            split: split.clone(),
            epoch,
            reason: reason.to_string(),
            reply,
        });
        if result.is_ok()
            && let Some(running) = self.running.as_mut()
        {
            running.held.remove(split.as_str());
        }
        result
    }

    fn release(&mut self, splits: &[SplitId]) -> Result<(), CoordinationError> {
        let result = self.command(|reply| Command::Release {
            splits: splits.to_vec(),
            departure: true,
            reply,
        });
        match result {
            Ok(()) => {
                if let Some(running) = self.running.as_mut() {
                    for split in splits {
                        running.held.remove(split.as_str());
                    }
                }
                Ok(())
            }
            Err(e) if self.task_alive() => {
                // The task is alive but slow (store outage): the queued
                // release may still execute, so a direct write now would
                // race our own task. Best-effort contract: leases the
                // task cannot hand back expire on their own.
                tracing::warn!(error = %e, "release deferred; leases expire if it never lands");
                Ok(())
            }
            Err(e) => {
                // The task or its runtime is gone. Fall back to direct
                // guarded writes so peers claim instantly instead of
                // waiting out the TTL.
                tracing::warn!(error = %e, "task-path release failed; releasing directly");
                let pairs: Vec<(SplitId, u64)> = match &self.running {
                    Some(running) => splits
                        .iter()
                        .filter_map(|s| {
                            running
                                .held
                                .get(s.as_str())
                                .map(|epoch| (s.clone(), *epoch))
                        })
                        .collect(),
                    None => Vec::new(),
                };
                self.release_direct(&pairs, self.config.op_timeout * 2);
                if let Some(running) = self.running.as_mut() {
                    for split in splits {
                        running.held.remove(split.as_str());
                    }
                }
                Ok(())
            }
        }
    }

    /// Hands back every split the task holds, including ones this handle
    /// has not polled yet, within one `op_timeout`. Past that the task is
    /// aborted and its leases expire.
    fn depart(&mut self, _held: &[SplitId]) -> Result<(), CoordinationError> {
        if self.running.is_none() || self.failed.is_some() {
            return Ok(());
        }
        let budget = self.config.op_timeout;
        let started = Instant::now();
        let deadline = started + budget;
        // A task re-establishing a broken watch refuses commands between
        // its attempts; ask again while the budget lasts.
        let reply = loop {
            // The task stops writing an eighth of what is left early, so its
            // reply arrives while this handle still has time to act on it.
            let now = Instant::now();
            let task_deadline = now + deadline.saturating_duration_since(now) * 7 / 8;
            let reply = self.send_until(deadline, budget, |reply| Command::Depart {
                deadline: task_deadline,
                reply,
            });
            match reply {
                Ok(DepartReply::Refused(_)) if started.elapsed() + DEPART_RETRY < budget => {
                    std::thread::sleep(DEPART_RETRY);
                }
                reply => break reply,
            }
        };
        // A task that ran the departure writes nothing more, and a gone one
        // cannot; either may leave a direct release work to do. A task that
        // never answered may still be writing, and one would race it.
        let (result, release_directly, mut unreleased) = match reply {
            Ok(DepartReply::Ran { result, unreleased }) => {
                let incomplete = result.is_err();
                (result, incomplete, unreleased)
            }
            Ok(DepartReply::Refused(e)) => (Err(e), false, Vec::new()),
            Err(e) => {
                let gone = !self.task_alive();
                (Err(e), gone, Vec::new())
            }
        };
        self.failed = Some((
            CoordinationErrorKind::Fatal,
            "the coordinator has departed".into(),
        ));
        let Some(running) = self.running.take() else {
            return result;
        };
        running.task.abort();
        if let Err(e) = &result
            && release_directly
        {
            tracing::warn!(error = %e, "task-path departure failed; releasing directly");
            // The task's list covers gains this handle never polled.
            for (id, epoch) in &running.held {
                if !unreleased.iter().any(|(s, _)| s.as_str() == id)
                    && let Ok(split) = SplitId::new(id.clone())
                {
                    unreleased.push((split, *epoch));
                }
            }
            if !unreleased.is_empty() {
                self.release_direct(&unreleased, budget.saturating_sub(started.elapsed()));
            }
        }
        result
    }

    fn release_drained(&mut self, splits: &[SplitId]) -> Result<(), CoordinationError> {
        // A drained revocation, not a departure: the task keeps this
        // worker in the fleet even when the last split is handed back.
        // Unlike `release`, there is NO direct-store teardown fallback.
        // The process is live, so a release the task cannot land right now
        // makes the rebalance slower (the leader forces it at
        // `drain_deadline`); an unreleased lease expires on its own.
        let result = self.command(|reply| Command::Release {
            splits: splits.to_vec(),
            departure: false,
            reply,
        });
        // Drop from the held set the same way `release` does: the split is
        // no longer this worker's to hand back at teardown.
        if let Some(running) = self.running.as_mut() {
            for split in splits {
                running.held.remove(split.as_str());
            }
        }
        if let Err(e) = &result {
            tracing::warn!(error = %e, "revocation release deferred; the lease expires if it never lands");
        }
        Ok(())
    }

    fn decline_revoke(&mut self, split: &SplitId) -> Result<(), CoordinationError> {
        // Best-effort: a decline the task never hears costs liveness, never
        // correctness. The revocation is still forced at `drain_deadline`,
        // later than it needed to be.
        let result = self.command(|reply| Command::DeclineRevoke {
            split: split.clone(),
            reply,
        });
        if let Err(e) = &result {
            tracing::warn!(
                split = %split,
                error = %e,
                "revocation decline deferred; the drain deadline forces it anyway"
            );
        }
        Ok(())
    }
}

impl<S: CoordinationStore + Clone> Drop for StoreCoordinator<S> {
    fn drop(&mut self) {
        if let Some(running) = self.running.take() {
            // Abort the task, then give anything still held a best-effort
            // direct release (its leases would expire anyway).
            let held: Vec<(SplitId, u64)> = running
                .held
                .iter()
                .filter_map(|(id, epoch)| SplitId::new(id.clone()).ok().map(|s| (s, *epoch)))
                .collect();
            running.task.abort();
            if !held.is_empty() {
                self.release_direct(&held, self.config.op_timeout);
            }
        }
    }
}
