//! The synchronous coordinator handle sources embed.
//!
//! One background task per process owns all store I/O (see `task.rs`);
//! this handle drives it over channels from the pipeline's controller
//! thread. Commands (commit/fail/release) get bounded blocking replies;
//! events arrive on an unbounded queue the controller drains via `poll`.
//! Nothing here blocks on the tokio runtime — the sync side uses plain
//! `std::sync::mpsc` receives, so a wedged runtime cannot deadlock the
//! controller.

use crate::config::CoordinationConfig;
use crate::error::fatal;
use crate::records::{self, LeaseVal, SplitProgressRecord};
use crate::store::metered::Metered;
use crate::store::{CoordinationStore, Keyspace};
use crate::task::{Command, Task, TaskEvent};
use etl_core::coordination::{
    CoordinationError, CoordinationErrorKind, CoordinationEvent, SplitCoordinator, SplitId,
    SplitPlanner, SplitProgress,
};
use etl_core::metrics::CoordinationMetrics;
use std::collections::BTreeMap;
use std::sync::mpsc as std_mpsc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Command-queue depth; the controller thread sends one command at a time,
/// so anything above a handful only covers bursts around shutdown.
const COMMAND_DEPTH: usize = 64;

/// A [`SplitCoordinator`] over any [`CoordinationStore`].
///
/// Built with a multi-thread runtime handle (the background task and the
/// store's I/O live there; a current-thread runtime would deadlock the
/// blocking replies and is rejected at construction).
pub struct StoreCoordinator<S: CoordinationStore + Clone> {
    store: S,
    config: CoordinationConfig,
    io: tokio::runtime::Handle,
    metrics: Option<CoordinationMetrics>,
    instance: String,
    nonce: String,
    running: Option<Running>,
    failed: Option<(CoordinationErrorKind, String)>,
}

struct Running {
    commands: mpsc::Sender<Command>,
    events: std_mpsc::Receiver<TaskEvent>,
    task: tokio::task::JoinHandle<()>,
    /// Splits observed Gained (with their tenancy epoch) minus
    /// Lost/completed — the release set the direct teardown fallback
    /// works from. The epoch pins the tenancy: a direct release must
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
    /// Fatal on invalid configuration, a current-thread runtime, or a
    /// store whose lease TTL diverges from `config.lease_duration`.
    pub fn new(
        store: S,
        config: CoordinationConfig,
        io: tokio::runtime::Handle,
        metrics: Option<CoordinationMetrics>,
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
        let instance = match &config.instance_id {
            Some(id) => id.clone(),
            None => format!("etl-{}", uuid::Uuid::new_v4().simple()),
        };
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        Ok(StoreCoordinator {
            store,
            config,
            io,
            metrics,
            instance,
            nonce,
            running: None,
            failed: None,
        })
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

    fn command(
        &mut self,
        build: impl FnOnce(std_mpsc::SyncSender<Result<(), CoordinationError>>) -> Command,
    ) -> Result<(), CoordinationError> {
        self.check_failed()?;
        let Some(running) = &self.running else {
            return Err(fatal("coordinator used before start"));
        };
        let commands = running.commands.clone();
        let deadline_at = Instant::now() + self.config.op_timeout * 3;
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
            Ok(result) => result,
            Err(std_mpsc::RecvTimeoutError::Timeout) => Err(CoordinationError::new(
                CoordinationErrorKind::Retryable,
                format!(
                    "coordination command timed out after {:?}; the store may be slow or \
                     unreachable",
                    self.config.op_timeout * 3
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
    /// runs guarded lease deletes and owner-clears under one aggregate
    /// deadline. Best-effort — anything it cannot reach simply expires.
    fn release_direct(&self, splits: &[(SplitId, u64)]) {
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
        else {
            return;
        };
        let deadline = self.config.op_timeout * 2;
        let store = self.store.clone();
        let instance = self.instance.clone();
        let nonce = self.nonce.clone();
        let ids: Vec<(String, u64)> = splits
            .iter()
            .map(|(s, epoch)| (s.as_str().to_string(), *epoch))
            .collect();
        let result = runtime.block_on(async move {
            tokio::time::timeout(deadline, async {
                for (id, epoch) in ids {
                    let key = records::split_key_str(&id);
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
                    // Record: clear the owner so the next claim is
                    // attempt-free. The owner string alone is not proof of
                    // tenancy — a restarted worker with the same stable
                    // instance id may have reclaimed the split under a
                    // higher epoch, and clearing ITS owner would fence a
                    // live peer. The epoch pins our tenancy. Parsed
                    // leniently (no fingerprint at hand) — the CAS on the
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
                }
            })
            .await
        });
        if result.is_err() {
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
        // The decorator applies the per-op deadline and the store-op
        // latency histograms to every primitive in one place.
        let store = Metered::new(self.store.clone(), self.config.op_timeout, metrics.clone());
        let task = Task::new(
            store,
            self.config.clone(),
            fingerprint,
            self.instance.clone(),
            self.nonce.clone(),
            planner,
            metrics,
            command_rx,
            event_tx,
        );
        let join = self.io.spawn(task.run());
        self.running = Some(Running {
            commands: command_tx,
            events: event_rx,
            task: join,
            held: BTreeMap::new(),
        });
        Ok(())
    }

    fn poll(&mut self, timeout: Duration) -> Result<Vec<CoordinationEvent>, CoordinationError> {
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
                Err(std_mpsc::TryRecvError::Empty) => {
                    if out.is_empty() && !timeout.is_zero() {
                        match running.events.recv_timeout(timeout) {
                            Ok(TaskEvent::Coordination(event)) => {
                                Self::observe(&mut running.held, &event);
                                out.push(event);
                                continue; // drain whatever queued behind it
                            }
                            Ok(TaskEvent::Failed(kind, reason)) => {
                                failure = Some((kind, reason));
                                break;
                            }
                            Err(_) => break, // timeout or disconnect: empty poll
                        }
                    }
                    break;
                }
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
        if let Some(running) = self.running.as_mut() {
            match &result {
                Ok(()) if progress.completed => {
                    running.held.remove(split.as_str());
                }
                Err(e) if e.kind == CoordinationErrorKind::Fenced => {
                    running.held.remove(split.as_str());
                }
                _ => {}
            }
        }
        result
    }

    fn fail(&mut self, split: &SplitId, reason: &str) -> Result<(), CoordinationError> {
        let result = self.command(|reply| Command::Fail {
            split: split.clone(),
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
                // The task or its runtime is gone (shutdown ordering can
                // tear the io runtime down before the source drops).
                // Fall back to direct guarded writes so peers claim
                // instantly instead of waiting out the TTL.
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
                self.release_direct(&pairs);
                if let Some(running) = self.running.as_mut() {
                    for split in splits {
                        running.held.remove(split.as_str());
                    }
                }
                Ok(())
            }
        }
    }
}

impl<S: CoordinationStore + Clone> Drop for StoreCoordinator<S> {
    fn drop(&mut self) {
        if let Some(running) = self.running.take() {
            // Anything still held gets a best-effort direct release; the
            // task is then aborted (its leases would expire anyway).
            let held: Vec<(SplitId, u64)> = running
                .held
                .iter()
                .filter_map(|(id, epoch)| SplitId::new(id.clone()).ok().map(|s| (s, *epoch)))
                .collect();
            running.task.abort();
            if !held.is_empty() {
                self.release_direct(&held);
            }
        }
    }
}
