//! The per-shard worker: merge chunks into big batches, seal, write with
//! replica rotation and retries, resolve acknowledgments.
//!
//! Acknowledgment handles never enter a write task: they stay in the
//! worker's pending map and are resolved from task *outcomes*. Aborting a
//! write task at the drain deadline is therefore safe; an aborted task
//! cannot resolve a batch as delivered.
//!
//! Batches of one shard may complete out of order across the `max_inflight`
//! window; the checkpointer's contiguity tracker absorbs this.

use super::breaker::BreakerSet;
use super::config::SinkPoolConfig;
use super::retry::Backoff;
use super::{EncodedChunk, SealedBatch, ShardWriter};
use crate::backpressure::InflightBudget;
use crate::checkpoint::AckSet;
use crate::error::{ErrorClass, SinkError};
use crate::metrics::{AttemptOutcome, FlushReason, SinkShardMetrics};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::{JoinError, JoinSet};
use tokio::time::Instant;

/// How long the drain sweep waits for aborted write tasks to stop.
/// An abort only lands at a yield point, so a writer that blocks its thread
/// would otherwise hold the worker here past the deadline. Kept below the
/// pool's own backstop (`SinkPool::drain`) so an overrunning worker still
/// returns its own report instead of being force-aborted and losing it.
/// Generous for the intended case, where a writer parked on I/O aborts at once.
const ABORT_GRACE: Duration = Duration::from_millis(500);

/// Bounds on the quarantine re-check heartbeat, which is otherwise
/// `breaker.open_for`.
///
/// The floor covers `open_for: 0s`, which is expressible through the public
/// `Copy` [`BreakerConfig`](super::config::BreakerConfig) that `SinkParts`
/// and `spate-test` build without going through a loader.
/// [`BreakerConfig::validate`](super::config::BreakerConfig::validate) rejects
/// it on the YAML path; this covers the other one. The wait is driven from
/// `open_for`, and `sleep_until(now)` returns instantly, so a zero would spin
/// on the breaker mutex.
///
/// The ceiling holds because `open_for` is a *probe cadence*, how hard to lean
/// on an endpoint that has been failing, and not a statement about how often a
/// task may re-read state it already shares. An operator asking for an hour
/// between probes is not asking for a sealed batch to sit that long behind a
/// signal that never came. The ceiling also keeps `now + backstop` away from
/// the `Instant + Duration` overflow an absurd `humantime` value would reach.
/// The floor never fires above 100ms and does nothing for that.
const QUARANTINE_BACKSTOP_MIN: Duration = Duration::from_millis(100);
/// Ceiling for the quarantine heartbeat. See [`QUARANTINE_BACKSTOP_MIN`].
const QUARANTINE_BACKSTOP_MAX: Duration = Duration::from_secs(30);

/// How long a write task parked on a fully-quarantined shard waits before
/// re-checking on its own initiative.
///
/// A heartbeat. The wake signal releases every state that *can* resolve, and
/// re-picking cannot repair one that cannot. At every default the clamp is the
/// identity.
///
/// Widening this constant changes no observable behavior. Every exit from
/// "half-open, budget spent" runs through `on_success`, `on_failure` or
/// `release_probe`, and all three publish a wake; `ProbeGuard` covers the
/// remaining path, a probe task dying silently.
///
/// Narrowing it is a different matter. Two pool tests get their discriminating
/// power from this ceiling being far longer than the wake they are asserting;
/// at a few hundred milliseconds the heartbeat alone carries them, so they pass
/// whether or not the wake works. What the heartbeat defends against is a
/// future mutation that adds a fourth exit and forgets to publish, which stalls
/// the shard for `open_for` rather than forever. Only
/// [`quarantine_backstop`]'s clamp is tested.
fn quarantine_backstop(open_for: Duration) -> Duration {
    open_for.clamp(QUARANTINE_BACKSTOP_MIN, QUARANTINE_BACKSTOP_MAX)
}

/// What one shard worker did over its lifetime, for the drain report.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct WorkerReport {
    /// Batches durably written.
    pub(crate) flushed: u64,
    /// Batches abandoned (acknowledgments failed; data replays after
    /// restart).
    pub(crate) abandoned: u64,
}

impl WorkerReport {
    pub(crate) fn absorb(&mut self, other: WorkerReport) {
        self.flushed += other.flushed;
        self.abandoned += other.abandoned;
    }
}

/// A batch awaiting resolution: everything the worker needs to resolve
/// acknowledgments and bookkeeping once its write task reports.
struct Pending {
    acks: AckSet,
    rows: u64,
    bytes: u64,
    reason: FlushReason,
    /// Stamped at **seal**, before the batch has even asked for an in-flight
    /// permit. `settle` reports its elapsed time as
    /// `spate_sink_flush_duration_seconds`, so that histogram is seal-to-settle
    /// and carries the permit wait, every attempt and every backoff sleep. The
    /// sink's round-trip is a separate family,
    /// `spate_sink_write_duration_seconds`. Do not narrow this stamp to recover
    /// a write time.
    started: Instant,
    /// Ingest time of the oldest record in the batch (e2e latency, ingest
    /// basis).
    oldest_ingest: std::time::Instant,
    /// Smallest record event time in the batch, ms since epoch (e2e
    /// latency, event basis).
    oldest_event_ms: i64,
}

/// Outcome reported by a write task. Tasks never touch acks.
struct WriteDone {
    seq: u64,
    written: bool,
}

struct Accumulator {
    frames: Vec<bytes::Bytes>,
    rows: u64,
    bytes: u64,
    acks: AckSet,
    first_at: Option<Instant>,
    oldest_ingest: Option<std::time::Instant>,
    oldest_event_ms: i64,
}

impl Accumulator {
    fn new() -> Self {
        Accumulator {
            frames: Vec::new(),
            rows: 0,
            bytes: 0,
            acks: AckSet::new(),
            first_at: None,
            oldest_ingest: None,
            oldest_event_ms: i64::MAX,
        }
    }

    fn push(&mut self, chunk: EncodedChunk, now: Instant) {
        self.first_at.get_or_insert(now);
        self.rows += u64::from(chunk.rows);
        self.bytes += chunk.frame.len() as u64;
        self.frames.push(chunk.frame);
        self.oldest_ingest = Some(match self.oldest_ingest {
            Some(cur) => cur.min(chunk.oldest_ingest),
            None => chunk.oldest_ingest,
        });
        self.oldest_event_ms = self.oldest_event_ms.min(chunk.oldest_event_ms);
        self.acks.absorb(chunk.acks);
    }

    fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
}

pub(crate) struct ShardWorker<W: ShardWriter> {
    pub(crate) shard: u32,
    pub(crate) writer: Arc<W>,
    pub(crate) endpoints: Arc<Vec<W::Endpoint>>,
    pub(crate) rx: mpsc::Receiver<EncodedChunk>,
    pub(crate) cfg: SinkPoolConfig,
    pub(crate) budget: Arc<InflightBudget>,
    pub(crate) metrics: Arc<SinkShardMetrics>,
    pub(crate) drain_deadline: watch::Receiver<Option<Instant>>,
    /// Deduplication tokens are `"{prefix}{seq}"`; the prefix carries the
    /// pipeline name and shard index.
    pub(crate) token_prefix: String,
}

/// Worker-loop state that outcome handling needs together.
struct Ledger {
    pending: HashMap<u64, Pending>,
    /// Tokio task id → batch seq. A panicked write task's `JoinError`
    /// carries no seq, only the task id, so this map lets us abandon exactly
    /// the batch that died instead of guessing by age (which strands the
    /// victim's acks and budget forever).
    ids: HashMap<tokio::task::Id, u64>,
    report: WorkerReport,
    /// Held so the reservations of anything still pending are released even
    /// when the worker never gets to sweep, whether because `SinkPool::drain`'s
    /// backstop aborted it or the I/O runtime shut down under it. `settle` and
    /// `abandon` remove their entry before releasing, so the ordinary path
    /// drops an empty map and cannot double-release.
    budget: Arc<InflightBudget>,
}

impl Drop for Ledger {
    fn drop(&mut self) {
        for p in self.pending.values() {
            self.budget
                .sub(usize::try_from(p.bytes).unwrap_or(usize::MAX));
        }
    }
}

impl<W: ShardWriter> ShardWorker<W> {
    pub(crate) async fn run(mut self) -> WorkerReport {
        let mut acc = Accumulator::new();
        let mut ledger = Ledger {
            pending: HashMap::new(),
            ids: HashMap::new(),
            report: WorkerReport::default(),
            budget: Arc::clone(&self.budget),
        };
        let mut tasks: JoinSet<WriteDone> = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(self.cfg.inflight.max_per_shard));
        let breakers = Arc::new(Mutex::new(BreakerSet::new(
            self.endpoints.len(),
            self.cfg.breaker,
            Arc::clone(&self.metrics),
        )));
        // A private clone of the deadline watch. The intake loop breaks into
        // drain the moment a deadline is published even with the queue still
        // open (a leaked chunk sender would otherwise keep `recv_many` from
        // ever closing); the drain loop observes one published after parking.
        let mut drain_deadline = self.drain_deadline.clone();
        // Once the deadline-watch sender drops, `changed()` errors forever;
        // disabling the branch keeps either select from busy-spinning on it.
        let mut deadline_watch_live = true;
        let mut seq: u64 = 0;
        let mut recv_buf: Vec<EncodedChunk> = Vec::with_capacity(64);
        // Batches sealed but not yet spawned, oldest first; their `Pending` is
        // already in the ledger, so the deadline sweep abandons them like any
        // other batch. Nothing here may *block* on the in-flight semaphore: an
        // `.await` in `dispatch` suspends `run` outside both `select!`s, where
        // neither the drain deadline nor finished write tasks are polled, and a
        // sink whose writes never return then deadlocks shutdown (#83). Intake
        // is gated on this being empty, which bounds it to one `recv_many` pass
        // plus the drain force-seal.
        let mut waiting: VecDeque<(u64, SealedBatch, Instant)> = VecDeque::new();

        loop {
            let linger_at = acc.first_at.map(|t| t + self.cfg.batch.linger);
            tokio::select! {
                biased;

                Some(joined) = tasks.join_next_with_id(), if !tasks.is_empty() => {
                    self.handle_join(joined, &mut ledger);
                }

                // The permit wait for an already-sealed batch, measured from
                // the stamp in the third element of `waiting`.
                permit = Arc::clone(&semaphore).acquire_owned(), if !waiting.is_empty() => {
                    let permit = permit.expect("sink semaphore closed");
                    self.launch_waiting(permit, &mut waiting, &mut tasks, &breakers, &mut ledger);
                }

                // Gated on `waiting`: while a sealed batch has no permit the
                // shard queue fills and back-pressures the chain.
                n = self.rx.recv_many(&mut recv_buf, 64), if waiting.is_empty() => {
                    if n == 0 {
                        break;
                    }
                    let now = Instant::now();
                    for chunk in recv_buf.drain(..) {
                        acc.push(chunk, now);
                        if let Some(reason) = self.seal_reason(&acc) {
                            self.dispatch(&mut acc, reason, &mut seq, &mut ledger, &mut tasks, &semaphore, &breakers, &mut waiting);
                        }
                    }
                }

                // Below `recv_many` so chunks already queued at shutdown are
                // consumed before the deadline is considered (biased select
                // polls in order). This arm wins while `waiting` gates intake.
                changed = drain_deadline.changed(), if deadline_watch_live => {
                    match changed {
                        // A deadline while intake is still open means a leaked
                        // chunk sender; the bounded drain beats a close that
                        // may never come.
                        Ok(()) if drain_deadline.borrow().is_some() => break,
                        Ok(()) => {}
                        // Sender gone: no drain will ever publish a deadline.
                        Err(_) => deadline_watch_live = false,
                    }
                }

                () = tokio::time::sleep_until(linger_at.unwrap_or_else(Instant::now)), if linger_at.is_some() && waiting.is_empty() => {
                    self.dispatch(&mut acc, FlushReason::Linger, &mut seq, &mut ledger, &mut tasks, &semaphore, &breakers, &mut waiting);
                }
            }
        }

        // ---- Drain (see graceful-shutdown.mdx) ----
        // The queue may still hold chunks the drivers handed over, closed but
        // not empty. A chunk dropped unsealed with the receiver fails its acks
        // with no `abandoned` count, no log and no `DrainReport` entry.
        while let Ok(chunk) = self.rx.try_recv() {
            acc.push(chunk, Instant::now());
            if let Some(reason) = self.seal_reason(&acc) {
                self.dispatch(
                    &mut acc,
                    reason,
                    &mut seq,
                    &mut ledger,
                    &mut tasks,
                    &semaphore,
                    &breakers,
                    &mut waiting,
                );
            }
        }

        // Force-seal whatever is left; `dispatch` parks the batch in `waiting`
        // when no permit is free.
        self.dispatch(
            &mut acc,
            FlushReason::Drain,
            &mut seq,
            &mut ledger,
            &mut tasks,
            &semaphore,
            &breakers,
            &mut waiting,
        );

        loop {
            while !waiting.is_empty() {
                let Ok(permit) = Arc::clone(&semaphore).try_acquire_owned() else {
                    break;
                };
                self.launch_waiting(permit, &mut waiting, &mut tasks, &breakers, &mut ledger);
            }
            if tasks.is_empty() && waiting.is_empty() {
                break;
            }

            let deadline = *drain_deadline.borrow();
            // The pool was dropped without draining, so no stop request is
            // coming. Abandon rather than wait on writes that may never finish.
            if deadline.is_none() && !deadline_watch_live {
                self.sweep(&mut tasks, &mut waiting, &mut ledger).await;
                break;
            }

            tokio::select! {
                biased;

                Some(joined) = tasks.join_next_with_id(), if !tasks.is_empty() => {
                    self.handle_join(joined, &mut ledger);
                }

                changed = drain_deadline.changed(), if deadline_watch_live => {
                    if changed.is_err() {
                        deadline_watch_live = false;
                    }
                }

                () = tokio::time::sleep_until(deadline.unwrap_or_else(Instant::now)), if deadline.is_some() => {
                    self.sweep(&mut tasks, &mut waiting, &mut ledger).await;
                    break;
                }
            }
        }
        ledger.report
    }

    /// Which seal threshold, if any, the accumulated batch has crossed.
    fn seal_reason(&self, acc: &Accumulator) -> Option<FlushReason> {
        if acc.rows >= self.cfg.batch.max_rows {
            Some(FlushReason::Rows)
        } else if acc.bytes >= self.cfg.batch.max_bytes {
            Some(FlushReason::Bytes)
        } else {
            None
        }
    }

    /// Start writing the oldest waiting batch with a permit the caller has
    /// already taken.
    ///
    /// # Panics
    ///
    /// Panics when `waiting` is empty; both call sites guard on that.
    fn launch_waiting(
        &self,
        permit: tokio::sync::OwnedSemaphorePermit,
        waiting: &mut VecDeque<(u64, SealedBatch, Instant)>,
        tasks: &mut JoinSet<WriteDone>,
        breakers: &Arc<Mutex<BreakerSet>>,
        ledger: &mut Ledger,
    ) {
        let (this_seq, batch, queued_at) = waiting
            .pop_front()
            .expect("callers guard on a non-empty `waiting`");
        self.metrics.permit_waited(queued_at.elapsed());
        self.spawn_write(batch, this_seq, permit, tasks, breakers, &mut ledger.ids);
    }

    /// Deadline reached: abort every write still in flight and fail every
    /// batch this worker still holds, loudly. Their data replays after
    /// restart; at-least-once holds.
    async fn sweep(
        &self,
        tasks: &mut JoinSet<WriteDone>,
        waiting: &mut VecDeque<(u64, SealedBatch, Instant)>,
        ledger: &mut Ledger,
    ) {
        // An abort only lands at a yield point, so a writer blocking its thread
        // holds us here indefinitely and `SinkPool::drain`'s backstop would
        // force-abort this worker and lose its report.
        if tokio::time::timeout(ABORT_GRACE, tasks.shutdown())
            .await
            .is_err()
        {
            tracing::error!(
                shard = self.shard,
                grace = ?ABORT_GRACE,
                "sink write tasks did not abort within the grace period; abandoning without them"
            );
        }
        // Sealed but never spawned; their `Pending`s are swept below.
        waiting.clear();
        let stranded: Vec<u64> = ledger.pending.keys().copied().collect();
        for s in stranded {
            self.abandon(s, ledger);
        }
        ledger.ids.clear();
    }

    fn handle_join(
        &self,
        joined: Result<(tokio::task::Id, WriteDone), JoinError>,
        ledger: &mut Ledger,
    ) {
        match joined {
            Ok((id, WriteDone { seq, written })) => {
                ledger.ids.remove(&id);
                if written {
                    self.settle(seq, ledger);
                } else {
                    self.abandon(seq, ledger);
                }
            }
            Err(join_err) => {
                // A panicked write task loses its seq but not its task id, so
                // abandon exactly the batch that died.
                let id = join_err.id();
                tracing::error!(error = %join_err, "sink write task panicked");
                match ledger.ids.remove(&id) {
                    Some(seq) => self.abandon(seq, ledger),
                    None => tracing::error!(
                        "panicked sink task had no ledger entry; batch already resolved"
                    ),
                }
            }
        }
    }

    fn settle(&self, seq: u64, ledger: &mut Ledger) {
        let Some(p) = ledger.pending.remove(&seq) else {
            return;
        };
        self.metrics
            .flushed(p.reason, p.rows, p.bytes, p.started.elapsed());
        self.metrics
            .e2e_observed(p.oldest_ingest.elapsed(), p.oldest_event_ms);
        self.budget
            .sub(usize::try_from(p.bytes).unwrap_or(usize::MAX));
        self.metrics.set_inflight(ledger.pending.len());
        ledger.report.flushed += 1;
        p.acks.deliver();
    }

    fn abandon(&self, seq: u64, ledger: &mut Ledger) {
        let Some(p) = ledger.pending.remove(&seq) else {
            return;
        };
        tracing::error!(
            rows = p.rows,
            bytes = p.bytes,
            "abandoning sink batch; data will replay after restart"
        );
        drop(p.acks); // AckSet drop resolution: Failed
        self.metrics.abandoned(1);
        self.budget
            .sub(usize::try_from(p.bytes).unwrap_or(usize::MAX));
        self.metrics.set_inflight(ledger.pending.len());
        ledger.report.abandoned += 1;
    }

    /// Assign a seq to the accumulated batch, seal it, and register its
    /// `Pending` in the ledger. Registering before the batch has a permit is
    /// what lets the deadline sweep abandon a batch parked in `waiting`
    /// without a special case of its own.
    fn seal(
        &self,
        acc: &mut Accumulator,
        reason: FlushReason,
        seq: &mut u64,
        ledger: &mut Ledger,
    ) -> (u64, SealedBatch) {
        let this_seq = *seq;
        *seq += 1;
        let full = std::mem::replace(acc, Accumulator::new());
        let batch = SealedBatch {
            frames: full.frames,
            rows: full.rows,
            bytes: full.bytes,
            dedup_token: format!("{}{}", self.token_prefix, this_seq),
        };
        ledger.pending.insert(
            this_seq,
            Pending {
                acks: full.acks,
                rows: full.rows,
                bytes: full.bytes,
                reason,
                started: Instant::now(),
                oldest_ingest: full.oldest_ingest.unwrap_or_else(std::time::Instant::now),
                oldest_event_ms: full.oldest_event_ms,
            },
        );
        self.metrics.set_inflight(ledger.pending.len());
        (this_seq, batch)
    }

    /// Spawn the write task for a sealed batch and record its task id, so a
    /// panic (whose `JoinError` carries only the id) abandons exactly this
    /// batch. The `permit` is held for the task's lifetime and releases on
    /// completion or abort.
    #[allow(clippy::too_many_arguments)]
    fn spawn_write(
        &self,
        batch: SealedBatch,
        this_seq: u64,
        permit: tokio::sync::OwnedSemaphorePermit,
        tasks: &mut JoinSet<WriteDone>,
        breakers: &Arc<Mutex<BreakerSet>>,
        ids: &mut HashMap<tokio::task::Id, u64>,
    ) {
        let writer = Arc::clone(&self.writer);
        let endpoints = Arc::clone(&self.endpoints);
        let breakers = Arc::clone(breakers);
        let metrics = Arc::clone(&self.metrics);
        let retry = self.cfg.retry;
        let backstop = quarantine_backstop(self.cfg.breaker.open_for);
        let shard = self.shard;
        let handle = tasks.spawn(async move {
            let _permit = permit;
            let mut backoff = Backoff::new(retry, this_seq);
            let mut attempts: u32 = 0;
            loop {
                // Pick a replica. "Nothing usable" is two conditions that end
                // differently; see the `breaker` module header. `backoff` stays
                // untouched: a quarantine wait is not a retry, and stepping it
                // climbs towards `retry.max` while quarantined, then serves that
                // ceiling to the first retry after recovery (#23). Waiting is
                // not an attempt either, so a quarantine of any length cannot
                // exhaust `retry.max_attempts`.
                let pick = loop {
                    // One timestamp for the whole critical section, which
                    // `next_replica` also promotes deadlines against. Reading
                    // the clock twice can hand `sleep_until` an instant already
                    // gone, which with no backoff step underneath it is a spin.
                    let now = Instant::now();
                    let (picked, probe_at) = {
                        let mut b = breakers.lock().expect("breaker lock");
                        let probe_at = b.next_probe_at(now);
                        let picked = match b.next_replica(now) {
                            Some(p) => Picked::Replica(p),
                            // Subscribed *inside* the section that found nothing
                            // pickable, so the cursor is that state's version
                            // and a later publisher's bump wakes it. Do not
                            // carry a receiver across iterations instead; that
                            // makes correctness depend on where the re-arm sits.
                            None => Picked::Park(b.subscribe()),
                        };
                        (picked, probe_at)
                    };
                    match picked {
                        Picked::Replica(p) => break p,
                        Picked::Park(mut wake) => {
                            // `min`, so the heartbeat still fires when a probe
                            // window is known but further away.
                            let heartbeat = now + backstop;
                            let until = probe_at.map_or(heartbeat, |t| t.min(heartbeat));
                            tokio::select! {
                                biased;

                                // Serving the deadline on `Err` costs a stall
                                // rather than a spinning core if the sender
                                // ever stops outliving this receiver.
                                changed = wake.changed() => {
                                    if changed.is_err() {
                                        tokio::time::sleep_until(until).await;
                                    }
                                }

                                () = tokio::time::sleep_until(until) => {}
                            }
                        }
                    }
                };
                let replica = pick.replica;
                // Armed only for a half-open probe, disarmed on the first
                // outcome. It covers the task dying with no outcome at all (a
                // writer panic), which would consume the probe slot for good.
                let mut probe = pick
                    .probe
                    .map(|episode| ProbeGuard::new(&breakers, replica, episode));

                attempts += 1;
                // Timed around `write_batch` alone: the replica pick, the probe
                // wait and the backoff sleep sit outside it, so this histogram
                // measures the sink's own round-trip. `flush_duration` does not.
                let attempt_at = Instant::now();
                let outcome = writer.write_batch(&endpoints[replica], &batch).await;
                metrics.write_attempt(
                    if outcome.is_ok() {
                        AttemptOutcome::Ok
                    } else {
                        AttemptOutcome::Error
                    },
                    attempt_at.elapsed(),
                );
                // Reporting clears the budget, so the guard has nothing left to
                // hand back. Disarming keeps the release harmless if an outcome
                // path ever leaves the replica half-open, where a second
                // decrement would admit a probe over budget. No `.await`
                // between here and the report.
                if let Some(g) = probe.as_mut() {
                    g.disarm();
                }
                match outcome {
                    Ok(()) => {
                        let transition = breakers.lock().expect("breaker lock").on_success(replica);
                        if let Some(t) = transition {
                            t.log(shard);
                        }
                        return WriteDone {
                            seq: this_seq,
                            written: true,
                        };
                    }
                    Err(err) => {
                        let transition = breakers
                            .lock()
                            .expect("breaker lock")
                            .on_failure(replica, Instant::now());
                        if let Some(t) = transition {
                            t.log(shard);
                        }
                        let class = class_of(&err);
                        metrics.errors(class, 1);
                        metrics.replica_error(replica);
                        tracing::warn!(replica, attempts, error = %err, "sink write failed");
                        if class != ErrorClass::Retryable
                            || (retry.max_attempts > 0 && attempts >= retry.max_attempts)
                        {
                            return WriteDone {
                                seq: this_seq,
                                written: false,
                            };
                        }
                        metrics.retries(1);
                        // The guard publishes the step for the length of the
                        // sleep and withdraws it on drop, including when the
                        // drain deadline aborts this task mid-sleep.
                        let delay = backoff.next_delay();
                        {
                            let _backoff = metrics.backing_off(this_seq, delay);
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
            }
        });
        ids.insert(handle.id(), this_seq);
    }

    /// Seal the accumulated batch and start writing it, or park it in
    /// `waiting` for the permit arm of whichever loop is running when the
    /// in-flight window is full.
    ///
    /// **Deliberately not `async`.** Blocking on the semaphore here would
    /// suspend `run` outside both of its `select!`s, where neither the drain
    /// deadline nor finished write tasks are polled; with every permit held by
    /// a write that does not return, nothing would ever wake it and shutdown
    /// would deadlock (#83). The permit wait belongs in a `select!` arm, and
    /// the arm's guard on `waiting` reproduces the backpressure a blocking
    /// acquire would provide.
    ///
    /// The permit wait is timed because `flush_duration` folds it in alongside
    /// the sink's own speed and our sleeps. A shard queueing behind its
    /// in-flight cap and one talking to a slow server produce the same flush
    /// histogram, so a healthy cluster can read as a slow one.
    #[allow(clippy::too_many_arguments)]
    fn dispatch(
        &self,
        acc: &mut Accumulator,
        reason: FlushReason,
        seq: &mut u64,
        ledger: &mut Ledger,
        tasks: &mut JoinSet<WriteDone>,
        semaphore: &Arc<Semaphore>,
        breakers: &Arc<Mutex<BreakerSet>>,
        waiting: &mut VecDeque<(u64, SealedBatch, Instant)>,
    ) {
        if acc.is_empty() {
            return;
        }
        let (this_seq, batch) = self.seal(acc, reason, seq, ledger);
        let queued_at = Instant::now();
        // Strict FIFO. Overtaking a parked batch is harmless for delivery (a
        // shard's batches already settle out of order) but charges the
        // overtaken batch's `permit_wait` for a queue it led.
        if waiting.is_empty()
            && let Ok(permit) = Arc::clone(semaphore).try_acquire_owned()
        {
            self.metrics.permit_waited(queued_at.elapsed());
            self.spawn_write(batch, this_seq, permit, tasks, breakers, &mut ledger.ids);
        } else {
            waiting.push_back((this_seq, batch, queued_at));
        }
    }
}

/// The outcome of one trip round the replica picker: either a replica to write
/// to, or the wake receiver to park on. Pairing them in one value makes "a
/// receiver is taken exactly when nothing was pickable, in the same critical
/// section" a property of the type.
enum Picked {
    Replica(super::breaker::Pick),
    Park(watch::Receiver<u64>),
}

/// Holds one half-open probe slot for the length of a write attempt, and hands
/// it back if the attempt never reports an outcome.
///
/// `probes_in_flight` is otherwise cleared only by *leaving* `HalfOpen`, which
/// is what reporting an outcome does. A write task that dies without reporting
/// (a writer panic, whose `JoinError` reaches `handle_join` carrying a task id
/// and no replica) would consume the slot for good. With the default
/// `half_open_probes: 1` that pins the replica in `HalfOpen` forever, leaving
/// the shard unwritable for the life of the process; re-picking does not repair
/// it, because the budget is gone.
///
/// The guard names the half-open *episode* it took its slot from, so a drop
/// that arrives after the replica has re-opened and started probing again is
/// discarded rather than granting that later run an extra concurrent probe.
/// The write loop still disarms on the reporting path (see the comment there),
/// because the episode check alone would credit a same-episode release.
///
/// Note what the episode does *not* bound. It stops a late release crediting
/// the wrong run; it does not stop the runs themselves overlapping. A failure
/// reported by an attempt that started before the quarantine re-opens the
/// replica and ends the episode, so a probe still in flight from that episode
/// can overlap the next one's. The budget is per episode, not per endpoint.
struct ProbeGuard {
    breakers: Arc<Mutex<BreakerSet>>,
    replica: usize,
    episode: u64,
    armed: bool,
}

impl ProbeGuard {
    fn new(breakers: &Arc<Mutex<BreakerSet>>, replica: usize, episode: u64) -> Self {
        ProbeGuard {
            breakers: Arc::clone(breakers),
            replica,
            episode,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ProbeGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Poison-tolerant, because this guard covers a writer panic: an
        // `expect` here would run while already unwinding and abort the process
        // instead of releasing the slot. The section only decrements a counter.
        self.breakers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .release_probe(self.replica, self.episode);
    }
}

fn class_of(err: &SinkError) -> ErrorClass {
    match err {
        SinkError::Client { class, .. } => *class,
        // Non-exhaustive enum: unknown variants are conservatively fatal.
        #[allow(unreachable_patterns)]
        _ => ErrorClass::Fatal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The heartbeat is driven by an operator-set value on a struct with no
    /// validator, so both ends have to be defended. A zero would make
    /// `sleep_until` return instantly and spin the pick loop on the breaker
    /// mutex; an hour would park a sealed batch for an hour behind a signal
    /// that never came; `Duration::MAX` would overflow `Instant::add`.
    #[test]
    fn the_quarantine_heartbeat_is_clamped_at_both_ends() {
        assert_eq!(
            quarantine_backstop(Duration::ZERO),
            QUARANTINE_BACKSTOP_MIN,
            "a zero heartbeat is a spin, not a wait"
        );
        assert_eq!(
            quarantine_backstop(Duration::from_secs(5)),
            Duration::from_secs(5),
            "the default is inside the range, so the clamp is the identity"
        );
        assert_eq!(
            quarantine_backstop(Duration::from_secs(3600)),
            QUARANTINE_BACKSTOP_MAX
        );
        assert_eq!(quarantine_backstop(Duration::MAX), QUARANTINE_BACKSTOP_MAX);
    }
}
