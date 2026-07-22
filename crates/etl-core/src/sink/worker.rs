//! The per-shard worker: merge chunks into big batches, seal, write with
//! replica rotation and retries, resolve acknowledgements.
//!
//! Acknowledgement handles never enter a write task: they stay in the
//! worker's pending map and are resolved from task *outcomes*. This makes
//! aborting write tasks at the drain deadline safe — an aborted task can
//! never accidentally resolve a batch as delivered.
//!
//! Batches of one shard may complete out of order across the `max_inflight`
//! window; the checkpointer's contiguity tracker absorbs this by design.

use super::breaker::BreakerSet;
use super::config::SinkPoolConfig;
use super::retry::Backoff;
use super::{EncodedChunk, SealedBatch, ShardWriter};
use crate::backpressure::InflightBudget;
use crate::checkpoint::AckSet;
use crate::error::{ErrorClass, SinkError};
use crate::metrics::{FlushReason, SinkShardMetrics};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::{JoinError, JoinSet};
use tokio::time::Instant;

/// What one shard worker did over its lifetime, for the drain report.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct WorkerReport {
    /// Batches durably written.
    pub(crate) flushed: u64,
    /// Batches abandoned (acknowledgements failed; data replays after
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
/// acknowledgements and bookkeeping once its write task reports.
struct Pending {
    acks: AckSet,
    rows: u64,
    bytes: u64,
    reason: FlushReason,
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
    /// real victim's acks and budget forever).
    ids: HashMap<tokio::task::Id, u64>,
    report: WorkerReport,
}

impl<W: ShardWriter> ShardWorker<W> {
    pub(crate) async fn run(mut self) -> WorkerReport {
        let mut acc = Accumulator::new();
        let mut ledger = Ledger {
            pending: HashMap::new(),
            ids: HashMap::new(),
            report: WorkerReport::default(),
        };
        let mut tasks: JoinSet<WriteDone> = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(self.cfg.inflight.max_per_shard));
        let breakers = Arc::new(Mutex::new(BreakerSet::new(
            self.endpoints.len(),
            self.cfg.breaker,
            Arc::clone(&self.metrics),
        )));
        // A private clone of the deadline watch. Both loops select on its
        // `changed()`: the intake loop breaks into drain the moment a
        // deadline is published even though the queue is still open (a
        // chunk sender leaked past shutdown would otherwise keep
        // `recv_many` from ever closing and wedge shutdown unboundedly),
        // and the drain loop observes a deadline published *after* it
        // parked on a hung write.
        let mut drain_deadline = self.drain_deadline.clone();
        // Once the deadline-watch sender drops, `changed()` errors forever;
        // disable that branch after the first error so neither select can
        // busy-spin on it.
        let mut deadline_watch_live = true;
        let mut seq: u64 = 0;
        let mut recv_buf: Vec<EncodedChunk> = Vec::with_capacity(64);

        loop {
            let linger_at = acc.first_at.map(|t| t + self.cfg.batch.linger);
            tokio::select! {
                biased;

                Some(joined) = tasks.join_next_with_id(), if !tasks.is_empty() => {
                    self.handle_join(joined, &mut ledger);
                }

                n = self.rx.recv_many(&mut recv_buf, 64) => {
                    if n == 0 {
                        break; // intake closed: drain
                    }
                    let now = Instant::now();
                    for chunk in recv_buf.drain(..) {
                        acc.push(chunk, now);
                        let reason = if acc.rows >= self.cfg.batch.max_rows {
                            Some(FlushReason::Rows)
                        } else if acc.bytes >= self.cfg.batch.max_bytes {
                            Some(FlushReason::Bytes)
                        } else {
                            None
                        };
                        if let Some(reason) = reason {
                            self.dispatch(&mut acc, reason, &mut seq, &mut ledger, &mut tasks, &semaphore, &breakers)
                                .await;
                        }
                    }
                }

                // Below `recv_many` on purpose: chunks already queued at
                // shutdown are consumed before the deadline is considered
                // (biased select polls in order), so a graceful drain still
                // flushes everything the drivers handed over.
                changed = drain_deadline.changed(), if deadline_watch_live => {
                    match changed {
                        // A deadline published while intake is still open:
                        // a chunk sender leaked past shutdown. Enter the
                        // bounded drain instead of waiting for a close that
                        // may never come; the deadline sweep below abandons
                        // whatever cannot finish in time.
                        Ok(()) if drain_deadline.borrow().is_some() => break,
                        Ok(()) => {}
                        // Sender gone: no drain will ever publish a deadline.
                        Err(_) => deadline_watch_live = false,
                    }
                }

                () = tokio::time::sleep_until(linger_at.unwrap_or_else(Instant::now)), if linger_at.is_some() => {
                    self.dispatch(&mut acc, FlushReason::Linger, &mut seq, &mut ledger, &mut tasks, &semaphore, &breakers)
                        .await;
                }
            }
        }

        // ---- Drain (DESIGN.md § Shutdown) ----
        // Force-seal the partial batch, then resolve in-flight writes under
        // the drain deadline. The force-seal must NOT block on the in-flight
        // semaphore: with every permit held by a hung write it would park
        // here — before the deadline loop is reached — and deadlock
        // shutdown. Instead the sealed batch waits in `waiting` (its
        // `Pending` already registered in the ledger) and is launched as
        // soon as a permit frees; the deadline sweep abandons it like any
        // other pending batch, dropping the sealed frames unspawned.
        let mut waiting: Option<(u64, SealedBatch)> = None;
        if !acc.is_empty() {
            let (this_seq, batch) = self.seal(&mut acc, FlushReason::Drain, &mut seq, &mut ledger);
            match Arc::clone(&semaphore).try_acquire_owned() {
                Ok(permit) => {
                    self.spawn_write(
                        batch,
                        this_seq,
                        permit,
                        &mut tasks,
                        &breakers,
                        &mut ledger.ids,
                    );
                }
                Err(_) => waiting = Some((this_seq, batch)),
            }
        }

        loop {
            // Launch the waiting batch the moment a permit is available.
            if waiting.is_some()
                && let Ok(permit) = Arc::clone(&semaphore).try_acquire_owned()
            {
                let (this_seq, batch) = waiting.take().expect("just checked is_some");
                self.spawn_write(
                    batch,
                    this_seq,
                    permit,
                    &mut tasks,
                    &breakers,
                    &mut ledger.ids,
                );
            }
            if tasks.is_empty() && waiting.is_none() {
                break;
            }

            let deadline = *drain_deadline.borrow();
            tokio::select! {
                biased;

                Some(joined) = tasks.join_next_with_id(), if !tasks.is_empty() => {
                    self.handle_join(joined, &mut ledger);
                }

                changed = drain_deadline.changed(), if deadline_watch_live => {
                    if changed.is_err() {
                        // Sender gone: no drain will ever publish a deadline.
                        deadline_watch_live = false;
                    }
                }

                () = tokio::time::sleep_until(deadline.unwrap_or_else(Instant::now)), if deadline.is_some() => {
                    // Deadline exceeded: abort every write still in flight and
                    // fail those batches loudly. Their data replays after
                    // restart — at-least-once holds.
                    tasks.shutdown().await;
                    drop(waiting.take()); // unspawned sealed batch; its Pending is swept below
                    let stranded: Vec<u64> = ledger.pending.keys().copied().collect();
                    for s in stranded {
                        self.abandon(s, &mut ledger);
                    }
                    ledger.ids.clear();
                    break;
                }
            }
        }
        ledger.report
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
                // A panicked write task (writer bug). Its seq is lost with
                // it, but its task id is not: abandon exactly the batch that
                // died so the healthy in-flight batch keeps its acks.
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
    /// `Pending` in the ledger. Shared by the normal dispatch path and the
    /// drain force-seal; the caller then obtains a permit (blocking on the
    /// normal path, non-blocking during drain) and calls [`spawn_write`].
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
        let shard = self.shard;
        let handle = tasks.spawn(async move {
            let _permit = permit;
            let mut backoff = Backoff::new(retry, this_seq);
            let mut attempts: u32 = 0;
            loop {
                // Pick a replica; while every breaker is open, wait for the
                // earliest probe window (or one backoff step) and re-pick.
                let replica = loop {
                    let now = Instant::now();
                    let (pick, probe_at) = {
                        let mut b = breakers.lock().expect("breaker lock");
                        (b.next_replica(now), b.next_probe_at(now))
                    };
                    match pick {
                        Some(r) => break r,
                        None => {
                            let wake = probe_at.unwrap_or_else(|| now + backoff.next_delay());
                            tokio::time::sleep_until(wake).await;
                        }
                    }
                };

                attempts += 1;
                match writer.write_batch(&endpoints[replica], &batch).await {
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
                        // sleep and withdraws it on drop — which includes the
                        // drain deadline aborting this task mid-sleep, the one
                        // exit that runs no code of ours.
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

    #[allow(clippy::too_many_arguments)]
    async fn dispatch(
        &self,
        acc: &mut Accumulator,
        reason: FlushReason,
        seq: &mut u64,
        ledger: &mut Ledger,
        tasks: &mut JoinSet<WriteDone>,
        semaphore: &Arc<Semaphore>,
        breakers: &Arc<Mutex<BreakerSet>>,
    ) {
        if acc.is_empty() {
            return;
        }
        let (this_seq, batch) = self.seal(acc, reason, seq, ledger);

        // Waiting for a permit intentionally stops intake: the shard queue
        // fills and backpressure propagates. Permits release on task end,
        // including aborts. (This is backpressure by design — the drain
        // phase, which cannot afford to block, uses `try_acquire_owned`.)
        let permit = Arc::clone(semaphore)
            .acquire_owned()
            .await
            .expect("sink semaphore closed");
        self.spawn_write(batch, this_seq, permit, tasks, breakers, &mut ledger.ids);
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
