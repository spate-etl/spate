//! Sink-shard handles (`etl_sink_*`), one struct per shard worker, plus the
//! end-to-end latency histogram observed at the terminal stage.

use super::labels::{ComponentLabels, OwnedGauge};
use super::names;
use super::ownership::{SeriesClaim, series_key};
use super::{E2eBasis, MetricsError};
use crate::error::ErrorClass;
use metrics::{Counter, Histogram, SharedString};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

/// Why a sink batch was sealed and flushed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlushReason {
    /// `max_rows` reached.
    Rows,
    /// `max_bytes` reached.
    Bytes,
    /// Linger deadline expired.
    Linger,
    /// Drain (shutdown or revocation) forced the seal.
    Drain,
}

impl FlushReason {
    fn label(self) -> &'static str {
        match self {
            FlushReason::Rows => "rows",
            FlushReason::Bytes => "bytes",
            FlushReason::Linger => "linger",
            FlushReason::Drain => "drain",
        }
    }
}

/// Outcome of one sink write attempt (the `outcome` label on
/// `etl_sink_write_duration_seconds`).
///
/// One family with a label rather than two names, which is the opposite of
/// the call made for the two coordination latencies — the distinction is
/// that those measure different things on different clocks with different
/// denominators, so a shared family would assert a composition that does not
/// exist. These two are the same measurement (time inside `write_batch`)
/// over the same population (attempts), so the aggregate is well-defined; it
/// is merely the wrong *diagnostic*, which a label documents and a split name
/// would over-state. It also matches `outcome` on the three counter families
/// that already use it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum AttemptOutcome {
    /// The write was accepted.
    Ok,
    /// The write failed; its taxonomy class goes to
    /// [`SinkShardMetrics::errors`].
    Error,
}

/// Per-replica handles inside one shard.
#[derive(Debug)]
struct ReplicaMetrics {
    healthy: OwnedGauge,
    breaker_opens: Counter,
    errors: Counter,
}

/// Sink-shard handles (`etl_sink_*`), one struct per shard worker.
#[derive(Debug)]
pub struct SinkShardMetrics {
    records: Counter,
    bytes: Counter,
    batch_rows: Histogram,
    batch_bytes: Histogram,
    flush_rows: Counter,
    flush_bytes: Counter,
    flush_linger: Counter,
    flush_drain: Counter,
    flush_duration: Histogram,
    write_ok: Histogram,
    write_err: Histogram,
    permit_wait: Histogram,
    retries: Counter,
    retry_backoff: OwnedGauge,
    /// Current backoff step, in seconds, of every batch of this shard that is
    /// sleeping between write attempts, keyed by batch sequence number. The
    /// gauge publishes the max; the map is bounded by `inflight.max_per_shard`
    /// and empties back to nothing whenever the shard stops backing off.
    backoff_steps: Mutex<HashMap<u64, f64>>,
    err_retryable: Counter,
    err_record: Counter,
    err_fatal: Counter,
    inflight: OwnedGauge,
    abandoned: Counter,
    shard_healthy: OwnedGauge,
    e2e: Histogram,
    e2e_basis: E2eBasis,
    replicas: Vec<ReplicaMetrics>,
    _claim: Option<SeriesClaim>,
}

impl SinkShardMetrics {
    /// Resolve all handles for one shard. `replicas` are display names used
    /// as the `replica` label (bounded by cluster topology). `e2e_basis`
    /// selects the time base for `etl_e2e_latency_seconds` (see
    /// `docs/METRICS.md`).
    ///
    /// Call **after** [`install`](crate::metrics::install): handles bind to
    /// the recorder present at construction, and a handle built before the
    /// exporter exists silently records into the void.
    ///
    /// Claims this shard's series — the labels plus `shard` — so that only one
    /// live handle set publishes them. The gauges here are edge-triggered
    /// (health flips on a breaker transition, backoff on a retry), so a second
    /// writer's reading would stand until the owner's next transition, which
    /// for a quarantined shard may be never. A collision therefore logs and
    /// leaves this instance a shadow: its counters still record, its gauges do
    /// not (see "Series ownership" in `docs/METRICS.md`). Assembly through
    /// [`Pipeline`](crate::pipeline::Pipeline) refuses to build instead.
    pub fn new(
        labels: &ComponentLabels,
        shard: u32,
        replicas: &[String],
        e2e_basis: E2eBasis,
    ) -> Self {
        let claim = SeriesClaim::claim_or_shadow(Self::key(labels, shard));
        Self::build(labels, shard, replicas, e2e_basis, claim)
    }

    /// Resolve all handles for one shard, failing when another live handle set
    /// already owns the shard's series. The pipeline builder's path.
    ///
    /// # Errors
    ///
    /// [`MetricsError::DuplicateSeries`] on a collision.
    pub fn try_new(
        labels: &ComponentLabels,
        shard: u32,
        replicas: &[String],
        e2e_basis: E2eBasis,
    ) -> Result<Self, MetricsError> {
        let claim = SeriesClaim::try_claim(Self::key(labels, shard))?;
        Ok(Self::build(labels, shard, replicas, e2e_basis, Some(claim)))
    }

    fn key(labels: &ComponentLabels, shard: u32) -> String {
        series_key("sink", labels, &format!("shard={shard}"))
    }

    fn build(
        labels: &ComponentLabels,
        shard: u32,
        replicas: &[String],
        e2e_basis: E2eBasis,
        claim: Option<SeriesClaim>,
    ) -> Self {
        // Resolved before any handle is written: the initial publishes below
        // are exactly the writes that would clobber a live owner's reading.
        let owned = claim.is_some();
        let shard: SharedString = shard.to_string().into();
        let replicas = replicas
            .iter()
            .map(|replica| {
                let m = ReplicaMetrics {
                    healthy: OwnedGauge::new(
                        labels.gauge2(
                            names::SINK_REPLICA_HEALTHY,
                            names::L_SHARD,
                            shard.clone(),
                            names::L_REPLICA,
                            replica.clone(),
                        ),
                        owned,
                    ),
                    breaker_opens: labels.counter2(
                        names::SINK_BREAKER_OPENS_TOTAL,
                        names::L_SHARD,
                        shard.clone(),
                        names::L_REPLICA,
                        replica.clone(),
                    ),
                    errors: labels.counter2(
                        names::SINK_REPLICA_ERRORS_TOTAL,
                        names::L_SHARD,
                        shard.clone(),
                        names::L_REPLICA,
                        replica.clone(),
                    ),
                };
                m.healthy.set(1.0);
                m
            })
            .collect();
        let shard_healthy = OwnedGauge::new(
            labels.gauge1(names::SINK_SHARD_HEALTHY, names::L_SHARD, shard.clone()),
            owned,
        );
        shard_healthy.set(1.0);
        // Published as `0` from construction rather than left absent until the
        // first retry: "this shard is not backing off" is true of a shard that
        // has never written, so there is no measurement to wait for. (Contrast
        // `etl_source_lag_records`, where absence carries information — see the
        // "Absent, zero, and stale" section of `docs/METRICS.md`.)
        let retry_backoff = OwnedGauge::new(
            labels.gauge1(
                names::SINK_RETRY_BACKOFF_SECONDS,
                names::L_SHARD,
                shard.clone(),
            ),
            owned,
        );
        retry_backoff.set(0.0);
        SinkShardMetrics {
            records: labels.counter1(names::SINK_RECORDS_TOTAL, names::L_SHARD, shard.clone()),
            bytes: labels.counter1(names::SINK_BYTES_TOTAL, names::L_SHARD, shard.clone()),
            batch_rows: labels.histogram(names::SINK_BATCH_ROWS),
            batch_bytes: labels.histogram(names::SINK_BATCH_BYTES),
            flush_rows: labels.counter2(
                names::SINK_FLUSHES_TOTAL,
                names::L_SHARD,
                shard.clone(),
                names::L_REASON,
                FlushReason::Rows.label(),
            ),
            flush_bytes: labels.counter2(
                names::SINK_FLUSHES_TOTAL,
                names::L_SHARD,
                shard.clone(),
                names::L_REASON,
                FlushReason::Bytes.label(),
            ),
            flush_linger: labels.counter2(
                names::SINK_FLUSHES_TOTAL,
                names::L_SHARD,
                shard.clone(),
                names::L_REASON,
                FlushReason::Linger.label(),
            ),
            flush_drain: labels.counter2(
                names::SINK_FLUSHES_TOTAL,
                names::L_SHARD,
                shard.clone(),
                names::L_REASON,
                FlushReason::Drain.label(),
            ),
            flush_duration: labels.histogram1(
                names::SINK_FLUSH_DURATION_SECONDS,
                names::L_SHARD,
                shard.clone(),
            ),
            write_ok: labels.histogram2(
                names::SINK_WRITE_DURATION_SECONDS,
                names::L_SHARD,
                shard.clone(),
                names::L_OUTCOME,
                "ok",
            ),
            write_err: labels.histogram2(
                names::SINK_WRITE_DURATION_SECONDS,
                names::L_SHARD,
                shard.clone(),
                names::L_OUTCOME,
                "error",
            ),
            permit_wait: labels.histogram1(
                names::SINK_PERMIT_WAIT_DURATION_SECONDS,
                names::L_SHARD,
                shard.clone(),
            ),
            retries: labels.counter1(names::SINK_RETRIES_TOTAL, names::L_SHARD, shard.clone()),
            retry_backoff,
            backoff_steps: Mutex::new(HashMap::new()),
            err_retryable: labels.counter2(
                names::SINK_ERRORS_TOTAL,
                names::L_SHARD,
                shard.clone(),
                names::L_ERROR_TYPE,
                ErrorClass::Retryable.label(),
            ),
            err_record: labels.counter2(
                names::SINK_ERRORS_TOTAL,
                names::L_SHARD,
                shard.clone(),
                names::L_ERROR_TYPE,
                ErrorClass::RecordLevel.label(),
            ),
            err_fatal: labels.counter2(
                names::SINK_ERRORS_TOTAL,
                names::L_SHARD,
                shard.clone(),
                names::L_ERROR_TYPE,
                ErrorClass::Fatal.label(),
            ),
            inflight: OwnedGauge::new(
                labels.gauge1(names::SINK_INFLIGHT_BATCHES, names::L_SHARD, shard.clone()),
                owned,
            ),
            abandoned: labels.counter1(names::SINK_ABANDONED_BATCHES_TOTAL, names::L_SHARD, shard),
            shard_healthy,
            e2e: labels.histogram(names::E2E_LATENCY_SECONDS),
            e2e_basis,
            replicas,
            _claim: claim,
        }
    }

    /// Observe end-to-end latency for one durably written batch, from its
    /// oldest record. `ingest_age` is time since that record entered the
    /// terminal stage; `oldest_event_ms` is its source event time. The
    /// configured basis picks which one lands in the histogram (event
    /// basis falls back to ingest when no event time was available).
    #[inline]
    pub fn e2e_observed(&self, ingest_age: Duration, oldest_event_ms: i64) {
        let latency = match self.e2e_basis {
            E2eBasis::Event if oldest_event_ms != i64::MAX => {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
                    .unwrap_or(0);
                Duration::from_millis(u64::try_from(now_ms - oldest_event_ms).unwrap_or(0))
            }
            _ => ingest_age,
        };
        self.e2e.record(latency.as_secs_f64());
    }

    /// Record one durably acknowledged flush.
    ///
    /// `d` is the batch's **seal-to-settle** time, and contains everything
    /// that stood between the two: the wait for an `inflight.max_per_shard`
    /// permit, every failed attempt, every retry-backoff sleep and
    /// all-replicas-quarantined probe wait, and the write that finally
    /// succeeded. It is the right input for a commit-lag budget and the wrong
    /// one for "how fast is the sink" — [`write_attempt`](Self::write_attempt)
    /// answers that, and [`permit_waited`](Self::permit_waited) the queueing
    /// share.
    ///
    /// Only settled batches are observed. An abandoned one never reaches
    /// here — whether it was aborted at the drain deadline, rejected with a
    /// fatal class, exhausted `retry.max_attempts`, or died with a panicking
    /// write task. All four are counted by [`abandoned`](Self::abandoned),
    /// and the last three happen in steady state with no drain in sight.
    #[inline]
    pub fn flushed(&self, reason: FlushReason, rows: u64, bytes: u64, d: Duration) {
        self.records.increment(rows);
        self.bytes.increment(bytes);
        self.batch_rows.record(rows as f64);
        self.batch_bytes.record(bytes as f64);
        self.flush_duration.record(d.as_secs_f64());
        match reason {
            FlushReason::Rows => self.flush_rows.increment(1),
            FlushReason::Bytes => self.flush_bytes.increment(1),
            FlushReason::Linger => self.flush_linger.increment(1),
            FlushReason::Drain => self.flush_drain.increment(1),
        }
    }

    /// Observe one write attempt: the time inside
    /// [`ShardWriter::write_batch`](crate::sink::ShardWriter::write_batch) and
    /// nothing else *of the framework's own*. Every attempt is observed,
    /// retries included, so this is the sink system's round-trip distribution
    /// — the signal `etl_sink_flush_duration_seconds` cannot give, because it
    /// also carries the permit wait and the sleeps between attempts.
    ///
    /// "Nothing else" is bounded by the writer's own implementation: a
    /// connector that sleeps *inside* `write_batch` puts that sleep in here.
    /// The Kafka sink does exactly this when the producer queue is full, and
    /// the wall-clock also charges whatever the sink's I/O runtime was busy
    /// with at each await point. What is excluded is the framework's
    /// scheduling around the call — the permit wait, the retry backoff, and
    /// the all-replicas-quarantined probe wait.
    ///
    /// `outcome` splits the family: a batch rejected fatally in a millisecond
    /// and one that times out after thirty seconds are both attempts, and
    /// mixing them moves the distribution in opposite directions. The error's
    /// taxonomy class stays on [`errors`](Self::errors).
    ///
    /// An attempt aborted at the drain deadline is never observed — the write
    /// task is dropped mid-call, and a histogram observation is a point event
    /// with nothing to strand (contrast
    /// [`backing_off`](Self::backing_off), whose guard exists precisely to
    /// survive that abort). Attempts that *completed* before the abort are
    /// observed as usual, so an abandoned batch can leave `error`
    /// observations here with no matching flush.
    #[inline]
    pub(crate) fn write_attempt(&self, outcome: AttemptOutcome, d: Duration) {
        let h = match outcome {
            AttemptOutcome::Ok => &self.write_ok,
            AttemptOutcome::Error => &self.write_err,
        };
        h.record(d.as_secs_f64());
    }

    /// Observe how long a sealed batch waited for one of its shard's
    /// `inflight.max_per_shard` slots before its first write attempt — the
    /// queueing share of a flush, and the reading that tells a healthy-but-slow
    /// dashboard apart from a saturated one.
    ///
    /// Observed for every sealed batch that starts a write, including the
    /// healthy case where the permit is free and the observation is ~0: a
    /// family that appeared only under contention would read as absent
    /// precisely when an operator wants to confirm there is none. A batch the
    /// drain deadline drops before it ever gets a permit is not observed
    /// (there is no wait that ended), and is counted by
    /// [`abandoned`](Self::abandoned).
    #[inline]
    pub(crate) fn permit_waited(&self, d: Duration) {
        self.permit_wait.record(d.as_secs_f64());
    }

    /// Count flush attempts beyond the first.
    #[inline]
    pub fn retries(&self, n: u64) {
        self.retries.increment(n);
    }

    /// Publish `delay` as `batch`'s current retry backoff step for as long as
    /// the returned guard lives.
    ///
    /// `etl_sink_retry_backoff_seconds` reads the **max** across the shard's
    /// backing-off batches (a shard writes up to `inflight.max_per_shard` of
    /// them at once, each with its own backoff), and `0` once none is — so it
    /// answers "how long is this shard currently sleeping between attempts",
    /// which no combination of the other sink series can.
    ///
    /// The value is the step being served, not the time left in it: it does
    /// not count down while the sleep runs.
    ///
    /// Scope: the sleep between attempts *on an available replica*. A shard
    /// whose every replica is quarantined also sleeps — waiting for the
    /// earliest probe window — and reads `0` throughout, because no attempt
    /// is being backed off. That state has its own signal, and the two are
    /// exactly coincident: the write loop waits for a probe precisely when
    /// no replica is circuit-closed, which is the definition of
    /// `etl_sink_shard_healthy == 0`.
    ///
    /// Clearing is tied to the guard's `Drop` rather than to a settle/abandon
    /// call because the sleeping task can be *aborted* — the sink's drain
    /// deadline cancels in-flight writes wherever they are parked. Dropping
    /// the task future drops the guard, so an abandoned batch cannot strand
    /// the gauge at a value the shard is no longer sleeping.
    ///
    /// # Panics
    ///
    /// Debug builds only: `batch` must be unique among this shard's *live*
    /// guards. Two live guards sharing a key collapse to one entry, and the
    /// first `Drop` withdraws both contributions — the gauge would then read
    /// `0` while the other sleep is still running. In-tree the key is the
    /// batch sequence number, which is monotonic per shard.
    #[must_use]
    pub fn backing_off(&self, batch: u64, delay: Duration) -> BackoffGuard<'_> {
        self.publish_backoff(|steps| {
            let previous = steps.insert(batch, delay.as_secs_f64());
            debug_assert!(
                previous.is_none(),
                "a live BackoffGuard already exists for batch {batch}"
            );
        });
        BackoffGuard {
            metrics: self,
            batch,
        }
    }

    /// Mutate the backing-off set and republish the max (`0` when empty).
    /// Called only from the retry path, never per record.
    fn publish_backoff(&self, mutate: impl FnOnce(&mut HashMap<u64, f64>)) {
        // Poison-tolerant because this also runs from `BackoffGuard::drop`:
        // a panicking `expect` there, reached while already unwinding, aborts
        // the process. The critical section only inserts, removes and folds,
        // so a poisoned map is not a corrupt one — recovering it publishes a
        // stale reading at worst.
        let mut steps = self
            .backoff_steps
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        mutate(&mut steps);
        let max = steps.values().copied().fold(0.0_f64, f64::max);
        // Published *under* the lock, deliberately. Releasing it first lets
        // two publishers' `set` calls land in the opposite order from the
        // snapshots they computed, stranding the gauge at a value no batch is
        // serving — until the next mutation, which is `retry.max` away under
        // a patient policy and never once the shard recovers. Two write tasks
        // per shard is the default (`inflight.max_per_shard: 2`) on a
        // multi-threaded I/O runtime, so this is the ordinary case, not a
        // corner one. `Gauge::set` is an atomic store that cannot re-enter
        // this function, so holding the lock across it cannot deadlock.
        self.retry_backoff.set(max);
    }

    /// Count write errors of one taxonomy class.
    #[inline]
    pub fn errors(&self, class: ErrorClass, n: u64) {
        match class {
            ErrorClass::Retryable => self.err_retryable.increment(n),
            ErrorClass::RecordLevel => self.err_record.increment(n),
            ErrorClass::Fatal => self.err_fatal.increment(n),
        }
    }

    /// Set the number of sealed batches currently in flight.
    #[inline]
    pub fn set_inflight(&self, batches: usize) {
        self.inflight.set(batches as f64);
    }

    /// Mark one replica healthy (circuit closed) or quarantined (open).
    pub fn set_replica_healthy(&self, replica: usize, healthy: bool) {
        if let Some(r) = self.replicas.get(replica) {
            r.healthy.set(if healthy { 1.0 } else { 0.0 });
        }
    }

    /// Count a circuit-breaker open transition on one replica.
    pub fn breaker_opened(&self, replica: usize) {
        if let Some(r) = self.replicas.get(replica) {
            r.breaker_opens.increment(1);
        }
    }

    /// Count one failed write attempt attributed to a replica.
    pub fn replica_error(&self, replica: usize) {
        if let Some(r) = self.replicas.get(replica) {
            r.errors.increment(1);
        }
    }

    /// Record whether the shard has at least one circuit-closed replica.
    /// Level-set and idempotent — the shard's breaker set republishes it on
    /// every write outcome, not only on a transition, so a reading that has
    /// gone stale corrects itself within one probe cycle.
    pub fn set_shard_healthy(&self, up: bool) {
        self.shard_healthy.set(if up { 1.0 } else { 0.0 });
    }

    /// Count batches abandoned at the drain deadline.
    pub fn abandoned(&self, n: u64) {
        self.abandoned.increment(n);
    }
}

/// One batch's contribution to `etl_sink_retry_backoff_seconds`, held for the
/// duration of a backoff sleep. Returned by
/// [`SinkShardMetrics::backing_off`]; dropping it — including by the write
/// task being aborted mid-sleep — withdraws this batch's step and republishes
/// the shard's max, `0` when it was the last one sleeping.
#[derive(Debug)]
pub struct BackoffGuard<'a> {
    metrics: &'a SinkShardMetrics,
    batch: u64,
}

impl Drop for BackoffGuard<'_> {
    fn drop(&mut self) {
        let batch = self.batch;
        self.metrics.publish_backoff(|steps| {
            steps.remove(&batch);
        });
    }
}
