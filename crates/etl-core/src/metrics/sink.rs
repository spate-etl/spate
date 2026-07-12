//! Sink-shard handles (`etl_sink_*`), one struct per shard worker, plus the
//! end-to-end latency histogram observed at the terminal stage.

use super::E2eBasis;
use super::labels::ComponentLabels;
use super::names;
use crate::error::ErrorClass;
use metrics::{Counter, Gauge, Histogram, SharedString};
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

/// Per-replica handles inside one shard.
#[derive(Debug)]
struct ReplicaMetrics {
    healthy: Gauge,
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
    retries: Counter,
    err_retryable: Counter,
    err_record: Counter,
    err_fatal: Counter,
    inflight: Gauge,
    abandoned: Counter,
    shard_healthy: Gauge,
    e2e: Histogram,
    e2e_basis: E2eBasis,
    replicas: Vec<ReplicaMetrics>,
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
    pub fn new(
        labels: &ComponentLabels,
        shard: u32,
        replicas: &[String],
        e2e_basis: E2eBasis,
    ) -> Self {
        let shard: SharedString = shard.to_string().into();
        let replicas = replicas
            .iter()
            .map(|replica| {
                let m = ReplicaMetrics {
                    healthy: labels.gauge2(
                        names::SINK_REPLICA_HEALTHY,
                        names::L_SHARD,
                        shard.clone(),
                        names::L_REPLICA,
                        replica.clone(),
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
        let shard_healthy = labels.gauge1(names::SINK_SHARD_HEALTHY, names::L_SHARD, shard.clone());
        shard_healthy.set(1.0);
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
            retries: labels.counter1(names::SINK_RETRIES_TOTAL, names::L_SHARD, shard.clone()),
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
            inflight: labels.gauge1(names::SINK_INFLIGHT_BATCHES, names::L_SHARD, shard.clone()),
            abandoned: labels.counter1(names::SINK_ABANDONED_BATCHES_TOTAL, names::L_SHARD, shard),
            shard_healthy,
            e2e: labels.histogram(names::E2E_LATENCY_SECONDS),
            e2e_basis,
            replicas,
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

    /// Count flush attempts beyond the first.
    #[inline]
    pub fn retries(&self, n: u64) {
        self.retries.increment(n);
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
    /// Level-set and idempotent — safe to call redundantly; the shard's
    /// breaker set drives it on transitions.
    pub fn set_shard_healthy(&self, up: bool) {
        self.shard_healthy.set(if up { 1.0 } else { 0.0 });
    }

    /// Count batches abandoned at the drain deadline.
    pub fn abandoned(&self, n: u64) {
        self.abandoned.increment(n);
    }
}
