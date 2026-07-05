//! Pre-registered metric handle structs, one per pipeline stage.
//!
//! Handles are resolved once at pipeline build time; the record loop only
//! touches `Counter`/`Gauge`/`Histogram` handles — never name or label
//! lookups. Methods take **per-batch aggregates**, enforcing the hot-path
//! counting discipline by API shape (see `docs/METRICS.md`).

use super::E2eBasis;
use super::names;
use crate::error::ErrorClass;
use crate::record::PartitionId;
use metrics::{Counter, Gauge, Histogram, SharedString, counter, gauge, histogram};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

/// The standard label set attached to every framework metric.
#[derive(Clone, Debug)]
pub struct ComponentLabels {
    /// Pipeline name.
    pub pipeline: SharedString,
    /// Component instance id from config/builder (e.g. `orders_kafka`).
    pub component: SharedString,
    /// Component implementation (e.g. `kafka`, `clickhouse`, `map`).
    pub component_type: SharedString,
}

impl ComponentLabels {
    /// Build the standard label set.
    pub fn new(
        pipeline: impl Into<SharedString>,
        component: impl Into<SharedString>,
        component_type: impl Into<SharedString>,
    ) -> Self {
        ComponentLabels {
            pipeline: pipeline.into(),
            component: component.into(),
            component_type: component_type.into(),
        }
    }

    fn counter(&self, name: &'static str) -> Counter {
        counter!(name,
            names::L_PIPELINE => self.pipeline.clone(),
            names::L_COMPONENT => self.component.clone(),
            names::L_COMPONENT_TYPE => self.component_type.clone(),
        )
    }

    fn counter1(&self, name: &'static str, k: &'static str, v: impl Into<SharedString>) -> Counter {
        counter!(name,
            names::L_PIPELINE => self.pipeline.clone(),
            names::L_COMPONENT => self.component.clone(),
            names::L_COMPONENT_TYPE => self.component_type.clone(),
            k => v.into(),
        )
    }

    fn counter2(
        &self,
        name: &'static str,
        k1: &'static str,
        v1: impl Into<SharedString>,
        k2: &'static str,
        v2: impl Into<SharedString>,
    ) -> Counter {
        counter!(name,
            names::L_PIPELINE => self.pipeline.clone(),
            names::L_COMPONENT => self.component.clone(),
            names::L_COMPONENT_TYPE => self.component_type.clone(),
            k1 => v1.into(),
            k2 => v2.into(),
        )
    }

    fn gauge(&self, name: &'static str) -> Gauge {
        gauge!(name,
            names::L_PIPELINE => self.pipeline.clone(),
            names::L_COMPONENT => self.component.clone(),
            names::L_COMPONENT_TYPE => self.component_type.clone(),
        )
    }

    fn gauge1(&self, name: &'static str, k: &'static str, v: impl Into<SharedString>) -> Gauge {
        gauge!(name,
            names::L_PIPELINE => self.pipeline.clone(),
            names::L_COMPONENT => self.component.clone(),
            names::L_COMPONENT_TYPE => self.component_type.clone(),
            k => v.into(),
        )
    }

    fn gauge2(
        &self,
        name: &'static str,
        k1: &'static str,
        v1: impl Into<SharedString>,
        k2: &'static str,
        v2: impl Into<SharedString>,
    ) -> Gauge {
        gauge!(name,
            names::L_PIPELINE => self.pipeline.clone(),
            names::L_COMPONENT => self.component.clone(),
            names::L_COMPONENT_TYPE => self.component_type.clone(),
            k1 => v1.into(),
            k2 => v2.into(),
        )
    }

    fn histogram(&self, name: &'static str) -> Histogram {
        histogram!(name,
            names::L_PIPELINE => self.pipeline.clone(),
            names::L_COMPONENT => self.component.clone(),
            names::L_COMPONENT_TYPE => self.component_type.clone(),
        )
    }

    fn histogram1(
        &self,
        name: &'static str,
        k: &'static str,
        v: impl Into<SharedString>,
    ) -> Histogram {
        histogram!(name,
            names::L_PIPELINE => self.pipeline.clone(),
            names::L_COMPONENT => self.component.clone(),
            names::L_COMPONENT_TYPE => self.component_type.clone(),
            k => v.into(),
        )
    }
}

impl ErrorClass {
    fn label(self) -> &'static str {
        match self {
            ErrorClass::Retryable => "retryable",
            ErrorClass::RecordLevel => "record_level",
            ErrorClass::Fatal => "fatal",
        }
    }
}

/// Dynamic per-partition gauge family, gated by `per_partition_detail`.
/// Registration happens on the control plane (rebalance/commit paths), so a
/// mutex is acceptable; the hot path never touches this.
#[derive(Debug)]
struct PartitionGauges {
    name: &'static str,
    labels: ComponentLabels,
    gauges: Mutex<HashMap<u32, Gauge>>,
}

impl PartitionGauges {
    fn set(&self, partition: PartitionId, value: f64) {
        let mut gauges = self.gauges.lock().expect("partition gauge lock");
        gauges
            .entry(partition.0)
            .or_insert_with(|| {
                self.labels
                    .gauge1(self.name, names::L_PARTITION, partition.0.to_string())
            })
            .set(value);
    }

    /// Drops handles for revoked partitions so they are no longer updated
    /// and don't accumulate across rebalances. The exporter may keep
    /// rendering the last value of a dropped series until its own idle
    /// timeout; that staleness is harmless and expected.
    fn retain(&self, keep: &[PartitionId]) {
        let mut gauges = self.gauges.lock().expect("partition gauge lock");
        gauges.retain(|p, _| keep.iter().any(|k| k.0 == *p));
    }
}

/// Source-stage handles (`etl_source_*`).
#[derive(Debug)]
pub struct SourceMetrics {
    records: Counter,
    bytes: Counter,
    poll_duration: Histogram,
    lag_max: Gauge,
    rebalance_assign: Counter,
    rebalance_revoke: Counter,
    lanes_active: Gauge,
    partition_lag: Option<PartitionGauges>,
}

impl SourceMetrics {
    /// Resolve all source handles. `per_partition_detail` gates the
    /// cardinality-sensitive per-partition lag series.
    pub fn new(labels: &ComponentLabels, per_partition_detail: bool) -> Self {
        SourceMetrics {
            records: labels.counter(names::SOURCE_RECORDS_TOTAL),
            bytes: labels.counter(names::SOURCE_BYTES_TOTAL),
            poll_duration: labels.histogram(names::SOURCE_POLL_DURATION_SECONDS),
            lag_max: labels.gauge(names::SOURCE_LAG_RECORDS),
            rebalance_assign: labels.counter1(
                names::SOURCE_REBALANCES_TOTAL,
                names::L_EVENT,
                "assign",
            ),
            rebalance_revoke: labels.counter1(
                names::SOURCE_REBALANCES_TOTAL,
                names::L_EVENT,
                "revoke",
            ),
            lanes_active: labels.gauge(names::SOURCE_LANES_ACTIVE),
            partition_lag: per_partition_detail.then(|| PartitionGauges {
                name: names::SOURCE_LAG_RECORDS,
                labels: labels.clone(),
                gauges: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Record one polled batch.
    #[inline]
    pub fn batch(&self, records: u64, bytes: u64) {
        self.records.increment(records);
        self.bytes.increment(bytes);
    }

    /// Observe one `poll` call's duration.
    #[inline]
    pub fn poll_duration(&self, d: Duration) {
        self.poll_duration.record(d.as_secs_f64());
    }

    /// Set the max consumer lag across partitions.
    pub fn set_lag_max(&self, lag: u64) {
        self.lag_max.set(lag as f64);
    }

    /// Set one partition's lag. No-op unless `per_partition_detail`.
    pub fn set_partition_lag(&self, partition: PartitionId, lag: u64) {
        if let Some(pg) = &self.partition_lag {
            pg.set(partition, lag as f64);
        }
    }

    /// Drop per-partition series for revoked partitions.
    pub fn retain_partitions(&self, keep: &[PartitionId]) {
        if let Some(pg) = &self.partition_lag {
            pg.retain(keep);
        }
    }

    /// Count a rebalance assignment event.
    pub fn rebalance_assigned(&self) {
        self.rebalance_assign.increment(1);
    }

    /// Count a rebalance revocation event.
    pub fn rebalance_revoked(&self) {
        self.rebalance_revoke.increment(1);
    }

    /// Set the number of currently assigned lanes.
    pub fn set_lanes_active(&self, lanes: usize) {
        self.lanes_active.set(lanes as f64);
    }
}

/// Deserializer-stage handles (`etl_deser_*`).
#[derive(Debug)]
pub struct DeserMetrics {
    ok: Counter,
    errors: Counter,
    dropped_skip: Counter,
    not_ready: Counter,
    batch_duration: Histogram,
}

impl DeserMetrics {
    /// Resolve all deserializer handles.
    pub fn new(labels: &ComponentLabels) -> Self {
        DeserMetrics {
            ok: labels.counter1(names::DESER_RECORDS_TOTAL, names::L_OUTCOME, "ok"),
            errors: labels.counter1(names::DESER_RECORDS_TOTAL, names::L_OUTCOME, "error"),
            dropped_skip: labels.counter1(
                names::DESER_RECORDS_DROPPED_TOTAL,
                names::L_REASON,
                "skip_policy",
            ),
            not_ready: labels.counter(names::DESER_NOT_READY_TOTAL),
            batch_duration: labels.histogram(names::DESER_BATCH_DURATION_SECONDS),
        }
    }

    /// Record one deserialized batch: emitted records, failed payloads,
    /// and time spent.
    #[inline]
    pub fn batch(&self, ok: u64, errors: u64, d: Duration) {
        self.ok.increment(ok);
        if errors > 0 {
            self.errors.increment(errors);
        }
        self.batch_duration.record(d.as_secs_f64());
    }

    /// Count payloads dropped by the Skip policy.
    #[inline]
    pub fn dropped(&self, n: u64) {
        self.dropped_skip.increment(n);
    }

    /// Count not-ready replays (payloads waiting on an upstream
    /// dependency such as a schema fetch).
    #[inline]
    pub fn not_ready(&self, n: u64) {
        self.not_ready.increment(n);
    }
}

/// Operator-stage handles (`etl_operator_*`).
#[derive(Debug)]
pub struct OperatorMetrics {
    records_in: Counter,
    records_out: Counter,
    dropped_filtered: Counter,
    dropped_skip: Counter,
    err_retryable: Counter,
    err_record: Counter,
    err_fatal: Counter,
    batch_duration: Histogram,
}

impl OperatorMetrics {
    /// Resolve all operator handles.
    pub fn new(labels: &ComponentLabels) -> Self {
        OperatorMetrics {
            records_in: labels.counter(names::OPERATOR_RECORDS_IN_TOTAL),
            records_out: labels.counter(names::OPERATOR_RECORDS_OUT_TOTAL),
            dropped_filtered: labels.counter1(
                names::OPERATOR_RECORDS_DROPPED_TOTAL,
                names::L_REASON,
                "filtered",
            ),
            dropped_skip: labels.counter1(
                names::OPERATOR_RECORDS_DROPPED_TOTAL,
                names::L_REASON,
                "skip_policy",
            ),
            err_retryable: labels.counter1(
                names::OPERATOR_ERRORS_TOTAL,
                names::L_ERROR_TYPE,
                ErrorClass::Retryable.label(),
            ),
            err_record: labels.counter1(
                names::OPERATOR_ERRORS_TOTAL,
                names::L_ERROR_TYPE,
                ErrorClass::RecordLevel.label(),
            ),
            err_fatal: labels.counter1(
                names::OPERATOR_ERRORS_TOTAL,
                names::L_ERROR_TYPE,
                ErrorClass::Fatal.label(),
            ),
            batch_duration: labels.histogram(names::OPERATOR_BATCH_DURATION_SECONDS),
        }
    }

    /// Record one processed batch.
    #[inline]
    pub fn batch(&self, records_in: u64, records_out: u64, d: Duration) {
        self.records_in.increment(records_in);
        self.records_out.increment(records_out);
        self.batch_duration.record(d.as_secs_f64());
    }

    /// Count records removed by a predicate.
    #[inline]
    pub fn filtered(&self, n: u64) {
        self.dropped_filtered.increment(n);
    }

    /// Count records dropped by the Skip error policy.
    #[inline]
    pub fn skipped(&self, n: u64) {
        self.dropped_skip.increment(n);
    }

    /// Count user-code errors of one taxonomy class.
    #[inline]
    pub fn errors(&self, class: ErrorClass, n: u64) {
        match class {
            ErrorClass::Retryable => self.err_retryable.increment(n),
            ErrorClass::RecordLevel => self.err_record.increment(n),
            ErrorClass::Fatal => self.err_fatal.increment(n),
        }
    }
}

/// Queue-edge handles (`etl_queue_*`).
#[derive(Debug)]
pub struct QueueMetrics {
    depth: Gauge,
    full_events: Counter,
}

impl QueueMetrics {
    /// Resolve handles for one queue edge (e.g. `chain->sink/shard-3`) and
    /// publish its configured capacity.
    pub fn new(labels: &ComponentLabels, queue: &str, capacity: usize) -> Self {
        let queue: SharedString = queue.to_owned().into();
        labels
            .gauge1(names::QUEUE_CAPACITY, names::L_QUEUE, queue.clone())
            .set(capacity as f64);
        QueueMetrics {
            depth: labels.gauge1(names::QUEUE_DEPTH, names::L_QUEUE, queue.clone()),
            full_events: labels.counter1(names::QUEUE_FULL_EVENTS_TOTAL, names::L_QUEUE, queue),
        }
    }

    /// Set the current queue depth.
    #[inline]
    pub fn set_depth(&self, depth: usize) {
        self.depth.set(depth as f64);
    }

    /// Count `try_send` rejections.
    #[inline]
    pub fn full_events(&self, n: u64) {
        self.full_events.increment(n);
    }
}

/// Backpressure handles (`etl_backpressure_*`).
#[derive(Debug)]
pub struct BackpressureMetrics {
    paused: Gauge,
    paused_seconds: Gauge,
    pause_events: Counter,
    inflight_bytes: Gauge,
}

impl BackpressureMetrics {
    /// Resolve all backpressure handles.
    pub fn new(labels: &ComponentLabels) -> Self {
        BackpressureMetrics {
            paused: labels.gauge(names::BACKPRESSURE_PAUSED),
            paused_seconds: labels.gauge(names::BACKPRESSURE_PAUSED_SECONDS_TOTAL),
            pause_events: labels.counter(names::BACKPRESSURE_PAUSE_EVENTS_TOTAL),
            inflight_bytes: labels.gauge(names::BACKPRESSURE_INFLIGHT_BYTES),
        }
    }

    /// Record a pause transition.
    pub fn pause_started(&self) {
        self.paused.set(1.0);
        self.pause_events.increment(1);
    }

    /// Record a resume transition and the time spent paused.
    pub fn pause_ended(&self, paused_for: Duration) {
        self.paused.set(0.0);
        // Monotonic accumulator; gauge because the facade counter is
        // integer-only (see names.rs).
        self.paused_seconds.increment(paused_for.as_secs_f64());
    }

    /// Set the current in-flight byte budget usage.
    #[inline]
    pub fn set_inflight_bytes(&self, bytes: usize) {
        self.inflight_bytes.set(bytes as f64);
    }
}

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
                };
                m.healthy.set(1.0);
                m
            })
            .collect();
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

    /// Count batches abandoned at the drain deadline.
    pub fn abandoned(&self, n: u64) {
        self.abandoned.increment(n);
    }
}

/// Checkpointer handles (`etl_checkpoint_*` and end-to-end latency).
#[derive(Debug)]
pub struct CheckpointMetrics {
    pending_max: Gauge,
    commits_ok: Counter,
    commits_err: Counter,
    commit_duration: Histogram,
    watermark_age: Gauge,
    partition_pending: Option<PartitionGauges>,
}

impl CheckpointMetrics {
    /// Resolve all checkpointer handles.
    pub fn new(labels: &ComponentLabels, per_partition_detail: bool) -> Self {
        CheckpointMetrics {
            pending_max: labels.gauge(names::CHECKPOINT_PENDING_BATCHES),
            commits_ok: labels.counter1(names::CHECKPOINT_COMMITS_TOTAL, names::L_OUTCOME, "ok"),
            commits_err: labels.counter1(
                names::CHECKPOINT_COMMITS_TOTAL,
                names::L_OUTCOME,
                "error",
            ),
            commit_duration: labels.histogram(names::CHECKPOINT_COMMIT_DURATION_SECONDS),
            watermark_age: labels.gauge(names::CHECKPOINT_WATERMARK_AGE_SECONDS),
            partition_pending: per_partition_detail.then(|| PartitionGauges {
                name: names::CHECKPOINT_PENDING_BATCHES,
                labels: labels.clone(),
                gauges: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Set the max pending-batch count across partitions.
    pub fn set_pending_max(&self, pending: usize) {
        self.pending_max.set(pending as f64);
    }

    /// Set one partition's pending count. No-op unless
    /// `per_partition_detail`.
    pub fn set_partition_pending(&self, partition: PartitionId, pending: usize) {
        if let Some(pg) = &self.partition_pending {
            pg.set(partition, pending as f64);
        }
    }

    /// Drop per-partition series for revoked partitions.
    pub fn retain_partitions(&self, keep: &[PartitionId]) {
        if let Some(pg) = &self.partition_pending {
            pg.retain(keep);
        }
    }

    /// Record one source commit call.
    pub fn commit(&self, ok: bool, d: Duration) {
        if ok {
            self.commits_ok.increment(1);
        } else {
            self.commits_err.increment(1);
        }
        self.commit_duration.record(d.as_secs_f64());
    }

    /// Set the age of the oldest unacknowledged batch.
    pub fn set_watermark_age(&self, age: Duration) {
        self.watermark_age.set(age.as_secs_f64());
    }
}

/// Lifecycle state of the pipeline, exported via `etl_pipeline_state`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PipelineState {
    /// Starting up: connecting, awaiting assignment.
    Starting,
    /// Processing records.
    Running,
    /// Draining after SIGTERM or a full revocation.
    Draining,
    /// Failed; the process will exit non-zero.
    Failed,
}

/// Pipeline-level handles (`etl_pipeline_*`).
#[derive(Debug)]
pub struct PipelineMetrics {
    starting: Gauge,
    running: Gauge,
    draining: Gauge,
    failed: Gauge,
    threads: Gauge,
}

impl PipelineMetrics {
    /// Resolve pipeline handles and publish the info series.
    pub fn new(labels: &ComponentLabels, version: &str) -> Self {
        labels
            .gauge1(names::PIPELINE_INFO, names::L_VERSION, version.to_owned())
            .set(1.0);
        let state = |s: &'static str| labels.gauge1(names::PIPELINE_STATE, names::L_STATE, s);
        let m = PipelineMetrics {
            starting: state("starting"),
            running: state("running"),
            draining: state("draining"),
            failed: state("failed"),
            threads: labels.gauge(names::PIPELINE_THREADS),
        };
        m.set_state(PipelineState::Starting);
        m
    }

    /// Flip the state gauges so exactly the current state reads 1.
    pub fn set_state(&self, state: PipelineState) {
        self.starting.set(if state == PipelineState::Starting {
            1.0
        } else {
            0.0
        });
        self.running.set(if state == PipelineState::Running {
            1.0
        } else {
            0.0
        });
        self.draining.set(if state == PipelineState::Draining {
            1.0
        } else {
            0.0
        });
        self.failed.set(if state == PipelineState::Failed {
            1.0
        } else {
            0.0
        });
    }

    /// Publish the pinned pipeline thread count.
    pub fn set_threads(&self, threads: usize) {
        self.threads.set(threads as f64);
    }
}
