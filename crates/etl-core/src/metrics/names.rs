//! Metric and label name constants — the single source of truth for the
//! taxonomy documented in `docs/METRICS.md`.
//!
//! Every framework metric is registered through these constants; nothing
//! else may hard-code a metric name. Names follow Prometheus conventions:
//! `_total` suffix on counters, unit suffixes (`_seconds`, `_bytes`,
//! `_rows`) on everything measured in a unit, `etl_` prefix throughout.

// Standard labels attached to every framework metric.

/// Pipeline name label.
pub const L_PIPELINE: &str = "pipeline";
/// Component instance id label (e.g. `orders_kafka`).
pub const L_COMPONENT: &str = "component";
/// Component implementation label (e.g. `kafka`, `clickhouse`, `map`).
pub const L_COMPONENT_TYPE: &str = "component_type";

// Metric-specific labels.

/// Source partition label (cardinality-gated by `per_partition_detail`).
pub const L_PARTITION: &str = "partition";
/// Sink shard label.
pub const L_SHARD: &str = "shard";
/// Sink replica label.
pub const L_REPLICA: &str = "replica";
/// Drop reason label (`filtered`, `skip_policy`).
pub const L_REASON: &str = "reason";
/// Outcome label (`ok`, `error`).
pub const L_OUTCOME: &str = "outcome";
/// Store-primitive label on coordination store-op timings.
pub const L_OP: &str = "op";
/// Rebalance event label (`assign`, `revoke`).
pub const L_EVENT: &str = "event";
/// Error taxonomy class label (`retryable`, `record_level`, `fatal`).
pub const L_ERROR_TYPE: &str = "error_type";
/// Queue edge label (`<upstream>-><downstream>`).
pub const L_QUEUE: &str = "queue";
/// Pipeline state label (`starting`, `running`, `draining`, `failed`).
pub const L_STATE: &str = "state";
/// Build version label on `etl_pipeline_info`.
pub const L_VERSION: &str = "version";

// Source.

/// Records emitted by the source (post-poll, pre-deserialization).
pub const SOURCE_RECORDS_TOTAL: &str = "etl_source_records_total";
/// Payload bytes emitted by the source.
pub const SOURCE_BYTES_TOTAL: &str = "etl_source_bytes_total";
/// Time spent inside `poll` per call.
pub const SOURCE_POLL_DURATION_SECONDS: &str = "etl_source_poll_duration_seconds";
/// Consumer lag, always labelled by [`L_PARTITION`] — there is no aggregate
/// series; aggregate with `sum`/`max` in the query layer. A partition whose
/// lag has never been measured is absent rather than `0`.
pub const SOURCE_LAG_RECORDS: &str = "etl_source_lag_records";
/// Rebalance events observed, labelled by [`L_EVENT`].
pub const SOURCE_REBALANCES_TOTAL: &str = "etl_source_rebalances_total";
/// Currently assigned lanes (partitions).
pub const SOURCE_LANES_ACTIVE: &str = "etl_source_lanes_active";

// Deserializer.

/// Deserialization outputs plus one `error` per failed payload, by
/// [`L_OUTCOME`].
pub const DESER_RECORDS_TOTAL: &str = "etl_deser_records_total";
/// Payloads dropped by the Skip error policy, by [`L_REASON`].
pub const DESER_RECORDS_DROPPED_TOTAL: &str = "etl_deser_records_dropped_total";
/// Deserialization time per source batch.
pub const DESER_BATCH_DURATION_SECONDS: &str = "etl_deser_batch_duration_seconds";
/// Counter: payload replays awaiting an upstream dependency (e.g. a schema
/// fetch) — `DeserError::NotReady` occurrences. Not an error and not
/// backpressure.
pub const DESER_NOT_READY_TOTAL: &str = "etl_deser_not_ready_total";

// Operators.

/// Records entering the operator.
pub const OPERATOR_RECORDS_IN_TOTAL: &str = "etl_operator_records_in_total";
/// Records emitted downstream by the operator.
pub const OPERATOR_RECORDS_OUT_TOTAL: &str = "etl_operator_records_out_total";
/// Records intentionally removed, by [`L_REASON`].
pub const OPERATOR_RECORDS_DROPPED_TOTAL: &str = "etl_operator_records_dropped_total";
/// User-code errors by [`L_ERROR_TYPE`].
pub const OPERATOR_ERRORS_TOTAL: &str = "etl_operator_errors_total";
/// Processing time per batch through this operator.
pub const OPERATOR_BATCH_DURATION_SECONDS: &str = "etl_operator_batch_duration_seconds";

// Queues.

/// Items currently queued, by [`L_QUEUE`].
pub const QUEUE_DEPTH: &str = "etl_queue_depth";
/// Configured queue bound, by [`L_QUEUE`].
pub const QUEUE_CAPACITY: &str = "etl_queue_capacity";
/// `try_send` rejections (each is a backpressure signal, never a block).
pub const QUEUE_FULL_EVENTS_TOTAL: &str = "etl_queue_full_events_total";

// Backpressure.

/// 1 while the source is paused by the watermark controller.
pub const BACKPRESSURE_PAUSED: &str = "etl_backpressure_paused";
/// Cumulative paused time in seconds. Monotonically increasing; exported
/// as a gauge because the `metrics` counter type is integer-only.
pub const BACKPRESSURE_PAUSED_SECONDS_TOTAL: &str = "etl_backpressure_paused_seconds_total";
/// Pause transitions (flapping indicator when high).
pub const BACKPRESSURE_PAUSE_EVENTS_TOTAL: &str = "etl_backpressure_pause_events_total";
/// Current global in-flight byte budget usage.
pub const BACKPRESSURE_INFLIGHT_BYTES: &str = "etl_backpressure_inflight_bytes";

// Sink.

/// Records durably written (acknowledged flushes only), by [`L_SHARD`].
pub const SINK_RECORDS_TOTAL: &str = "etl_sink_records_total";
/// Bytes durably written, by [`L_SHARD`].
pub const SINK_BYTES_TOTAL: &str = "etl_sink_bytes_total";
/// Rows per sealed batch.
pub const SINK_BATCH_ROWS: &str = "etl_sink_batch_rows";
/// Bytes per sealed batch.
pub const SINK_BATCH_BYTES: &str = "etl_sink_batch_bytes";
/// Flushes by trigger, by [`L_SHARD`] and [`L_REASON`].
pub const SINK_FLUSHES_TOTAL: &str = "etl_sink_flushes_total";
/// Seal-to-settle time of one durably written batch, by [`L_SHARD`]: the
/// in-flight permit wait, every attempt, every backoff sleep and probe wait,
/// and the write that finally succeeded. The commit-lag input, **not** a
/// measure of how fast the sink is — that is
/// [`SINK_WRITE_DURATION_SECONDS`], and the queueing component is
/// [`SINK_PERMIT_WAIT_DURATION_SECONDS`]. Only settled batches are observed;
/// an abandoned one — at the drain deadline or, with no drain in sight, on a
/// fatal class, exhausted attempts or a panicking write task — is counted by
/// [`SINK_ABANDONED_BATCHES_TOTAL`].
pub const SINK_FLUSH_DURATION_SECONDS: &str = "etl_sink_flush_duration_seconds";
/// One write attempt, by [`L_SHARD`] and [`L_OUTCOME`] (`ok`, `error`) — the
/// sink system's round-trip, with the framework's own scheduling around the
/// call (the permit wait, the retry backoff, the probe wait) left out.
/// A connector that sleeps *inside* its write puts that sleep in here.
/// Labelled by outcome because a fast fatal reject and a slow timeout are
/// both attempts; the error *class* stays on [`SINK_ERRORS_TOTAL`].
pub const SINK_WRITE_DURATION_SECONDS: &str = "etl_sink_write_duration_seconds";
/// Time a sealed batch waited for one of its shard's `inflight.max_per_shard`
/// slots before its first write attempt, by [`L_SHARD`] — the queueing
/// component of [`SINK_FLUSH_DURATION_SECONDS`]. Observed for every sealed
/// batch that starts a write, the healthy near-zero case included; a batch
/// the drain deadline drops before it ever gets a permit is not.
pub const SINK_PERMIT_WAIT_DURATION_SECONDS: &str = "etl_sink_permit_wait_duration_seconds";
/// Flush attempts beyond the first, by [`L_SHARD`].
pub const SINK_RETRIES_TOTAL: &str = "etl_sink_retries_total";
/// Current retry backoff step of the shard's longest-sleeping in-flight
/// batch, by [`L_SHARD`]; `0` when no write is backing off. The step being
/// served, not the time left in it — it does not count down.
pub const SINK_RETRY_BACKOFF_SECONDS: &str = "etl_sink_retry_backoff_seconds";
/// Write errors, by [`L_SHARD`] and [`L_ERROR_TYPE`].
pub const SINK_ERRORS_TOTAL: &str = "etl_sink_errors_total";
/// Sealed batches currently in flight, by [`L_SHARD`].
pub const SINK_INFLIGHT_BATCHES: &str = "etl_sink_inflight_batches";
/// 1 = circuit closed, 0 = open, by [`L_SHARD`] and [`L_REPLICA`].
pub const SINK_REPLICA_HEALTHY: &str = "etl_sink_replica_healthy";
/// Circuit-breaker open transitions, by [`L_SHARD`] and [`L_REPLICA`].
pub const SINK_BREAKER_OPENS_TOTAL: &str = "etl_sink_breaker_opens_total";
/// Failed write attempts attributed to a replica, by [`L_SHARD`] and
/// [`L_REPLICA`]. Pinpoints which endpoint of a shard is erroring (the
/// shard-level [`SINK_ERRORS_TOTAL`] gives the class breakdown).
pub const SINK_REPLICA_ERRORS_TOTAL: &str = "etl_sink_replica_errors_total";
/// 1 = the shard has at least one circuit-closed replica, 0 = none is
/// circuit-closed (every replica quarantined or half-open probing) — intake
/// stalls and the shard back-pressures the source while recovery probes
/// continue, by [`L_SHARD`]. The whole-shard escalation of
/// [`SINK_REPLICA_HEALTHY`].
pub const SINK_SHARD_HEALTHY: &str = "etl_sink_shard_healthy";
/// Batches abandoned at the drain deadline (replayed after restart).
pub const SINK_ABANDONED_BATCHES_TOTAL: &str = "etl_sink_abandoned_batches_total";

// Checkpointing.

/// Unacknowledged batches tracked; unlabelled series is the max across
/// partitions.
pub const CHECKPOINT_PENDING_BATCHES: &str = "etl_checkpoint_pending_batches";
/// Source commit calls, by [`L_OUTCOME`].
pub const CHECKPOINT_COMMITS_TOTAL: &str = "etl_checkpoint_commits_total";
/// Commit round-trip time.
pub const CHECKPOINT_COMMIT_DURATION_SECONDS: &str = "etl_checkpoint_commit_duration_seconds";
/// Age of the oldest unacknowledged batch — the primary "stuck pipeline"
/// alert signal.
pub const CHECKPOINT_WATERMARK_AGE_SECONDS: &str = "etl_checkpoint_watermark_age_seconds";

// Coordination (multi-instance split leases for broker-less sources).

/// Splits currently leased by this worker.
pub const COORDINATION_SPLITS_OWNED: &str = "etl_coordination_splits_owned";
/// Splits observed completed across the fleet (bounded jobs).
pub const COORDINATION_SPLITS_COMPLETED: &str = "etl_coordination_splits_completed";
/// Splits parked in quarantine after exhausting delivery attempts.
pub const COORDINATION_SPLITS_QUARANTINED: &str = "etl_coordination_splits_quarantined";
/// Distinct live workers observed, including this instance.
pub const COORDINATION_LIVE_WORKERS: &str = "etl_coordination_live_workers";
/// 1 while this worker holds the planner leadership lease.
pub const COORDINATION_LEADER: &str = "etl_coordination_leader";
/// 1 while this worker owns no splits and observes as a standby.
pub const COORDINATION_IDLE: &str = "etl_coordination_idle";
/// Split acquisitions, by [`L_REASON`] (`create`, `reclaimed`, `expired`,
/// `reassigned`).
pub const COORDINATION_ACQUISITIONS_TOTAL: &str = "etl_coordination_acquisitions_total";
/// Splits lost involuntarily, by [`L_REASON`] (`fenced`, `starved`,
/// `revoked`).
pub const COORDINATION_SPLIT_LOSSES_TOTAL: &str = "etl_coordination_split_losses_total";
/// Voluntary split releases (graceful shutdown or scale-down).
pub const COORDINATION_RELEASES_TOTAL: &str = "etl_coordination_releases_total";
/// Split revocations, by [`L_OUTCOME`] (`requested`, `drained`, `forced`,
/// `cancelled`) — the leader moving a split away from a live owner. All
/// four count on the releasing worker; `drained` is the replay-free
/// outcome, and `cancelled` is the leader taking the move back.
pub const COORDINATION_REVOCATIONS_TOTAL: &str = "etl_coordination_revocations_total";
/// Cooperative drain time on the **releasing** worker: revocation
/// requested to the release landing. Only drains that end a revocation
/// cooperatively are observed — a forced release is a `forced` revocation,
/// not a drain, and a drain whose revocation was `cancelled` is no longer
/// ending a revocation at all when it lands.
pub const COORDINATION_DRAIN_DURATION_SECONDS: &str = "etl_coordination_drain_duration_seconds";
/// Assignment convergence on the **gaining** worker: a split appearing in
/// this worker's assignment to this worker holding its lease.
pub const COORDINATION_ASSIGNMENT_LATENCY_SECONDS: &str =
    "etl_coordination_assignment_latency_seconds";
/// Splits this worker is currently draining away under revocation —
/// including a drain whose revocation was cancelled and which is still
/// winding down, so this is the drain count rather than the revocation
/// count.
pub const COORDINATION_SPLITS_DRAINING: &str = "etl_coordination_splits_draining";
/// Splits written into the plan by this worker while leader (seeded
/// create-if-absent; replayed ids that already exist are not counted).
pub const COORDINATION_SPLITS_PLANNED_TOTAL: &str = "etl_coordination_splits_planned_total";
/// Planner runs while leader, by [`L_OUTCOME`] (`ok`, `error`, `noop`).
pub const COORDINATION_REPLANS_TOTAL: &str = "etl_coordination_replans_total";
/// Explicit split failure reports (`SplitCoordinator::fail`).
pub const COORDINATION_SPLIT_FAILURES_TOTAL: &str = "etl_coordination_split_failures_total";
/// Splits parked after exhausting their delivery attempts.
pub const COORDINATION_QUARANTINES_TOTAL: &str = "etl_coordination_quarantines_total";
/// Split-record writes, by [`L_OUTCOME`] (`ok`, `conflict`, `error`).
pub const COORDINATION_WRITES_TOTAL: &str = "etl_coordination_writes_total";
/// Split-record write round-trip time.
pub const COORDINATION_WRITE_DURATION_SECONDS: &str = "etl_coordination_write_duration_seconds";
/// One planner run while leader (enumeration included).
pub const COORDINATION_REPLAN_DURATION_SECONDS: &str = "etl_coordination_replan_duration_seconds";
/// One full reconcile listing (the watch-loss backstop).
pub const COORDINATION_RECONCILE_DURATION_SECONDS: &str =
    "etl_coordination_reconcile_duration_seconds";
/// Store primitive round-trip time, by [`L_OP`] (`get`, `put`, `delete`,
/// `list`, `watch`).
pub const COORDINATION_STORE_OP_DURATION_SECONDS: &str =
    "etl_coordination_store_op_duration_seconds";

// End to end.

/// Source-to-durable-write latency, observed per acknowledged batch.
pub const E2E_LATENCY_SECONDS: &str = "etl_e2e_latency_seconds";

// Pipeline.

/// Constant 1; carries build metadata via [`L_VERSION`].
pub const PIPELINE_INFO: &str = "etl_pipeline_info";
/// 1 for the current state, 0 otherwise, by [`L_STATE`].
pub const PIPELINE_STATE: &str = "etl_pipeline_state";
/// Pinned pipeline thread count.
pub const PIPELINE_THREADS: &str = "etl_pipeline_threads";

/// Every counter name (must end in `_total`).
pub const COUNTERS: &[&str] = &[
    SOURCE_RECORDS_TOTAL,
    SOURCE_BYTES_TOTAL,
    SOURCE_REBALANCES_TOTAL,
    DESER_RECORDS_TOTAL,
    DESER_RECORDS_DROPPED_TOTAL,
    DESER_NOT_READY_TOTAL,
    OPERATOR_RECORDS_IN_TOTAL,
    OPERATOR_RECORDS_OUT_TOTAL,
    OPERATOR_RECORDS_DROPPED_TOTAL,
    OPERATOR_ERRORS_TOTAL,
    QUEUE_FULL_EVENTS_TOTAL,
    BACKPRESSURE_PAUSE_EVENTS_TOTAL,
    SINK_RECORDS_TOTAL,
    SINK_BYTES_TOTAL,
    SINK_FLUSHES_TOTAL,
    SINK_RETRIES_TOTAL,
    SINK_ERRORS_TOTAL,
    SINK_BREAKER_OPENS_TOTAL,
    SINK_REPLICA_ERRORS_TOTAL,
    SINK_ABANDONED_BATCHES_TOTAL,
    CHECKPOINT_COMMITS_TOTAL,
    COORDINATION_ACQUISITIONS_TOTAL,
    COORDINATION_SPLIT_LOSSES_TOTAL,
    COORDINATION_RELEASES_TOTAL,
    COORDINATION_REVOCATIONS_TOTAL,
    COORDINATION_SPLITS_PLANNED_TOTAL,
    COORDINATION_REPLANS_TOTAL,
    COORDINATION_SPLIT_FAILURES_TOTAL,
    COORDINATION_QUARANTINES_TOTAL,
    COORDINATION_WRITES_TOTAL,
];

/// Every gauge name.
pub const GAUGES: &[&str] = &[
    SOURCE_LAG_RECORDS,
    SOURCE_LANES_ACTIVE,
    QUEUE_DEPTH,
    QUEUE_CAPACITY,
    BACKPRESSURE_PAUSED,
    BACKPRESSURE_PAUSED_SECONDS_TOTAL,
    BACKPRESSURE_INFLIGHT_BYTES,
    SINK_INFLIGHT_BATCHES,
    SINK_RETRY_BACKOFF_SECONDS,
    SINK_REPLICA_HEALTHY,
    SINK_SHARD_HEALTHY,
    CHECKPOINT_PENDING_BATCHES,
    CHECKPOINT_WATERMARK_AGE_SECONDS,
    COORDINATION_SPLITS_OWNED,
    COORDINATION_SPLITS_COMPLETED,
    COORDINATION_SPLITS_QUARANTINED,
    COORDINATION_LIVE_WORKERS,
    COORDINATION_LEADER,
    COORDINATION_IDLE,
    COORDINATION_SPLITS_DRAINING,
    PIPELINE_INFO,
    PIPELINE_STATE,
    PIPELINE_THREADS,
];

/// Every histogram name (must carry a unit suffix).
pub const HISTOGRAMS: &[&str] = &[
    SOURCE_POLL_DURATION_SECONDS,
    DESER_BATCH_DURATION_SECONDS,
    OPERATOR_BATCH_DURATION_SECONDS,
    SINK_BATCH_ROWS,
    SINK_BATCH_BYTES,
    SINK_FLUSH_DURATION_SECONDS,
    SINK_WRITE_DURATION_SECONDS,
    SINK_PERMIT_WAIT_DURATION_SECONDS,
    CHECKPOINT_COMMIT_DURATION_SECONDS,
    COORDINATION_WRITE_DURATION_SECONDS,
    COORDINATION_REPLAN_DURATION_SECONDS,
    COORDINATION_RECONCILE_DURATION_SECONDS,
    COORDINATION_STORE_OP_DURATION_SECONDS,
    COORDINATION_DRAIN_DURATION_SECONDS,
    COORDINATION_ASSIGNMENT_LATENCY_SECONDS,
    E2E_LATENCY_SECONDS,
];

// Namespace policy for connector- and user-owned families (see
// `docs/METRICS.md` and the `Meter` docs). Every framework metric lives under
// the `etl_` umbrella so an operator greps one root for the whole pipeline's
// telemetry; a custom family registered through `Meter` must join that
// umbrella but must not land under a reserved stage root, so it can never
// collide with a current or future framework metric.

/// The umbrella prefix on every framework metric, required of every
/// `Meter`-registered custom family too.
pub const PREFIX: &str = "etl_";

/// The `etl_<root>_` segments the framework's own stage taxonomy owns. A
/// `Meter` rejects any custom name whose first segment after `PREFIX` is one
/// of these. Keep in sync with the stage sections above — the
/// `every_framework_name_is_under_a_reserved_root` test enforces it.
pub const RESERVED_ROOTS: &[&str] = &[
    "source",
    "deser",
    "operator",
    "queue",
    "backpressure",
    "sink",
    "checkpoint",
    "coordination",
    "e2e",
    "pipeline",
];

/// The default namespace for pipeline-author custom metrics (`Meter::new` /
/// `ChainCtx::meter`) → `etl_custom_*`. Well-formed and not a reserved root, so
/// a `Meter` may use it — but the runtime's `Meter::for_component` will *not*
/// scope a built-in component here: a component must declare its own
/// `component_type`, keeping author `etl_custom_*` families and component
/// families in separate buckets.
pub const CUSTOM_NAMESPACE: &str = "custom";

/// The role segments the runtime injects between a component's namespace and
/// its metric names (`etl_<ns>_source_*` / `_sink_*`) to separate a connector's
/// source and sink families. Reserved as the leading segment of a `Meter` local
/// name, so a hand-written `source_`/`sink_` name cannot alias a role-scoped
/// family. Must stay in sync with `MetricRole::segment`.
pub const ROLE_SEGMENTS: &[&str] = &["source", "sink"];

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn names_follow_prometheus_conventions() {
        for name in COUNTERS {
            assert!(
                name.ends_with("_total"),
                "counter `{name}` must end in _total"
            );
        }
        for name in HISTOGRAMS {
            assert!(
                name.ends_with("_seconds") || name.ends_with("_rows") || name.ends_with("_bytes"),
                "histogram `{name}` must carry a unit suffix"
            );
        }
        for name in GAUGES {
            // One documented exception: the paused-time accumulator keeps
            // its counter-style name but is exported as a gauge because the
            // facade's counter type is integer-only.
            if *name == BACKPRESSURE_PAUSED_SECONDS_TOTAL {
                continue;
            }
            assert!(
                !name.ends_with("_total"),
                "gauge `{name}` must not end in _total"
            );
        }
    }

    #[test]
    fn names_are_prefixed_and_unique() {
        let all: Vec<&str> = COUNTERS
            .iter()
            .chain(GAUGES)
            .chain(HISTOGRAMS)
            .copied()
            .collect();
        let unique: HashSet<&str> = all.iter().copied().collect();
        assert_eq!(unique.len(), all.len(), "duplicate metric name");
        for name in &all {
            assert!(name.starts_with(PREFIX), "`{name}` must be etl_-prefixed");
        }
    }

    /// Every framework metric must fall under one of `RESERVED_ROOTS`, so the
    /// `Meter` reserved-root guard actually protects the whole taxonomy. If a
    /// new stage adds an `etl_<root>_*` family with a fresh root, add that root
    /// to `RESERVED_ROOTS`.
    #[test]
    fn every_framework_name_is_under_a_reserved_root() {
        for name in COUNTERS.iter().chain(GAUGES).chain(HISTOGRAMS) {
            let rest = name
                .strip_prefix(PREFIX)
                .unwrap_or_else(|| panic!("`{name}` must be etl_-prefixed"));
            let root = rest.split('_').next().unwrap_or("");
            assert!(
                RESERVED_ROOTS.contains(&root),
                "`{name}` has root `{root}` not in RESERVED_ROOTS — add it so \
                 the Meter collision guard covers this family"
            );
        }
    }
}
