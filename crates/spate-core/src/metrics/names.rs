//! Metric and label name constants, the single source of truth for the
//! taxonomy documented in [the metrics reference].
//!
//! Every framework metric is registered through these constants; nothing
//! else may hard-code a metric name. Names follow Prometheus conventions:
//! `_total` suffix on counters, unit suffixes (`_seconds`, `_bytes`,
//! `_rows`) on everything measured in a unit, `spate_` prefix throughout.
//!
//! [the metrics reference]: https://spate.kainth.dev/docs/METRICS

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
/// Error class label. [`SINK_ERRORS_TOTAL`] carries the class the connector
/// assigned (`retryable`, `record_level`, `fatal`); [`OPERATOR_ERRORS_TOTAL`]
/// carries what the stage did with the error (`record_level`, `fatal`).
pub const L_ERROR_TYPE: &str = "error_type";
/// Queue edge label (`<upstream>-><downstream>`).
pub const L_QUEUE: &str = "queue";
/// Pipeline state label (`starting`, `running`, `draining`, `failed`).
pub const L_STATE: &str = "state";
/// Build version label on `spate_pipeline_info`.
pub const L_VERSION: &str = "version";

// Declares each name as a constant and builds the kind's list from the same entries.
macro_rules! metric_names {
    ($(#[$list_doc:meta])* $list:ident { $( $(#[$doc:meta])* $id:ident = $name:literal, )* }) => {
        $( $(#[$doc])* pub const $id: &str = $name; )*
        $(#[$list_doc])* pub const $list: &[&str] = &[$($id),*];
    };
}

metric_names! {
    /// Every counter name (must end in `_total`).
    COUNTERS {
        // Source.

        /// Records emitted by the source (post-poll, pre-deserialization).
        SOURCE_RECORDS_TOTAL = "spate_source_records_total",
        /// Payload bytes emitted by the source.
        SOURCE_BYTES_TOTAL = "spate_source_bytes_total",
        /// Rebalance events observed, labeled by [`L_EVENT`].
        SOURCE_REBALANCES_TOTAL = "spate_source_rebalances_total",

        // Deserializer.

        /// Deserialization outputs plus one `error` per failed payload, by
        /// [`L_OUTCOME`].
        DESER_RECORDS_TOTAL = "spate_deser_records_total",
        /// Payloads dropped by the Skip error policy, by [`L_REASON`].
        DESER_RECORDS_DROPPED_TOTAL = "spate_deser_records_dropped_total",
        /// Counter: payload replays awaiting an upstream dependency (e.g. a schema
        /// fetch), counting `DeserError::NotReady` occurrences. Not an error and not
        /// backpressure.
        DESER_NOT_READY_TOTAL = "spate_deser_not_ready_total",

        // Operators.

        /// Records entering the operator.
        OPERATOR_RECORDS_IN_TOTAL = "spate_operator_records_in_total",
        /// Records emitted downstream by the operator.
        OPERATOR_RECORDS_OUT_TOTAL = "spate_operator_records_out_total",
        /// Records intentionally removed, by [`L_REASON`].
        OPERATOR_RECORDS_DROPPED_TOTAL = "spate_operator_records_dropped_total",
        /// User-code errors by [`L_ERROR_TYPE`].
        OPERATOR_ERRORS_TOTAL = "spate_operator_errors_total",

        // Queues.

        /// `try_send` rejections (each is a backpressure signal, never a block).
        QUEUE_FULL_EVENTS_TOTAL = "spate_queue_full_events_total",

        // Backpressure.

        /// Pause transitions (flapping indicator when high).
        BACKPRESSURE_PAUSE_EVENTS_TOTAL = "spate_backpressure_pause_events_total",

        // Sink.

        /// Records durably written (acknowledged flushes only), by [`L_SHARD`].
        SINK_RECORDS_TOTAL = "spate_sink_records_total",
        /// Bytes durably written, by [`L_SHARD`].
        SINK_BYTES_TOTAL = "spate_sink_bytes_total",
        /// Flushes by trigger, by [`L_SHARD`] and [`L_REASON`].
        SINK_FLUSHES_TOTAL = "spate_sink_flushes_total",
        /// Flush attempts beyond the first, by [`L_SHARD`].
        SINK_RETRIES_TOTAL = "spate_sink_retries_total",
        /// Write errors, by [`L_SHARD`] and [`L_ERROR_TYPE`].
        SINK_ERRORS_TOTAL = "spate_sink_errors_total",
        /// Circuit-breaker open transitions, by [`L_SHARD`] and [`L_REPLICA`].
        SINK_BREAKER_OPENS_TOTAL = "spate_sink_breaker_opens_total",
        /// Failed write attempts attributed to a replica, by [`L_SHARD`] and
        /// [`L_REPLICA`]. Pinpoints which endpoint of a shard is erroring (the
        /// shard-level [`SINK_ERRORS_TOTAL`] gives the class breakdown).
        SINK_REPLICA_ERRORS_TOTAL = "spate_sink_replica_errors_total",
        /// Batches abandoned at the drain deadline (replayed after restart).
        SINK_ABANDONED_BATCHES_TOTAL = "spate_sink_abandoned_batches_total",
        /// Shard workers force-aborted for failing to return by the drain deadline,
        /// by [`L_SHARD`]. Non-zero is a framework bug, not an operating condition.
        /// The drain still completed and the data still replays, but that shard's
        /// contribution to the drain report was lost with it.
        SINK_DRAIN_OVERRUN_TOTAL = "spate_sink_drain_overrun_total",

        // Checkpointing.

        /// Source commit calls, by [`L_OUTCOME`].
        CHECKPOINT_COMMITS_TOTAL = "spate_checkpoint_commits_total",

        // Coordination (multi-instance split leases for broker-less sources).

        /// Split acquisitions, by [`L_REASON`] (`create`, `reclaimed`, `expired`,
        /// `reassigned`).
        COORDINATION_ACQUISITIONS_TOTAL = "spate_coordination_acquisitions_total",
        /// Splits lost involuntarily, by [`L_REASON`] (`fenced`, `starved`,
        /// `revoked`).
        COORDINATION_SPLIT_LOSSES_TOTAL = "spate_coordination_split_losses_total",
        /// Voluntary split releases (graceful shutdown or scale-down).
        COORDINATION_RELEASES_TOTAL = "spate_coordination_releases_total",
        /// Split revocations, by [`L_OUTCOME`] (`requested`, `drained`, `forced`,
        /// `cancelled`), the leader moving a split away from a live owner. All
        /// four count on the releasing worker; `drained` is the replay-free
        /// outcome, and `cancelled` is the leader taking the move back.
        COORDINATION_REVOCATIONS_TOTAL = "spate_coordination_revocations_total",
        /// Splits written into the plan by this worker while leader (seeded
        /// create-if-absent; replayed ids that already exist are not counted).
        COORDINATION_SPLITS_PLANNED_TOTAL = "spate_coordination_splits_planned_total",
        /// Planner runs while leader, by [`L_OUTCOME`] (`ok`, `error`, `noop`).
        COORDINATION_REPLANS_TOTAL = "spate_coordination_replans_total",
        /// Explicit split failure reports (`SplitCoordinator::fail`).
        COORDINATION_SPLIT_FAILURES_TOTAL = "spate_coordination_split_failures_total",
        /// Splits parked after exhausting their delivery attempts.
        COORDINATION_QUARANTINES_TOTAL = "spate_coordination_quarantines_total",
        /// Split-record writes, by [`L_OUTCOME`] (`ok`, `conflict`, `error`).
        COORDINATION_WRITES_TOTAL = "spate_coordination_writes_total",
    }
}

metric_names! {
    /// Every gauge name.
    GAUGES {
        // Source.

        /// Consumer lag, always labeled by [`L_PARTITION`]. There is no aggregate
        /// series; aggregate with `sum`/`max` in the query layer. A partition whose
        /// lag has never been measured is absent rather than `0`.
        SOURCE_LAG_RECORDS = "spate_source_lag_records",
        /// Currently assigned lanes (partitions).
        SOURCE_LANES_ACTIVE = "spate_source_lanes_active",

        // Queues.

        /// Items currently queued, by [`L_QUEUE`].
        QUEUE_DEPTH = "spate_queue_depth",
        /// Configured queue bound, by [`L_QUEUE`].
        QUEUE_CAPACITY = "spate_queue_capacity",

        // Backpressure.

        /// 1 while the source is paused by the watermark controller.
        BACKPRESSURE_PAUSED = "spate_backpressure_paused",
        /// Cumulative paused time in seconds. Monotonically increasing; exported
        /// as a gauge because the `metrics` counter type is integer-only.
        BACKPRESSURE_PAUSED_SECONDS_TOTAL = "spate_backpressure_paused_seconds_total",
        /// Current global in-flight byte budget usage.
        BACKPRESSURE_INFLIGHT_BYTES = "spate_backpressure_inflight_bytes",

        // Sink.

        /// Sealed batches not yet settled, by [`L_SHARD`]: those being written plus
        /// any sealed batch still queueing for an `inflight.max_per_shard` slot. It
        /// can therefore read above the cap while a batch waits; compare it to the
        /// cap for saturation, not for equality.
        SINK_INFLIGHT_BATCHES = "spate_sink_inflight_batches",
        /// Current retry backoff step of the shard's longest-sleeping in-flight
        /// batch, by [`L_SHARD`]; `0` when no write is backing off. The step being
        /// served, not the time left in it; the value does not count down.
        SINK_RETRY_BACKOFF_SECONDS = "spate_sink_retry_backoff_seconds",
        /// 1 = circuit closed, 0 = open, by [`L_SHARD`] and [`L_REPLICA`].
        SINK_REPLICA_HEALTHY = "spate_sink_replica_healthy",
        /// 1 = the shard has at least one circuit-closed replica, 0 = none is
        /// circuit-closed (every replica quarantined or half-open probing), by
        /// [`L_SHARD`]. At 0, intake stalls and the shard back-pressures the source
        /// while recovery probes continue. The whole-shard escalation of
        /// [`SINK_REPLICA_HEALTHY`].
        SINK_SHARD_HEALTHY = "spate_sink_shard_healthy",

        // Checkpointing.

        /// Unacknowledged batches tracked. The unlabeled series is the max across
        /// partitions; one series per tracked partition carries [`L_PARTITION`] when
        /// `per_partition_detail` is set, so a sum over the bare name adds the max to
        /// the partitions themselves.
        CHECKPOINT_PENDING_BATCHES = "spate_checkpoint_pending_batches",
        /// Age of the oldest unacknowledged batch. The primary "stuck pipeline"
        /// alert signal.
        CHECKPOINT_WATERMARK_AGE_SECONDS = "spate_checkpoint_watermark_age_seconds",

        // Coordination (multi-instance split leases for broker-less sources).

        /// Splits currently leased by this worker.
        COORDINATION_SPLITS_OWNED = "spate_coordination_splits_owned",
        /// Splits observed completed across the fleet (bounded jobs).
        COORDINATION_SPLITS_COMPLETED = "spate_coordination_splits_completed",
        /// Splits parked in quarantine after exhausting delivery attempts.
        COORDINATION_SPLITS_QUARANTINED = "spate_coordination_splits_quarantined",
        /// Distinct live workers observed, including this instance; a worker that
        /// has handed back its last split through `SplitCoordinator::release`
        /// counts itself only while its own presence key is in its view.
        COORDINATION_LIVE_WORKERS = "spate_coordination_live_workers",
        /// 1 while this worker holds the planner leadership lease.
        COORDINATION_LEADER = "spate_coordination_leader",
        /// 1 while this worker owns no splits and observes as a standby.
        COORDINATION_IDLE = "spate_coordination_idle",
        /// Splits this worker is currently draining away under revocation,
        /// including a drain whose revocation was cancelled and which is still
        /// winding down. This is the drain count rather than the revocation count.
        COORDINATION_SPLITS_DRAINING = "spate_coordination_splits_draining",

        // Pipeline.

        /// Constant 1; carries build metadata via [`L_VERSION`].
        PIPELINE_INFO = "spate_pipeline_info",
        /// 1 for the current state, 0 otherwise, by [`L_STATE`].
        PIPELINE_STATE = "spate_pipeline_state",
        /// Pinned pipeline thread count.
        PIPELINE_THREADS = "spate_pipeline_threads",
    }
}

metric_names! {
    /// Every histogram name (must carry a unit suffix).
    HISTOGRAMS {
        // Source.

        /// Time spent inside `poll` per call.
        SOURCE_POLL_DURATION_SECONDS = "spate_source_poll_duration_seconds",

        // Deserializer.

        /// Deserialization time per source batch.
        DESER_BATCH_DURATION_SECONDS = "spate_deser_batch_duration_seconds",

        // Operators.

        /// Processing time per batch through this operator.
        OPERATOR_BATCH_DURATION_SECONDS = "spate_operator_batch_duration_seconds",

        // Sink.

        /// Rows per sealed batch.
        SINK_BATCH_ROWS = "spate_sink_batch_rows",
        /// Bytes per sealed batch.
        SINK_BATCH_BYTES = "spate_sink_batch_bytes",
        /// Seal-to-settle time of one durably written batch, by [`L_SHARD`]: the
        /// in-flight permit wait, every attempt, every backoff sleep and probe wait,
        /// and the write that finally succeeded. This is the commit-lag input,
        /// **not** a measure of how fast the sink is; for that use
        /// [`SINK_WRITE_DURATION_SECONDS`], and for the queueing component
        /// [`SINK_PERMIT_WAIT_DURATION_SECONDS`]. Only settled batches are observed.
        /// An abandoned batch (at the drain deadline or, with no drain in sight, on a
        /// fatal class, exhausted attempts or a panicking write task) is counted by
        /// [`SINK_ABANDONED_BATCHES_TOTAL`].
        SINK_FLUSH_DURATION_SECONDS = "spate_sink_flush_duration_seconds",
        /// One write attempt, by [`L_SHARD`] and [`L_OUTCOME`] (`ok`, `error`). This
        /// is the sink system's round-trip, with the framework's own scheduling
        /// around the call (the permit wait, the retry backoff, the probe wait) left
        /// out. A connector that sleeps *inside* its write puts that sleep in here.
        /// Labeled by outcome because a fast fatal reject and a slow timeout are
        /// both attempts; the error *class* stays on [`SINK_ERRORS_TOTAL`].
        SINK_WRITE_DURATION_SECONDS = "spate_sink_write_duration_seconds",
        /// Time a sealed batch waited for one of its shard's `inflight.max_per_shard`
        /// slots before its first write attempt, by [`L_SHARD`]. This is the
        /// queueing component of [`SINK_FLUSH_DURATION_SECONDS`]. Observed for every
        /// sealed batch that starts a write, the healthy near-zero case included; a
        /// batch the drain deadline drops before it ever gets a permit is not.
        SINK_PERMIT_WAIT_DURATION_SECONDS = "spate_sink_permit_wait_duration_seconds",

        // Checkpointing.

        /// Commit round-trip time.
        CHECKPOINT_COMMIT_DURATION_SECONDS = "spate_checkpoint_commit_duration_seconds",

        // Coordination (multi-instance split leases for broker-less sources).

        /// Split-record write round-trip time.
        COORDINATION_WRITE_DURATION_SECONDS = "spate_coordination_write_duration_seconds",
        /// One planner run while leader (enumeration included).
        COORDINATION_REPLAN_DURATION_SECONDS = "spate_coordination_replan_duration_seconds",
        /// One full reconcile listing (the watch-loss backstop).
        COORDINATION_RECONCILE_DURATION_SECONDS = "spate_coordination_reconcile_duration_seconds",
        /// Store primitive round-trip time, by [`L_OP`] (`get`, `put`, `delete`,
        /// `list`, `watch`, `poll`).
        COORDINATION_STORE_OP_DURATION_SECONDS = "spate_coordination_store_op_duration_seconds",
        /// Cooperative drain time on the **releasing** worker: revocation
        /// requested to the release landing. Only drains that end a revocation
        /// cooperatively are observed. A forced release is a `forced` revocation,
        /// not a drain, and a drain whose revocation was `cancelled` is no longer
        /// ending a revocation when it lands.
        COORDINATION_DRAIN_DURATION_SECONDS = "spate_coordination_drain_duration_seconds",
        /// Assignment convergence on the **gaining** worker: a split appearing in
        /// this worker's assignment to this worker holding its lease.
        COORDINATION_ASSIGNMENT_LATENCY_SECONDS = "spate_coordination_assignment_latency_seconds",
        /// Time left before the owning worker's starvation self-fence when a
        /// split-lease renewal is confirmed, on that worker's clock. 0 when none was
        /// left.
        COORDINATION_SPLIT_LEASE_HEADROOM_SECONDS = "spate_coordination_split_lease_headroom_seconds",

        // End to end.

        /// Source-to-durable-write latency, observed per acknowledged batch.
        E2E_LATENCY_SECONDS = "spate_e2e_latency_seconds",
    }
}

// Namespace policy for connector- and user-owned families (see
// `docs/METRICS.md` and the `Meter` docs). Every framework metric lives under
// the `spate_` umbrella so an operator greps one root for the whole
// pipeline's telemetry. A custom family registered through `Meter` must join
// that umbrella and must not land under a reserved stage root.

/// The umbrella prefix on every framework metric, required of every
/// `Meter`-registered custom family too.
pub const PREFIX: &str = "spate_";

/// The `spate_<root>_` segments the framework's own stage taxonomy owns. A
/// `Meter` rejects any custom name whose first segment after `PREFIX` is one
/// of these.
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
/// `ChainCtx::meter`) → `spate_custom_*`. Well-formed and not a reserved
/// root, so a `Meter` may use it. The runtime's `Meter::for_component` will
/// *not* scope a built-in component here; a component must declare its own
/// `component_type`, keeping author `spate_custom_*` families and component
/// families in separate buckets.
pub const CUSTOM_NAMESPACE: &str = "custom";

/// The role segments the runtime injects between a component's namespace and
/// its metric names (`spate_<ns>_source_*` / `_sink_*`) to separate a connector's
/// source and sink families. Reserved as the leading segment of a `Meter` local
/// name, so a hand-written `source_`/`sink_` name cannot alias a role-scoped
/// family. Must stay in sync with `MetricRole::segment`.
pub const ROLE_SEGMENTS: &[&str] = &["source", "sink"];

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeSet, HashSet};

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
            assert!(name.starts_with(PREFIX), "`{name}` must be spate_-prefixed");
        }
    }

    /// Every framework metric must fall under one of `RESERVED_ROOTS`, so the
    /// `Meter` reserved-root guard protects the whole taxonomy. If a new stage
    /// adds an `spate_<root>_*` family with a fresh root, add that root to
    /// `RESERVED_ROOTS`.
    #[test]
    fn every_framework_name_is_under_a_reserved_root() {
        for name in COUNTERS.iter().chain(GAUGES).chain(HISTOGRAMS) {
            let rest = name
                .strip_prefix(PREFIX)
                .unwrap_or_else(|| panic!("`{name}` must be spate_-prefixed"));
            let root = rest.split('_').next().unwrap_or("");
            assert!(
                RESERVED_ROOTS.contains(&root),
                "`{name}` has root `{root}` not in RESERVED_ROOTS — add it so \
                 the Meter collision guard covers this family"
            );
        }
    }

    /// Pins that every `"spate_…"` name literal above the test module is listed in `COUNTERS`, `GAUGES` or `HISTOGRAMS`.
    #[test]
    fn every_name_literal_in_this_file_is_listed() {
        let decls = include_str!("names.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("split yields a first part");
        let mut literals = BTreeSet::new();
        let mut rest = decls;
        while let Some(i) = rest.find("\"spate_") {
            let tail = &rest[i + 1..];
            let end = tail.find('"').expect("closed literal");
            if end > PREFIX.len() {
                literals.insert(&tail[..end]);
            }
            rest = &tail[end + 1..];
        }
        let listed: BTreeSet<&str> = COUNTERS
            .iter()
            .chain(GAUGES)
            .chain(HISTOGRAMS)
            .copied()
            .collect();
        assert!(
            !literals.is_empty(),
            "no name literals found; the scan is vacuous"
        );
        assert_eq!(
            literals, listed,
            "a name declared in names.rs is missing from its list"
        );
    }
}
