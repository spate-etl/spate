//! Metrics: exporter installation and pre-registered handle structs for
//! every pipeline stage.
//!
//! `etl-rs` instruments through the [`metrics`] facade — pipeline authors
//! register custom metrics with the same macros and they are exported
//! alongside the framework's. [`install`] wires the exporter selected by
//! configuration; the taxonomy contract lives in `docs/METRICS.md` and its
//! names in [`names`].
//!
//! # Connector- and user-owned families
//!
//! Beyond the fixed handle structs, a [`Meter`] mints
//! `Counter`/`Gauge`/`Histogram` handles that inherit the three standard
//! labels (`pipeline`, `component`, `component_type`), so a connector's or
//! pipeline author's own series join cleanly against the framework's. The
//! handle types are re-exported here so a connector can store them without a
//! direct `metrics` dependency.
//!
//! # One pipeline per process
//!
//! The exporter installs a **process-global** recorder (the `metrics`
//! facade has one global recorder), matching the framework's
//! one-pipeline-per-process deployment model. [`install`] therefore
//! succeeds at most once per process; a second call returns
//! [`MetricsError::AlreadyInstalled`].
//!
//! # Hot-path discipline
//!
//! All handles are pre-registered at pipeline build time via the structs in
//! this module ([`SourceMetrics`], [`SinkShardMetrics`], ...). The record
//! loop only ever touches resolved `Counter`/`Gauge`/`Histogram` handles,
//! and methods take per-batch aggregates.

mod backpressure;
mod checkpoint;
mod coordination;
mod deser;
mod labels;
mod meter;
pub mod names;
mod operator;
mod pipeline;
mod queue;
mod sink;
mod source;

pub use backpressure::BackpressureMetrics;
pub use checkpoint::CheckpointMetrics;
pub use coordination::{
    AcquireReason, CoordinationMetrics, ReplanOutcome, SplitLossReason, StoreOp, WriteOutcome,
};
pub use deser::DeserMetrics;
pub use labels::ComponentLabels;
pub use meter::Meter;
// Role is derived by the runtime/builder from wiring position, never named by
// connectors — crate-internal only (see `Meter::for_component`).
pub(crate) use meter::MetricRole;
pub use operator::OperatorMetrics;
pub use pipeline::{PipelineMetrics, PipelineState};
pub use queue::QueueMetrics;
pub use sink::{FlushReason, SinkShardMetrics};
pub use source::SourceMetrics;

// The framework's instrumentation API *is* the `metrics` facade, so its
// handle types are part of this crate's public surface — a connector storing
// a [`Meter`]-minted handle in its own struct names them without taking a
// direct `metrics` dependency, keeping one facade version across the tree.
// This is the one sanctioned 0.x public-API exception (see `docs/DESIGN.md`).
pub use metrics::{Counter, Gauge, Histogram, SharedString};

use metrics_exporter_prometheus::{BuildError, Matcher, PrometheusBuilder, PrometheusHandle};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

/// Buckets for `*_duration_seconds` histograms and
/// `etl_e2e_latency_seconds` (1 ms .. 60 s, roughly exponential).
pub const DURATION_SECONDS_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

/// Buckets for `etl_sink_batch_rows` (powers of 4, 64 .. 1Mi rows).
pub const BATCH_ROWS_BUCKETS: &[f64] = &[
    64.0, 256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0,
];

/// Buckets for `etl_sink_batch_bytes` (powers of 4, 4 KiB .. 256 MiB).
pub const BATCH_BYTES_BUCKETS: &[f64] = &[
    4096.0,
    16384.0,
    65536.0,
    262144.0,
    1048576.0,
    4194304.0,
    16777216.0,
    67108864.0,
    268435456.0,
];

/// Which exporter to install.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Exporter {
    /// Prometheus scrape endpoint, served by the admin server.
    #[default]
    Prometheus,
    /// No export; all handles become no-ops.
    None,
}

/// Time basis for `etl_e2e_latency_seconds`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum E2eBasis {
    /// Framework ingest time — clock-skew free (default).
    #[default]
    Ingest,
    /// The record's event time (e.g. Kafka message timestamp) —
    /// clock-skew sensitive but reflects true upstream delay.
    Event,
}

/// Exporter settings, mapped from the `metrics` config section by the
/// pipeline runtime. Defined here (not in `config`) so this module has no
/// config dependency.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricsSettings {
    /// Which exporter to install.
    pub exporter: Exporter,
    /// Admin-server listen address (`/metrics`, `/healthz`, `/readyz`).
    pub listen: SocketAddr,
    /// Enable cardinality-sensitive per-partition series.
    pub per_partition_detail: bool,
    /// Time basis for end-to-end latency.
    pub e2e_basis: E2eBasis,
}

impl Default for MetricsSettings {
    fn default() -> Self {
        MetricsSettings {
            exporter: Exporter::Prometheus,
            listen: SocketAddr::from((Ipv4Addr::UNSPECIFIED, 9090)),
            per_partition_detail: false,
            e2e_basis: E2eBasis::Ingest,
        }
    }
}

/// Exporter installation failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MetricsError {
    /// A global recorder is already installed in this process (one pipeline
    /// per process).
    #[error("a metrics recorder is already installed in this process")]
    AlreadyInstalled,
    /// The exporter rejected its configuration.
    #[error("failed to build the metrics exporter: {0}")]
    Build(String),
}

/// Handle to the installed exporter. Cheap to clone.
#[derive(Clone, Debug)]
pub struct MetricsHandle {
    inner: Inner,
    process: Option<Arc<metrics_process::Collector>>,
}

#[derive(Clone, Debug)]
enum Inner {
    Prometheus(PrometheusHandle),
    Noop,
}

impl MetricsHandle {
    /// Render the current exposition-format snapshot (empty for the no-op
    /// exporter).
    #[must_use]
    pub fn render(&self) -> String {
        match &self.inner {
            Inner::Prometheus(handle) => {
                if let Some(process) = &self.process {
                    process.collect();
                }
                handle.render()
            }
            Inner::Noop => String::new(),
        }
    }

    /// The render function seam handed to the admin server, keeping it
    /// independent of exporter internals.
    #[must_use]
    pub fn render_fn(&self) -> Arc<dyn Fn() -> String + Send + Sync> {
        let this = self.clone();
        Arc::new(move || this.render())
    }

    /// One maintenance tick: drains histogram state and refreshes process
    /// metrics. Cheap; call on an interval.
    pub fn upkeep_tick(&self) {
        if let Inner::Prometheus(handle) = &self.inner {
            handle.run_upkeep();
        }
        if let Some(process) = &self.process {
            process.collect();
        }
    }

    /// Spawn the periodic upkeep task on the current tokio runtime.
    #[must_use]
    pub fn spawn_upkeep(&self, period: Duration) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(period);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                this.upkeep_tick();
            }
        })
    }
}

/// A Prometheus builder pre-configured with the bucket layout from
/// `docs/METRICS.md`.
fn configured_builder() -> Result<PrometheusBuilder, BuildError> {
    PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Suffix("_duration_seconds".into()),
            DURATION_SECONDS_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Full(names::E2E_LATENCY_SECONDS.into()),
            DURATION_SECONDS_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Full(names::SINK_BATCH_ROWS.into()),
            BATCH_ROWS_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Full(names::SINK_BATCH_BYTES.into()),
            BATCH_BYTES_BUCKETS,
        )
}

/// The handle from this process's successful [`install`]. Installation is
/// once-per-process (the recorder is global); later `install` calls reuse
/// this handle instead of failing, so assembly code can install the
/// exporter *before* pre-registering metric handles and the runtime's own
/// install becomes a no-op.
static INSTALLED: std::sync::OnceLock<MetricsHandle> = std::sync::OnceLock::new();

/// The settings of the first successful [`install`], kept so later calls
/// with different settings can warn that theirs are ignored.
static INSTALLED_SETTINGS: std::sync::OnceLock<MetricsSettings> = std::sync::OnceLock::new();

/// Install the configured exporter as this process's global recorder and
/// return the handle the admin server renders from.
///
/// **Call this before constructing any metric handle structs**
/// ([`SinkShardMetrics`](crate::metrics::SinkShardMetrics) and friends):
/// handles bind to the recorder present at construction, and handles built
/// earlier record into the void. Idempotent — a second call returns the
/// first call's handle (with a warning when the requested settings differ).
/// [`MetricsError::AlreadyInstalled`] is only returned when a *foreign*
/// global recorder (not installed through this function) already exists.
///
/// For [`Exporter::Prometheus`] this also registers the `process_*`
/// collector (CPU, memory, fds). No HTTP listener is spawned here — the
/// admin server owns the socket.
pub fn install(settings: &MetricsSettings) -> Result<MetricsHandle, MetricsError> {
    // Exporter::None installs no global recorder at all, so it neither
    // claims nor consults the once-per-process slot — a later Prometheus
    // install still works (and tests with metrics disabled stay isolated).
    if settings.exporter == Exporter::None {
        return Ok(MetricsHandle {
            inner: Inner::Noop,
            process: None,
        });
    }
    if let Some(existing) = INSTALLED.get() {
        if INSTALLED_SETTINGS
            .get()
            .is_some_and(|first| first != settings)
        {
            tracing::warn!(
                requested = ?settings,
                active = ?INSTALLED_SETTINGS.get(),
                "metrics exporter already installed with different settings; \
                 the first install's exporter stays in effect"
            );
        }
        return Ok(existing.clone());
    }
    let builder = configured_builder().map_err(|e| MetricsError::Build(e.to_string()))?;
    let handle = builder.install_recorder().map_err(|e| match e {
        BuildError::FailedToSetGlobalRecorder(_) => MetricsError::AlreadyInstalled,
        other => MetricsError::Build(other.to_string()),
    })?;
    let process = metrics_process::Collector::new("process_");
    process.describe();
    process.collect();
    let handle = MetricsHandle {
        inner: Inner::Prometheus(handle),
        process: Some(Arc::new(process)),
    };
    let _ = INSTALLED_SETTINGS.set(settings.clone());
    Ok(INSTALLED.get_or_init(|| handle).clone())
}

#[cfg(all(test, not(loom)))] // exporter internals (quanta) are loom-aware; not our model
mod tests {
    use super::*;
    use crate::error::ErrorClass;
    use crate::record::PartitionId;

    /// Build a local (non-global) recorder with the production bucket
    /// configuration, run `f` against it, and return the rendered output.
    fn render_with_local_recorder(f: impl FnOnce()) -> String {
        let recorder = configured_builder()
            .expect("bucket config must be valid")
            .build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, f);
        handle.run_upkeep();
        handle.render()
    }

    fn labels() -> ComponentLabels {
        ComponentLabels::new("orders", "orders_kafka", "kafka")
    }

    #[test]
    fn handle_structs_register_and_render_the_taxonomy() {
        let rendered = render_with_local_recorder(|| {
            let src = SourceMetrics::new(&labels(), true);
            src.batch(512, 131_072);
            src.poll_duration(Duration::from_millis(3));
            src.set_lag_max(42);
            src.set_partition_lag(PartitionId(7), 40);
            src.rebalance_assigned();
            src.set_lanes_active(4);

            let deser = DeserMetrics::new(&labels());
            deser.batch(510, 2, Duration::from_millis(1));
            deser.dropped(2);

            let op = OperatorMetrics::new(&labels());
            op.batch(510, 380, Duration::from_micros(600));
            op.filtered(130);
            op.errors(ErrorClass::RecordLevel, 1);

            let q = QueueMetrics::new(&labels(), "chain->sink/0", 4096);
            q.set_depth(17);
            q.full_events(1);

            let bp = BackpressureMetrics::new(&labels());
            bp.pause_started();
            bp.pause_ended(Duration::from_millis(250));
            bp.set_inflight_bytes(1 << 20);

            let shard = SinkShardMetrics::new(
                &labels(),
                3,
                &["ch-3-0".into(), "ch-3-1".into()],
                E2eBasis::Ingest,
            );
            shard.flushed(
                FlushReason::Rows,
                500_000,
                64 << 20,
                Duration::from_millis(90),
            );
            shard.retries(1);
            shard.errors(ErrorClass::Retryable, 1);
            shard.set_inflight(2);
            shard.set_replica_healthy(1, false);
            shard.breaker_opened(1);
            shard.replica_error(1);
            shard.set_shard_healthy(false);
            shard.abandoned(0);

            let cp = CheckpointMetrics::new(&labels(), false);
            cp.set_pending_max(12);
            cp.commit(true, Duration::from_millis(4));
            cp.set_watermark_age(Duration::from_secs(1));

            let coord = CoordinationMetrics::new(&labels());
            coord.set_splits_owned(3);
            coord.set_splits_completed(1);
            coord.set_splits_quarantined(1);
            coord.set_live_workers(2);
            coord.set_leader(true);
            coord.set_idle(false);
            coord.acquired(AcquireReason::Expired);
            coord.lost(SplitLossReason::Fenced);
            coord.released(1);
            coord.planned(8);
            coord.replan(ReplanOutcome::Noop, Duration::from_millis(20));
            coord.failed();
            coord.quarantined();
            coord.write(WriteOutcome::Conflict, Duration::from_millis(8));
            coord.reconcile(Duration::from_millis(15));
            coord.store_op(StoreOp::Put, Duration::from_micros(600));

            let pl = PipelineMetrics::new(&labels(), "0.1.0");
            pl.set_state(PipelineState::Running);
            pl.set_threads(4);
        });

        // Spot-check one series per stage, with labels.
        for needle in [
            r#"etl_source_records_total{pipeline="orders",component="orders_kafka",component_type="kafka"} 512"#,
            r#"partition="7""#,
            r#"etl_deser_records_total{pipeline="orders",component="orders_kafka",component_type="kafka",outcome="ok"} 510"#,
            r#"etl_operator_records_dropped_total{pipeline="orders",component="orders_kafka",component_type="kafka",reason="filtered"} 130"#,
            r#"etl_queue_capacity{pipeline="orders",component="orders_kafka",component_type="kafka",queue="chain->sink/0"} 4096"#,
            r#"etl_backpressure_pause_events_total{pipeline="orders",component="orders_kafka",component_type="kafka"} 1"#,
            r#"etl_sink_flushes_total{pipeline="orders",component="orders_kafka",component_type="kafka",shard="3",reason="rows"} 1"#,
            r#"etl_sink_replica_healthy{pipeline="orders",component="orders_kafka",component_type="kafka",shard="3",replica="ch-3-1"} 0"#,
            r#"etl_sink_replica_errors_total{pipeline="orders",component="orders_kafka",component_type="kafka",shard="3",replica="ch-3-1"} 1"#,
            r#"etl_sink_shard_healthy{pipeline="orders",component="orders_kafka",component_type="kafka",shard="3"} 0"#,
            r#"etl_checkpoint_commits_total{pipeline="orders",component="orders_kafka",component_type="kafka",outcome="ok"} 1"#,
            r#"etl_coordination_acquisitions_total{pipeline="orders",component="orders_kafka",component_type="kafka",reason="expired"} 1"#,
            r#"etl_coordination_replans_total{pipeline="orders",component="orders_kafka",component_type="kafka",outcome="noop"} 1"#,
            r#"etl_coordination_writes_total{pipeline="orders",component="orders_kafka",component_type="kafka",outcome="conflict"} 1"#,
            r#"etl_coordination_leader{pipeline="orders",component="orders_kafka",component_type="kafka"} 1"#,
            r#"etl_pipeline_state{pipeline="orders",component="orders_kafka",component_type="kafka",state="running"} 1"#,
            r#"etl_pipeline_info{pipeline="orders",component="orders_kafka",component_type="kafka",version="0.1.0"} 1"#,
        ] {
            assert!(
                rendered.contains(needle),
                "rendered output missing `{needle}`:\n{rendered}"
            );
        }
    }

    #[test]
    fn custom_meter_inherits_standard_labels_and_namespace() {
        let rendered = render_with_local_recorder(|| {
            // A connector owns the `kafka` namespace: local names are
            // auto-prefixed `etl_kafka_`.
            let meter = Meter::with_namespace("kafka", "orders", "orders_kafka", "kafka");
            meter
                .counter("schema_fetches_total", &[("registry", "prod".into())])
                .increment(3);
            meter.gauge("cache_entries", &[]).set(17.0);
            meter.histogram("fetch_duration_seconds", &[]).record(0.012);

            // A pipeline author's default scope lands under `etl_custom_`.
            Meter::new("orders", "enrich", "map")
                .counter("orders_enriched_total", &[])
                .increment(9);

            // The same scope also builds a framework stage handle.
            let deser = DeserMetrics::new(meter.labels());
            deser.batch(510, 0, Duration::from_millis(1));
        });

        for needle in [
            // Auto-prefixed name; standard labels first, then the extra label.
            r#"etl_kafka_schema_fetches_total{pipeline="orders",component="orders_kafka",component_type="kafka",registry="prod"} 3"#,
            r#"etl_kafka_cache_entries{pipeline="orders",component="orders_kafka",component_type="kafka"} 17"#,
            r#"etl_kafka_fetch_duration_seconds_bucket{pipeline="orders",component="orders_kafka",component_type="kafka""#,
            // The author's default scope uses the `etl_custom_` bucket.
            r#"etl_custom_orders_enriched_total{pipeline="orders",component="enrich",component_type="map"} 9"#,
            // The framework handle from the same Meter carries the same labels.
            r#"etl_deser_records_total{pipeline="orders",component="orders_kafka",component_type="kafka",outcome="ok"} 510"#,
        ] {
            assert!(
                rendered.contains(needle),
                "rendered output missing `{needle}`:\n{rendered}"
            );
        }
    }

    #[test]
    #[should_panic(expected = "reserved framework root")]
    fn custom_meter_rejects_reserved_namespace() {
        Meter::with_namespace("sink", "p", "c", "t");
    }

    #[test]
    #[should_panic(expected = "lowercase")]
    fn custom_meter_rejects_invalid_namespace() {
        Meter::with_namespace("Bad Name", "p", "c", "t");
    }

    #[test]
    #[should_panic(expected = "without the `etl_` prefix")]
    fn custom_meter_rejects_prefixed_local_name() {
        let _ = Meter::new("p", "c", "t").counter("etl_custom_hits_total", &[]);
    }

    #[test]
    #[should_panic(expected = "shadows a standard label")]
    fn custom_meter_rejects_shadowed_standard_label() {
        let _ = Meter::new("p", "c", "t").counter("hits_total", &[("component", "x".into())]);
    }

    #[test]
    #[should_panic(expected = "role segment")]
    fn custom_meter_rejects_role_prefixed_local_name() {
        // `sink_`/`source_` are reserved to the runtime's role scoping, so a
        // hand-written name starting with one can't alias a role-scoped family.
        let _ = Meter::new("p", "c", "t").counter("sink_writes_total", &[]);
    }

    #[test]
    fn for_component_scopes_by_role_and_gates_ineligible_types() {
        // A reserved component_type (an undeclared source's default) yields no
        // Meter rather than panicking.
        assert!(Meter::for_component("source", MetricRole::Source, "p", "c").is_none());
        assert!(Meter::for_component("sink", MetricRole::Sink, "p", "c").is_none());
        // The `custom` author bucket is off-limits to component scoping (a
        // sink's default `component_type`), so it too gets no Meter.
        assert!(Meter::for_component("custom", MetricRole::Sink, "p", "c").is_none());
        // A malformed component_type (a legal label, an illegal name segment)
        // yields None (a warning is logged, not asserted here).
        assert!(Meter::for_component("clickhouse-v2", MetricRole::Sink, "p", "c").is_none());
        assert!(Meter::for_component("", MetricRole::Source, "p", "c").is_none());

        let rendered = render_with_local_recorder(|| {
            Meter::for_component("kafka", MetricRole::Source, "orders", "orders_in")
                .expect("valid namespace")
                .counter("bytes_total", &[])
                .increment(10);
            Meter::for_component("clickhouse", MetricRole::Sink, "orders", "orders_out")
                .expect("valid namespace")
                .counter("bytes_total", &[])
                .increment(20);
        });
        // Role in the name; component_type is both namespace and label.
        assert!(rendered.contains(
            r#"etl_kafka_source_bytes_total{pipeline="orders",component="orders_in",component_type="kafka"} 10"#
        ));
        assert!(rendered.contains(
            r#"etl_clickhouse_sink_bytes_total{pipeline="orders",component="orders_out",component_type="clickhouse"} 20"#
        ));
    }

    #[test]
    fn duration_histograms_use_configured_buckets() {
        let rendered = render_with_local_recorder(|| {
            let src = SourceMetrics::new(&labels(), false);
            src.poll_duration(Duration::from_millis(3));
        });
        assert!(
            rendered.contains(r#"le="0.005""#),
            "expected a 5ms bucket boundary:\n{rendered}"
        );
        assert!(
            rendered.contains("etl_source_poll_duration_seconds_bucket"),
            "expected histogram exposition:\n{rendered}"
        );
    }

    #[test]
    fn per_partition_series_are_gated_and_retained() {
        let rendered = render_with_local_recorder(|| {
            let gated = SourceMetrics::new(&labels(), false);
            gated.set_partition_lag(PartitionId(1), 5);

            let detailed = CheckpointMetrics::new(&labels(), true);
            detailed.set_partition_pending(PartitionId(1), 5);
            detailed.set_partition_pending(PartitionId(2), 9);
            detailed.retain_partitions(&[PartitionId(2)]);
            // Re-set after retention: only partition 2 should re-register.
            detailed.set_partition_pending(PartitionId(2), 11);
        });
        let gated_series_leaked = rendered
            .lines()
            .any(|l| l.starts_with("etl_source_lag_records") && l.contains("partition="));
        assert!(
            !gated_series_leaked,
            "per-partition lag must be gated off:\n{rendered}"
        );
        assert!(rendered.contains(
            r#"etl_checkpoint_pending_batches{pipeline="orders",component="orders_kafka",component_type="kafka",partition="2"} 11"#
        ));
    }

    #[test]
    fn state_gauge_flips_exactly_one_state() {
        let rendered = render_with_local_recorder(|| {
            let pl = PipelineMetrics::new(&labels(), "0.1.0");
            pl.set_state(PipelineState::Draining);
        });
        assert!(rendered.contains(r#"state="draining"} 1"#));
        for other in ["starting", "running", "failed"] {
            assert!(
                rendered.contains(&format!(r#"state="{other}"}} 0"#)),
                "state `{other}` should read 0:\n{rendered}"
            );
        }
    }

    #[test]
    fn noop_exporter_renders_empty() {
        let handle = install(&MetricsSettings {
            exporter: Exporter::None,
            ..MetricsSettings::default()
        })
        .expect("noop install");
        assert_eq!(handle.render(), "");
        handle.upkeep_tick(); // must not panic
    }

    /// The single test that installs the process-global recorder: install,
    /// register, render, upkeep. Kept as ONE test because a global recorder
    /// can only be installed once per test process; all other tests use
    /// local recorders.
    #[test]
    fn install_prometheus_end_to_end() {
        let handle = install(&MetricsSettings::default()).expect("first install succeeds");

        let pl = PipelineMetrics::new(&labels(), "0.1.0");
        pl.set_threads(4);

        let rendered = handle.render();
        assert!(rendered.contains("etl_pipeline_info"));
        assert!(rendered.contains("etl_pipeline_threads"));
        assert!(
            rendered.contains("process_cpu_seconds_total"),
            "process collector wired:\n{rendered}"
        );

        handle.upkeep_tick();
        let render_fn = handle.render_fn();
        assert!(render_fn().contains("etl_pipeline_threads"));

        // Install is idempotent: a second call returns the SAME exporter,
        // so handles registered between the two calls stay visible. This is
        // the assembly-order guarantee: user code installs early, registers
        // sink handles, and the runtime's own install() reuses the exporter
        // (the flagship-example pattern).
        let shard = SinkShardMetrics::new(&labels(), 7, &["reuse-7-0".into()], E2eBasis::Ingest);
        shard.flushed(FlushReason::Rows, 10, 1_000, Duration::from_millis(3));
        shard.e2e_observed(Duration::from_millis(25), i64::MAX);
        let second = install(&MetricsSettings::default()).expect("second install reuses");
        let rendered = second.render();
        assert!(
            rendered.contains("etl_sink_records_total"),
            "handles registered before the second install render through it:\n{rendered}"
        );
        assert!(rendered.contains("etl_e2e_latency_seconds"));

        // Exporter::None never claims the process slot.
        let noop = install(&MetricsSettings {
            exporter: Exporter::None,
            ..MetricsSettings::default()
        })
        .expect("noop install");
        assert!(noop.render().is_empty());
        assert!(
            install(&MetricsSettings::default())
                .expect("prometheus still reusable")
                .render()
                .contains("etl_pipeline_info")
        );
    }
}
