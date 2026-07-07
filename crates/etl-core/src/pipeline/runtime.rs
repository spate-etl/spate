//! Process assembly: threads, runtimes, observability, and the run loop.

use super::controller::{ControllerContext, ControllerSignal, run_controller};
use super::driver::{DriverContext, DriverExit, DriverParams, run_driver};
use super::{DriverEvent, ExitReport, ExitState, FatalErrorReport, SinkRuntime, ThreadControl};
use crate::admin::{AdminServer, HealthState, HealthThresholds};
use crate::backpressure::{BackpressureParams, InflightBudget, WatermarkController};
use crate::checkpoint::Checkpointer;
use crate::config::{MetricsExporter, PinningMode, PipelineConfig};
use crate::metrics::{
    self, BackpressureMetrics, CheckpointMetrics, ComponentLabels, E2eBasis, Exporter,
    MetricsHandle, MetricsSettings, PipelineMetrics, PipelineState, SourceMetrics,
};
use crate::ops::RunnableChain;
use crate::source::{DrainBarrier, Source};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Knobs that are not part of the user-facing YAML (loop granularities,
/// test hooks). The defaults suit production; tests shrink the timings.
#[derive(Clone, Debug)]
pub struct RuntimeOptions {
    /// Install SIGTERM/SIGINT handlers that trigger a graceful drain.
    /// Disable in tests and drive [`ShutdownHandle`] instead.
    pub handle_signals: bool,
    /// Max payloads per lane poll.
    pub max_records: usize,
    /// Lane poll timeout (also the paused/idle loop sleep).
    pub poll_timeout: Duration,
    /// Flush the chain after this long without new data.
    pub idle_flush: Duration,
    /// Sleep between retries of a blocked batch.
    pub blocked_retry: Duration,
    /// Controller `poll_events` timeout.
    pub event_poll_timeout: Duration,
    /// Version string published on `etl_pipeline_info`.
    pub version: String,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        RuntimeOptions {
            handle_signals: true,
            max_records: 512,
            poll_timeout: Duration::from_millis(10),
            idle_flush: Duration::from_millis(100),
            blocked_retry: Duration::from_millis(2),
            event_poll_timeout: Duration::from_millis(50),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// Triggers a graceful drain from anywhere (tests, custom signal wiring).
#[derive(Clone, Debug)]
pub struct ShutdownHandle(Arc<AtomicBool>);

impl ShutdownHandle {
    /// Begin the drain. Idempotent.
    pub fn trigger(&self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// The pipeline could not start.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StartError {
    /// Invalid effective configuration.
    #[error("invalid runtime configuration: {0}")]
    Config(String),
    /// The metrics exporter could not be installed.
    #[error("metrics: {0}")]
    Metrics(String),
    /// The I/O runtime or the admin server could not start.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// One pipeline process: source, per-thread chains, and a sink, assembled
/// per `docs/DESIGN.md` § Process anatomy.
///
/// The caller creates the shared [`InflightBudget`] first and wires it into
/// the chain terminals (which `add` on enqueue) and the sink workers (which
/// `sub` on durable write or abandonment) before handing everything here.
pub struct PipelineRuntime<S: Source> {
    config: PipelineConfig,
    source: S,
    chains: Box<dyn FnMut(usize) -> Box<dyn RunnableChain> + Send>,
    sink: SinkRuntime,
    budget: Arc<InflightBudget>,
    shutdown: Arc<AtomicBool>,
    options: RuntimeOptions,
    io: Option<tokio::runtime::Runtime>,
}

impl<S: Source> std::fmt::Debug for PipelineRuntime<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineRuntime")
            .field("pipeline", &self.config.pipeline.name)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl<S: Source + 'static> PipelineRuntime<S> {
    /// Assemble a runtime. `chains` builds one erased chain per pipeline
    /// thread (thread index in).
    pub fn new(
        config: PipelineConfig,
        source: S,
        chains: impl FnMut(usize) -> Box<dyn RunnableChain> + Send + 'static,
        sink: SinkRuntime,
        budget: Arc<InflightBudget>,
    ) -> Self {
        PipelineRuntime {
            config,
            source,
            chains: Box::new(chains),
            sink,
            budget,
            shutdown: Arc::new(AtomicBool::new(false)),
            options: RuntimeOptions::default(),
            io: None,
        }
    }

    /// Override runtime options (returns `self` for chaining).
    #[must_use]
    pub fn with_options(mut self, options: RuntimeOptions) -> Self {
        self.options = options;
        self
    }

    /// Use a caller-owned tokio runtime as the I/O runtime instead of
    /// building one inside [`run`](Self::run) — for assemblies whose
    /// connectors needed a handle before the runtime existed (sink workers
    /// spawned at construction, schema-registry fetchers, async pre-flight
    /// validation). `run` shuts it down on exit exactly as it does the
    /// internally built one; connector tasks spawned on it earlier keep
    /// running until then. Without this, assemblies end up running a
    /// second runtime, doubling `pipeline.io_threads`.
    #[must_use]
    pub fn with_io_runtime(mut self, io: tokio::runtime::Runtime) -> Self {
        self.io = Some(io);
        self
    }

    /// A handle that triggers a graceful drain.
    #[must_use]
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle(Arc::clone(&self.shutdown))
    }

    /// Effective pipeline thread count: the config override, else
    /// `available_parallelism` minus the I/O reserve (I/O workers + the
    /// controller), at least 1. `available_parallelism` respects cgroup
    /// CPU quotas, so Kubernetes limits size this correctly; pods without
    /// limits see the node's cores — set `pipeline.threads` explicitly
    /// there.
    fn thread_count(&self) -> usize {
        self.config.pipeline.threads.unwrap_or_else(|| {
            let cores = std::thread::available_parallelism().map_or(2, usize::from);
            cores
                .saturating_sub(self.config.pipeline.io_threads + 1)
                .max(1)
        })
    }

    /// Run the pipeline to completion (blocking). Returns when the
    /// pipeline drained after a shutdown trigger/signal or failed.
    pub fn run(mut self) -> Result<ExitReport, StartError> {
        let threads = self.thread_count();
        if threads == 0 || self.config.pipeline.io_threads == 0 {
            return Err(StartError::Config("thread counts must be non-zero".into()));
        }
        let pipeline_name = self.config.pipeline.name.clone();

        // Observability first: everything after this records metrics.
        let handle = install_or_reuse(&metrics_settings(&self.config))?;

        let runtime_labels = ComponentLabels::new(pipeline_name.clone(), "runtime", "pipeline");
        let pipeline_metrics = PipelineMetrics::new(&runtime_labels, &self.options.version);
        pipeline_metrics.set_state(PipelineState::Starting);
        pipeline_metrics.set_threads(threads);

        let health = HealthState::new(threads, HealthThresholds::default());

        // I/O runtime: sink workers (spawned by the caller-built SinkPool
        // onto this runtime via its own handle), admin server, upkeep,
        // signals. A caller-owned runtime (`with_io_runtime`) is adopted
        // instead of built; either way this function owns its shutdown.
        let io = match self.io.take() {
            Some(io) => io,
            None => tokio::runtime::Builder::new_multi_thread()
                .worker_threads(self.config.pipeline.io_threads)
                .thread_name("etl-io")
                .enable_all()
                .build()?,
        };

        // Admin bind, upkeep, and the controller thread are started *after*
        // the driver threads (below) so a failure in any of them can stop
        // the already-running drivers instead of leaking them.

        if self.options.handle_signals {
            let shutdown = Arc::clone(&self.shutdown);
            io.spawn(async move {
                wait_for_signal().await;
                tracing::info!("shutdown signal received; draining");
                shutdown.store(true, Ordering::Relaxed);
            });
        }

        // Sink readiness: probe at startup and periodically (tighter while
        // failing), driving the sinks-connected half of `/readyz`. No probe
        // hook means nothing to check — report connected.
        match self.sink.probe.take() {
            Some(probe) => {
                let health_probe = Arc::clone(&health);
                io.spawn(async move {
                    loop {
                        let connected = match probe().await {
                            Ok(()) => true,
                            Err(e) => {
                                tracing::warn!(error = %e, "sink probe failed");
                                false
                            }
                        };
                        health_probe.set_sinks_connected(connected);
                        let recheck = if connected {
                            Duration::from_secs(30)
                        } else {
                            Duration::from_secs(5)
                        };
                        tokio::time::sleep(recheck).await;
                    }
                });
            }
            None => health.set_sinks_connected(true),
        }

        // Wiring.
        let (events_tx, events_rx) = crossbeam_channel::unbounded::<DriverEvent>();
        let (to_main_tx, to_main_rx) = crossbeam_channel::unbounded::<ControllerSignal>();
        let (sink_drained_tx, sink_drained_rx) = crossbeam_channel::unbounded::<()>();
        let checkpointer = Checkpointer::new();

        let bp_params = BackpressureParams::from_budget(
            usize::try_from(self.config.backpressure.max_inflight_bytes.as_u64())
                .unwrap_or(usize::MAX),
            self.config.backpressure.high_ratio,
            self.config.backpressure.low_ratio,
            self.config.backpressure.min_pause,
        );

        // Compact pinning: thread i on core i, low cores first, leaving the
        // remaining cores for the I/O runtime and librdkafka's threads.
        // Note for Kubernetes: exclusive cores require the kubelet static
        // CPU manager with Guaranteed QoS and integer CPU requests;
        // otherwise pinning only sets affinity within the shared cpuset.
        let core_ids: Vec<Option<core_affinity::CoreId>> =
            if self.config.pipeline.pinning == PinningMode::Compact {
                let mut ids = core_affinity::get_core_ids().unwrap_or_default();
                ids.sort_by_key(|c| c.id);
                if ids.len() < threads {
                    tracing::warn!(
                        cores = ids.len(),
                        threads,
                        "fewer cores than pipeline threads; surplus threads run unpinned"
                    );
                }
                (0..threads).map(|i| ids.get(i).copied()).collect()
            } else {
                vec![None; threads]
            };

        // Short grace for cleanup-time driver stops (startup errors, a
        // controller panic): the same budget the drain barrier uses.
        let drain_timeout = self.config.checkpoint.drain_timeout;

        let mut control_txs = Vec::with_capacity(threads);
        let mut driver_handles = Vec::with_capacity(threads);
        for i in 0..threads {
            let (control_tx, control_rx) = crossbeam_channel::unbounded::<ThreadControl<S::Lane>>();
            control_txs.push(control_tx);
            let ctx = DriverContext {
                params: DriverParams {
                    thread: i,
                    max_records: self.options.max_records,
                    poll_timeout: self.options.poll_timeout,
                    idle_flush: self.options.idle_flush,
                    blocked_retry: self.options.blocked_retry,
                    queue_low_ratio: self.config.backpressure.low_ratio,
                },
                control: control_rx,
                events: events_tx.clone(),
                chain: (self.chains)(i),
                bp: WatermarkController::new(bp_params),
                budget: Arc::clone(&self.budget),
                queues: self.sink.queues.clone(),
                health: Arc::clone(&health),
                bp_metrics: BackpressureMetrics::new(&ComponentLabels::new(
                    pipeline_name.clone(),
                    format!("driver-{i}"),
                    "driver",
                )),
                source_metrics: SourceMetrics::new(
                    &ComponentLabels::new(pipeline_name.clone(), "source", "source"),
                    self.config.metrics.per_partition_detail,
                ),
                shutdown: Arc::clone(&self.shutdown),
            };
            let core = core_ids.get(i).copied().flatten();
            let spawned = std::thread::Builder::new()
                .name(format!("etl-pipeline-{i}"))
                .spawn(move || {
                    if let Some(core) = core
                        && !core_affinity::set_for_current(core)
                    {
                        tracing::warn!(core = core.id, "failed to pin pipeline thread");
                    }
                    run_driver(ctx)
                });
            match spawned {
                Ok(handle) => driver_handles.push(handle),
                Err(e) => {
                    // Stop the drivers already spawned before bailing out.
                    stop_drivers(&self.shutdown, &control_txs, driver_handles, drain_timeout);
                    return Err(StartError::Io(e));
                }
            }
        }

        // The chain factory has served its purpose. Factories naturally
        // capture ShardQueues clones (their terminals need them), and the
        // sink only drains once every queue clone is gone — holding the
        // factory through the drain would deadlock shutdown.
        drop(self.chains);

        // A cloned set of driver control senders kept by main, so it can stop
        // the drivers itself if a later startup step fails or the controller
        // thread dies (the originals are moved into the controller below).
        let control_txs_for_stop = control_txs.clone();

        // Admin bind now that the drivers are live: a bind failure (e.g. the
        // metrics port is taken) stops them instead of leaking them.
        let admin = match io.block_on(AdminServer::bind(
            self.config.metrics.listen,
            handle.render_fn(),
            Arc::clone(&health),
        )) {
            Ok(admin) => admin,
            Err(e) => {
                stop_drivers(
                    &self.shutdown,
                    &control_txs_for_stop,
                    driver_handles,
                    drain_timeout,
                );
                return Err(StartError::Io(e));
            }
        };
        let (admin_stop_tx, admin_stop_rx) = tokio::sync::watch::channel(false);
        io.spawn(admin.run(admin_stop_rx));
        {
            // spawn_upkeep uses tokio::spawn internally; enter the runtime.
            let _guard = io.enter();
            let _upkeep = handle.spawn_upkeep(Duration::from_secs(5));
        }

        let controller_ctx = ControllerContext {
            source: self.source,
            checkpointer,
            control_txs,
            events_rx,
            to_main: to_main_tx,
            sink_drained_rx,
            shutdown: Arc::clone(&self.shutdown),
            health: Arc::clone(&health),
            commit_interval: self.config.checkpoint.interval,
            drain_timeout: self.config.checkpoint.drain_timeout,
            event_poll_timeout: self.options.event_poll_timeout,
            max_pending_batches: self.config.checkpoint.max_pending_batches,
            stalled_fail_after: self.config.checkpoint.stalled_fail_after,
            checkpoint_metrics: CheckpointMetrics::new(
                &ComponentLabels::new(pipeline_name.clone(), "checkpoint", "checkpoint"),
                self.config.metrics.per_partition_detail,
            ),
            source_metrics: SourceMetrics::new(
                &ComponentLabels::new(pipeline_name.clone(), "source", "source"),
                self.config.metrics.per_partition_detail,
            ),
            pipeline_metrics,
        };
        let controller_handle = match std::thread::Builder::new()
            .name("etl-controller".into())
            .spawn(move || run_controller(controller_ctx))
        {
            Ok(handle) => handle,
            Err(e) => {
                stop_drivers(
                    &self.shutdown,
                    &control_txs_for_stop,
                    driver_handles,
                    drain_timeout,
                );
                return Err(StartError::Io(e));
            }
        };

        // Main: wait for the controller's choreography.
        let mut sink_drain = None;
        let mut driver_panic: Option<FatalErrorReport> = None;
        let sink_runtime = self.sink;
        let mut drain_fn = Some(sink_runtime.drain);
        drop(sink_runtime.queues);

        let (mut state, final_watermarks) = loop {
            match to_main_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(ControllerSignal::LanesDrained { sink_deadline }) => {
                    for (i, h) in driver_handles.drain(..).enumerate() {
                        if h.join().is_err() {
                            driver_panic.get_or_insert(FatalErrorReport {
                                component: format!("driver-{i}"),
                                reason: "pipeline thread panicked outside the batch guard".into(),
                            });
                        }
                    }
                    if let Some(drain) = drain_fn.take() {
                        let budget = sink_deadline.saturating_duration_since(Instant::now());
                        sink_drain = Some(io.block_on(drain(budget)));
                    }
                    let _ = sink_drained_tx.send(());
                }
                Ok(ControllerSignal::Finished(report)) => {
                    break (report.state, report.final_watermarks);
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout)
                    if !controller_handle.is_finished() =>
                {
                    // Controller still working; keep waiting on the 100ms tick.
                }
                Err(_) => {
                    // The controller thread ended without a Finished report
                    // (a timeout with a finished handle, or the signal
                    // channel disconnected): it panicked. It never told the
                    // drivers to stop and never set the shutdown flag, so an
                    // untimed join here would wedge forever — stop them
                    // ourselves, drain the sink, and fail the run.
                    stop_drivers(
                        &self.shutdown,
                        &control_txs_for_stop,
                        std::mem::take(&mut driver_handles),
                        drain_timeout,
                    );
                    if let Some(drain) = drain_fn.take() {
                        sink_drain = Some(io.block_on(drain(drain_timeout)));
                    }
                    break (
                        ExitState::Failed(FatalErrorReport {
                            component: "controller".into(),
                            reason: "controller thread panicked".into(),
                        }),
                        Vec::new(),
                    );
                }
            }
        };
        // Drivers are already joined on the drain path; on the
        // controller-died path make a best effort not to leak them.
        for h in driver_handles {
            let _ = h.join();
        }
        // A driver that panicked outside the batch guard is a bug worth
        // failing the run over, even if the drain otherwise completed.
        if let (ExitState::Completed, Some(report)) = (&state, driver_panic) {
            state = ExitState::Failed(report);
        }

        let _ = admin_stop_tx.send(true);
        io.shutdown_timeout(Duration::from_secs(2));
        let _ = controller_handle.join();

        Ok(ExitReport {
            state,
            sink_drain,
            final_watermarks,
        })
    }
}

/// Set the shutdown flag, tell every driver thread to stop within a bounded
/// drain barrier, and join them. Shared by the startup-error paths and the
/// controller-death path so an early failure or a controller panic never
/// leaves running pinned pipeline threads behind (`run` is a library API).
///
/// Joining is what actually bounds this — a driver observes the shutdown
/// flag (abandoning any blocked batch) and the `Shutdown` control message
/// (flushing within `grace`), then exits and drops its chain, closing the
/// shard queues so the sink can drain afterwards.
fn stop_drivers<L>(
    shutdown: &AtomicBool,
    control_txs: &[crossbeam_channel::Sender<ThreadControl<L>>],
    driver_handles: Vec<std::thread::JoinHandle<DriverExit>>,
    grace: Duration,
) {
    shutdown.store(true, Ordering::Relaxed);
    let deadline = Instant::now() + grace;
    let barrier = DrainBarrier::new(control_txs.len());
    for tx in control_txs {
        let _ = tx.send(ThreadControl::Shutdown {
            barrier: barrier.clone(),
            deadline,
        });
    }
    for handle in driver_handles {
        let _ = handle.join();
    }
}

/// Install the exporter, degrading gracefully when a foreign recorder
/// already owns the process: the pipeline keeps running against the
/// existing recorder with a detached (empty-rendering) handle for the
/// admin server. Shared by the runtime and the pipeline builder.
pub(crate) fn install_or_reuse(settings: &MetricsSettings) -> Result<MetricsHandle, StartError> {
    match metrics::install(settings) {
        Ok(h) => Ok(h),
        Err(metrics::MetricsError::AlreadyInstalled) => {
            tracing::warn!(
                "a metrics recorder is already installed; continuing \
                 with the existing one and a detached render handle"
            );
            metrics::install(&MetricsSettings {
                exporter: Exporter::None,
                ..settings.clone()
            })
            .map_err(|e| StartError::Metrics(e.to_string()))
        }
        Err(e) => Err(StartError::Metrics(e.to_string())),
    }
}

/// The [`MetricsSettings`] a pipeline configuration maps to.
///
/// Assemblies that pre-register metric handles (sink shard metrics, custom
/// metrics) should call
/// [`metrics::install`](crate::metrics::install)`(&metrics_settings(&config))`
/// **before** constructing them; the runtime's own install then reuses the
/// exporter. Handles built before any install bind to the no-op recorder
/// and render nothing.
#[must_use]
pub fn metrics_settings(config: &PipelineConfig) -> MetricsSettings {
    MetricsSettings {
        exporter: match config.metrics.exporter {
            MetricsExporter::Prometheus => Exporter::Prometheus,
            MetricsExporter::None => Exporter::None,
        },
        listen: config.metrics.listen,
        per_partition_detail: config.metrics.per_partition_detail,
        e2e_basis: match config.metrics.e2e_basis {
            crate::config::E2eBasis::Ingest => E2eBasis::Ingest,
            crate::config::E2eBasis::Event => E2eBasis::Event,
        },
    }
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        let mut term =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "failed to install SIGTERM handler");
                    std::future::pending::<()>().await;
                    return;
                }
            };
        tokio::select! {
            _ = term.recv() => {}
            r = tokio::signal::ctrl_c() => {
                if let Err(e) = r {
                    tracing::error!(error = %e, "ctrl_c handler failed");
                    std::future::pending::<()>().await;
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %e, "ctrl_c handler failed");
            std::future::pending::<()>().await;
        }
    }
}
