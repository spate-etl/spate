//! Process assembly: threads, runtimes, observability, and the run loop.

use super::controller::{ControllerContext, ControllerSignal, run_controller};
use super::driver::{DriverContext, DriverParams, run_driver};
use super::{DriverEvent, ExitReport, ExitState, FatalErrorReport, SinkRuntime, ThreadControl};
use crate::admin::{AdminServer, HealthState, HealthThresholds};
use crate::backpressure::{BackpressureParams, InflightBudget, WatermarkController};
use crate::checkpoint::Checkpointer;
use crate::config::{MetricsExporter, PinningMode, PipelineConfig};
use crate::metrics::{
    self, BackpressureMetrics, CheckpointMetrics, ComponentLabels, E2eBasis, Exporter,
    MetricsSettings, PipelineMetrics, PipelineState, SourceMetrics,
};
use crate::ops::RunnableChain;
use crate::source::Source;
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
        }
    }

    /// Override runtime options (returns `self` for chaining).
    #[must_use]
    pub fn with_options(mut self, options: RuntimeOptions) -> Self {
        self.options = options;
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
        let handle = match metrics::install(&map_settings(&self.config)) {
            Ok(h) => h,
            Err(metrics::MetricsError::AlreadyInstalled) => {
                tracing::warn!(
                    "a metrics recorder is already installed; continuing \
                     with the existing one and a detached render handle"
                );
                metrics::install(&MetricsSettings {
                    exporter: Exporter::None,
                    ..map_settings(&self.config)
                })
                .map_err(|e| StartError::Metrics(e.to_string()))?
            }
            Err(e) => return Err(StartError::Metrics(e.to_string())),
        };

        let runtime_labels = ComponentLabels::new(pipeline_name.clone(), "runtime", "pipeline");
        let pipeline_metrics = PipelineMetrics::new(&runtime_labels, &self.options.version);
        pipeline_metrics.set_state(PipelineState::Starting);
        pipeline_metrics.set_threads(threads);

        let health = HealthState::new(threads, HealthThresholds::default());

        // I/O runtime: sink workers (spawned by the caller-built SinkPool
        // onto this runtime via its own handle), admin server, upkeep,
        // signals.
        let io = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(self.config.pipeline.io_threads)
            .thread_name("etl-io")
            .enable_all()
            .build()?;

        let admin = io.block_on(AdminServer::bind(
            self.config.metrics.listen,
            handle.render_fn(),
            Arc::clone(&health),
        ))?;
        let (admin_stop_tx, admin_stop_rx) = tokio::sync::watch::channel(false);
        io.spawn(admin.run(admin_stop_rx));
        {
            // spawn_upkeep uses tokio::spawn internally; enter the runtime.
            let _guard = io.enter();
            let _upkeep = handle.spawn_upkeep(Duration::from_secs(5));
        }

        if self.options.handle_signals {
            let shutdown = Arc::clone(&self.shutdown);
            io.spawn(async move {
                wait_for_signal().await;
                tracing::info!("shutdown signal received; draining");
                shutdown.store(true, Ordering::Relaxed);
            });
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

        if self.config.pipeline.pinning == PinningMode::Compact {
            tracing::warn!(
                "pipeline.pinning=compact requested; core pinning is not \
                 wired yet and threads run unpinned"
            );
        }

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
            };
            let handle = std::thread::Builder::new()
                .name(format!("etl-pipeline-{i}"))
                .spawn(move || run_driver(ctx))?;
            driver_handles.push(handle);
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
        let controller_handle = std::thread::Builder::new()
            .name("etl-controller".into())
            .spawn(move || run_controller(controller_ctx))?;

        // Main: wait for the controller's choreography.
        let mut sink_drain = None;
        let mut driver_panic: Option<FatalErrorReport> = None;
        let sink_runtime = self.sink;
        let mut drain_fn = Some(sink_runtime.drain);
        drop(sink_runtime.queues);

        let (mut state, final_watermarks) = loop {
            match to_main_rx.recv() {
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
                Err(_) => {
                    // Controller died without a report: fail loudly.
                    break (
                        ExitState::Failed(FatalErrorReport {
                            component: "controller".into(),
                            reason: "controller thread exited without a report".into(),
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

fn map_settings(config: &PipelineConfig) -> MetricsSettings {
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
