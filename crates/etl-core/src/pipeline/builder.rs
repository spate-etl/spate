//! The pipeline builder: the primary assembly path.
//!
//! [`Pipeline::from_config`] owns startup initialization — telemetry, the
//! metrics exporter, and the shared I/O runtime — so holding a `Pipeline`
//! *guarantees* a live recorder: every metric handle built afterwards
//! (framework or custom) is live, and connectors get an I/O handle before
//! any thread spawns. The builder is a thin composition of the public
//! primitives it replaces; nothing here is required — the desugaring below
//! remains a fully supported assembly path.
//!
//! The shape of an assembly (illustrative — connector construction elided;
//! see the `etl` crate's examples for complete, compiling binaries):
//!
//! ```ignore
//! let pipeline = Pipeline::from_path(Path::new("pipeline.yaml"))?;
//! let source = MySource::from_component_config(&pipeline.config().source)?;
//! let sink = my_connector::from_component_config(&pipeline.config().sink)?;
//! let report = pipeline
//!     .sink(sink)?
//!     .chains(move |ctx| {
//!         chain_owned::<Row, _>(deserializer.clone())
//!             .with_metrics(ctx.pipeline, "main")
//!             .sink(encoder.clone(), KeyHashRouter, ChunkConfig::default(),
//!                   ctx.queues, ctx.budget)
//!             .build()
//!     })
//!     .run(source)?;
//! report.log();
//! std::process::exit(report.exit_code());
//! ```
//!
//! # Desugaring
//!
//! Each builder step is a direct lift of the manual assembly it replaces
//! (all of it public API):
//!
//! | Builder | Primitives |
//! |---|---|
//! | `from_config(config)` | [`telemetry::init`](crate::telemetry::init) → [`metrics::install`](crate::metrics::install)`(&`[`metrics_settings`](crate::pipeline::metrics_settings)`(&config))` → `tokio::runtime::Builder` (`io_threads` workers) → [`InflightBudget::new`](crate::backpressure::InflightBudget::new) |
//! | `.sink(bundle)` | [`SinkBundle::into_parts`](crate::sink::SinkBundle::into_parts) → [`shard_queues`](crate::sink::shard_queues) → [`SinkShardMetrics::new`](crate::metrics::SinkShardMetrics::new) per shard → [`SinkPool::spawn`](crate::sink::SinkPool::spawn) → a boxed drain closure |
//! | `.chains(f)` | the factory handed to [`PipelineRuntime::new`], with queue/budget/name plumbing pre-threaded per call |
//! | `.into_runtime(source)` / `.run(source)` | [`PipelineRuntime::new`]`(config, source, factory, `[`SinkRuntime`]`{..}, budget)` + [`PipelineRuntime::with_io_runtime`] |
//!
//! # Shutdown and drop ordering
//!
//! The sink only drains once every [`ShardQueues`] clone is gone. The
//! builder discharges this structurally: it never exposes the queues
//! outside the chain factory — each factory call receives a fresh clone in
//! its [`ChainCtx`], which the chain's terminal stage consumes and drops
//! with the driver threads, and the wrapper factory itself is dropped by
//! the runtime before the drain. Do not smuggle `ctx.queues` into
//! long-lived state outside the returned chain; a clone that outlives the
//! drivers turns a graceful drain into a deadline-bounded abandon.

use super::SinkRuntime;
use super::runtime::{
    PipelineRuntime, RuntimeOptions, StartError, install_or_reuse, metrics_settings,
};
use crate::backpressure::InflightBudget;
use crate::config::{ConfigError, PipelineConfig};
use crate::metrics::{ComponentLabels, MetricsHandle, SinkShardMetrics};
use crate::ops::RunnableChain;
use crate::pipeline::ExitReport;
use crate::sink::{ShardQueues, SinkBundle, SinkDrainFn, SinkPool, SinkProbeFn, shard_queues};
use crate::source::Source;
use crate::telemetry::{self, LogFormat};
use std::path::Path;
use std::sync::Arc;

/// Error assembling a pipeline (cold path, before anything runs).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BuildError {
    /// The configuration failed to load or validate.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// The metrics exporter failed to install.
    #[error("metrics: {0}")]
    Metrics(String),
    /// The I/O runtime failed to build.
    #[error("io runtime: {0}")]
    Io(#[from] std::io::Error),
    /// The sink bundle's topology or labels are unusable.
    #[error("sink: {0}")]
    Sink(String),
    /// [`Pipeline::sink`] was called twice.
    #[error("a sink is already installed")]
    SinkAlreadySet,
    /// [`Pipeline::into_runtime`]/[`Pipeline::run`] without a sink.
    #[error("no sink installed (call Pipeline::sink first)")]
    MissingSink,
    /// [`Pipeline::into_runtime`]/[`Pipeline::run`] without a chain factory.
    #[error("no chain factory installed (call Pipeline::chains first)")]
    MissingChains,
    /// The builder was constructed inside an async runtime. It owns a
    /// blocking tokio runtime (dropping or `block_on`-ing one inside async
    /// context panics), so build pipelines from a plain thread — usually
    /// `main`.
    #[error(
        "Pipeline::from_config must be called outside any async runtime \
         (it owns a blocking tokio runtime)"
    )]
    AsyncContext,
}

/// Error from [`Pipeline::run`]: assembly or startup failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PipelineError {
    /// The pipeline could not be assembled.
    #[error(transparent)]
    Build(#[from] BuildError),
    /// The assembled pipeline failed to start.
    #[error(transparent)]
    Start(#[from] StartError),
}

/// Per-thread wiring handed to the chain factory — everything the terminal
/// [`.sink(...)`](crate::ops::ChainBuilder) stage needs, so assemblies stop
/// threading queues, budget, and the pipeline name by hand.
///
/// Passed by value, once per pipeline thread; move the fields into the
/// chain being built. Deliberately not `Clone` — see the module docs on
/// drop ordering.
#[derive(Debug)]
#[non_exhaustive]
pub struct ChainCtx {
    /// Zero-based pipeline thread index.
    pub thread: usize,
    /// This thread's clone of the shard-queue senders.
    pub queues: ShardQueues,
    /// The shared in-flight byte budget.
    pub budget: Arc<InflightBudget>,
    /// The pipeline name — [`ChainBuilder::with_metrics`](crate::ops::ChainBuilder::with_metrics)'s
    /// first argument.
    pub pipeline: String,
}

/// Sink wiring knobs that live outside connector config.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SinkOptions {
    /// Per-shard chunk queue capacity, in chunks. The default suits most
    /// pipelines; see `docs/DESIGN.md` § Backpressure for the sizing rule.
    pub queue_capacity: usize,
}

impl SinkOptions {
    /// Override the per-shard queue capacity. (`SinkOptions` is
    /// `#[non_exhaustive]`, so construct via `default()` + `with_*`.)
    #[must_use]
    pub fn with_queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity;
        self
    }
}

impl Default for SinkOptions {
    fn default() -> Self {
        SinkOptions { queue_capacity: 8 }
    }
}

struct SinkAssembly {
    queues: ShardQueues,
    drain: SinkDrainFn,
    probe: Option<SinkProbeFn>,
}

type ChainFactoryFn = Box<dyn FnMut(ChainCtx) -> Box<dyn RunnableChain> + Send>;

/// The pipeline builder — see the [module docs](self) for the full picture.
///
/// Non-generic, nameable, and storable: the source type enters only at the
/// terminal [`into_runtime`](Self::into_runtime)/[`run`](Self::run) call.
pub struct Pipeline {
    config: PipelineConfig,
    metrics: MetricsHandle,
    io: tokio::runtime::Runtime,
    budget: Arc<InflightBudget>,
    sink: Option<SinkAssembly>,
    chains: Option<ChainFactoryFn>,
    options: RuntimeOptions,
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline")
            .field("pipeline", &self.config.pipeline.name)
            .field("sink", &self.sink.is_some())
            .field("chains", &self.chains.is_some())
            .finish_non_exhaustive()
    }
}

impl Pipeline {
    /// Load configuration from a YAML file and initialize the process; see
    /// [`from_config`](Self::from_config).
    pub fn from_path(path: &Path) -> Result<Self, BuildError> {
        Self::from_config(PipelineConfig::from_path(path)?)
    }

    /// Initialize the process from an already-loaded configuration:
    ///
    /// 1. **Telemetry** — [`telemetry::init`]`(Json, "info")`. Idempotent:
    ///    to customize the format or filter, call [`telemetry::init`]
    ///    yourself *first* (the binaries-init convention).
    /// 2. **Metrics exporter** — installed from the config's `metrics`
    ///    section before you can construct any handle, so every handle
    ///    built while holding the `Pipeline` is live. When a foreign
    ///    recorder already owns the process, the pipeline continues
    ///    against it with a warning.
    /// 3. **The I/O runtime** — `pipeline.io_threads` workers, thread name
    ///    `etl-io`. Connectors that need a handle before `run` (schema
    ///    fetchers, async pre-flight validation) use
    ///    [`io_handle`](Self::io_handle)/[`block_on`](Self::block_on).
    ///
    /// # Errors
    ///
    /// [`BuildError::AsyncContext`] when called from inside an async
    /// runtime — build pipelines from a plain thread, usually `main`.
    pub fn from_config(config: PipelineConfig) -> Result<Self, BuildError> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(BuildError::AsyncContext);
        }
        if config.pipeline.io_threads == 0 {
            return Err(BuildError::Config(ConfigError::Validation(
                "pipeline.io_threads must be non-zero".into(),
            )));
        }
        telemetry::init(LogFormat::Json, "info");
        let metrics = install_or_reuse(&metrics_settings(&config)).map_err(|e| match e {
            StartError::Metrics(m) => BuildError::Metrics(m),
            other => BuildError::Metrics(other.to_string()),
        })?;
        let io = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(config.pipeline.io_threads)
            .thread_name("etl-io")
            .enable_all()
            .build()?;
        Ok(Pipeline {
            config,
            metrics,
            io,
            budget: Arc::new(InflightBudget::new()),
            sink: None,
            chains: None,
            options: RuntimeOptions::default(),
        })
    }

    /// The loaded configuration — connector sections (`config().source`,
    /// `.deserializer`, `.sink`) still belong to the caller's connector
    /// factories.
    #[must_use]
    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    /// The installed exporter's handle (rendering, upkeep).
    #[must_use]
    pub fn metrics(&self) -> &MetricsHandle {
        &self.metrics
    }

    /// The shared in-flight byte budget.
    #[must_use]
    pub fn budget(&self) -> &Arc<InflightBudget> {
        &self.budget
    }

    /// A handle to the I/O runtime, for connector edge work that must
    /// start before the chain exists (schema-registry fetchers, ...).
    /// Valid until `run` returns.
    #[must_use]
    pub fn io_handle(&self) -> tokio::runtime::Handle {
        self.io.handle().clone()
    }

    /// Run a future on the I/O runtime, blocking this thread — for async
    /// pre-flight steps such as schema validation.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.io.block_on(future)
    }

    /// Install the sink with default [`SinkOptions`]; see
    /// [`sink_with`](Self::sink_with).
    pub fn sink<B: SinkBundle>(self, bundle: B) -> Result<Self, BuildError> {
        self.sink_with(bundle, SinkOptions::default())
    }

    /// Install the sink: builds the per-shard chunk queues, registers the
    /// per-shard metrics (E2E basis from the config), spawns the
    /// [`SinkPool`] workers on the I/O runtime, and wires the drain and
    /// readiness probe.
    ///
    /// # Errors
    ///
    /// [`BuildError::SinkAlreadySet`] on a second call;
    /// [`BuildError::Sink`] for an empty or ragged topology, label shapes
    /// that do not match it, or a zero queue capacity.
    pub fn sink_with<B: SinkBundle>(
        mut self,
        bundle: B,
        options: SinkOptions,
    ) -> Result<Self, BuildError> {
        if self.sink.is_some() {
            return Err(BuildError::SinkAlreadySet);
        }
        if options.queue_capacity == 0 {
            return Err(BuildError::Sink("queue_capacity must be non-zero".into()));
        }
        let parts = bundle.into_parts();
        let num_shards = parts.shard_endpoints.len();
        if num_shards == 0 {
            return Err(BuildError::Sink("sink topology has no shards".into()));
        }
        if let Some(shard) = parts.shard_endpoints.iter().position(Vec::is_empty) {
            return Err(BuildError::Sink(format!("shard {shard} has no replicas")));
        }
        let replica_labels = parts.effective_replica_labels();
        let label_shape: Vec<usize> = replica_labels.iter().map(Vec::len).collect();
        let endpoint_shape: Vec<usize> = parts.shard_endpoints.iter().map(Vec::len).collect();
        if label_shape != endpoint_shape {
            return Err(BuildError::Sink(format!(
                "replica_labels shape {label_shape:?} does not match the \
                 endpoint topology {endpoint_shape:?}"
            )));
        }

        let name = self.config.pipeline.name.clone();
        let (queues, receivers) = shard_queues(num_shards, options.queue_capacity);
        let sink_labels = ComponentLabels::new(name.clone(), "sink", parts.component_type.clone());
        let e2e_basis = metrics_settings(&self.config).e2e_basis;
        let shard_metrics: Vec<SinkShardMetrics> = replica_labels
            .iter()
            .enumerate()
            .map(|(shard, replicas)| {
                SinkShardMetrics::new(
                    &sink_labels,
                    u32::try_from(shard).unwrap_or(u32::MAX),
                    replicas,
                    e2e_basis,
                )
            })
            .collect();
        let pool = SinkPool::spawn(
            Arc::new(parts.writer),
            parts.shard_endpoints,
            receivers,
            parts.pool,
            Arc::clone(&self.budget),
            shard_metrics,
            &name,
            self.io.handle(),
        );
        self.sink = Some(SinkAssembly {
            queues,
            drain: Box::new(move |deadline| Box::pin(async move { pool.drain(deadline).await })),
            probe: parts.probe,
        });
        Ok(self)
    }

    /// Install the chain factory, called once per pipeline thread with
    /// that thread's [`ChainCtx`]. Composition inside the closure is fully
    /// monomorphized ([`chain_owned`](crate::ops::chain_owned) and
    /// friends); the returned `Box<dyn RunnableChain>` is the same single
    /// per-batch erasure boundary as always.
    #[must_use]
    pub fn chains<F>(mut self, factory: F) -> Self
    where
        F: FnMut(ChainCtx) -> Box<dyn RunnableChain> + Send + 'static,
    {
        self.chains = Some(Box::new(factory));
        self
    }

    /// Override the runtime options (signal handling, loop timings).
    #[must_use]
    pub fn runtime_options(mut self, options: RuntimeOptions) -> Self {
        self.options = options;
        self
    }

    /// Finish assembly into a [`PipelineRuntime`] — for callers that need
    /// [`shutdown_handle`](PipelineRuntime::shutdown_handle) before a
    /// spawned `run` (tests, embedded pipelines). The I/O runtime moves
    /// into it and is shut down when `run` returns.
    ///
    /// # Errors
    ///
    /// [`BuildError::MissingSink`] / [`BuildError::MissingChains`] when a
    /// step was skipped.
    pub fn into_runtime<S: Source + 'static>(
        mut self,
        source: S,
    ) -> Result<PipelineRuntime<S>, BuildError> {
        let assembly = self.sink.take().ok_or(BuildError::MissingSink)?;
        let mut factory = self.chains.take().ok_or(BuildError::MissingChains)?;
        let queues = assembly.queues.clone();
        let budget = Arc::clone(&self.budget);
        let name = self.config.pipeline.name.clone();
        // This wrapper is the factory the runtime drops before the sink
        // drain — the queue clone it captures dies exactly there.
        let chains = move |thread: usize| {
            factory(ChainCtx {
                thread,
                queues: queues.clone(),
                budget: Arc::clone(&budget),
                pipeline: name.clone(),
            })
        };
        Ok(PipelineRuntime::new(
            self.config,
            source,
            chains,
            SinkRuntime {
                queues: assembly.queues,
                drain: assembly.drain,
                probe: assembly.probe,
            },
            self.budget,
        )
        .with_options(self.options)
        .with_io_runtime(self.io))
    }

    /// [`into_runtime`](Self::into_runtime) + [`PipelineRuntime::run`]:
    /// run the pipeline to completion, blocking until a shutdown signal
    /// drains it or a fatal error stops it.
    pub fn run<S: Source + 'static>(self, source: S) -> Result<ExitReport, PipelineError> {
        Ok(self.into_runtime(source)?.run()?)
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::error::SinkError;
    use crate::pipeline::ExitState;
    use crate::pipeline::fakes::{
        ChainMode, ChainShared, FakeChain, FakeSource, LaneSpec, Script, SourceLog, batches,
        test_config, test_options, wait_for,
    };
    use crate::record::PartitionId;
    use crate::sink::{SealedBatch, SinkParts, SinkPoolConfig};
    use crate::source::LaneId;
    use std::sync::Mutex;
    use std::time::Duration;

    struct NullWriter;
    impl crate::sink::ShardWriter for NullWriter {
        type Endpoint = ();
        async fn write_batch(&self, (): &(), _batch: &SealedBatch) -> Result<(), SinkError> {
            Ok(())
        }
    }

    fn null_sink(shards: usize) -> SinkParts<NullWriter> {
        SinkParts::new(
            NullWriter,
            (0..shards).map(|_| vec![()]).collect(),
            SinkPoolConfig::default(),
        )
        .with_component_type("null")
    }

    fn fake_chain(
        shared: &Arc<ChainShared>,
        log: &Arc<Mutex<SourceLog>>,
    ) -> Box<dyn RunnableChain> {
        Box::new(FakeChain {
            shared: Arc::clone(shared),
            log: Arc::clone(log),
            mode: ChainMode::Ok,
            batches_seen: 0,
        })
    }

    #[test]
    fn missing_sink_then_missing_chains_error() {
        let (source, _shared, _script) = FakeSource::new();
        let p = Pipeline::from_config(test_config(1)).expect("builder");
        assert!(matches!(
            p.into_runtime(source).err(),
            Some(BuildError::MissingSink)
        ));

        let (source, _shared, _script) = FakeSource::new();
        let p = Pipeline::from_config(test_config(1))
            .expect("builder")
            .sink(null_sink(1))
            .expect("sink");
        assert!(matches!(
            p.into_runtime(source).err(),
            Some(BuildError::MissingChains)
        ));
    }

    #[test]
    fn second_sink_errors() {
        let p = Pipeline::from_config(test_config(1))
            .expect("builder")
            .sink(null_sink(1))
            .expect("first sink");
        assert!(matches!(
            p.sink(null_sink(1)).err(),
            Some(BuildError::SinkAlreadySet)
        ));
    }

    #[test]
    fn bad_topologies_error_instead_of_panicking() {
        let p = Pipeline::from_config(test_config(1)).expect("builder");
        let empty = SinkParts::new(NullWriter, Vec::new(), SinkPoolConfig::default());
        assert!(matches!(p.sink(empty).err(), Some(BuildError::Sink(_))));

        let p = Pipeline::from_config(test_config(1)).expect("builder");
        let ragged = SinkParts::new(
            NullWriter,
            vec![vec![()], vec![]],
            SinkPoolConfig::default(),
        );
        assert!(matches!(p.sink(ragged).err(), Some(BuildError::Sink(_))));

        let p = Pipeline::from_config(test_config(1)).expect("builder");
        let bad_labels = SinkParts::new(NullWriter, vec![vec![()]], SinkPoolConfig::default())
            .with_replica_labels(vec![vec!["a".into(), "b".into()]]);
        assert!(matches!(
            p.sink(bad_labels).err(),
            Some(BuildError::Sink(_))
        ));

        let p = Pipeline::from_config(test_config(1)).expect("builder");
        assert!(matches!(
            p.sink_with(null_sink(1), SinkOptions { queue_capacity: 0 })
                .err(),
            Some(BuildError::Sink(_))
        ));
    }

    #[tokio::test]
    async fn from_config_inside_async_context_errors() {
        assert!(matches!(
            Pipeline::from_config(test_config(1)).err(),
            Some(BuildError::AsyncContext)
        ));
    }

    /// The chain factory sees every thread index exactly once, with the
    /// pipeline name from the config, and the assembled pipeline runs to a
    /// clean `Completed` through the real `SinkPool`. This guards `ChainCtx`
    /// coverage and end-to-end assembly — not drop ordering: the drain
    /// containment fix (`sink/worker.rs`) deliberately converts a leaked
    /// `ShardQueues` clone from an unbounded hang into a bounded, loud
    /// abandon, so completion here no longer implies clean drop ordering.
    /// The drop-ordering + at-least-once contract is covered where it *can*
    /// still fail observably: the whole-assembly test in `etl-test`'s
    /// `tests/bundle.rs`, which routes real data through `ctx.queues` and
    /// asserts the watermark only advances past the last record after a
    /// durable write.
    #[test]
    fn chain_ctx_covers_every_thread_and_run_completes() {
        let (source, shared, script) = FakeSource::new();
        script
            .lock()
            .unwrap()
            .push_back(Script::Assign(vec![LaneSpec {
                id: LaneId(0),
                partition: PartitionId(0),
                batches: batches(&[0..10, 10..20]),
            }]));
        let chain_shared = Arc::new(ChainShared::default());
        let seen_threads: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));

        let cs = Arc::clone(&chain_shared);
        let log = Arc::clone(&shared);
        let seen = Arc::clone(&seen_threads);
        let runtime = Pipeline::from_config(test_config(2))
            .expect("builder")
            .sink(null_sink(1))
            .expect("sink")
            .chains(move |ctx| {
                assert_eq!(ctx.pipeline, "test");
                seen.lock().unwrap().push(ctx.thread);
                fake_chain(&cs, &log)
            })
            .runtime_options(test_options())
            .into_runtime(source)
            .expect("into_runtime");

        let shutdown = runtime.shutdown_handle();
        let join = std::thread::spawn(move || runtime.run());
        wait_for("payloads consumed", Duration::from_secs(5), || {
            chain_shared
                .consumed
                .load(std::sync::atomic::Ordering::Relaxed)
                == 20
        });
        shutdown.trigger();
        let report = join.join().unwrap().unwrap();
        assert_eq!(report.state, ExitState::Completed);

        let mut threads = seen_threads.lock().unwrap().clone();
        threads.sort_unstable();
        assert_eq!(threads, vec![0, 1], "one ChainCtx per pipeline thread");
    }

    /// The whole-builder happy path through `run()` (not `into_runtime`),
    /// exercised over the real SinkPool: completes and commits.
    #[test]
    fn run_completes_via_builder_terminal() {
        let (source, shared, script) = FakeSource::new();
        script
            .lock()
            .unwrap()
            .push_back(Script::Assign(vec![LaneSpec {
                id: LaneId(0),
                partition: PartitionId(0),
                batches: batches(std::slice::from_ref(&(0..5))),
            }]));
        let chain_shared = Arc::new(ChainShared::default());
        let cs = Arc::clone(&chain_shared);
        let log = Arc::clone(&shared);

        let pipeline = Pipeline::from_config(test_config(1))
            .expect("builder")
            .sink(null_sink(2))
            .expect("sink")
            .chains(move |_ctx| fake_chain(&cs, &log))
            .runtime_options(test_options());

        // Drive shutdown from a watcher thread once the payloads land.
        let consumed = Arc::clone(&chain_shared);
        let runtime = pipeline.into_runtime(source).expect("into_runtime");
        let shutdown = runtime.shutdown_handle();
        std::thread::spawn(move || {
            wait_for("payloads consumed", Duration::from_secs(5), || {
                consumed.consumed.load(std::sync::atomic::Ordering::Relaxed) == 5
            });
            shutdown.trigger();
        });
        let report = runtime.run().unwrap();
        assert_eq!(report.state, ExitState::Completed);
        assert_eq!(report.final_watermarks, vec![(PartitionId(0), 5)]);
    }
}
