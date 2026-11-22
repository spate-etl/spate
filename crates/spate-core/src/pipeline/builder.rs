//! The pipeline builder: the primary assembly path.
//!
//! [`Pipeline::from_config`] owns startup initialization (telemetry, the
//! metrics exporter, and the shared I/O runtime). Every metric handle built
//! afterwards (framework or custom) binds to the process's recorder, which
//! always exists under `exporter: prometheus`, and connectors get an I/O
//! handle before any thread spawns. The builder is a thin composition of the public
//! primitives it replaces; nothing here is required, and the desugaring
//! below remains a fully supported assembly path.
//!
//! The shape of an assembly (illustrative; connector construction is
//! elided, and the `spate` crate's examples carry complete, compiling
//! binaries):
//!
//! ```ignore
//! let pipeline = Pipeline::from_path(Path::new("pipeline.yaml"))?;
//! let source = MySource::from_component_config(&pipeline.config().source)?;
//! let sink = my_connector::from_component_config(pipeline.config().sink_config("default")?)?;
//! let report = pipeline
//!     .sink(sink)?
//!     .chains(move |ctx| {
//!         let chunk_cfg = ctx.chunk(); // bind before `with_metrics` moves `ctx.pipeline`
//!         chain_owned::<Row, _>(deserializer.clone())
//!             .with_metrics(ctx.pipeline, "main")
//!             .sink(encoder.clone(), KeyHashRouter, chunk_cfg,
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
//! | `.sink(bundle)` | [`SinkBundle::into_parts`](crate::sink::SinkBundle::into_parts) → [`SinkOptions::resolve`] (the sink's chunk, queue capacity and pool, validated) → [`shard_queues`](crate::sink::shard_queues) → [`ShardQueues::with_frame_codec`](crate::sink::ShardQueues::with_frame_codec) when the parts carry a [`frame_codec`](crate::sink::SinkParts::frame_codec) → [`SinkShardMetrics::try_new`](crate::metrics::SinkShardMetrics::try_new) per shard → [`SinkPool::spawn`](crate::sink::SinkPool::spawn) → a boxed drain closure |
//! | `.chains(f)` | the factory handed to [`PipelineRuntime::new`], with queue/budget/name plumbing pre-threaded per call |
//! | `.feed(feed)` | [`PipelineRuntime::with_feed`] |
//! | `.seeded_feed(feed)` | [`PipelineRuntime::with_seeded_feed`] |
//! | `.into_runtime(source)` / `.run(source)` | [`PipelineRuntime::new`]`(config, source, factory, `[`SinkRuntime`]`{..}, budget)` + [`PipelineRuntime::with_io_runtime`] |
//!
//! # Shutdown and drop ordering
//!
//! The sink only drains once every [`ShardQueues`] clone is gone. The
//! builder discharges this structurally by never exposing the queues
//! outside the chain factory. Each factory call receives a fresh clone in
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
use crate::config::{ConfigError, PipelineConfig, SectionPath, SinkSection};
use crate::feed::{Feed, FeedOrigin};
use crate::metrics::{
    ComponentLabels, Meter, MetricRole, MetricsHandle, SharedString, SinkShardMetrics,
};
use crate::ops::{ChunkConfig, RunnableChain, SplitTarget};
use crate::pipeline::ExitReport;
use crate::sink::{
    BatchConfig, BreakerConfig, DrainReport, InflightConfig, QueueSection, RetryConfig,
    ShardQueues, ShardWriter, SinkBundle, SinkDrainFn, SinkFailures, SinkMetricsCtx, SinkPool,
    SinkPoolConfig, SinkProbeFn, shard_queues,
};
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
    /// [`Pipeline::add_sink`] was called twice with the same name (a second
    /// bare [`Pipeline::sink`] collides on the reserved `"default"` name).
    #[error("a sink named {0:?} is already installed")]
    DuplicateSinkName(String),
    /// Another live handle set in this process already owns a metric series
    /// this sink would publish, usually a second pipeline with the same
    /// pipeline and sink name. Gauge series cannot be shared, so assembly
    /// stops here rather than letting one of the two publish readings the
    /// other overwrites. A pipeline rebuilt *sequentially* is fine: drop the
    /// old one first.
    #[error("{0}")]
    DuplicateSeries(String),
    /// [`Pipeline::into_runtime`]/[`Pipeline::run`] without a sink.
    #[error("no sink installed (call Pipeline::sink or Pipeline::add_sink first)")]
    MissingSink,
    /// [`Pipeline::into_runtime`]/[`Pipeline::run`] without a chain factory.
    #[error("no chain factory installed (call Pipeline::chains first)")]
    MissingChains,
    /// The builder was constructed inside an async runtime. It owns a
    /// blocking tokio runtime (dropping or `block_on`-ing one inside async
    /// context panics), so build pipelines from a plain thread, usually
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

/// Per-thread wiring handed to the chain factory, carrying everything the
/// terminal [`.sink(...)`](crate::ops::ChainBuilder) stage needs, so assemblies
/// stop threading queues, budget, and the pipeline name by hand.
///
/// Passed by value, once per pipeline thread; move the fields into the
/// chain being built. Not `Clone` (see the module docs on drop ordering).
#[derive(Debug)]
#[non_exhaustive]
pub struct ChainCtx {
    /// Zero-based pipeline thread index.
    pub thread: usize,
    /// This thread's clone of the shard-queue senders for the **first**
    /// installed sink, the back-compat handle for single-sink pipelines
    /// (`.sink(...)`). Multi-sink pipelines resolve each branch's queues by
    /// name via [`sink`](Self::sink) instead.
    pub queues: ShardQueues,
    /// The shared in-flight byte budget.
    pub budget: Arc<InflightBudget>,
    /// The pipeline name
    /// ([`ChainBuilder::with_metrics`](crate::ops::ChainBuilder::with_metrics)'s
    /// first argument).
    pub pipeline: String,
    /// The **first** installed sink's resolved terminal-stage chunking, the
    /// back-compat handle for single-sink pipelines, mirroring [`queues`](Self::queues).
    /// Pass it to the chain's [`.sink(...)`](crate::ops::ChainBuilder::sink)
    /// terminal via [`chunk`](Self::chunk); split pipelines resolve each
    /// branch's chunk by name through [`sink`](Self::sink) instead.
    chunk: ChunkConfig,
    /// This thread's clone of every installed sink's queues and resolved
    /// chunking, keyed by the name passed to [`Pipeline::add_sink`]. Resolved
    /// through [`sink`](Self::sink). The clones die with the driver.
    named: Vec<(String, ShardQueues, ChunkConfig)>,
}

impl ChainCtx {
    /// The named sink's handles (name, shard queues, shared in-flight budget)
    /// for a split-terminal branch. Pass the result straight to
    /// [`SplitBuilder::add`](crate::ops::SplitBuilder::add). The single-sink
    /// `.sink()` sugar installs its sink under the name `"default"`.
    ///
    /// # Panics
    ///
    /// Panics if no sink was installed under `name`. The chain factory runs
    /// once per thread, on the cold path, before any data flows.
    #[must_use]
    pub fn sink(&self, name: &str) -> SplitTarget {
        let (_, queues, chunk) = self
            .named
            .iter()
            .find(|(n, _, _)| n == name)
            .unwrap_or_else(|| {
                let known: Vec<&str> = self.named.iter().map(|(n, _, _)| n.as_str()).collect();
                panic!("ChainCtx::sink: no sink named {name:?} (installed sinks: {known:?})")
            });
        SplitTarget::new(name.to_string(), queues.clone(), Arc::clone(&self.budget))
            .with_chunk(*chunk)
    }

    /// The **first** installed sink's resolved terminal-stage chunking, the
    /// single-sink counterpart to [`queues`](Self::queues), fed straight to the
    /// chain's [`.sink(...)`](crate::ops::ChainBuilder::sink) terminal:
    ///
    /// ```
    /// # use spate_core::deser::{BytesPassthrough, Owned};
    /// # use spate_core::error::SinkError;
    /// # use spate_core::ops::chain;
    /// # use spate_core::pipeline::ChainCtx;
    /// # use spate_core::record::Record;
    /// # use spate_core::sink::{KeyHashRouter, RowEncoder};
    /// # #[derive(Clone)]
    /// # struct RowBytes;
    /// # impl RowEncoder<Owned<Vec<u8>>> for RowBytes {
    /// #     fn encode<'buf>(
    /// #         &mut self,
    /// #         rec: &Record<Vec<u8>>,
    /// #         buf: &mut bytes::BytesMut,
    /// #     ) -> Result<(), SinkError> {
    /// #         buf.extend_from_slice(&rec.payload);
    /// #         Ok(())
    /// #     }
    /// # }
    /// # fn wire(ctx: ChainCtx) {
    /// # let encoder = RowBytes;
    /// let chunk_cfg = ctx.chunk(); // bind before `with_metrics` moves `ctx.pipeline`
    /// let orders = chain(BytesPassthrough)
    ///     .with_metrics(ctx.pipeline, "orders")
    ///     .sink(encoder, KeyHashRouter, chunk_cfg, ctx.queues, ctx.budget)
    ///     .build();
    /// # let _ = orders;
    /// # }
    /// ```
    ///
    /// It resolves the per-sink YAML `chunk:` block (or `SinkOptions::with_chunk`,
    /// or the 64 KiB default) at assembly time. Split pipelines take each
    /// branch's chunk from [`sink`](Self::sink) instead.
    #[must_use]
    pub fn chunk(&self) -> ChunkConfig {
        self.chunk
    }

    /// A [`Meter`] for a pipeline author's own metrics, pre-labeled with the
    /// pipeline name plus the `component` / `component_type` you name and
    /// scoped to the `spate_custom_` namespace. Resolve handles from it **once
    /// here** (the factory runs once per thread, before data flows) and move
    /// them into the operator closures that touch them:
    ///
    /// ```no_run
    /// # use spate_core::pipeline::ChainCtx;
    /// # fn wire(ctx: ChainCtx) {
    /// // Pass the LOCAL name — this registers `spate_custom_enrich_hits_total`.
    /// let hits = ctx.meter("enrich", "map").counter("enrich_hits_total", &[]);
    /// // ... move `hits` into a `.inspect(move |r| { hits.increment(1); })`
    /// # let _ = hits;
    /// # }
    /// ```
    ///
    /// The resulting series carry `pipeline`/`component`/`component_type` like
    /// every framework series and live under the `spate_` umbrella, so they join
    /// cleanly in a query. You pass local names; the `Meter` adds the
    /// `spate_custom_` prefix. [The metrics reference] carries the umbrella and
    /// the label set in full.
    ///
    /// [the metrics reference]: https://spate.kainth.dev/docs/METRICS
    #[must_use]
    pub fn meter(
        &self,
        component: impl Into<SharedString>,
        component_type: impl Into<SharedString>,
    ) -> Meter {
        Meter::new(self.pipeline.clone(), component, component_type)
    }
}

/// Each shard queue's slot count when neither the YAML `queue:` block nor
/// [`SinkOptions::with_queue`] sets one.
const DEFAULT_QUEUE_CAPACITY: usize = 8;

/// The framework's sink keys, set in code. Each one is the code form of the
/// YAML block of the same name beside the sink's backend key, and a sink
/// takes each key from one place: setting it here **and** in that sink's
/// YAML section is a [`ConfigError`] at install. `None` (the default) defers
/// to the YAML block, or to the default when that is absent too.
///
/// Construct with `default()` and the `with_*` methods (the struct is
/// `#[non_exhaustive]`).
///
/// ```
/// use spate_core::pipeline::SinkOptions;
/// use spate_core::sink::{BatchConfig, QueueSection};
///
/// let mut batch = BatchConfig::default();
/// batch.max_rows = 1_000_000;
/// let options = SinkOptions::default()
///     .with_queue(QueueSection::new(8 * 1024 * 1024))
///     .with_batch(batch);
/// assert_eq!(options.batch.map(|b| b.max_rows), Some(1_000_000));
/// ```
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct SinkOptions {
    /// Each shard queue's size, which sets its slot count from the resolved
    /// chunk size. Without it from here or YAML, each queue holds 8 chunks.
    pub queue: Option<QueueSection>,
    /// This sink's terminal-stage chunking. Without it from here or YAML,
    /// [`ChunkConfig::default`]. The resolved value reaches the chain
    /// terminal via [`ChainCtx::chunk`] / [`ChainCtx::sink`].
    pub chunk: Option<ChunkConfig>,
    /// Batch sealing thresholds, replacing the connector's base whole.
    pub batch: Option<BatchConfig>,
    /// In-flight write limits, replacing the connector's base whole.
    pub inflight: Option<InflightConfig>,
    /// Write retry policy, replacing the connector's base whole.
    pub retry: Option<RetryConfig>,
    /// Replica circuit breaker, replacing the connector's base whole.
    pub breaker: Option<BreakerConfig>,
}

impl SinkOptions {
    /// Set this sink's queue size (see [`queue`](Self::queue)).
    #[must_use]
    pub fn with_queue(mut self, queue: QueueSection) -> Self {
        self.queue = Some(queue);
        self
    }

    /// Set this sink's chunking (see [`chunk`](Self::chunk)).
    #[must_use]
    pub fn with_chunk(mut self, chunk: ChunkConfig) -> Self {
        self.chunk = Some(chunk);
        self
    }

    /// Set this sink's batch thresholds (see [`batch`](Self::batch)).
    #[must_use]
    pub fn with_batch(mut self, batch: BatchConfig) -> Self {
        self.batch = Some(batch);
        self
    }

    /// Set this sink's in-flight limits (see [`inflight`](Self::inflight)).
    #[must_use]
    pub fn with_inflight(mut self, inflight: InflightConfig) -> Self {
        self.inflight = Some(inflight);
        self
    }

    /// Set this sink's retry policy (see [`retry`](Self::retry)).
    #[must_use]
    pub fn with_retry(mut self, retry: RetryConfig) -> Self {
        self.retry = Some(retry);
        self
    }

    /// Set this sink's circuit breaker (see [`breaker`](Self::breaker)).
    #[must_use]
    pub fn with_breaker(mut self, breaker: BreakerConfig) -> Self {
        self.breaker = Some(breaker);
        self
    }

    /// The values [`Pipeline::add_sink_with`] installs for the sink `name`,
    /// from its YAML section in `config`, these options and the connector's
    /// pool `base` ([`SinkParts::pool`](crate::sink::SinkParts::pool)). A
    /// manual assembly calls it to size its queues and spawn its pool as
    /// the builder would.
    ///
    /// A YAML pool block overlays `base` field by field; a pool block set
    /// here replaces it whole. The result passes
    /// [`SinkPoolConfig::validate`]. A framework block declared on a sink
    /// that nothing installs is warned about by
    /// [`Pipeline::into_runtime`], which knows every installed name.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] naming the sink when a key is set both here and in
    /// YAML, a block does not decode, `chunk.target_bytes` is zero,
    /// `queue.max_bytes` is zero or makes more chunks than a queue holds,
    /// or the pool fails [`SinkPoolConfig::validate`].
    ///
    /// ```
    /// use spate_core::config::PipelineConfig;
    /// use spate_core::pipeline::SinkOptions;
    /// use spate_core::sink::SinkPoolConfig;
    ///
    /// let config = PipelineConfig::from_str(
    ///     "pipeline: { name: demo }\n\
    ///      source: { memory: {} }\n\
    ///      sink:\n\
    ///        queue: { max_bytes: 2MiB }\n\
    ///        chunk: { target_bytes: 128KiB }\n\
    ///        memory: {}\n",
    /// )?;
    /// let resolved = SinkOptions::default().resolve(&config, "default", SinkPoolConfig::default())?;
    /// assert_eq!(resolved.queue_capacity, 16);
    /// assert_eq!(resolved.chunk.target_bytes, 128 * 1024);
    /// # Ok::<(), spate_core::config::ConfigError>(())
    /// ```
    pub fn resolve(
        &self,
        config: &PipelineConfig,
        name: &str,
        base: SinkPoolConfig,
    ) -> Result<ResolvedSink, ConfigError> {
        let address = config.sink_address(name);
        // No YAML section under this name: the sink is configured in code
        // (capture sinks in tests, named sinks beside a placeholder `sink:`).
        let section = config.sink_section(name).ok();
        let at = &*address;

        let chunk = one_place(
            from_yaml(section, |s| s.chunk(at))?,
            self.chunk,
            "chunk",
            at,
        )?
        .unwrap_or_default();
        // The YAML path already checked `> 0`; `with_chunk` skips that. It
        // also comes before the queue's slot count divides by it.
        if chunk.target_bytes == 0 {
            return Err(ConfigError::Validation(format!(
                "{address}: chunk.target_bytes must be greater than zero"
            )));
        }
        let queue = one_place(
            from_yaml(section, |s| s.queue(at))?,
            self.queue,
            "queue",
            at,
        )?;
        let queue_capacity = match queue {
            Some(queue) => queue.capacity(chunk.target_bytes, &SectionPath::at(at))?,
            None => DEFAULT_QUEUE_CAPACITY,
        };
        let batch = from_yaml(section, |s| s.batch(at, &base.batch))?;
        let inflight = from_yaml(section, |s| s.inflight(at, &base.inflight))?;
        let retry = from_yaml(section, |s| s.retry(at, &base.retry))?;
        let breaker = from_yaml(section, |s| s.breaker(at, &base.breaker))?;
        let pool = SinkPoolConfig::new(
            one_place(batch, self.batch, "batch", at)?.unwrap_or(base.batch),
            one_place(inflight, self.inflight, "inflight", at)?.unwrap_or(base.inflight),
            one_place(retry, self.retry, "retry", at)?.unwrap_or(base.retry),
            one_place(breaker, self.breaker, "breaker", at)?.unwrap_or(base.breaker),
        );
        pool.validate()
            .map_err(|e| ConfigError::Validation(format!("{address}: {e}")))?;
        Ok(ResolvedSink {
            chunk,
            queue_capacity,
            pool,
        })
    }
}

/// A YAML block's value, when the sink has a YAML section.
fn from_yaml<T>(
    section: Option<&SinkSection>,
    block: impl FnOnce(&SinkSection) -> Result<Option<T>, ConfigError>,
) -> Result<Option<T>, ConfigError> {
    section.map(block).transpose().map(Option::flatten)
}

/// The one of `yaml` and `code` that is set, refusing both for the sink at
/// `address`.
fn one_place<T>(
    yaml: Option<T>,
    code: Option<T>,
    key: &str,
    address: &str,
) -> Result<Option<T>, ConfigError> {
    match (yaml, code) {
        (Some(_), Some(_)) => Err(ConfigError::Validation(format!(
            "{address}: set `{key}` in exactly one place, the YAML `{key}:` block or \
             `SinkOptions::with_{key}`, not both"
        ))),
        (yaml, code) => Ok(yaml.or(code)),
    }
}

/// What [`SinkOptions::resolve`] gives for one sink: the values
/// [`Pipeline::add_sink_with`] installs.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct ResolvedSink {
    /// The chain terminal's chunking.
    pub chunk: ChunkConfig,
    /// Each shard queue's slot count, for [`shard_queues`].
    pub queue_capacity: usize,
    /// The worker pool's configuration, for [`SinkPool::spawn`].
    pub pool: SinkPoolConfig,
}

struct SinkAssembly {
    queues: ShardQueues,
    drain: SinkDrainFn,
    probe: Option<SinkProbeFn>,
    /// This sink's resolved terminal-stage chunking, threaded into every
    /// per-thread [`ChainCtx`] (the default sink's becomes [`ChainCtx::chunk`];
    /// named sinks reach it through [`ChainCtx::sink`]).
    chunk: ChunkConfig,
}

type ChainFactoryFn = Box<dyn FnMut(ChainCtx) -> Box<dyn RunnableChain> + Send>;

/// The pipeline builder. See the [module docs](crate::pipeline) for the full
/// picture.
///
/// Non-generic, nameable, and storable; the source type enters only at the
/// terminal [`into_runtime`](Self::into_runtime)/[`run`](Self::run) call.
pub struct Pipeline {
    config: PipelineConfig,
    metrics: MetricsHandle,
    io: tokio::runtime::Runtime,
    budget: Arc<InflightBudget>,
    sink_failures: SinkFailures,
    sinks: Vec<(String, SinkAssembly)>,
    chains: Option<ChainFactoryFn>,
    options: RuntimeOptions,
    feed: Option<(Arc<dyn Feed>, FeedOrigin)>,
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sink_names: Vec<&str> = self.sinks.iter().map(|(n, _)| n.as_str()).collect();
        f.debug_struct("Pipeline")
            .field("pipeline", &self.config.pipeline.name)
            .field("sinks", &sink_names)
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
    ///    built while holding the `Pipeline` binds to the process's
    ///    recorder, which always exists under `exporter: prometheus`.
    ///    When a foreign recorder already owns the process, the pipeline
    ///    continues against it with a warning.
    /// 3. **The I/O runtime** — `pipeline.io_threads` workers, thread name
    ///    `spate-io`. Connectors that need a handle before `run` (schema
    ///    fetchers, async pre-flight validation) use
    ///    [`io_handle`](Self::io_handle)/[`block_on`](Self::block_on).
    ///
    /// # Errors
    ///
    /// [`BuildError::AsyncContext`] when called from inside an async runtime.
    /// Build pipelines from a plain thread, usually `main`.
    pub fn from_config(config: PipelineConfig) -> Result<Self, BuildError> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(BuildError::AsyncContext);
        }
        if config.pipeline.io_threads == 0 {
            return Err(BuildError::Config(ConfigError::Validation(
                "pipeline.io_threads must be non-zero".into(),
            )));
        }
        // The YAML loaders run the full `PipelineConfig::validate`; a
        // programmatically built config skips it by design (minimal test
        // fixtures). A sink that decodes no body would ignore a `chunk:`
        // inside it, so this check runs here too.
        config.reject_body_chunk().map_err(BuildError::Config)?;
        telemetry::init(LogFormat::Json, "info");
        let metrics = install_or_reuse(&metrics_settings(&config)).map_err(|e| match e {
            StartError::Metrics(m) => BuildError::Metrics(m),
            other => BuildError::Metrics(other.to_string()),
        })?;
        let io = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(config.pipeline.io_threads)
            .thread_name("spate-io")
            .enable_all()
            .build()?;
        Ok(Pipeline {
            config,
            metrics,
            io,
            budget: Arc::new(InflightBudget::new()),
            sink_failures: SinkFailures::new(),
            sinks: Vec::new(),
            chains: None,
            options: RuntimeOptions::default(),
            feed: None,
        })
    }

    /// The loaded configuration. Connector sections (`config().source`,
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
    /// Valid until the I/O runtime shuts down, which `run` does after the
    /// source's and the feed's `Drop` once the pipeline has started, even when
    /// they outlast `run`.
    #[must_use]
    pub fn io_handle(&self) -> tokio::runtime::Handle {
        self.io.handle().clone()
    }

    /// Run a future on the I/O runtime, blocking this thread. Use it for
    /// async pre-flight steps such as schema validation.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.io.block_on(future)
    }

    /// Install the single sink under the reserved name `"default"` with
    /// default [`SinkOptions`], the ergonomic path for single-sink
    /// pipelines. Sugar for [`add_sink`](Self::add_sink)`("default", bundle)`.
    pub fn sink<B: SinkBundle>(self, bundle: B) -> Result<Self, BuildError> {
        self.add_sink_with("default", bundle, SinkOptions::default())
    }

    /// [`sink`](Self::sink) with explicit [`SinkOptions`]. Sugar for
    /// [`add_sink_with`](Self::add_sink_with)`("default", bundle, options)`.
    pub fn sink_with<B: SinkBundle>(
        self,
        bundle: B,
        options: SinkOptions,
    ) -> Result<Self, BuildError> {
        self.add_sink_with("default", bundle, options)
    }

    /// Install a named sink with default [`SinkOptions`]; see
    /// [`add_sink_with`](Self::add_sink_with). Call once per destination
    /// table/stream; the chain's [`split`](crate::ops::ChainBuilder) terminal
    /// resolves each branch's queues by this name via
    /// [`ChainCtx::sink`](ChainCtx::sink).
    pub fn add_sink<B: SinkBundle>(
        self,
        name: impl Into<String>,
        bundle: B,
    ) -> Result<Self, BuildError> {
        self.add_sink_with(name, bundle, SinkOptions::default())
    }

    /// Install a named sink. This builds the per-shard chunk queues, registers
    /// the per-shard metrics (E2E basis from the config; the sink `name`
    /// becomes the `component` label, so each sink's `spate_sink_*` series is
    /// distinct), spawns the [`SinkPool`] workers on the I/O runtime, and
    /// wires the drain and readiness probe. The named sinks share the one
    /// pipeline
    /// [`InflightBudget`] and one backpressure controller (a stall on any sink
    /// pauses the shared source).
    ///
    /// # Errors
    ///
    /// [`BuildError::DuplicateSinkName`] when `name` is already installed;
    /// [`BuildError::Sink`] for an empty or reserved name (`"sink"` is the
    /// default sink's metric label), an empty or ragged topology, or label
    /// shapes that do not match it;
    /// [`BuildError::Config`] when [`SinkOptions::resolve`] fails;
    /// [`BuildError::DuplicateSeries`] when its metric series are already
    /// claimed.
    pub fn add_sink_with<B: SinkBundle>(
        mut self,
        name: impl Into<String>,
        bundle: B,
        options: SinkOptions,
    ) -> Result<Self, BuildError> {
        let sink_name = name.into();
        if self.sinks.iter().any(|(n, _)| n == &sink_name) {
            return Err(BuildError::DuplicateSinkName(sink_name));
        }
        if sink_name.is_empty() {
            return Err(BuildError::Sink("sink name must be non-empty".into()));
        }
        // "default" maps to the historical component="sink" metric label, so
        // a sink literally named "sink" would silently merge its spate_sink_*
        // series with the default's. Reject it up front.
        if sink_name == "sink" {
            return Err(BuildError::Sink(
                "the sink name \"sink\" is reserved (it is the default sink's \
                 metric label); pick another name"
                    .into(),
            ));
        }
        let parts = bundle.into_parts();
        // Before any I/O worker spawns or metric series is claimed, so a
        // config error leaks neither.
        let ResolvedSink {
            chunk,
            queue_capacity,
            pool: pool_config,
        } = options.resolve(&self.config, &sink_name, parts.pool)?;
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

        let pipeline_name = self.config.pipeline.name.clone();
        // The single-sink default keeps the historical `component="sink"`
        // label; named sinks use their name so their series never collide.
        let component = if sink_name == "default" {
            "sink".to_string()
        } else {
            sink_name.clone()
        };
        let (mut queues, receivers) = shard_queues(num_shards, queue_capacity);
        // The sink's custom-metrics scope (`spate_<component_type>_sink_*`),
        // handed to the writer before it is shared across shard workers.
        let sink_meter = Meter::for_component(
            &parts.component_type,
            MetricRole::Sink,
            pipeline_name.clone(),
            component.clone(),
        );
        let sink_labels = ComponentLabels::new(
            pipeline_name.clone(),
            component,
            parts.component_type.clone(),
        );
        // Pre-register the queue-edge handles before `queues` is cloned into
        // any terminal, so every producer shares the same `spate_queue_*` series.
        queues
            .attach_metrics(&sink_labels)
            .map_err(|e| BuildError::DuplicateSeries(e.to_string()))?;
        // Before any clone, like the metrics: every chain's terminal makes its
        // own codec instance from these queues.
        if let Some(codec) = parts.frame_codec {
            queues = queues.with_frame_codec(codec);
        }
        let e2e_basis = metrics_settings(&self.config).e2e_basis;
        // A shard's gauges are edge-triggered, so two live handle sets on one
        // series leave a stale reading standing rather than a double count. On
        // the assembly path that is a wiring mistake we can still refuse,
        // before any data flows.
        let shard_metrics: Vec<SinkShardMetrics> = replica_labels
            .iter()
            .enumerate()
            .map(|(shard, replicas)| {
                SinkShardMetrics::try_new(
                    &sink_labels,
                    u32::try_from(shard).unwrap_or(u32::MAX),
                    replicas,
                    e2e_basis,
                )
                .map(|m| m.with_queue(queues.metrics(), shard))
                .map_err(|e| BuildError::DuplicateSeries(e.to_string()))
            })
            .collect::<Result<_, _>>()?;
        let mut writer = parts.writer;
        writer.attach_metrics(SinkMetricsCtx::new(sink_meter));
        let pool = SinkPool::spawn(
            Arc::new(writer),
            parts.shard_endpoints,
            receivers,
            pool_config,
            Arc::clone(&self.budget),
            shard_metrics,
            &pipeline_name,
            &sink_name,
            self.sink_failures.clone(),
            self.io.handle(),
        );
        self.sinks.push((
            sink_name,
            SinkAssembly {
                queues,
                drain: Box::new(move |deadline| {
                    Box::pin(async move { pool.drain(deadline).await })
                }),
                probe: parts.probe,
                chunk,
            },
        ));
        Ok(self)
    }

    /// Install the chain factory, called once per pipeline thread with
    /// that thread's [`ChainCtx`]. Composition inside the closure is fully
    /// monomorphized ([`chain_owned`](crate::ops::chain_owned) and
    /// friends); the returned `Box<dyn RunnableChain>` is the single
    /// per-batch erasure boundary.
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

    /// Hand the source a feed built in code, for the config's `feed:`
    /// section to name. `run` passes it to [`Source::configure_feed`] with
    /// the section, wrapped so the source's calls on it are recorded under
    /// `spate_feed_*`.
    ///
    /// The feed's [`backend`](Feed::backend) must equal the key the section
    /// names, and the config must have a `feed:` section; `run` fails at
    /// startup otherwise. The section's backend keys, and `attempt_cap` as
    /// the feed's delivery cap, apply only when a feed is built from the
    /// section, so a feed passed here keeps the settings it was built with.
    /// The source still reads `units_held`, `location`, `settling_window`
    /// and, as the cap of a source that re-enqueues units, `attempt_cap`
    /// from the section. Without this call, `run`
    /// builds the in-process feed itself for `feed: { memory: {} }` and
    /// refuses any other backend. To publish to
    /// the in-process feed, keep a handle to a `MemoryFeed` and pass it here.
    /// Calls on a handle kept this way are not recorded. A feed the code
    /// fills before the run and never publishes into again goes to
    /// [`seeded_feed`](Self::seeded_feed) instead.
    #[must_use]
    pub fn feed(mut self, feed: Arc<dyn Feed>) -> Self {
        self.feed = Some((feed, FeedOrigin::Given));
        self
    }

    /// Hand the source a feed that holds the run's whole input: code
    /// publishes every unit before `run`, and nothing after. The source
    /// learns this as [`FeedOrigin::Seeded`], and may then list nothing into
    /// the feed and end the run once its units have settled. The rules
    /// `run` checks are those of [`feed`](Self::feed).
    ///
    /// A run that ends with [`ExitState::Completed`](super::ExitState) may
    /// have stopped before every unit settled. Keep a handle to the feed
    /// and read which units did not: on a `MemoryFeed`,
    /// [`unsettled`](crate::feed::memory::MemoryFeed::unsettled) and
    /// [`dead_letters`](crate::feed::memory::MemoryFeed::dead_letters).
    #[must_use]
    pub fn seeded_feed(mut self, feed: Arc<dyn Feed>) -> Self {
        self.feed = Some((feed, FeedOrigin::Seeded));
        self
    }

    /// Finish assembly into a [`PipelineRuntime`], for callers that need
    /// [`shutdown_handle`](PipelineRuntime::shutdown_handle) before a
    /// spawned `run` (tests, embedded pipelines). The I/O runtime moves
    /// into it, and `run` shuts it down once the source and the feed have
    /// closed, which can be after `run` returns.
    ///
    /// # Errors
    ///
    /// [`BuildError::MissingSink`] / [`BuildError::MissingChains`] when a
    /// step was skipped.
    pub fn into_runtime<S: Source + 'static>(
        mut self,
        source: S,
    ) -> Result<PipelineRuntime<S>, BuildError> {
        if self.sinks.is_empty() {
            return Err(BuildError::MissingSink);
        }
        let mut factory = self.chains.take().ok_or(BuildError::MissingChains)?;

        // Decompose the installed sinks into: the per-thread named queue set
        // (cloned into each ChainCtx), the introspection queues (the
        // backpressure resume gate spans every sink), and the drain/probe
        // hooks (composed into one of each for the runtime).
        let mut intro_queues = Vec::with_capacity(self.sinks.len());
        let mut drains = Vec::with_capacity(self.sinks.len());
        let mut probes = Vec::new();
        let mut named = Vec::with_capacity(self.sinks.len());
        for (sink_name, assembly) in std::mem::take(&mut self.sinks) {
            intro_queues.push(assembly.queues.clone());
            named.push((sink_name, assembly.queues, assembly.chunk));
            drains.push(assembly.drain);
            if let Some(probe) = assembly.probe {
                probes.push(probe);
            }
        }
        // A declared sink section whose name nothing installed can carry
        // framework blocks that no install-time resolution will ever read, the
        // one mismatch invisible to `add_sink_with`. (A *malformed* block is
        // already rejected by `PipelineConfig::validate` on the YAML loaders.)
        for config_name in self.config.sink_names() {
            if named.iter().any(|(n, _, _)| *n == config_name) {
                continue;
            }
            let declared = self
                .config
                .sink_section(&config_name)
                .map(SinkSection::declared)
                .unwrap_or_default();
            if !declared.is_empty() {
                tracing::warn!(
                    sink = %config_name,
                    "config declares `{}` for this sink, but no sink was installed under \
                     that name, so nothing reads them (name mismatch between the config \
                     and add_sink)",
                    declared.join("`, `")
                );
            }
        }
        let default_queues = named[0].1.clone();
        let default_chunk = named[0].2;
        let budget = Arc::clone(&self.budget);
        let name = self.config.pipeline.name.clone();
        // This wrapper is the factory the runtime drops before the sink
        // drain; the queue clones it captures die there.
        let chains = move |thread: usize| {
            factory(ChainCtx {
                thread,
                queues: default_queues.clone(),
                budget: Arc::clone(&budget),
                pipeline: name.clone(),
                chunk: default_chunk,
                named: named.clone(),
            })
        };
        let runtime = PipelineRuntime::new(
            self.config,
            source,
            chains,
            SinkRuntime {
                queues: intro_queues,
                drain: combine_drains(drains),
                probe: combine_probes(probes),
                failures: self.sink_failures,
            },
            self.budget,
        )
        .with_options(self.options)
        .with_io_runtime(self.io);
        Ok(match self.feed {
            Some((feed, FeedOrigin::Seeded)) => runtime.with_seeded_feed(feed),
            Some((feed, _)) => runtime.with_feed(feed),
            None => runtime,
        })
    }

    /// [`into_runtime`](Self::into_runtime) + [`PipelineRuntime::run`]:
    /// run the pipeline to completion, blocking until a shutdown signal
    /// drains it or a fatal error stops it.
    pub fn run<S: Source + 'static>(self, source: S) -> Result<ExitReport, PipelineError> {
        Ok(self.into_runtime(source)?.run()?)
    }
}

/// Compose per-sink drain hooks into one. Every sink drains concurrently under
/// the shared deadline and their reports are summed, so a multi-sink drain
/// respects one wall-clock budget instead of N sequential ones.
fn combine_drains(drains: Vec<SinkDrainFn>) -> SinkDrainFn {
    Box::new(move |deadline| {
        Box::pin(async move {
            let mut set = tokio::task::JoinSet::new();
            for drain in drains {
                #[expect(
                    clippy::disallowed_methods,
                    reason = "the set drops with the drain future, which aborts every sink's drain still running"
                )]
                set.spawn(drain(deadline));
            }
            let mut total = DrainReport::default();
            while let Some(res) = set.join_next().await {
                match res {
                    Ok(report) => {
                        total.flushed += report.flushed;
                        total.abandoned += report.abandoned;
                    }
                    // The panicked sink's counts are unknowable; its parked
                    // acks still fail on drop, so at-least-once holds. The
                    // report is incomplete and says so.
                    Err(e) => tracing::error!(
                        error = %e,
                        "a sink drain task panicked; its counts are missing \
                         from the drain report"
                    ),
                }
            }
            total
        })
    })
}

/// Compose per-sink readiness probes into one. Every sink is probed and the
/// result is connected only when all succeed (readiness is not a hot path, so
/// the sequential short-circuit is fine).
fn combine_probes(probes: Vec<SinkProbeFn>) -> Option<SinkProbeFn> {
    if probes.is_empty() {
        return None;
    }
    let probes = Arc::new(probes);
    Some(Box::new(move || {
        let probes = Arc::clone(&probes);
        Box::pin(async move {
            for probe in probes.iter() {
                probe().await?;
            }
            Ok(())
        })
    }))
}

#[expect(
    clippy::disallowed_methods,
    reason = "a test thread ends with the call it runs, or serves a stub until the test process exits"
)]
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::config::ComponentConfig;
    use crate::error::SinkError;
    use crate::feed::memory::MemoryFeed;
    use crate::pipeline::ExitState;
    use crate::pipeline::fakes::{
        ChainMode, ChainShared, FakeChain, FakeSource, LaneSpec, Script, SourceLog, batches,
        test_config, test_options,
    };
    use crate::record::PartitionId;
    use crate::sink::{SealedBatch, SinkParts, SinkPoolConfig};
    use crate::source::LaneId;
    use spate_test::wait_until;
    use std::sync::Mutex;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    struct NullWriter;
    impl crate::sink::ShardWriter for NullWriter {
        type Endpoint = ();
        async fn write_batch(&self, (): &(), _batch: &SealedBatch) -> Result<(), SinkError> {
            Ok(())
        }
    }

    /// Records each [`SinkMetricsCtx::meter`] the builder hands it.
    struct ProbeWriter {
        seen: Arc<Mutex<Vec<Option<crate::metrics::Meter>>>>,
    }
    impl crate::sink::ShardWriter for ProbeWriter {
        type Endpoint = ();
        fn attach_metrics(&mut self, cx: SinkMetricsCtx) {
            self.seen.lock().expect("probe lock").push(cx.meter);
        }
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

    /// The builder hands a sink writer one context whose `Meter` is scoped to
    /// the sink role and the declared `component_type`, and no `Meter` when
    /// the sink keeps the default `"custom"`.
    #[test]
    fn a_sink_writer_receives_one_context_scoped_to_the_sink_role() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let cfg = test_config(1);
        let name = cfg.pipeline.name.clone();
        let parts = SinkParts::new(
            ProbeWriter {
                seen: Arc::clone(&seen),
            },
            vec![vec![()]],
            SinkPoolConfig::default(),
        )
        .with_component_type("probe");
        let _pipeline = Pipeline::from_config(cfg)
            .expect("builder")
            .sink(parts)
            .expect("sink");
        let meter = {
            let mut seen = seen.lock().expect("probe lock");
            assert_eq!(seen.len(), 1, "attach_metrics runs once per writer");
            seen.pop()
                .flatten()
                .expect("a declared component_type scopes a Meter")
        };
        let rendered = crate::metrics::render_local(|| {
            meter.counter("probe_total", &[]).increment(1);
        });
        assert_eq!(
            spate_test::metric_value(
                &rendered,
                "spate_probe_sink_probe_total",
                &[
                    ("pipeline", name.as_str()),
                    ("component", "sink"),
                    ("component_type", "probe"),
                ],
            ),
            Some(1.0),
            "{rendered}"
        );

        let seen = Arc::new(Mutex::new(Vec::new()));
        let parts = SinkParts::new(
            ProbeWriter {
                seen: Arc::clone(&seen),
            },
            vec![vec![()]],
            SinkPoolConfig::default(),
        );
        let _pipeline = Pipeline::from_config(test_config(1))
            .expect("builder")
            .sink(parts)
            .expect("sink");
        let seen = seen.lock().expect("probe lock");
        assert_eq!(seen.len(), 1, "attach_metrics runs once per writer");
        assert!(seen[0].is_none(), "the default `custom` type gets no Meter");
    }

    #[test]
    fn duplicate_sink_name_errors() {
        // A second bare `.sink()` collides on the reserved "default" name.
        let p = Pipeline::from_config(test_config(1))
            .expect("builder")
            .sink(null_sink(1))
            .expect("first sink");
        assert!(matches!(
            p.sink(null_sink(1)).err(),
            Some(BuildError::DuplicateSinkName(name)) if name == "default"
        ));

        // The same explicit name twice also collides.
        let p = Pipeline::from_config(test_config(1))
            .expect("builder")
            .add_sink("a", null_sink(1))
            .expect("first");
        assert!(matches!(
            p.add_sink("a", null_sink(1)).err(),
            Some(BuildError::DuplicateSinkName(name)) if name == "a"
        ));
    }

    #[test]
    fn distinct_named_sinks_install() {
        Pipeline::from_config(test_config(1))
            .expect("builder")
            .add_sink("a", null_sink(1))
            .expect("first")
            .add_sink("b", null_sink(2))
            .expect("second install with a distinct name");
    }

    /// Two *live* pipelines with the same pipeline and sink name would resolve
    /// the same `spate_sink_*` and `spate_queue_*` gauge series. The second's
    /// `add_sink` must refuse; those gauges cannot be shared, and letting both
    /// through leaves each overwriting the other's readings.
    ///
    /// A pipeline rebuilt *sequentially* (the supported way to replace one in
    /// a process) is fine. The first's claim frees when it is dropped, so the
    /// rebuild re-owns the series. Only overlap collides.
    #[test]
    fn two_live_pipelines_on_one_name_collide_but_sequential_reuse_is_fine() {
        let named = || {
            let mut cfg = test_config(1);
            cfg.pipeline.name = "shared-name".into();
            cfg
        };

        // First pipeline, held alive across the second's build.
        let first = Pipeline::from_config(named())
            .expect("builder")
            .sink(null_sink(2))
            .expect("first pipeline claims the sink series");

        let collision = Pipeline::from_config(named())
            .expect("builder")
            .sink(null_sink(2));
        assert!(
            matches!(collision.err(), Some(BuildError::DuplicateSeries(msg)) if msg.contains("spate_")),
            "a second live pipeline on the same name must fail on the series claim"
        );

        // Drop the first, freeing its claims, and rebuild: the series is
        // available again.
        drop(first);
        Pipeline::from_config(named())
            .expect("builder")
            .sink(null_sink(2))
            .expect("sequential rebuild re-owns the freed series");
    }

    #[test]
    fn reserved_and_empty_sink_names_error() {
        // "sink" is the default sink's metric label; installing a sink under
        // that name would silently merge the two series.
        let p = Pipeline::from_config(test_config(1)).expect("builder");
        assert!(matches!(
            p.add_sink("sink", null_sink(1)).err(),
            Some(BuildError::Sink(msg)) if msg.contains("reserved")
        ));

        let p = Pipeline::from_config(test_config(1)).expect("builder");
        assert!(matches!(
            p.add_sink("", null_sink(1)).err(),
            Some(BuildError::Sink(msg)) if msg.contains("non-empty")
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
    /// coverage and end-to-end assembly, not drop ordering. Drain containment
    /// (`sink/worker.rs`) turns a leaked `ShardQueues` clone into a bounded
    /// abandon rather than an unbounded hang, so completion here does not
    /// imply clean drop ordering. The drop-ordering and at-least-once contract
    /// is covered where it can still fail observably: the whole-assembly test
    /// in `spate-test`'s `tests/bundle.rs`, which routes real data through
    /// `ctx.queues` and asserts the watermark only advances past the last
    /// record after a durable write.
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
        let config = test_config(2);
        let pipeline_name = config.pipeline.name.clone();
        let runtime = Pipeline::from_config(config)
            .expect("builder")
            .sink(null_sink(1))
            .expect("sink")
            .chains(move |ctx| {
                assert_eq!(ctx.pipeline, pipeline_name);
                seen.lock().unwrap().push(ctx.thread);
                fake_chain(&cs, &log)
            })
            .runtime_options(test_options())
            .into_runtime(source)
            .expect("into_runtime");

        let shutdown = runtime.shutdown_handle();
        let join = std::thread::spawn(move || runtime.run());
        wait_until(Duration::from_secs(5), "payloads consumed", || {
            chain_shared.consumed.load(Ordering::Relaxed) == 20
        });
        shutdown.trigger();
        let report = join.join().unwrap().unwrap();
        assert_eq!(report.state, ExitState::Completed);

        let mut threads = seen_threads.lock().unwrap().clone();
        threads.sort_unstable();
        assert_eq!(threads, vec![0, 1], "one ChainCtx per pipeline thread");
    }

    /// Pins that `Pipeline::feed` and `Pipeline::seeded_feed` reach the
    /// source as `Given` and `Seeded`.
    #[test]
    fn each_feed_setter_hands_the_source_its_origin() {
        type Setter = fn(Pipeline, Arc<dyn Feed>) -> Pipeline;
        let setters: [(Setter, FeedOrigin); 2] = [
            (Pipeline::feed, FeedOrigin::Given),
            (Pipeline::seeded_feed, FeedOrigin::Seeded),
        ];
        for (set, origin) in setters {
            let (source, shared, script) = FakeSource::new();
            shared.lock().unwrap().accept_feed = true;
            script.lock().unwrap().push_back(Script::Drained);
            let chain_shared = Arc::new(ChainShared::default());
            let log = Arc::clone(&shared);
            let mut config = test_config(1);
            config.feed = Some(serde_yaml::from_str("memory: {}").expect("section"));
            let pipeline = Pipeline::from_config(config)
                .expect("builder")
                .sink(null_sink(1))
                .expect("sink")
                .chains(move |_ctx| fake_chain(&chain_shared, &log))
                .runtime_options(test_options());
            let report = set(pipeline, Arc::new(MemoryFeed::new()))
                .run(source)
                .expect("run");
            assert_eq!(report.state, ExitState::Completed);
            assert_eq!(shared.lock().unwrap().feed_origin, Some(origin));
        }
    }

    /// A sink section for the `fake` backend with the framework `blocks`
    /// beside it, as a YAML file would give it.
    fn fake_section(blocks: &str) -> SinkSection {
        let mut keys: serde_yaml::Mapping = serde_yaml::from_str(blocks).expect("blocks yaml");
        keys.insert("fake".into(), serde_yaml::Value::Null);
        serde_yaml::from_value(serde_yaml::Value::Mapping(keys)).expect("a sink section")
    }

    /// A sink section assigned to the config in code carries no label, and
    /// its `chunk:` errors still name the sink, at install and from
    /// `validate`.
    #[test]
    fn a_chunk_mistake_names_a_sink_assigned_in_code() {
        use crate::config::{PipelineConfig, PipelineSection, YamlValue};
        let memory = || ComponentConfig::new("memory", YamlValue::Mapping(Default::default()));
        let zero = || fake_section("chunk: { target_bytes: 0 }");
        let base = || PipelineConfig::new(PipelineSection::new("demo"), memory(), memory());
        let mut multi = base();
        multi.sink = None;
        multi.sinks = Some(std::collections::BTreeMap::from([(
            "eu".to_owned(),
            zero(),
        )]));
        let mut single = base();
        single.sink = Some(zero());
        for (config, name, want) in [
            (multi, "eu", "sinks.eu.chunk.target_bytes"),
            (single, "default", "sink.chunk.target_bytes"),
        ] {
            let want = format!("{want}: must be greater than zero");
            assert_eq!(config.validate().expect_err("refused").to_string(), want);
            let err = Pipeline::from_config(config)
                .expect("builder")
                .add_sink(name, null_sink(1))
                .map(drop)
                .expect_err("refused at install");
            assert_eq!(err.to_string(), want);
        }
    }

    /// A `chunk:` mistake on a `sinks:` entry reads the same whether config
    /// load or the builder catches it, and on the `sink:` form names the
    /// section.
    #[test]
    fn a_chunk_mistake_reads_the_same_at_load_and_at_install() {
        use crate::config::{PipelineConfig, PipelineSection, YamlValue};
        use std::collections::BTreeMap;
        let load = |sinks: &str| {
            let yaml = format!("pipeline: {{ name: demo }}\nsource: {{ memory: {{}} }}\n{sinks}");
            PipelineConfig::from_str(&yaml)
                .expect_err("refused at load")
                .to_string()
        };
        let install = |body: &str| {
            let cfg = PipelineConfig::new_multi_sink(
                PipelineSection::new("demo"),
                ComponentConfig::new("memory", YamlValue::Mapping(Default::default())),
                BTreeMap::from([(
                    "eu".to_owned(),
                    ComponentConfig::new("fake", YamlValue::Null),
                )]),
            );
            let mut cfg = cfg;
            cfg.sinks = Some(BTreeMap::from([("eu".to_owned(), fake_section(body))]));
            Pipeline::from_config(cfg)
                .expect("builder")
                .add_sink_with("eu", null_sink(1), SinkOptions::default())
                .map(drop)
                .expect_err("refused at install")
                .to_string()
        };
        for (body, want) in [
            (
                "chunk: { target_bytes: 0 }",
                "sinks.eu.chunk.target_bytes: must be greater than zero",
            ),
            (
                "chunk: { bogus: 1 }",
                "sinks.eu.chunk.bogus: unknown field `bogus`, expected `target_bytes` or \
                 `encode_policy`",
            ),
        ] {
            assert_eq!(
                load(&format!("sinks:\n  eu: {{ fake: {{}}, {body} }}\n")),
                want
            );
            assert_eq!(install(body), want);
        }
        assert_eq!(
            load("sink: { fake: {}, chunk: { target_bytes: 0 } }\n"),
            "sink.chunk.target_bytes: must be greater than zero"
        );
    }

    #[test]
    fn yaml_and_programmatic_chunk_on_one_sink_collide() {
        // Decl-once: setting the YAML `chunk:` block AND `SinkOptions::with_chunk`
        // on the same sink is a config error, not a silent override.
        let mut config = test_config(1);
        config.sink = Some(fake_section("chunk: { target_bytes: 128KiB }"));
        let err = Pipeline::from_config(config)
            .expect("builder")
            .sink_with(
                null_sink(1),
                SinkOptions::default().with_chunk(ChunkConfig::default()),
            )
            .err();
        assert!(
            matches!(&err, Some(BuildError::Config(ConfigError::Validation(m))) if m.contains("exactly one place")),
            "{err:?}"
        );
    }

    #[test]
    fn programmatic_zero_target_bytes_is_rejected_at_install() {
        let err = Pipeline::from_config(test_config(1))
            .expect("builder")
            .sink_with(
                null_sink(1),
                SinkOptions::default().with_chunk(ChunkConfig {
                    target_bytes: 0,
                    encode_policy: crate::error::ErrorPolicy::Skip,
                }),
            )
            .err();
        assert!(
            matches!(&err, Some(BuildError::Config(ConfigError::Validation(m))) if m.contains("chunk.target_bytes")),
            "{err:?}"
        );
    }

    /// Pins that a pool built in code is held to `SinkPoolConfig::validate`
    /// when the builder installs it, naming the sink and the key.
    #[test]
    fn programmatic_invalid_pool_is_rejected_at_install() {
        let pool = |mutate: fn(&mut SinkPoolConfig)| {
            let mut pool = SinkPoolConfig::default();
            mutate(&mut pool);
            pool
        };
        let cases = [
            (
                pool(|c| c.batch.max_rows = 0),
                "batch.max_rows must be at least 1",
            ),
            (
                pool(|c| c.inflight.max_per_shard = 0),
                "inflight.max_per_shard must be at least 1",
            ),
            (
                pool(|c| c.retry.initial = Duration::ZERO),
                "retry.initial and retry.max must be non-zero",
            ),
            (
                pool(|c| c.breaker.failure_threshold = 0),
                "breaker.failure_threshold must be at least 1",
            ),
        ];
        for (pool, key) in cases {
            let sink = SinkParts::new(NullWriter, vec![vec![()]], pool).with_component_type("null");
            let err = Pipeline::from_config(test_config(1))
                .expect("builder")
                .sink(sink)
                .err();
            let want = format!("sink: {key}");
            assert!(
                matches!(&err, Some(BuildError::Config(ConfigError::Validation(m))) if *m == want),
                "{err:?}"
            );
        }
        // A named sink installed beside the `sink:` form reads as a `sinks:`
        // entry.
        let sink = SinkParts::new(NullWriter, vec![vec![()]], pool(|c| c.batch.max_rows = 0))
            .with_component_type("null");
        let err = Pipeline::from_config(test_config(1))
            .expect("builder")
            .add_sink_with("eu", sink, SinkOptions::default())
            .err();
        assert!(
            matches!(
                &err,
                Some(BuildError::Config(ConfigError::Validation(m)))
                    if m == "sinks.eu: batch.max_rows must be at least 1"
            ),
            "{err:?}"
        );
    }

    /// Pins that an invalid pool fails before the builder claims any metric
    /// series. A live pipeline already owns the sink's series, and the
    /// rejected install still reports the pool error.
    #[test]
    fn an_invalid_pool_is_rejected_before_its_series_are_claimed() {
        let named = || {
            let mut cfg = test_config(1);
            cfg.pipeline.name = "pool-check-order".into();
            cfg
        };
        let _live = Pipeline::from_config(named())
            .expect("builder")
            .sink(null_sink(1))
            .expect("the live pipeline claims the sink series");
        let mut pool = SinkPoolConfig::default();
        pool.batch.max_rows = 0;
        let sink = SinkParts::new(NullWriter, vec![vec![()]], pool).with_component_type("null");
        let err = Pipeline::from_config(named())
            .expect("builder")
            .sink(sink)
            .err();
        assert!(
            matches!(
                &err,
                Some(BuildError::Config(ConfigError::Validation(m)))
                    if m == "sink: batch.max_rows must be at least 1"
            ),
            "{err:?}"
        );
    }

    #[test]
    fn each_split_branch_resolves_its_own_chunk() {
        let (source, shared, script) = FakeSource::new();
        script
            .lock()
            .unwrap()
            .push_back(Script::Assign(vec![LaneSpec {
                id: LaneId(0),
                partition: PartitionId(0),
                batches: batches(&[0..1, 1..2]),
            }]));

        let mut config = test_config(1);
        config.sink = None;
        let mut sinks = std::collections::BTreeMap::new();
        sinks.insert(
            "a".to_string(),
            fake_section("chunk: { target_bytes: 128KiB }"),
        );
        sinks.insert(
            "b".to_string(),
            fake_section("chunk: { target_bytes: 512KiB }"),
        );
        config.sinks = Some(sinks);

        let captured: Arc<Mutex<Vec<(usize, usize)>>> = Arc::new(Mutex::new(Vec::new()));
        let cap = Arc::clone(&captured);
        let chain_shared = Arc::new(ChainShared::default());
        let cs = Arc::clone(&chain_shared);
        let log = Arc::clone(&shared);
        let runtime = Pipeline::from_config(config)
            .expect("builder")
            .add_sink("a", null_sink(1))
            .expect("sink a")
            .add_sink("b", null_sink(1))
            .expect("sink b")
            .chains(move |ctx| {
                cap.lock().unwrap().push((
                    ctx.sink("a").chunk.target_bytes,
                    ctx.sink("b").chunk.target_bytes,
                ));
                fake_chain(&cs, &log)
            })
            .runtime_options(test_options())
            .into_runtime(source)
            .expect("into_runtime");

        let shutdown = runtime.shutdown_handle();
        let join = std::thread::spawn(move || runtime.run());
        wait_until(Duration::from_secs(5), "chain factory ran", || {
            !captured.lock().unwrap().is_empty()
        });
        shutdown.trigger();
        let _ = join.join().unwrap();

        assert_eq!(
            captured.lock().unwrap()[0],
            (128 * 1024, 512 * 1024),
            "each branch resolves its own per-sink chunk"
        );
    }

    /// A [`FrameCodec`](crate::sink::FrameCodec) that writes a fixed prefix
    /// before the chunk.
    struct PrefixCodec {
        prefix: &'static [u8],
        out: Vec<u8>,
    }

    impl crate::sink::FrameCodec for PrefixCodec {
        fn encode_frame(&mut self, chunk: &[u8]) -> Result<&[u8], SinkError> {
            self.out.clear();
            self.out.extend_from_slice(self.prefix);
            self.out.extend_from_slice(chunk);
            Ok(&self.out)
        }
    }

    fn prefix_codec(prefix: &'static [u8]) -> crate::sink::FrameCodecFactory {
        crate::sink::FrameCodecFactory::new(move || {
            Box::new(PrefixCodec {
                prefix,
                out: Vec::new(),
            })
        })
    }

    /// The codec a sink's parts carry reaches that sink's queues in every
    /// chain, and no other sink's.
    #[test]
    fn each_sinks_frame_codec_reaches_every_chain() {
        let (source, shared, script) = FakeSource::new();
        script
            .lock()
            .unwrap()
            .push_back(Script::Assign(vec![LaneSpec {
                id: LaneId(0),
                partition: PartitionId(0),
                batches: batches(&[0..1, 1..2]),
            }]));

        let config = test_config(2);
        type Coded = Option<Vec<u8>>;
        let captured: Arc<Mutex<Vec<(Coded, Coded)>>> = Arc::new(Mutex::new(Vec::new()));
        let cap = Arc::clone(&captured);
        let chain_shared = Arc::new(ChainShared::default());
        let cs = Arc::clone(&chain_shared);
        let log = Arc::clone(&shared);
        let code = |ctx: &ChainCtx, sink: &str| -> Coded {
            let mut codec = ctx.sink(sink).queues.new_frame_codec()?;
            Some(codec.encode_frame(b"probe").expect("prefix codec").to_vec())
        };
        let runtime = Pipeline::from_config(config)
            .expect("builder")
            .add_sink("a", null_sink(1).with_frame_codec(prefix_codec(b"A:")))
            .expect("sink a")
            .add_sink("b", null_sink(1).with_frame_codec(prefix_codec(b"B:")))
            .expect("sink b")
            .chains(move |ctx| {
                cap.lock().unwrap().push((code(&ctx, "a"), code(&ctx, "b")));
                fake_chain(&cs, &log)
            })
            .runtime_options(test_options())
            .into_runtime(source)
            .expect("into_runtime");

        let shutdown = runtime.shutdown_handle();
        let join = std::thread::spawn(move || runtime.run());
        wait_until(Duration::from_secs(5), "every chain factory ran", || {
            captured.lock().unwrap().len() == 2
        });
        shutdown.trigger();
        let _ = join.join().unwrap();

        let captured = captured.lock().unwrap();
        for (thread, coded) in captured.iter().enumerate() {
            assert_eq!(
                coded,
                &(Some(b"A:probe".to_vec()), Some(b"B:probe".to_vec())),
                "chain {thread} codes each sink with that sink's codec"
            );
        }
    }

    /// Drive a capture of `ctx.chunk()` through a full assembly, shared by
    /// the YAML-block and `with_chunk` propagation tests below.
    fn captured_default_chunk(
        build: impl FnOnce(Pipeline) -> Result<Pipeline, BuildError>,
        config: PipelineConfig,
    ) -> usize {
        let (source, shared, script) = FakeSource::new();
        script
            .lock()
            .unwrap()
            .push_back(Script::Assign(vec![LaneSpec {
                id: LaneId(0),
                partition: PartitionId(0),
                batches: batches(&[0..1, 1..2]),
            }]));
        let captured: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let cap = Arc::clone(&captured);
        let chain_shared = Arc::new(ChainShared::default());
        let cs = Arc::clone(&chain_shared);
        let log = Arc::clone(&shared);
        let runtime = build(Pipeline::from_config(config).expect("builder"))
            .expect("sink install")
            .chains(move |ctx| {
                cap.lock().unwrap().push(ctx.chunk().target_bytes);
                fake_chain(&cs, &log)
            })
            .runtime_options(test_options())
            .into_runtime(source)
            .expect("into_runtime");

        let shutdown = runtime.shutdown_handle();
        let join = std::thread::spawn(move || runtime.run());
        wait_until(Duration::from_secs(5), "chain factory ran", || {
            !captured.lock().unwrap().is_empty()
        });
        shutdown.trigger();
        let _ = join.join().unwrap();
        let captured = captured.lock().unwrap();
        captured[0]
    }

    #[test]
    fn default_sink_yaml_chunk_reaches_ctx_chunk() {
        // The headline single-sink path: `sink.chunk` must arrive at
        // the chain factory via `ctx.chunk()`, not silently stay the default.
        let mut config = test_config(1);
        config.sink = Some(fake_section("chunk: { target_bytes: 128KiB }"));
        let bytes = captured_default_chunk(|p| p.sink(null_sink(1)), config);
        assert_eq!(bytes, 128 * 1024, "YAML chunk block reaches ctx.chunk()");
    }

    #[test]
    fn with_chunk_reaches_ctx_chunk() {
        let bytes = captured_default_chunk(
            |p| {
                p.sink_with(
                    null_sink(1),
                    SinkOptions::default().with_chunk(ChunkConfig {
                        target_bytes: 96 * 1024,
                        encode_policy: crate::error::ErrorPolicy::Skip,
                    }),
                )
            },
            test_config(1),
        );
        assert_eq!(bytes, 96 * 1024, "with_chunk reaches ctx.chunk()");
    }

    /// The slot count of the queues installed for the sink `name`.
    fn installed_capacity(p: &Pipeline, name: &str) -> usize {
        let (_, assembly) = p.sinks.iter().find(|(n, _)| n == name).expect("installed");
        assembly.queues.capacity()
    }

    /// The message of a config error from installing `options` on the default
    /// sink of `config`.
    fn install_error(config: PipelineConfig, options: SinkOptions) -> String {
        match Pipeline::from_config(config)
            .expect("builder")
            .sink_with(null_sink(1), options)
        {
            Err(BuildError::Config(e)) => e.to_string(),
            Err(other) => panic!("not a config error: {other:?}"),
            Ok(_) => panic!("installed"),
        }
    }

    fn chunk_of(target_bytes: usize) -> ChunkConfig {
        ChunkConfig {
            target_bytes,
            encode_policy: crate::error::ErrorPolicy::Skip,
        }
    }

    /// The YAML `queue:` block reaches the installed queues, sized from the
    /// sink's own chunk: 2 MiB of 128 KiB chunks is 16 slots, which neither
    /// the default (8) nor the default chunk (32) gives.
    #[test]
    fn the_yaml_queue_sizes_the_installed_queues_from_the_sink_chunk() {
        let mut config = test_config(1);
        config.sink = Some(fake_section(
            "queue: { max_bytes: 2MiB }\nchunk: { target_bytes: 128KiB }",
        ));
        let p = Pipeline::from_config(config)
            .expect("builder")
            .sink(null_sink(2))
            .expect("installed");
        assert_eq!(installed_capacity(&p, "default"), 16);
    }

    /// Pins that a sink with no `queue:` keeps 8 slots whatever its chunk size.
    #[test]
    fn an_unset_queue_keeps_eight_slots() {
        let mut config = test_config(1);
        config.sink = Some(fake_section("chunk: { target_bytes: 1MiB }"));
        let p = Pipeline::from_config(config)
            .expect("builder")
            .sink(null_sink(1))
            .expect("installed");
        assert_eq!(installed_capacity(&p, "default"), 8);
    }

    /// A queue set in code meets the same bounds as one in YAML.
    #[test]
    fn a_queue_set_in_code_is_bounded_at_install() {
        let too_many = (tokio::sync::Semaphore::MAX_PERMITS as u64) + 1;
        for (bytes, want) in [
            (0, "sink.queue.max_bytes: must be greater than zero"),
            (too_many, "sink.queue.max_bytes: makes "),
        ] {
            let options = SinkOptions::default()
                .with_chunk(chunk_of(1))
                .with_queue(QueueSection::new(bytes));
            let err = install_error(test_config(1), options);
            assert!(err.starts_with(want), "{bytes}: {err}");
        }
    }

    /// A zero chunk set in code is refused before the queue divides by it.
    #[test]
    fn a_zero_chunk_with_a_queue_is_refused_not_a_panic() {
        let options = SinkOptions::default()
            .with_chunk(chunk_of(0))
            .with_queue(QueueSection::new(1024 * 1024));
        assert_eq!(
            install_error(test_config(1), options),
            "invalid configuration: sink: chunk.target_bytes must be greater than zero"
        );
    }

    /// Each framework key is taken from one place: YAML and `SinkOptions`
    /// together on one sink are refused, naming the sink and the key.
    #[test]
    fn each_key_set_in_yaml_and_in_code_collides() {
        let cases: [(&str, SinkOptions); 5] = [
            (
                "queue: { max_bytes: 1MiB }",
                SinkOptions::default().with_queue(QueueSection::new(1024 * 1024)),
            ),
            (
                "batch: {}",
                SinkOptions::default().with_batch(BatchConfig::default()),
            ),
            (
                "inflight: {}",
                SinkOptions::default().with_inflight(InflightConfig::default()),
            ),
            (
                "retry: {}",
                SinkOptions::default().with_retry(RetryConfig::default()),
            ),
            (
                "breaker: {}",
                SinkOptions::default().with_breaker(BreakerConfig::default()),
            ),
        ];
        for (block, options) in cases {
            let key = block.split(':').next().expect("key");
            let mut config = test_config(1);
            config.sink = Some(fake_section(block));
            assert_eq!(
                install_error(config, options),
                format!(
                    "invalid configuration: sink: set `{key}` in exactly one place, the YAML `{key}:` block or \
                     `SinkOptions::with_{key}`, not both"
                )
            );
        }
    }

    /// A pool resolved for the default sink of a config whose section holds
    /// `blocks`, over `base`.
    fn resolve_pool(
        blocks: &str,
        options: &SinkOptions,
        base: SinkPoolConfig,
    ) -> Result<SinkPoolConfig, String> {
        let mut config = test_config(1);
        config.sink = Some(fake_section(blocks));
        options
            .resolve(&config, "default", base)
            .map(|r| r.pool)
            .map_err(|e| e.to_string())
    }

    /// A YAML pool block overlays the connector's base field by field, and a
    /// block set in code replaces it whole.
    #[test]
    fn a_yaml_pool_block_overlays_the_base_and_code_replaces_it() {
        let mut base = SinkPoolConfig::default();
        base.batch.max_bytes = 1_000_001;
        base.retry.max_attempts = 5;
        base.breaker.failure_threshold = 9;
        base.inflight.max_per_shard = 6;
        let none = SinkOptions::default();

        let pool = resolve_pool(
            "batch: { max_rows: 7 }\nretry: { initial: 1ms }\nbreaker: { open_for: 2s }",
            &none,
            base,
        )
        .expect("resolves");
        assert_eq!((pool.batch.max_rows, pool.batch.max_bytes), (7, 1_000_001));
        assert_eq!(
            (pool.retry.initial, pool.retry.max_attempts),
            (Duration::from_millis(1), 5)
        );
        assert_eq!(
            (pool.breaker.open_for, pool.breaker.failure_threshold),
            (Duration::from_secs(2), 9)
        );
        assert_eq!(pool.inflight.max_per_shard, 6);

        let code = SinkOptions::default().with_batch(BatchConfig::default());
        let pool = resolve_pool("{}", &code, base).expect("resolves");
        assert_eq!(pool.batch, BatchConfig::default());
    }

    /// An overlay block is decoded with its path, and the merged pool is held
    /// to `SinkPoolConfig::validate`.
    #[test]
    fn a_bad_yaml_pool_block_is_refused_at_install() {
        let none = SinkOptions::default();
        let base = SinkPoolConfig::default();
        let err = resolve_pool("batch: { max_rowz: 1 }", &none, base).unwrap_err();
        assert!(
            err.starts_with("sink.batch.max_rowz: unknown field"),
            "{err}"
        );
        let err = resolve_pool("inflight: { max_per_shard: 0 }", &none, base).unwrap_err();
        assert!(
            err.starts_with("invalid configuration: sink: inflight.max_per_shard"),
            "{err}"
        );
        let err = resolve_pool("retry: { max: 50ms }", &none, base).unwrap_err();
        assert!(
            err.starts_with("invalid configuration: sink: retry.initial"),
            "{err}"
        );
    }

    /// A framework block on a sink section that nothing installs is warned
    /// about, one block at a time.
    #[test]
    fn each_block_on_a_sink_nothing_installs_is_warned_about() {
        for block in [
            "queue: { max_bytes: 1MiB }",
            "chunk: {}",
            "batch: {}",
            "inflight: {}",
            "retry: {}",
            "breaker: {}",
        ] {
            let key = block.split(':').next().expect("key");
            let mut config = test_config(1);
            config.sink = None;
            config.sinks = Some(std::collections::BTreeMap::from([
                ("hot".to_owned(), fake_section("{}")),
                ("cold".to_owned(), fake_section(block)),
            ]));
            let lines = spate_test::capture_logs(tracing::Level::WARN, || {
                let (source, shared, _script) = FakeSource::new();
                let chain_shared = Arc::new(ChainShared::default());
                let runtime = Pipeline::from_config(config)
                    .expect("builder")
                    .add_sink("hot", null_sink(1))
                    .expect("installed")
                    .chains(move |_| fake_chain(&chain_shared, &shared))
                    .runtime_options(test_options())
                    .into_runtime(source)
                    .expect("into_runtime");
                drop(runtime);
            });
            let warned: Vec<&String> = lines
                .iter()
                .filter(|l| l.contains("no sink was installed"))
                .collect();
            assert_eq!(warned.len(), 1, "{key}: {lines:?}");
            assert!(warned[0].contains(&format!("`{key}`")), "{key}: {lines:?}");
            assert!(warned[0].contains("cold"), "{key}: {lines:?}");
        }
    }

    /// A manual assembly that calls `SinkOptions::resolve` gets the chunk and
    /// slot count `add_sink_with` installs.
    #[test]
    fn resolve_gives_what_add_sink_with_installs() {
        let mut config = test_config(1);
        config.sink = Some(fake_section(
            "queue: { max_bytes: 3MiB }\nchunk: { target_bytes: 256KiB }",
        ));
        let resolved = SinkOptions::default()
            .resolve(&config, "default", SinkPoolConfig::default())
            .expect("resolves");
        assert_eq!(resolved.queue_capacity, 12);
        let p = Pipeline::from_config(config)
            .expect("builder")
            .sink(null_sink(1))
            .expect("installed");
        assert_eq!(installed_capacity(&p, "default"), resolved.queue_capacity);
        let (_, assembly) = &p.sinks[0];
        assert_eq!(assembly.chunk.target_bytes, resolved.chunk.target_bytes);
    }

    #[test]
    fn a_body_chunk_is_rejected_by_from_config() {
        // A config built in code skips `validate`, and a sink that decodes no
        // body would ignore a `chunk:` in it, so `from_config` refuses one in
        // any body itself.
        let chunk = || serde_yaml::from_str("chunk: { target_bytes: 64KiB }").expect("yaml");
        let mut on_source = test_config(1);
        on_source.source = ComponentConfig::new("fake", chunk());
        let mut on_sink = test_config(1);
        on_sink.sink = Some(SinkSection::new(ComponentConfig::new("capture", chunk())));
        for (config, want) in [
            (
                on_source,
                "fake.chunk: `chunk:` is only valid on a sink section",
            ),
            (
                on_sink,
                "capture.chunk: `chunk:` sits beside `capture:` in the sink section, not inside it",
            ),
        ] {
            let Err(err) = Pipeline::from_config(config) else {
                panic!("built")
            };
            assert_eq!(err.to_string(), want);
        }
    }

    /// Regression: the runtime must hand the source the framework's
    /// source-stage handles at `open`.
    ///
    /// `SourceMetrics::set_partition_lag` has exactly one possible caller, the
    /// source, because only the client can see the log end. When those handles
    /// are reachable only through a connector-side builder that nothing calls,
    /// `spate_source_lag_records` renders a permanent `0` on every Kafka
    /// pipeline. A maximally backlogged consumer reports no lag, and any alert
    /// or autoscaler keyed on it reads as caught up. Nothing fails; the series
    /// is always zero. This asserts the seam is connected. The Kafka crate's
    /// `a_backlogged_consumer_publishes_its_lag` covers the value arriving,
    /// which this test does not.
    #[test]
    fn source_open_receives_the_stage_metrics() {
        let (source, shared, script) = FakeSource::new();
        script
            .lock()
            .unwrap()
            .push_back(Script::Assign(vec![LaneSpec {
                id: LaneId(0),
                partition: PartitionId(0),
                batches: batches(&[0..2, 2..4]),
            }]));
        let chain_shared = Arc::new(ChainShared::default());
        let cs = Arc::clone(&chain_shared);
        let log = Arc::clone(&shared);
        let runtime = Pipeline::from_config(test_config(1))
            .expect("builder")
            .sink(null_sink(1))
            .expect("sink")
            .chains(move |_| fake_chain(&cs, &log))
            .runtime_options(test_options())
            .into_runtime(source)
            .expect("into_runtime");

        let shutdown = runtime.shutdown_handle();
        let join = std::thread::spawn(move || runtime.run());
        wait_until(Duration::from_secs(5), "source opened", || {
            shared.lock().unwrap().opened
        });
        shutdown.trigger();
        join.join().unwrap().unwrap();

        assert!(
            shared.lock().unwrap().stage_metrics_attached,
            "the runtime must share SourceMetrics with the source at open, \
             or consumer lag can never be published"
        );
    }

    /// The whole-builder happy path through `run()` (not `into_runtime`),
    /// exercised over the real SinkPool. Completes and commits.
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

        // The run goes on its own thread, so a wait that fails here fails the
        // test instead of leaving the run without a shutdown.
        let runtime = pipeline.into_runtime(source).expect("into_runtime");
        let shutdown = runtime.shutdown_handle();
        let run = std::thread::spawn(move || runtime.run());
        wait_until(spate_test::HANG, "payloads consumed", || {
            chain_shared.consumed.load(Ordering::Relaxed) == 5
        });
        shutdown.trigger();
        wait_until(spate_test::HANG, "run to return", || run.is_finished());
        let report = run.join().unwrap().unwrap();
        assert_eq!(report.state, ExitState::Completed);
        assert_eq!(report.final_watermarks, vec![(PartitionId(0), 5)]);
    }
}
