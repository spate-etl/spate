# Manual assembly

The `Pipeline` builder is a **thin composition of public primitives** — the
hyper/axum layering contract: the convenience layer composes lower-level
APIs that are themselves public, semver-committed, and documented. Nothing
in the builder is magic or privileged; this page desugars every step into
the primitives it calls, so you can drop below the builder at any point —
for exotic wiring, for embedding, or just to understand what you are
running. The rustdoc on `etl::pipeline::Pipeline` carries the same mapping
and is kept in lockstep with the code; rationale in
[docs/DESIGN.md](../../DESIGN.md) § Assembly.

> [!NOTE]
> Prefer the builder. Manual assembly re-exposes exactly the footguns the
> builder was designed to make unconstructible — init ordering, doubled I/O
> runtimes, leaked queue clones. Use it when you need it, not by default.

## The desugaring map

| Builder step | Public primitives |
|---|---|
| `Pipeline::from_config(config)` | `telemetry::init` → `metrics::install(&metrics_settings(&config))` → `tokio::runtime::Builder` (`io_threads` workers) → `InflightBudget::new` |
| `.sink(bundle)` | `SinkBundle::into_parts` → `shard_queues` → `ComponentLabels` + `SinkShardMetrics::new` per shard → `SinkPool::spawn` → a boxed drain closure |
| `.chains(f)` | the chain factory handed to `PipelineRuntime::new`, with queue/budget/name plumbing pre-threaded per call |
| `.into_runtime(source)` / `.run(source)` | `PipelineRuntime::new(config, source, factory, SinkRuntime, budget)` + `with_options` + `with_io_runtime` + `run` |

Module paths (all under the `etl` facade, which re-exports `etl_core` at
the root): `etl::telemetry`, `etl::metrics`, `etl::pipeline`
(`PipelineRuntime`, `SinkRuntime`, `metrics_settings`, `RuntimeOptions`),
`etl::sink` (`shard_queues`, `SinkPool`, `SinkBundle`, `SinkParts`), and
`etl::backpressure` (`InflightBudget`).

## Step by step

The sketch below mirrors what the builder's own implementation does
(`crates/etl-core/src/pipeline/builder.rs`); construction of your
connectors and chain is elided.

### 1. Process init (what `from_config` does)

```rust
use etl::config::PipelineConfig;
use etl::pipeline::metrics_settings;
use etl::telemetry::{self, LogFormat};
use etl::backpressure::InflightBudget;
use std::sync::Arc;

let config = PipelineConfig::from_path(std::path::Path::new("pipeline.yaml"))?;

// Order matters and is the whole point of the builder:
telemetry::init(LogFormat::Json, "info");            // idempotent, first init wins

// Exporter BEFORE any metric handle exists — handles bind to the recorder
// present at construction; one built earlier records into the void.
let metrics = etl::metrics::install(&metrics_settings(&config))?;

// ONE I/O runtime for the whole process. run() adopts it later via
// with_io_runtime; building a second one doubles io_threads silently.
let io = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(config.pipeline.io_threads)
    .thread_name("etl-io")
    .enable_all()
    .build()?;

let budget = Arc::new(InflightBudget::new());
```

Do this from a plain thread — the runtime you just built must not be
dropped inside async context.

### 2. Connectors

Exactly as with the builder: your factories read the opaque sections.
Connectors needing async pre-steps use `io.handle()` / `io.block_on(...)`
directly — that is all `Pipeline::io_handle`/`block_on` forward to.

### 3. The sink (what `.sink(bundle)` does)

```rust
use etl::metrics::{ComponentLabels, SinkShardMetrics};
use etl::sink::{SinkBundle, SinkPool, shard_queues};

let parts = bundle.into_parts();                 // SinkParts { writer, shard_endpoints, pool, ... }
let num_shards = parts.shard_endpoints.len();

// One bounded chunk queue per shard: senders for the chain terminals,
// receivers for the workers. Capacity is SinkOptions::queue_capacity (8).
let (queues, receivers) = shard_queues(num_shards, 8);

// Pre-registered metric handles, one set per shard — never resolved on the
// per-record path. Must run after metrics::install (step 1).
let name = config.pipeline.name.clone();
let labels = ComponentLabels::new(name.clone(), "sink", parts.component_type.clone());
let e2e_basis = metrics_settings(&config).e2e_basis;
let shard_metrics: Vec<SinkShardMetrics> = parts
    .effective_replica_labels()
    .iter()
    .enumerate()
    .map(|(shard, replicas)| SinkShardMetrics::new(&labels, shard as u32, replicas, e2e_basis))
    .collect();

// Spawn one worker per shard onto the I/O runtime.
let pool = SinkPool::spawn(
    Arc::new(parts.writer),
    parts.shard_endpoints,
    receivers,
    parts.pool,
    Arc::clone(&budget),
    shard_metrics,
    &name,
    io.handle(),
);

// The drain hook the runtime invokes once at shutdown.
let drain = Box::new(move |deadline| Box::pin(async move { pool.drain(deadline).await }));
```

The builder also validates the topology here (no empty shards, label shape
matching endpoint shape) — manual assemblies get panics from
`SinkPool::spawn` instead of `BuildError::Sink`.

### 4. The runtime (what `.chains` + `.into_runtime` do)

```rust
use etl::pipeline::{PipelineRuntime, RuntimeOptions, SinkRuntime};

let sink_runtime = SinkRuntime {
    queues: queues.clone(),
    drain,
    probe: parts.probe,           // drives /readyz; None reports connected
};

// The chain factory takes a bare thread index — you thread the queues,
// budget, and pipeline name yourself (the builder's ChainCtx does exactly
// this). CONTRACT: every queue clone must die with the chains; a clone
// held elsewhere degrades shutdown to a deadline-bounded abandon.
let chains = move |thread: usize| {
    build_my_chain(thread, queues.clone(), Arc::clone(&budget), name.clone())
};

let report = PipelineRuntime::new(config, source, chains, sink_runtime, budget)
    .with_options(RuntimeOptions::default())
    .with_io_runtime(io)          // adopt the ONE runtime from step 1
    .run()?;
report.log();
std::process::exit(report.exit_code());
```

`with_io_runtime` matters: without it, `run()` builds its own runtime and
your process silently runs `2 x io_threads` workers. `run()` owns the
runtime's shutdown either way; tasks you spawned on it earlier keep running
until then.

## What you take responsibility for

The builder turned each of these from a documented rule into a structural
impossibility; manually, they are yours again:

- **Init order** — exporter before any handle, telemetry before anything
  logs, all of it before threads spawn.
- **One I/O runtime** — build once, pass to `SinkPool::spawn` by handle and
  to the runtime by value.
- **Queue drop ordering** — the sink drains only when every `ShardQueues`
  clone is gone; audit every clone you make (see
  [Graceful shutdown](graceful-shutdown.md)).
- **Topology validation** — shard/label/receiver counts must agree.

## The layering contract

Both layers are public and semver-committed. The builder will never grow a
capability the primitives cannot express — it is a composition, not a
gatekeeper — and the primitives will not break under you to make the
builder prettier. If you outgrow one builder step, desugar *that step* and
keep the rest; the two paths compose because they are the same path.

## Related

- [Assembling a pipeline](assembling-a-pipeline.md) — the sugar this page
  removes.
- [Custom sources](../06-extending/custom-source.md) and
  [custom sinks](../06-extending/custom-sink.md) — the connector traits
  these primitives drive.
- [Architecture](../02-concepts/01-architecture.md) — the threads and
  runtimes being wired here.
