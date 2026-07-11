# Assembling a pipeline

`etl::pipeline::Pipeline` is the primary way to turn a config file, a
source, a sink, and your operator chain into a running process. This guide
walks the builder end to end; every step is a thin composition of public
primitives you can drop down to at any point (see
[Manual assembly](manual-assembly.md) for the full desugaring).

The complete, compiling reference for everything below is
`crates/etl/examples/kafka_avro_to_clickhouse.rs` with its YAML next to it;
the zero-infrastructure variant is `crates/etl/examples/memory_pipeline.rs`.

## The shape of an assembly

```rust
use etl::prelude::*;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipeline = Pipeline::from_path(Path::new("pipeline.yaml"))?;

    // Connector construction: yours, one explicit line each.
    let source = KafkaSource::from_component_config(&pipeline.config().source)?;
    let sink = etl::clickhouse::config::from_component_config(&pipeline.config().sink)?;

    let report = pipeline
        .sink(sink)?
        .chains(move |ctx| {
            chain_owned::<Order, _>(deserializer.clone())
                .with_metrics(ctx.pipeline, "main")
                .sink(encoder.clone(), KeyHashRouter, ChunkConfig::default(),
                      ctx.queues, ctx.budget)
                .build()
        })
        .run(source)?;

    report.log();
    std::process::exit(report.exit_code());
}
```

## Step 1: `from_path` / `from_config` — the constructor owns process init

`Pipeline::from_path` loads the YAML (see
[Configuring pipelines](configuring-pipelines.md)) and calls
`Pipeline::from_config`, which initializes the process in a fixed order:

1. **Telemetry** — `telemetry::init(Json, "info")`. Idempotent, first init
   wins: to customize the log format or filter, call `etl::telemetry::init`
   yourself *before* building the pipeline (the demos use `Pretty`).
2. **The metrics exporter** — installed from the config's `metrics` section.
3. **The I/O runtime** — a tokio runtime with `pipeline.io_threads` workers
   (thread name `etl-io`) that will host sink workers, the checkpointer,
   and the admin server.

Why does a builder do process init? Because **metric handles bind to the
recorder present at the moment they are constructed**. A handle built before
the exporter exists silently records into the void — no error, just missing
series. By making the constructor install the exporter, holding a `Pipeline`
*guarantees* a live recorder: every handle built afterwards, framework or
custom, is live. The ordering bug is not documented away; it is made
unconstructible. (Rationale: [docs/DESIGN.md](../../DESIGN.md) § Assembly and
§ Metrics.)

> [!WARNING]
> `Pipeline::from_config` must be called **outside any async runtime** —
> from a plain thread, usually `main`. It owns a blocking tokio runtime, and
> dropping or `block_on`-ing one inside async context panics, so the
> constructor refuses with `BuildError::AsyncContext` instead.

## Step 2: connector pre-steps — `config()`, `io_handle()`, `block_on()`

The framework never interprets the `source`, `deserializer`, and `sink`
sections of your YAML; `pipeline.config()` hands them to *your* connector
factories as opaque `ComponentConfig`s:

```rust
let source = KafkaSource::from_component_config(&pipeline.config().source)?;
```

Some connectors need async edge work before the pipeline runs. The builder's
I/O runtime is already alive, so:

- `pipeline.io_handle()` — a `tokio::runtime::Handle` for background tasks
  that must start before `run` (the Avro schema-registry fetcher takes one;
  see [Avro](../04-connectors/avro/README.md)).
- `pipeline.block_on(future)` — run an async pre-flight step to completion
  on the I/O runtime, blocking the current thread. The canonical use is
  ClickHouse schema validation
  (see [Schema validation](schema-validation.md)):

```rust
let encoder = match pipeline.block_on(sink.validate_schema())? {
    Some(schema) => ClickHouseEncoder::<Owned<Order>>::with_schema(schema),
    None => ClickHouseEncoder::<Owned<Order>>::new(),
};
```

## Step 3: `.sink(bundle)` / `.sink_with(bundle, options)`

`sink` accepts anything implementing `etl::sink::SinkBundle` — every
connector's sink type does, and a hand-rolled `SinkParts` does too, so
custom sinks need no extra impl. From the bundle the builder derives the
per-shard chunk queues, registers per-shard metrics, spawns the sink workers
(`SinkPool`) on the I/O runtime, and wires the drain hook and the readiness
probe that drives `/readyz`.

`sink_with` adds `SinkOptions`, currently one knob: `queue_capacity`, the
per-shard chunk queue depth in chunks (default 8). It participates in the
backpressure sizing rule — see
[Backpressure](../02-concepts/03-backpressure.md) and
[docs/DESIGN.md](../../DESIGN.md) § Backpressure before raising it.

Calling `.sink` twice is an error (`BuildError::SinkAlreadySet`): the slot
is single-occupancy so multi-output routing can arrive later as an additive
API.

## Step 4: `.chains(|ctx| ...)` — one chain per pipeline thread

The closure you pass to `.chains` is called once per pipeline thread and
must return that thread's operator chain as a `Box<dyn RunnableChain>`
(what `.build()` on a chain builder produces). Composition inside the
closure is fully monomorphized; the box is the same single per-batch erasure
boundary the engine always has.

Each call receives a `ChainCtx` **by value** with everything the terminal
`.sink(...)` stage needs:

| Field | What it is |
|---|---|
| `ctx.thread` | Zero-based pipeline thread index. |
| `ctx.queues` | This thread's clone of the shard-queue senders. |
| `ctx.budget` | The shared in-flight byte budget. |
| `ctx.pipeline` | The pipeline name — first argument to `.with_metrics`. |

Move the fields into the chain you build:

```rust
.chains(move |ctx| {
    chain_owned::<Order, _>(deserializer.clone())
        .with_metrics(ctx.pipeline, "main")
        .try_map(|order: Order| {
            if order.amount_cents >= 0 { Ok(order) } else { Err("negative amount") }
        }, ErrorPolicy::Skip)
        .sink(encoder.clone(), KeyHashRouter, ChunkConfig::default(),
              ctx.queues, ctx.budget)
        .build()
})
```

`KeyHashRouter` is the default meta-only router; to route on a record's own
payload instead — matching a sink cluster's sharding key, or splitting
`flat_map` children across shards — pass a record-aware `RecordRouter` here
instead (see [Sink sharding](../02-concepts/05-sink-sharding.md)).

> [!IMPORTANT]
> **The drain contract: do not stash `ctx.queues` outside the chain.** The
> sink only begins draining once every `ShardQueues` clone is dropped —
> that is how workers know no more data can arrive. The builder discharges
> this structurally: queues are lent per factory call, `ChainCtx` is
> deliberately not `Clone`, and the chain's terminal stage drops its clone
> with the driver threads before the drain runs. A clone smuggled into
> long-lived state outside the returned chain outlives the drivers and turns
> a graceful drain into a deadline-bounded loud abandon (bounded by
> `checkpoint.drain_timeout` — see [Graceful shutdown](graceful-shutdown.md)).

## Step 5: `.runtime_options(...)` (optional)

`RuntimeOptions` holds knobs that are deliberately not YAML: signal
handling, poll loop timings, the version string on `etl_pipeline_info`. The
defaults suit production. The one you will actually touch is
`handle_signals: false` in tests and demos, where you drive shutdown
yourself:

```rust
.runtime_options(RuntimeOptions {
    handle_signals: false,
    ..RuntimeOptions::default()
})
```

## Step 6: `.run(source)` or `.into_runtime(source)`

The source type enters only at this terminal call — the builder itself is
non-generic, nameable, and storable.

- **`.run(source)`** — assemble and run to completion, blocking until a
  shutdown signal drains the pipeline or a fatal error stops it. This is the
  production path.
- **`.into_runtime(source)`** — assemble into a `PipelineRuntime` without
  running it, for callers that need a `shutdown_handle()` before a spawned
  `run` — tests and embedded pipelines:

```rust
let runtime = pipeline.into_runtime(source)?;
let shutdown = runtime.shutdown_handle();
let join = std::thread::spawn(move || runtime.run());
// ... drive data, assert, then:
shutdown.trigger();
let report = join.join().expect("pipeline thread")?;
```

Skipping a step is caught here: `BuildError::MissingSink` /
`BuildError::MissingChains`.

## Step 7: the `ExitReport`

`run` returns an `ExitReport` carrying the terminal state (`Completed` or
`Failed`), the sink's drain report, and the final committed watermark per
partition. Three methods cover the common endings of `main`:

- `report.log()` — log the outcome at the matching level (`info` clean,
  `error` failed).
- `report.exit_code()` — `0` for a clean drain, `1` for a failure; pair
  with `std::process::exit` so Kubernetes restarts failed pipelines.
- `report.ok()` — convert to a `Result` so `main` can `?` a failed run.

See [Graceful shutdown](graceful-shutdown.md) for what "clean" means in
detail.

## Related

- [Configuring pipelines](configuring-pipelines.md) — the YAML the builder
  loads.
- [Testing pipelines](testing-pipelines.md) — the `into_runtime` pattern
  over `etl-test` mocks.
- [Manual assembly](manual-assembly.md) — every builder step desugared to
  public primitives.
- [Architecture](../02-concepts/01-architecture.md) — what the assembled
  process looks like at runtime.
