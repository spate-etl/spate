//! The builder, desugared: assembling a pipeline from the primitives
//! `Pipeline` composes.
//!
//! [`Pipeline::from_config`] → `.sink` → `.chains` → `.run` is a thin
//! composition of public, semver-committed primitives — not a privileged
//! path. This example spends each of those four lines by hand, in the order
//! the builder spends them, so the layering contract is a program that
//! compiles and runs:
//!
//! ```sh
//! cargo run -p spate --example manual_assembly
//! ```
//!
//! Reach for this when you need to drop below one builder step — embedding
//! the runtime in a host process, exotic sink wiring, or simply reading what
//! you are running. Prefer the builder otherwise: every ordering rule
//! commented below is a rule the builder makes structurally impossible to
//! break, and manual assembly hands all of them back.
//!
//! The `ANCHOR` comments below mark the regions the manual-assembly guide
//! renders. They are stripped from what it shows; see `docs/STYLE.md` § 10.
//!
//! [`Pipeline::from_config`]: spate::pipeline::Pipeline::from_config

// The examples index renders these four fields; see scripts/examples-index.sh.
// INDEX-TIER:  extending
// INDEX-GOAL:  drop below the builder to the primitives it composes
// INDEX-TECH:  no infrastructure
// INDEX-NEEDS: nothing

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use spate::backpressure::InflightBudget;
use spate::metrics::{ComponentLabels, MetricsSettings, SinkShardMetrics, install};
use spate::pipeline::{PipelineRuntime, SinkRuntime, metrics_settings};
use spate::prelude::*;
use spate::sink::{SinkDrainFn, SinkPool, shard_queues};
use spate::source::LaneId;
use spate::telemetry;
use spate_test::{TestDeserializer, TestEncoder, capture_sink, memory_source, wait_until};
use std::sync::Arc;
use std::time::Duration;

/// The same YAML a builder assembly loads — nothing about manual assembly
/// changes the configuration layer. `metrics.listen` takes port 0 rather
/// than the `0.0.0.0:9090` default: the runtime binds the admin server
/// wherever the config points it, and two pipelines in flight at once on one
/// port is `EADDRINUSE` (see `.config/nextest.toml`).
const CONFIG: &str = r#"
pipeline: { name: manual-assembly-demo, threads: 1, io_threads: 1 }
checkpoint: { interval: 200ms }
metrics: { listen: "127.0.0.1:0" }
source: { memory: {} }
sink: { capture: {} }
"#;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ANCHOR: init
    let config = PipelineConfig::from_str(CONFIG)?;
    let pipeline_name = config.pipeline.name.clone();

    // ── 1. Process init — what `Pipeline::from_config` does first ───────
    // Telemetry, then the exporter, then threads. Both inits are idempotent
    // and first-init wins: `from_config` installs the JSON default here, so
    // a binary wanting pretty logs calls `telemetry::init` before it.
    telemetry::init(telemetry::LogFormat::Pretty, "info");

    // The exporter goes in BEFORE any metric handle is constructed —
    // handles bind to the recorder present at their construction, so one
    // built earlier records into a no-op recorder forever, silently. The
    // shard handles in step 3 are exactly such handles.
    //
    // `install` errors only when a recorder some other library made global
    // already owns the process. The builder demotes that to a warning and
    // carries on against the existing recorder; `?` here stops instead.
    let settings: MetricsSettings = metrics_settings(&config);
    let metrics = install(&settings)?;

    // ── 2. One I/O runtime, one budget ──────────────────────────────────
    // The sink workers spawn onto this runtime and `PipelineRuntime::run`
    // adopts it below; skipping `with_io_runtime` would leave the process
    // running two runtimes and 2 × `io_threads` workers, with nothing
    // failing to say so. Build it from a plain thread: dropping a runtime
    // inside async context panics.
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.pipeline.io_threads)
        .thread_name("spate-io")
        .enable_all()
        .build()?;

    // The one in-flight byte budget the whole process shares: chain
    // terminals charge it on enqueue, sink workers credit it on durable
    // write, and the backpressure controller pauses the source off it.
    let budget = Arc::new(InflightBudget::new());
    // ANCHOR_END: init

    // ANCHOR: sink
    // ── The layer above ─────────────────────────────────────────────────
    // `SinkOptions` is the builder's home for wiring knobs that are neither
    // connector config nor framework YAML. At that layer the whole of step
    // 3 below is one line:
    //
    //     Pipeline::from_config(config)?.sink_with(sink, sink_options)?
    //
    // Here `queue_capacity` is a number this file has to spend itself, on
    // `shard_queues`. Sizing it is not free: queued chunks are charged to
    // the budget above, so `shards × queue_capacity × chunk.target_bytes`
    // has to fit under its low watermark alongside pending writes.
    let sink_options = SinkOptions::default().with_queue_capacity(16);

    // ── 3. The sink — what `.sink(bundle)` does ─────────────────────────
    // Connectors are built exactly as they are under the builder: a bundle
    // from the connector's factory, here the in-memory capturing sink with
    // its readback handle (one shard, one replica).
    let (sink, script) = capture_sink(1, 1);
    let sink = sink.with_pool_config({
        let mut pool = SinkPoolConfig::default();
        pool.batch.linger = Duration::from_millis(50); // flush quickly for the demo
        pool
    });

    // `into_parts` is the seam every sink crosses: writer, `[shard][replica]`
    // topology, pool tuning, labels, probe. The builder validates the
    // topology here — non-empty, no ragged shards, replica labels shaped like
    // the endpoints — and returns `BuildError::Sink`. By hand the first two
    // are panics instead (an empty topology out of `shard_queues`, a shard
    // with no replicas out of `SinkPool::spawn`), and the third is caught
    // nowhere: a surplus replica label publishes `spate_sink_replica_healthy`
    // for a replica that does not exist, and a missing one drops a live
    // replica's health updates.
    let parts = sink.into_parts();
    let shards = parts.shard_endpoints.len();
    let component_type = parts.component_type.clone();
    let replica_labels = parts.effective_replica_labels();

    // One bounded chunk queue per shard: `queues` is the sending half the
    // chain terminals hold, `receivers` the half the workers own.
    let (queues, receivers) = shard_queues(shards, sink_options.queue_capacity);

    // Per-shard metric handles, resolved once at assembly and never on the
    // record path. `"sink"` is the `component` label the builder gives the
    // single default sink; a named sink gets its own name there instead.
    // `try_new` is the builder's constructor: it refuses a series another
    // live handle set already owns (INV-10), where the infallible `new`
    // leaves the loser a shadow whose counters record and whose gauges never
    // publish.
    let labels = ComponentLabels::new(pipeline_name.clone(), "sink", component_type);
    let shard_metrics: Vec<SinkShardMetrics> = replica_labels
        .iter()
        .enumerate()
        .map(|(shard, replicas)| {
            SinkShardMetrics::try_new(
                &labels,
                u32::try_from(shard).unwrap_or(u32::MAX),
                replicas,
                settings.e2e_basis,
            )
        })
        .collect::<Result<_, _>>()?;

    // Two metric seams the builder wires and this file does not. The
    // `spate_queue_*` handles attach through a crate-internal seam on
    // `ShardQueues`, so an assembly below the builder publishes no queue
    // series at all. The sink writer's own `spate_<component_type>_sink_*`
    // scope attaches through the public `ShardWriter::attach_metrics`; only
    // the derivation of its prefix is crate-internal — `Meter::for_component`
    // appends the sink role to the component type — so by hand the namespace
    // is one you name yourself with `Meter::with_namespace`. Everything else
    // in the taxonomy is identical.

    // One worker per shard, spawned onto the runtime from step 2.
    let pool = SinkPool::spawn(
        Arc::new(parts.writer),
        parts.shard_endpoints,
        receivers,
        parts.pool,
        Arc::clone(&budget),
        shard_metrics,
        &pipeline_name,
        io.handle(),
    );

    // The hook the runtime calls once, at shutdown, with what is left of
    // the drain budget.
    let drain: SinkDrainFn =
        Box::new(move |deadline| Box::pin(async move { pool.drain(deadline).await }));
    let sink_runtime = SinkRuntime {
        // One entry per installed sink; the runtime only introspects
        // capacity for the backpressure resume gate.
        queues: vec![queues.clone()],
        drain,
        // Drives the sinks half of `/readyz`; `None` reports connected.
        probe: parts.probe,
    };
    // ANCHOR_END: sink

    // ANCHOR: runtime
    // ── 4. The runtime — what `.chains` + `.into_runtime` do ────────────
    // The factory takes a bare thread index and threads the queues, budget
    // and pipeline name itself — most of what `ChainCtx` carries. The rest is
    // `source_framing`, read from `Source::framing_contract` before the source
    // moves into the runtime so a deserializer derives its granularity from
    // the source rather than restating it (this chain's deserializer takes
    // none), and, for a split pipeline, each named sink's queues and chunking.
    //
    // The contract `ChainCtx` discharges structurally is yours here: every
    // `ShardQueues` clone must die with the chains that hold it, because the
    // sink drains only once the last one is gone — one smuggled into
    // longer-lived state turns a graceful drain into a deadline-bounded
    // abandon. This closure is dropped by the runtime before the drain, so
    // its clone is safe.
    let (source, handle) = memory_source();
    let chain_name = pipeline_name.clone();
    let chain_budget = Arc::clone(&budget);
    let chains = move |_thread: usize| {
        chain_owned::<Vec<u8>, _>(TestDeserializer::split_on(b','))
            .with_metrics(chain_name.clone(), "main")
            .filter(|order_id: &Vec<u8>| !order_id.is_empty())
            .map(|order_id: Vec<u8>| order_id.to_ascii_uppercase())
            .sink(
                TestEncoder,
                KeyHashRouter,
                // The one builder step with no manual equivalent: resolving
                // this sink's chunking — the YAML `chunk:` block, or
                // `SinkOptions::with_chunk` — is the config layer's job and
                // its resolver is crate-internal. By hand it is a
                // `ChunkConfig` you pass, and a `chunk:` block on the config
                // above is read by nothing.
                ChunkConfig::default(),
                queues.clone(),
                Arc::clone(&chain_budget),
            )
            .build()
    };

    let runtime = PipelineRuntime::new(config, source, chains, sink_runtime, budget)
        .with_options(RuntimeOptions {
            handle_signals: false, // the demo triggers shutdown itself
            ..RuntimeOptions::default()
        })
        .with_io_runtime(io);

    // ── Drive it ────────────────────────────────────────────────────────
    // Identical to a builder assembly from here: the primitives below the
    // builder produce the same runtime, so they drive the same way.
    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());
    // ANCHOR_END: runtime

    let orders = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), orders)]);
    let mut last = 0;
    for payload in [&b"order-1,order-2"[..], b"order-3,,order-4", b"order-5"] {
        last = handle.push(orders, Some(b"eu-west"), payload);
    }

    // Bounded on purpose: an unbounded wait turns a broken pipeline into a
    // hung process rather than a failing one.
    wait_until(Duration::from_secs(10), "the last offset to commit", || {
        handle.last_committed(orders) == Some(last + 1)
    });
    shutdown.trigger();
    let report = join.join().expect("pipeline thread")?;

    // What the capturing sink wrote, decoded back to rows.
    let rows: Vec<String> = script
        .writes()
        .iter()
        .flat_map(|w| spate_test::decode_rows(&w.payload))
        .map(|row| String::from_utf8_lossy(&row).into_owned())
        .collect();
    assert_eq!(rows.len(), 5, "six fields minus one filtered empty");
    assert!(rows.contains(&"ORDER-1".to_string()));

    // The hand-installed exporter is the live one: the shard handles built
    // in step 3 render, which is the ordering rule of step 1 holding.
    let exposition = metrics.render();
    assert!(
        exposition.contains("spate_sink_"),
        "the sink shard handles must render through the exporter installed by hand"
    );

    println!("\npipeline exit: {:?}", report.state);
    println!("final watermarks: {:?}", report.final_watermarks);
    println!("rows written ({}): {rows:?}", rows.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    /// The example is the test. `cargo run --example` still runs `main`;
    /// under `--test` the harness makes `main` an ordinary function and this
    /// its only caller, so the assertions above stop being decorative.
    #[test]
    fn runs_to_completion() {
        super::main().expect("the example must run clean");
    }
}
