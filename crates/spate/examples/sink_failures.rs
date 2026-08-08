//! What a failing sink does to your watermark.
//!
//! "At-least-once" is a claim about the moment the sink stops answering, so
//! this example makes it fail four ways and asserts what the framework does
//! about each — no infrastructure, using `spate-test`'s scripted sink:
//!
//! ```sh
//! cargo run -p spate --example sink_failures
//! ```
//!
//! 1. **Retryable** — the same sealed batch, same deduplication token,
//!    rotated onto the next replica. Costs latency, not progress.
//! 2. **A failed readiness probe** — what `/readyz` reports, and why it is
//!    not the same question as "is data still landing".
//! 3. **Fatal** — the batch is abandoned, and the partition watermark
//!    *stops* rather than committing past records the sink never
//!    acknowledged (INV-1). `checkpoint.stalled_fail_after` turns that
//!    permanent stall into a `Failed` exit, so the process restarts and
//!    replays instead of consuming forever while committing nothing. This
//!    is the one that decides whether the delivery claim is real.
//! 4. **Slow** — the in-flight byte budget fills and the source lanes are
//!    *paused*. It is a pause and not a blocking send because INV-2 says a
//!    source thread never blocks on a channel send: a blocked thread stops
//!    servicing the control plane, so a rebalance or a shutdown that arrives
//!    during a sink outage would never be seen. Pausing keeps the poll loop
//!    turning with nothing to hand downstream, which is why the outage shows
//!    up as `handle.paused_lanes()` rather than as a wedged thread.
//!
//! Each scenario runs its own pipeline under its own `pipeline.name` — a
//! fatal outcome ends a run, so they cannot share one, and a gauge series
//! has exactly one live owner per process (INV-10), so they cannot share a
//! name either.

// The examples index renders these four fields; see crates/spate/tests/examples_index.rs.
// INDEX-TIER:  operating
// INDEX-GOAL:  see what a failing sink does to the watermark
// INDEX-TECH:  no infrastructure
// INDEX-NEEDS: nothing

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use spate::prelude::*;
use spate::source::LaneId;
use spate_test::{
    BytesPassthrough, CaptureSink, MemorySource, PipelineRun, ScriptedResult, TestEncoder,
    WriteOutcome, capture_sink, decode_rows, memory_source, wait_until,
};
use std::time::Duration;

/// Three storefront payloads, one row each — enough to fill a batch and not
/// enough to hide what the sink was asked to do.
const ORDERS: [&str; 3] = [
    "order_placed:1001",
    "payment_captured:1001",
    "refund_issued:1001",
];

/// Seal batches promptly: the default one-second linger would make every
/// wait below a second longer than the behaviour it is demonstrating.
fn prompt_pool() -> SinkPoolConfig {
    let mut cfg = SinkPoolConfig::default();
    cfg.batch.linger = Duration::from_millis(20);
    cfg
}

/// The same assembly for every scenario — a byte passthrough straight into
/// the capturing sink — so the only thing that differs between them is how
/// the sink is scripted to fail.
fn assemble(
    config: &str,
    sink: CaptureSink,
    source: MemorySource,
) -> Result<PipelineRuntime<MemorySource>, Box<dyn std::error::Error>> {
    Ok(Pipeline::from_config(PipelineConfig::from_str(config)?)?
        .sink(sink)?
        .chains(|ctx| {
            let chunk_cfg = ctx.chunk();
            chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .with_metrics(ctx.pipeline, "main")
                .sink(
                    TestEncoder,
                    KeyHashRouter,
                    chunk_cfg,
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        .runtime_options(RuntimeOptions {
            handle_signals: false, // each scenario ends itself
            ..RuntimeOptions::default()
        })
        .into_runtime(source)?)
}

// ── 1. A retryable write ───────────────────────────────────────────────
//
// The class of error every replicated sink sees hourly: a restarting node, a
// dropped connection, a 503. The pool re-sends the *sealed* batch — same
// rows, same deduplication token, so the destination can recognize the
// replay — on the next replica in rotation.
fn retryable_write() -> Result<(), Box<dyn std::error::Error>> {
    const CONFIG: &str = r#"
pipeline: { name: sink-failures-retryable, threads: 1, io_threads: 2 }
checkpoint: { interval: 100ms, drain_timeout: 5s }
metrics: { listen: "127.0.0.1:0" }
source: { memory: {} }
sink: { capture: {} }
"#;
    let (source, handle) = memory_source();
    // One shard, two replicas: a retry has somewhere else to go.
    let (sink, script) = capture_sink(1, 2);
    let sink = sink.with_pool_config(prompt_pool());
    // The next write to any replica fails retryably; everything after it
    // succeeds, because an unscripted write succeeds.
    script.enqueue_global(WriteOutcome::retryable("payments replica restarting"));

    let runtime = assemble(CONFIG, sink, source)?;
    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());

    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);
    let last = *handle.push_many(p, ORDERS).last().expect("three offsets");

    // The watermark still reaches one-past-last. A retryable failure buys
    // the sink time; it does not cost the pipeline progress.
    assert!(
        handle.wait_committed(p, last + 1, Duration::from_secs(30)),
        "the watermark advances after the retry (last committed: {:?})",
        handle.last_committed(p),
    );
    shutdown.trigger();
    let report = run.join()?;
    assert_eq!(report.exit_code(), 0, "a retried write still drains clean");

    // Every attempt is captured, failures included, so the retry is visible
    // as two attempts at one batch rather than as two batches.
    let writes = script.writes();
    let failed = writes
        .iter()
        .position(|w| matches!(w.result, ScriptedResult::Retryable(_)))
        .expect("the scripted failure was attempted");
    let retry = writes[failed + 1..]
        .iter()
        .find(|w| w.dedup_token == writes[failed].dedup_token)
        .expect("the same batch was re-sent");
    assert_eq!(retry.result, ScriptedResult::Ok, "the retry landed");
    assert_ne!(
        retry.replica, writes[failed].replica,
        "the retry rotated onto the other replica"
    );
    assert_eq!(retry.rows, writes[failed].rows, "the same sealed batch");

    println!(
        "\n1. retryable: batch {} refused by replica {}, accepted by replica {} ({} rows)",
        retry.dedup_token, writes[failed].replica, retry.replica, retry.rows,
    );
    Ok(())
}

// ── 2. A failed readiness probe ────────────────────────────────────────
//
// Readiness is a *connectivity* question, answered by a probe the runtime
// calls on a timer and publishes as `/readyz`. It deliberately says nothing
// about whether records are landing: a sink with one sick replica out of two
// is not ready to take a new pod's traffic and is perfectly capable of
// writing through the one that works.
fn failed_probe() -> Result<(), Box<dyn std::error::Error>> {
    const CONFIG: &str = r#"
pipeline: { name: sink-failures-probe, threads: 1, io_threads: 2 }
checkpoint: { interval: 100ms, drain_timeout: 5s }
metrics: { listen: "127.0.0.1:0" }
source: { memory: {} }
sink: { capture: {} }
"#;
    let (source, handle) = memory_source();
    let (sink, script) = capture_sink(1, 2);
    let sink = sink.with_pool_config(prompt_pool());
    // A bundle's parts carry the probe the readiness loop polls. Cloning the
    // bundle to take a copy of it is how this example asks the same question
    // `/readyz` asks, in-process, without an HTTP round trip.
    let probe = sink
        .clone()
        .into_parts()
        .probe
        .expect("the capturing sink ships a readiness probe");
    // The probe is async because a real one talks to a server; a
    // single-threaded runtime is enough to ask it three questions.
    let asker = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    asker
        .block_on(probe())
        .expect("every replica answers before the outage");

    let runtime = assemble(CONFIG, sink, source)?;
    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());

    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);
    let before = *handle.push_many(p, ORDERS).last().expect("three offsets");
    assert!(handle.wait_committed(p, before + 1, Duration::from_secs(30)));

    // One replica stops answering. The probe covers every replica of every
    // shard, so one bad endpoint flips the whole sink to not-ready — 503 on
    // `/readyz`, and an orchestrator keeps new traffic off this instance.
    script.fail_probe(0, 1, "orders replica 1 unreachable");
    assert!(
        asker.block_on(probe()).is_err(),
        "one unreachable replica fails readiness for the sink"
    );

    // Meanwhile the data plane is untouched: the write path still has a
    // healthy replica, so records keep committing through the outage. Not
    // ready is not the same as not working.
    let during = *handle.push_many(p, ORDERS).last().expect("three offsets");
    assert!(
        handle.wait_committed(p, during + 1, Duration::from_secs(30)),
        "records still commit while readiness is red (last committed: {:?})",
        handle.last_committed(p),
    );

    script.heal_probe(0, 1);
    asker.block_on(probe()).expect("readiness recovers");
    shutdown.trigger();
    let report = run.join()?;
    assert_eq!(report.exit_code(), 0);

    println!("\n2. probe: readiness went red and back while every record committed");
    Ok(())
}

// ── 3. A fatal write — the one that matters ────────────────────────────
//
// A dropped table, a revoked credential, a schema the sink will never
// accept: retrying is pointless, so the batch is abandoned. Its records were
// never acknowledged, so the partition watermark cannot pass them — and a
// stalled watermark is permanent, because acknowledgments only ever fail,
// never un-fail. `checkpoint.stalled_fail_after` is what converts that into
// a `Failed` exit instead of a process that reads the source forever and
// commits nothing.
fn fatal_write() -> Result<(), Box<dyn std::error::Error>> {
    const CONFIG: &str = r#"
pipeline: { name: sink-failures-fatal, threads: 1, io_threads: 2 }
checkpoint: { interval: 100ms, drain_timeout: 5s, stalled_fail_after: 1s }
metrics: { listen: "127.0.0.1:0" }
source: { memory: {} }
sink: { capture: {} }
"#;
    let (source, handle) = memory_source();
    let (sink, script) = capture_sink(1, 1);
    let sink = sink.with_pool_config(prompt_pool());

    let runtime = assemble(CONFIG, sink, source)?;
    let run = PipelineRun::spawn(move || runtime.run());

    // A healthy first wave, so the watermark has somewhere to have got to.
    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);
    let healthy = *handle.push_many(p, ORDERS).last().expect("three offsets");
    assert!(handle.wait_committed(p, healthy + 1, Duration::from_secs(30)));
    let committed_before = handle.last_committed(p).expect("first wave committed");

    // The destination breaks for good, and a second wave arrives into it.
    script.enqueue_global(WriteOutcome::fatal("table orders_local does not exist"));
    let doomed = *handle.push_many(p, ORDERS).last().expect("three offsets");

    // Nothing triggers shutdown here: the stall watchdog does. `run` returns
    // on its own, which is the behaviour an operator sees as a crash-looping
    // pod rather than a silently idle one.
    let report = run
        .wait_exit(Duration::from_secs(30))
        .expect("the stall watchdog fails the pipeline")?;

    // THE assertion. The watermark is exactly where it was before the fatal
    // write — never advanced over the records the sink never acknowledged,
    // which is the whole of at-least-once (INV-1). Those records replay
    // after a restart because the source was never told they were done.
    assert_eq!(
        handle.last_committed(p),
        Some(committed_before),
        "the watermark stalls rather than committing past unacknowledged data",
    );
    assert!(
        committed_before <= doomed,
        "the abandoned wave is still uncommitted and will replay",
    );

    let ExitState::Failed(failure) = &report.state else {
        panic!("a permanent stall must fail the pipeline, not idle forever");
    };
    assert_eq!(failure.component, "checkpoint");
    assert!(failure.reason.contains("stalled"), "{}", failure.reason);
    assert_ne!(report.exit_code(), 0, "the process must exit non-zero");

    // Recovered, not propagated: `main` still returns `Ok`, because a failed
    // pipeline is this scenario's expected outcome.
    println!(
        "\n3. fatal: watermark held at {committed_before} (records up to {doomed} unacknowledged), \
         exit code {} — {}",
        report.exit_code(),
        failure.reason,
    );
    Ok(())
}

// ── 4. A slow sink ─────────────────────────────────────────────────────
//
// The sink answers, eventually. Bytes admitted but not yet acknowledged pile
// up against `backpressure.max_inflight_bytes`, and past the high watermark
// the source lanes are paused — see the module docs for why that is a pause
// and not a blocking send.
fn slow_sink() -> Result<(), Box<dyn std::error::Error>> {
    const CONFIG: &str = r#"
pipeline: { name: sink-failures-slow, threads: 1, io_threads: 2 }
checkpoint: { interval: 100ms, drain_timeout: 10s }
backpressure: { max_inflight_bytes: 1KiB, high_ratio: 0.5, low_ratio: 0.25, min_pause: 50ms }
metrics: { listen: "127.0.0.1:0" }
source: { memory: {} }
sink: { capture: { chunk: { target_bytes: 256B } } }
"#;
    let (source, handle) = memory_source();
    let (sink, script) = capture_sink(1, 1);
    let sink = sink.with_pool_config(prompt_pool());
    // Every write takes its time. Unscripted writes are instant, so the
    // scenario recovers on its own once these are consumed.
    for _ in 0..6 {
        script.enqueue_global(WriteOutcome::ok().after(Duration::from_millis(300)));
    }

    let runtime = assemble(CONFIG, sink, source)?;
    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());

    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);
    let backlog: Vec<String> = (0..60).map(|i| format!("order_placed:{i:08}")).collect();
    let last = *handle.push_many(p, &backlog).last().expect("offsets");

    // The lane the source is polling is paused, by the source's own `pause`
    // call — the mock records it, a Kafka source would pause the partition.
    wait_until(Duration::from_secs(30), "lanes paused under load", || {
        !handle.paused_lanes().is_empty()
    });

    // And it recovers: the budget drains as writes acknowledge, the lanes
    // resume, and every record commits.
    assert!(
        handle.wait_committed(p, last + 1, Duration::from_secs(60)),
        "the backlog drains once the sink catches up (last committed: {:?})",
        handle.last_committed(p),
    );
    shutdown.trigger();
    let report = run.join()?;
    assert_eq!(report.exit_code(), 0);

    let rows: usize = script
        .writes()
        .iter()
        .map(|w| decode_rows(&w.payload).len())
        .sum();
    assert!(rows >= backlog.len(), "every record reached the sink");
    println!("\n4. slow sink: lanes paused, then {rows} rows written and committed");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Demos want pretty logs; the sink failures below are logged at warn.
    spate::telemetry::init(spate::telemetry::LogFormat::Pretty, "warn");
    retryable_write()?;
    failed_probe()?;
    fatal_write()?;
    slow_sink()?;
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
