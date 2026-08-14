//! Reproduction: under sustained load, a split branch whose shard buffer
//! stays below `ChunkConfig::target_bytes` never seals, so the `AckRef`
//! clones it holds never resolve and the partition watermark cannot advance
//! past the oldest record parked in it. Checkpoint pending batches then grow
//! at the full batch-issuance rate while the hot branch delivers normally.
//!
//! The only flush path for a partial buffer is `flush_terminal()` via the
//! driver's `idle_flush`, which requires an *empty* poll plus an idle period
//! and is unreachable while data flows. The first test models continuous
//! ingest (a source under sustained load never polls empty) and asserts the
//! healthy contract: the watermark passes a low-volume-branch record while
//! load continues. The second test is the control: identical topology, but
//! the load stops, `idle_flush` fires, and everything commits, showing the
//! stall is load-gated rather than data loss or a slow sink.

// The sample table on stderr is the test's diagnostic payload on failure.
#![allow(clippy::print_stderr)]

use spate_core::config::PipelineConfig;
use spate_core::deser::Owned;
use spate_core::error::ErrorPolicy;
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, PipelineRuntime, RuntimeOptions};
use spate_core::record::PartitionId;
use spate_core::sink::KeyHashRouter;
use spate_core::source::LaneId;
use spate_test::{
    BytesPassthrough, CaptureSink, MemorySource, TestEncoder, capture_sink, memory_source,
};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Fixed admin port for the sustained-load test. Only this test binds an
/// admin listener (the control test asks for none), so `cargo test`'s
/// in-process test concurrency cannot collide on it.
const ADMIN: &str = "127.0.0.1:39184";

const SUSTAINED_CONFIG: &str = r#"
pipeline: { name: age-seal-repro, threads: 1, io_threads: 1 }
admin: { listen: "127.0.0.1:39184" }
metrics: { exporter: prometheus }
checkpoint: { interval: 200ms, max_pending_batches: 1000000, drain_timeout: 10s }
source: { memory: {} }
sinks:
  hot: { capture: {} }
  rare: { capture: {} }
"#;

const CONTROL_CONFIG: &str = r#"
pipeline: { name: age-seal-control, threads: 1, io_threads: 1 }
admin: { listen: none }
metrics: { exporter: none }
checkpoint: { interval: 200ms, max_pending_batches: 1000000, drain_timeout: 10s }
source: { memory: {} }
sinks:
  hot: { capture: {} }
  rare: { capture: {} }
"#;

/// Blocking HTTP GET; `(0, "")` while the admin server is still binding.
fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    let Ok(mut stream) = std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(2)) else {
        return (0, String::new());
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    if write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: repro\r\nConnection: close\r\n\r\n"
    )
    .is_err()
    {
        return (0, String::new());
    }
    let mut response = String::new();
    if stream.read_to_string(&mut response).is_err() {
        return (0, String::new());
    }
    let status: u16 = response
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

/// Sum of every `spate_checkpoint_pending_batches` sample in the exposition.
fn pending_gauge(addr: SocketAddr) -> f64 {
    let (status, body) = http_get(addr, "/metrics");
    if status != 200 {
        return -1.0; // admin not up yet; distinguishable from a real 0
    }
    body.lines()
        .filter(|l| !l.starts_with('#') && l.starts_with("spate_checkpoint_pending_batches"))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

struct Sample {
    at: Duration,
    pushed: i64,
    committed: Option<i64>,
    pending: f64,
    hot_writes: usize,
    rare_writes: usize,
}

/// Assemble the two-branch split pipeline: first payload byte `h` → sink
/// "hot", `r` → sink "rare". Mirrors a per-record-type split where one type
/// is orders of magnitude colder than the rest.
fn build_pipeline(
    config: &str,
    source: MemorySource,
    sink_hot: CaptureSink,
    sink_rare: CaptureSink,
    options: RuntimeOptions,
) -> PipelineRuntime<MemorySource> {
    Pipeline::from_config(PipelineConfig::from_str(config).expect("config"))
        .expect("builder")
        .add_sink("hot", sink_hot)
        .expect("sink hot")
        .add_sink("rare", sink_rare)
        .expect("sink rare")
        .chains(|ctx| {
            let mut split = chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .with_metrics(ctx.pipeline.clone(), "main")
                // The default 64 KiB chunk target, unchanged on purpose.
                .split(ErrorPolicy::Skip);
            let hot =
                split.add::<Owned<Vec<u8>>, _, _>(TestEncoder, KeyHashRouter, ctx.sink("hot"));
            let rare =
                split.add::<Owned<Vec<u8>>, _, _>(TestEncoder, KeyHashRouter, ctx.sink("rare"));
            split
                .route(move |row: Vec<u8>, out| match row.first() {
                    Some(b'h') => out.emit(hot, row),
                    Some(b'r') => out.emit(rare, row),
                    _ => {}
                })
                .build()
        })
        .runtime_options(options)
        .into_runtime(source)
        .expect("into_runtime")
}

/// The healthy contract this repro pins down: a record routed to a
/// low-volume split branch must not hold the partition watermark for longer
/// than a bounded interval **while ingest continues**. Today it is held
/// until the branch's shard buffer reaches `target_bytes`, potentially
/// forever, because nothing seals a partial buffer under sustained load.
#[test]
fn commits_advance_past_low_volume_branch_under_sustained_load() {
    let (source, handle) = memory_source();
    let (sink_hot, script_hot) = capture_sink(1, 1);
    let (sink_rare, script_rare) = capture_sink(1, 1);

    let runtime = build_pipeline(
        SUSTAINED_CONFIG,
        source,
        sink_hot,
        sink_rare,
        RuntimeOptions {
            handle_signals: false,
            // Model continuous ingest: at production rates a lane poll never
            // comes back empty, so the driver's idle-flush path (the only
            // thing that seals a partial buffer) is never reached. A large
            // value here removes scheduler-jitter flakiness without changing
            // what is being tested; the fix must bound the hold time
            // *without* relying on an idle lull.
            idle_flush: Duration::from_secs(30),
            ..RuntimeOptions::default()
        },
    );
    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());
    let admin: SocketAddr = ADMIN.parse().expect("admin addr");

    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);

    let hot_payload = vec![b'h'; 120];
    let rare_payload = vec![b'r'; 16];

    // The first record routes to the cold branch: its ack goes into that
    // branch's shard buffer and the watermark is pinned at offset 0 until the
    // buffer seals. ~5 more rares trickle in below, together a few hundred
    // bytes against a 64 KiB target, a cold DNS-type table's arrival pattern.
    let rare0 = handle.push(p, None, &rare_payload);
    let mut pushed = rare0;

    let started = Instant::now();
    let mut bursts: u32 = 0;
    let mut samples: Vec<Sample> = Vec::new();
    let mut watermark_passed_rare: Option<Duration> = None;
    while started.elapsed() < Duration::from_secs(6) {
        for _ in 0..64 {
            pushed = handle.push(p, None, &hot_payload);
        }
        bursts += 1;
        if bursts.is_multiple_of(256) {
            pushed = handle.push(p, None, &rare_payload);
        }
        if bursts.is_multiple_of(100) {
            let committed = handle.last_committed(p);
            samples.push(Sample {
                at: started.elapsed(),
                pushed,
                committed,
                pending: pending_gauge(admin),
                hot_writes: script_hot.writes().len(),
                rare_writes: script_rare.writes().len(),
            });
            if watermark_passed_rare.is_none() && committed.is_some_and(|c| c > rare0) {
                watermark_passed_rare = Some(started.elapsed());
            }
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let final_sample = Sample {
        at: started.elapsed(),
        pushed,
        committed: handle.last_committed(p),
        pending: pending_gauge(admin),
        hot_writes: script_hot.writes().len(),
        rare_writes: script_rare.writes().len(),
    };
    if watermark_passed_rare.is_none() && final_sample.committed.is_some_and(|c| c > rare0) {
        watermark_passed_rare = Some(final_sample.at);
    }
    samples.push(final_sample);

    eprintln!("      t   pushed      committed  pending  hot_writes  rare_writes");
    for s in &samples {
        eprintln!(
            "{:>6.1}s  {:>7}  {:>13}  {:>7.0}  {:>10}  {:>11}",
            s.at.as_secs_f64(),
            s.pushed,
            s.committed.map_or_else(|| "-".into(), |c| c.to_string()),
            s.pending,
            s.hot_writes,
            s.rare_writes,
        );
    }

    let last = samples.last().expect("at least one sample");
    // Delivery-health precondition: the hot branch flushed throughout, so a
    // failure below is an acknowledgment hold, not a wedged sink.
    assert!(
        last.hot_writes >= 3,
        "hot branch delivered only {} writes; the repro premise (healthy \
         delivery under load) did not hold",
        last.hot_writes
    );

    shutdown.trigger();
    if let Some(at) = watermark_passed_rare {
        // Healthy behavior: the commit tick sealed the cold branch's partial
        // buffer and the watermark moved past it while ingest continued.
        // Bound: commit interval (200ms flush cadence) + capture-sink linger
        // (1s) + write + one more tick, with generous slack. A regression to
        // a laxer cadence (e.g. leaning on batch-size alone) fails here.
        eprintln!("watermark passed the cold-branch record after {at:?} of sustained load");
        assert!(
            at <= Duration::from_secs(4),
            "watermark took {at:?} to pass the cold-branch record; the \
             commit-tick flush bounds this to ~commit interval + linger"
        );
        join.join().expect("join").expect("run");
        return;
    }
    panic!(
        "watermark never passed the cold split branch's first record during \
         {:.1}s of sustained load: committed={:?} (needs > {rare0}), while \
         checkpoint pending batches grew to {:.0} and the hot branch \
         delivered {} writes ({} rows pushed). The cold branch flushed {} \
         times — its shard buffer sat below the 64 KiB chunk target holding \
         its acks, and nothing seals a partial buffer while polls keep \
         returning data.",
        last.at.as_secs_f64(),
        last.committed,
        last.pending,
        last.hot_writes,
        last.pushed + 1,
        last.rare_writes,
    );
}

/// Control: the *same* topology and record mix commits completely once the
/// load stops; the driver's idle flush seals the cold branch's partial
/// buffer. Passing proves the sustained-load stall above is not data loss or
/// sink slowness; the acks are held while ingest continues.
#[test]
fn low_volume_branch_acks_resolve_once_load_stops() {
    let (source, handle) = memory_source();
    let (sink_hot, _script_hot) = capture_sink(1, 1);
    let (sink_rare, _script_rare) = capture_sink(1, 1);

    let runtime = build_pipeline(
        CONTROL_CONFIG,
        source,
        sink_hot,
        sink_rare,
        RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        },
    );
    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());

    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);

    let hot_payload = vec![b'h'; 120];
    let rare_payload = vec![b'r'; 16];
    let mut last = handle.push(p, None, &rare_payload);
    for i in 0..5_000 {
        last = handle.push(p, None, &hot_payload);
        if i % 1_000 == 0 {
            last = handle.push(p, None, &rare_payload);
        }
    }

    // Load has stopped: polls come back empty, `idle_flush` runs
    // `flush_terminal`, the cold buffer seals, and everything commits.
    assert!(
        handle.wait_committed(p, last + 1, Duration::from_secs(15)),
        "idle pipeline failed to commit everything (last committed: {:?})",
        handle.last_committed(p)
    );
    shutdown.trigger();
    join.join().expect("join").expect("run");
}
