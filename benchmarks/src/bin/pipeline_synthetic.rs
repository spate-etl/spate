//! Framework-overhead ceiling: generator source → real chain → real sink
//! pool → null writer, driven by the real `PipelineRuntime`. No broker, no
//! network — what's left is the framework itself.
//!
//! Usage:
//!   pipeline_synthetic          # matrix over THREADS_LIST, one child per config
//!   RUN_ONE=1 pipeline_synthetic  # single measurement, JSON line on stdout
//!
//! Env: THREADS_LIST (1,2,4,8) | THREADS (1) DURATION_S (30) PAYLOAD (256)
//! WORK_US (0) QUEUE_CAP (64) CHECKPOINT_INTERVAL_MS (200)
//! METRICS_PORT (19095) RESULTS (append JSONL path)
//!
//! Egress width — `EGRESS` (fixed|scaled|over) derives `SHARDS` and
//! `IO_THREADS` from `THREADS`; either may still be pinned explicitly. This
//! exists because the published curve swept `THREADS` against a *fixed* 2
//! shards / 2 I/O workers and peaked at 2 threads, which measures the egress
//! width rather than the framework's thread scaling.
//!
//! `LANES` (default `THREADS`) decouples lane count from thread count, so the
//! rig can reproduce the real connector shape (e.g. 100 Kafka partitions on T
//! threads) where the driver's head-of-line guard behaves differently than it
//! does at one lane per thread.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use benchmarks::report::{Metric, Report};
use benchmarks::synthetic::{NullWriter, RawDeser, RawEncoder, RawFam, RawView, SyntheticSource};
use benchmarks::{busy_work, env_str, env_u64, prom};
use spate_core::backpressure::InflightBudget;
use spate_core::config::PipelineConfig;
use spate_core::metrics::{ComponentLabels, E2eBasis, SinkShardMetrics};
use spate_core::ops::{ChunkConfig, chain};
use spate_core::pipeline::{PipelineRuntime, RuntimeOptions, SinkRuntime};
use spate_core::sink::{KeyHashRouter, SinkPool, SinkPoolConfig, shard_queues};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Per-record simulated work, read by the `spin` map stage (fn items
/// cannot capture, so the knob is a global).
static WORK_US: AtomicU64 = AtomicU64::new(0);

/// Concurrent writes per shard. Pinned here because the in-flight budget is
/// derived from it; the two must not drift apart.
const INFLIGHT_PER_SHARD: u64 = 2;

fn spin(r: RawView<'_>) -> RawView<'_> {
    busy_work(WORK_US.load(Ordering::Relaxed));
    r
}

fn run_one() {
    let threads = env_u64("THREADS", 1) as usize;
    let duration_s = env_u64("DURATION_S", 30);
    let duration = Duration::from_secs(duration_s);
    let payload = env_u64("PAYLOAD", 256) as usize;
    let work_us = env_u64("WORK_US", 0);
    let lanes = env_u64("LANES", threads as u64) as usize;
    // Egress width. `fixed` reproduces the published curve; `scaled` and
    // `over` are the arms that decide whether the observed plateau is the
    // framework's thread scaling or simply the sink's width.
    let egress = env_str("EGRESS", "fixed");
    let (shard_default, io_default) = match egress.as_str() {
        "scaled" => (threads.max(2), (threads / 2).max(2)),
        "over" => (threads.saturating_mul(2).max(2), threads.max(2)),
        "fixed" => (2, 2),
        other => panic!("unknown EGRESS {other}; want fixed|scaled|over"),
    };
    let shards = env_u64("SHARDS", shard_default as u64) as usize;
    let io_threads = env_u64("IO_THREADS", io_default as u64) as usize;
    let queue_cap = env_u64("QUEUE_CAP", 64) as usize;
    let checkpoint_ms = env_u64("CHECKPOINT_INTERVAL_MS", 200);
    let batch_max_rows = env_u64("BATCH_MAX_ROWS", 65_536);
    // Chunk size governs how many handoffs per second the shard workers must
    // service: at 64 KiB and a 256 B payload that is ~250 rows per chunk, so a
    // fast chain generates tens of thousands of queue operations per shard per
    // second. Swept because it is the most direct lever on that rate.
    let chunk_kib = env_u64("CHUNK_KIB", 64);
    WORK_US.store(work_us, Ordering::Relaxed);

    // In-flight budget, derived per arm from the sizing rule in
    // docs/DESIGN.md. Pinning a constant here would make the budget itself the
    // binding constraint as `shards` grows, and the rig would report
    // backpressure duty-cycling as if it were a thread-scaling limit.
    //
    //   max_inflight_bytes x low_ratio >= 2 x ( shards x max_per_shard x batch_bytes
    //                                         + shards x queue_cap x chunk_target )
    //
    // Batches here seal on rows long before `batch.max_bytes`, so the honest
    // per-batch figure is rows x row width, not the 128 MiB cap.
    const CHUNK_TARGET: u64 = 64 * 1024;
    const LOW_RATIO: f64 = 0.5;
    let row_bytes = payload as u64 + 4; // RawEncoder writes a u32 length prefix
    let batch_bytes = batch_max_rows.saturating_mul(row_bytes);
    let pending =
        (shards as u64) * (INFLIGHT_PER_SHARD * batch_bytes + queue_cap as u64 * CHUNK_TARGET);
    let budget_bytes = ((2.0 * pending as f64) / LOW_RATIO).ceil() as u64;
    let budget_mib = env_u64(
        "MAX_INFLIGHT_MB",
        budget_bytes.div_ceil(1024 * 1024).max(256),
    );

    // Install the recorder before ANY metric handle exists: handles
    // created before install bind to the noop recorder and render nothing.
    // The pipeline config uses `exporter: none` so the runtime doesn't
    // fight over the global recorder; we render our own handle directly.
    let metrics_handle = spate_core::metrics::install(&spate_core::metrics::MetricsSettings {
        exporter: spate_core::metrics::Exporter::Prometheus,
        ..Default::default()
    })
    .expect("install metrics recorder");

    let config = PipelineConfig::from_str(&format!(
        r"
pipeline: {{ name: synthetic, threads: {threads} }}
checkpoint: {{ interval: {checkpoint_ms}ms }}
backpressure: {{ max_inflight_bytes: {budget_mib}MiB }}
metrics: {{ exporter: none }}
source: {{ synthetic: {{}} }}
sink: {{ nullsink: {{}} }}
"
    ))
    .expect("config");

    let produced = Arc::new(AtomicU64::new(0));
    let source = SyntheticSource::new(lanes, payload, Arc::clone(&produced));
    let commits = source.commits();

    let writer = Arc::new(NullWriter::default());
    let endpoints: Vec<Vec<()>> = (0..shards).map(|_| vec![()]).collect();
    let (queues, receivers) = shard_queues(shards, queue_cap);
    let budget = Arc::new(InflightBudget::new());
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(io_threads)
        .enable_all()
        .build()
        .expect("io runtime");
    let labels = ComponentLabels::new("synthetic", "sink", "null");
    let metrics = (0..shards)
        .map(|s| {
            SinkShardMetrics::new(
                &labels,
                u32::try_from(s).expect("shard"),
                &[format!("null-{s}")],
                E2eBasis::Ingest,
            )
        })
        .collect();
    // Small, fast-cycling batches: the null writer completes instantly, so
    // sealing often keeps the in-flight byte budget low and measures the
    // framework rather than backpressure duty cycles (see
    // docs/benchmarks/framework-overhead.mdx).
    let pool_cfg = {
        let mut cfg = SinkPoolConfig::default();
        cfg.batch.max_rows = batch_max_rows;
        cfg.batch.linger = Duration::from_millis(5);
        // Pinned rather than defaulted so the budget derivation above and the
        // pool cannot silently disagree.
        cfg.inflight.max_per_shard = INFLIGHT_PER_SHARD as usize;
        cfg
    };
    let pool = SinkPool::spawn(
        Arc::clone(&writer),
        endpoints,
        receivers,
        pool_cfg,
        Arc::clone(&budget),
        metrics,
        "synthetic",
        io.handle(),
    );
    let sink = SinkRuntime {
        queues: vec![queues.clone()],
        drain: Box::new(move |deadline| Box::pin(async move { pool.drain(deadline).await })),
        probe: None,
    };

    let chunk_cfg = ChunkConfig {
        target_bytes: (chunk_kib * 1024) as usize,
        ..ChunkConfig::default()
    };
    let chain_queues = queues;
    let chain_budget = Arc::clone(&budget);
    let chains = move |_thread: usize| {
        let b = chain::<RawFam, _>(RawDeser).filter(|_r: &RawView<'_>| true);
        if WORK_US.load(Ordering::Relaxed) > 0 {
            b.map_rec::<RawFam, _>(spin)
                .sink(
                    RawEncoder,
                    KeyHashRouter,
                    chunk_cfg,
                    chain_queues.clone(),
                    Arc::clone(&chain_budget),
                )
                .build()
        } else {
            b.sink(
                RawEncoder,
                KeyHashRouter,
                chunk_cfg,
                chain_queues.clone(),
                Arc::clone(&chain_budget),
            )
            .build()
        }
    };

    let runtime =
        PipelineRuntime::new(config, source, chains, sink, budget).with_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        });
    let shutdown = runtime.shutdown_handle();
    let pipeline = std::thread::spawn(move || runtime.run());

    // Warm up, then measure a steady-state window.
    std::thread::sleep(Duration::from_secs(3));
    let text0 = metrics_handle.render();
    let c0 = produced.load(Ordering::Relaxed);
    let t0 = Instant::now();
    std::thread::sleep(duration);
    let c1 = produced.load(Ordering::Relaxed);
    let window = t0.elapsed().as_secs_f64();

    let metrics_text = metrics_handle.render();
    // Validity counters, windowed. A scaled-egress arm that pauses or fills a
    // queue was limited by the sink, not by the framework — the number is not
    // a thread-scaling datapoint and the note has to say so.
    let delta = |name: &str| {
        let a = prom::value(&text0, name, "").unwrap_or(0.0);
        let b = prom::value(&metrics_text, name, "").unwrap_or(0.0);
        (b - a).max(0.0)
    };
    let queue_full = delta("spate_queue_full_events_total");
    let pause_events = delta("spate_backpressure_pause_events_total");
    let paused_seconds = delta("spate_backpressure_paused_seconds_total");
    let pending_batches =
        prom::value(&metrics_text, "spate_checkpoint_pending_batches", "").unwrap_or(0.0);
    let sink_limited = queue_full > 0.0 || pause_events > 0.0;
    shutdown.trigger();
    let exit = pipeline.join().expect("pipeline thread").expect("run");

    // Two windows, reported separately so they can never be conflated:
    // `records` covers the steady-state measurement window only, while the
    // lifetime figures (produced_total / sink_rows_total) also include
    // warmup and drain. Comparing window records against lifetime rows is
    // what once made the sink look like it double-counted.
    let records = c1 - c0;
    let rate = records as f64 / window;
    let produced_total = produced.load(Ordering::Relaxed);
    assert!(
        writer.rows() <= produced_total,
        "conservation: rows written ({}) must never exceed records produced ({})",
        writer.rows(),
        produced_total,
    );
    // The published `pipeline_synthetic` dataset predates the egress/lane/chunk
    // knobs below, so its records carry a smaller variant key set. Emitting the
    // v2 harness under that name would give the new records a different variant
    // identity and render duplicate bars on the framework-overhead page — hence
    // a distinct bench name for the scaling study.
    let mut rep = Report::measurement(env_str("BENCH", "pipeline_synthetic").as_str())
        // v1 records swept THREADS against a fixed 2-shard / 2-worker egress
        // and cannot be compared with these; the key keeps the two sets from
        // ever aggregating into one bar.
        .variant("harness", "v2")
        .variant("threads", threads as u64)
        .variant("lanes", lanes as u64)
        .variant("egress", egress.as_str())
        .variant("shards", shards as u64)
        .variant("io_threads", io_threads as u64)
        .variant("queue_cap", queue_cap as u64)
        .variant("batch_max_rows", batch_max_rows)
        .variant("chunk_kib", chunk_kib)
        // Derived, but deterministic from the keys above — so reps of one arm
        // still share an identity.
        .variant("max_inflight_mb", budget_mib)
        .variant("payload_bytes", payload as u64)
        .variant("work_us", work_us)
        // The *configured* duration, not the measured window: a variant value
        // that differs between reps gives each rep its own identity and
        // silently defeats median aggregation across reps.
        .variant("duration_s", duration_s)
        .metric("records", Metric::maximize(records as f64, "records"))
        .metric("records_per_s", Metric::maximize(rate, "records/s"))
        .metric(
            "records_per_s_per_thread",
            Metric::maximize(rate / threads as f64, "records/s"),
        )
        .metric(
            "produced_total",
            Metric::maximize(produced_total as f64, "records"),
        )
        .metric(
            "sink_rows_total",
            Metric::maximize(writer.rows() as f64, "rows"),
        )
        .metric(
            "sink_batches_total",
            Metric::maximize(writer.batches() as f64, "batches"),
        )
        .metric(
            "commits",
            Metric::maximize(commits.load(Ordering::Relaxed) as f64, "commits"),
        )
        .metric("queue_full_events", Metric::minimize(queue_full, "events"))
        .metric(
            "backpressure_pause_events",
            Metric::minimize(pause_events, "events"),
        )
        .metric(
            "checkpoint_pending_batches",
            Metric::minimize(pending_batches, "batches"),
        );
    // e2e latency is only populated when the exporter has histogram samples;
    // omit the metric entirely rather than emitting a null.
    if let Some(p50) = prom::histogram_quantile(&metrics_text, "spate_e2e_latency_seconds", 0.5) {
        rep = rep.metric("e2e_p50_s", Metric::minimize(p50, "s"));
    }
    if let Some(p99) = prom::histogram_quantile(&metrics_text, "spate_e2e_latency_seconds", 0.99) {
        rep = rep.metric("e2e_p99_s", Metric::minimize(p99, "s"));
    }
    // Oversubscription check: pipeline threads + I/O workers + controller
    // against available cores. Past that line a falling efficiency figure is
    // the box running out of cores, not the framework failing to scale.
    let busy = threads + io_threads + 1;
    let cores = std::thread::available_parallelism().map_or(0, usize::from);
    rep.note(format!(
        "exit={:?}; window_s={window:.3}; verdict={}; \
         pause_events={pause_events}; paused_s={paused_seconds:.3}; \
         threads_busy={busy}/{cores}{}; \
         queue_full={queue_full} (unwired: this rig assembles the runtime \
         directly, so ShardQueues::attach_metrics is never called)",
        exit.state,
        if sink_limited {
            "SINK-LIMITED (not a thread-scaling datapoint)"
        } else {
            "framework-bound"
        },
        if cores > 0 && busy > cores {
            " OVERSUBSCRIBED"
        } else {
            ""
        },
    ))
    .emit();
}

fn main() {
    // Validates BENCH_TRIGGER before any work: it is otherwise read when the
    // first report is built, which is after the measurement.
    benchmarks::preflight();
    if std::env::var("RUN_ONE").is_ok() {
        run_one();
        return;
    }
    let list = env_str("THREADS_LIST", "1,2,4,8");
    let reps = env_u64("REPS", 1).max(1);
    let egress = env_str("EGRESS", "fixed");
    for threads in list.split(',').filter_map(|t| t.trim().parse::<u64>().ok()) {
        for rep in 1..=reps {
            eprintln!(
                "── pipeline_synthetic EGRESS={egress} THREADS={threads} rep {rep}/{reps} ──"
            );
            let status = std::process::Command::new(std::env::current_exe().expect("exe"))
                .env("RUN_ONE", "1")
                .env("THREADS", threads.to_string())
                // The child must not re-enter the sweep.
                .env_remove("THREADS_LIST")
                .env_remove("REPS")
                .status()
                .expect("child run");
            assert!(status.success(), "child run failed for THREADS={threads}");
        }
    }
}
