//! Framework-overhead ceiling: generator source → real chain → real sink
//! pool → null writer, driven by the real `PipelineRuntime`. No broker, no
//! network — what's left is the framework itself.
//!
//! Usage:
//!   pipeline_synthetic          # matrix over THREADS_LIST, one child per config
//!   RUN_ONE=1 pipeline_synthetic  # single measurement, JSON line on stdout
//!
//! Env: THREADS_LIST (1,2,4,8) | THREADS (1) DURATION_S (30) PAYLOAD (256)
//! WORK_US (0) SHARDS (2) QUEUE_CAP (64) CHECKPOINT_INTERVAL_MS (200)
//! METRICS_PORT (19095) RESULTS (append JSONL path)
#![allow(clippy::print_stdout, clippy::print_stderr)]

use benchmarks::report::{Metric, Report};
use benchmarks::synthetic::{NullWriter, RawDeser, RawEncoder, RawFam, RawView, SyntheticSource};
use benchmarks::{busy_work, env_str, env_u64, prom};
use etl_core::backpressure::InflightBudget;
use etl_core::config::PipelineConfig;
use etl_core::metrics::{ComponentLabels, E2eBasis, SinkShardMetrics};
use etl_core::ops::{ChunkConfig, chain};
use etl_core::pipeline::{PipelineRuntime, RuntimeOptions, SinkRuntime};
use etl_core::sink::{KeyHashRouter, SinkPool, SinkPoolConfig, shard_queues};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Per-record simulated work, read by the `spin` map stage (fn items
/// cannot capture, so the knob is a global).
static WORK_US: AtomicU64 = AtomicU64::new(0);

fn spin(r: RawView<'_>) -> RawView<'_> {
    busy_work(WORK_US.load(Ordering::Relaxed));
    r
}

fn run_one() {
    let threads = env_u64("THREADS", 1) as usize;
    let duration = Duration::from_secs(env_u64("DURATION_S", 30));
    let payload = env_u64("PAYLOAD", 256) as usize;
    let work_us = env_u64("WORK_US", 0);
    let shards = env_u64("SHARDS", 2) as usize;
    let queue_cap = env_u64("QUEUE_CAP", 64) as usize;
    let checkpoint_ms = env_u64("CHECKPOINT_INTERVAL_MS", 200);
    WORK_US.store(work_us, Ordering::Relaxed);

    // Install the recorder before ANY metric handle exists: handles
    // created before install bind to the noop recorder and render nothing.
    // The pipeline config uses `exporter: none` so the runtime doesn't
    // fight over the global recorder; we render our own handle directly.
    let metrics_handle = etl_core::metrics::install(&etl_core::metrics::MetricsSettings {
        exporter: etl_core::metrics::Exporter::Prometheus,
        ..Default::default()
    })
    .expect("install metrics recorder");

    let config = PipelineConfig::from_str(&format!(
        r"
pipeline: {{ name: synthetic, threads: {threads} }}
checkpoint: {{ interval: {checkpoint_ms}ms }}
metrics: {{ exporter: none }}
source: {{ synthetic: {{}} }}
sink: {{ nullsink: {{}} }}
"
    ))
    .expect("config");

    let produced = Arc::new(AtomicU64::new(0));
    let source = SyntheticSource::new(threads, payload, Arc::clone(&produced));
    let commits = source.commits();

    let writer = Arc::new(NullWriter::default());
    let endpoints: Vec<Vec<()>> = (0..shards).map(|_| vec![()]).collect();
    let (queues, receivers) = shard_queues(shards, queue_cap);
    let budget = Arc::new(InflightBudget::new());
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
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
        cfg.batch.max_rows = 65_536;
        cfg.batch.linger = Duration::from_millis(5);
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
        queues: queues.clone(),
        drain: Box::new(move |deadline| Box::pin(async move { pool.drain(deadline).await })),
        probe: None,
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
                    ChunkConfig::default(),
                    chain_queues.clone(),
                    Arc::clone(&chain_budget),
                )
                .build()
        } else {
            b.sink(
                RawEncoder,
                KeyHashRouter,
                ChunkConfig::default(),
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
    let c0 = produced.load(Ordering::Relaxed);
    let t0 = Instant::now();
    std::thread::sleep(duration);
    let c1 = produced.load(Ordering::Relaxed);
    let window = t0.elapsed().as_secs_f64();

    let metrics_text = metrics_handle.render();
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
    let mut rep = Report::measurement("pipeline_synthetic")
        .variant("threads", threads as u64)
        .variant("shards", shards as u64)
        .variant("payload_bytes", payload as u64)
        .variant("work_us", work_us)
        // The measured window (~DURATION_S) is a measurement context, not a
        // goal — carried in the variant so no "higher is better" is implied.
        .variant("window_s", window)
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
        );
    // e2e latency is only populated when the exporter has histogram samples;
    // omit the metric entirely rather than emitting a null.
    if let Some(p50) = prom::histogram_quantile(&metrics_text, "etl_e2e_latency_seconds", 0.5) {
        rep = rep.metric("e2e_p50_s", Metric::minimize(p50, "s"));
    }
    if let Some(p99) = prom::histogram_quantile(&metrics_text, "etl_e2e_latency_seconds", 0.99) {
        rep = rep.metric("e2e_p99_s", Metric::minimize(p99, "s"));
    }
    rep.note(format!("exit={:?}", exit.state)).emit();
}

fn main() {
    if std::env::var("RUN_ONE").is_ok() {
        run_one();
        return;
    }
    let list = env_str("THREADS_LIST", "1,2,4,8");
    for threads in list.split(',').filter_map(|t| t.trim().parse::<u64>().ok()) {
        eprintln!("── pipeline_synthetic THREADS={threads} ──");
        let status = std::process::Command::new(std::env::current_exe().expect("exe"))
            .env("RUN_ONE", "1")
            .env("THREADS", threads.to_string())
            .status()
            .expect("child run");
        assert!(status.success(), "child run failed for THREADS={threads}");
    }
}
