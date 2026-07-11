//! ClickHouse sink saturation: an in-process generator source (no broker)
//! driven through the real chain, sink pool, and runtime into ClickHouse at
//! full tilt, to find the ETL sink's ceiling and a good part/batch size.
//!
//! Two engines, selected by `ENGINE`:
//! - `Null` (default): rows are discarded after parse — no part write, fsync,
//!   merge, or async-buffer flush. This is the *ceiling*: framework egress +
//!   HTTP transport + server-side parse/block-form. `SELECT count()` is 0 here,
//!   so throughput is read from `etl_sink_records_total` (durable acks) over a
//!   steady-state window, not from the table.
//! - `MergeTree`: the real sink — part writes and merges. The `Null → MergeTree`
//!   throughput gap is the flush/part-write cost, and `chstats` reads the
//!   server's own part sizes and CPU so we can judge whether the ETL batch size
//!   yields healthy (large) parts.
//!
//! Whether the sink is actually the bottleneck is not assumed: each run records
//! a `limiter` verdict in its note (`sink` / `budget` / `generator` /
//! `indeterminate-checkpoint`) from the windowed backpressure and checkpoint
//! signals, so a generator- or budget-limited arm can't be misread as a sink
//! ceiling. The default inflight budget is derived from the sink's configured
//! batch/queue sizing so the rig doesn't self-throttle below sink capacity.
//!
//! Profiles:
//! - **Local** (default): starts/reuses the `etl-bench-clickhouse` container
//!   (`CLICKHOUSE_IMAGE`, `CLICKHOUSE_CPUS`), so the client and server share
//!   this host — a shared-host ceiling. The parent process creates the server
//!   once; children reuse it warm.
//! - **External**: set `CLICKHOUSE_URL` (http://host:port) [+ `CLICKHOUSE_USER`
//!   / `CLICKHOUSE_PASSWORD`] for a dedicated server with its own cores.
//!
//! Usage:
//!   ch_sink_saturation            # matrix over THREADS_LIST, one child per
//!                                 # thread count
//!   RUN_ONE=1 ch_sink_saturation  # a single measurement, one JSON line
//!
//! Env: THREADS_LIST (1,2,4) | THREADS (1) SHARDS (4) IO_THREADS (4)
//! INFLIGHT_PER_SHARD (2) BATCH_MAX_ROWS (262144) BATCH_MAX_MB (128)
//! LINGER_MS (500) QUEUE_CAP (256) MAX_INFLIGHT_MB (derived, see run_one)
//! MAX_PENDING_BATCHES (8192) FORMAT (rowbinary) COMPRESSION (lz4)
//! ENGINE (Null) ASYNC_INSERT (0) MAX_INSERT_BLOCK_SIZE (unset)
//! PAYLOAD (256) DURATION_S (30) WARMUP_S (5) CHECKPOINT_INTERVAL_MS (1000)
//! CLICKHOUSE_CPUS (8) RESULTS (append JSONL path)
#![allow(clippy::print_stdout, clippy::print_stderr)]

use benchmarks::report::{Metric, Report};
use benchmarks::synthetic::SyntheticSource;
use benchmarks::{chstats, docker, env_str, env_u64, prom};
use etl_clickhouse::{ClickHouseEncoder, NativeEncoder};
use etl_core::backpressure::InflightBudget;
use etl_core::config::{ComponentConfig, PipelineConfig};
use etl_core::deser::{BytesPassthrough, Owned};
use etl_core::metrics::{ComponentLabels, E2eBasis, MetricsHandle, SinkShardMetrics};
use etl_core::ops::{ChunkConfig, RunnableChain, chain_owned};
use etl_core::pipeline::{PipelineRuntime, RuntimeOptions, SinkRuntime};
use etl_core::record::RecordMeta;
use etl_core::sink::{ShardRouter, SinkPool, shard_queues};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// The row we insert. Field order == `columns` order — the wire contract for
/// both RowBinary and Native.
#[derive(serde::Serialize)]
struct BenchRow {
    id: u64,
    body: String,
}

/// Turn a generated payload into a row. The generator's per-lane arena holds
/// 4096 distinct payloads; the first 8 bytes of each are its little-endian
/// arena index, used as `id` (4096 distinct values). The same-length ASCII
/// `body` is a cheap xorshift seeded from `id | 1`, so rows are neither
/// identical (dedup) nor constant (trivially compressible); folding even/odd
/// ids together with `| 1` leaves ~2048 distinct bodies. This is low-entropy —
/// compression numbers are a compressible-data case, noted on the record.
///
/// `BytesPassthrough` hands us an owned copy of the arena bytes, so we overwrite
/// the payload in place (one alloc per record) rather than allocating a body.
fn to_row(mut payload: Vec<u8>) -> BenchRow {
    let mut idb = [0u8; 8];
    let n = idb.len().min(payload.len());
    idb[..n].copy_from_slice(&payload[..n]);
    let id = u64::from_le_bytes(idb);

    let mut x = id | 1;
    for b in &mut payload {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *b = b'a' + (x % 26) as u8;
    }
    let body = String::from_utf8(payload).expect("ascii body");
    BenchRow { id, body }
}

/// Balanced shard routing. The generator emits keyless records, so the default
/// `KeyHashRouter` would pin a whole lane to one shard by partition hash;
/// round-robin fans a thread's records evenly across every shard worker,
/// saturating them regardless of the thread/shard ratio.
#[derive(Debug, Default)]
struct RoundRobinRouter {
    next: AtomicUsize,
}

impl ShardRouter for RoundRobinRouter {
    #[inline]
    fn route(&self, _meta: &RecordMeta, num_shards: usize) -> usize {
        self.next.fetch_add(1, Ordering::Relaxed) % num_shards
    }
}

/// ClickHouse connection resolved from `CLICKHOUSE_URL` or the local container.
struct Conn {
    url: String,
    host: String,
    port: u16,
    user: String,
    password: String,
}

fn resolve_conn() -> Conn {
    let (url, host, port, user, password) = docker::resolve_clickhouse();
    Conn {
        url,
        host,
        port,
        user,
        password,
    }
}

fn install_metrics() -> MetricsHandle {
    etl_core::metrics::install(&etl_core::metrics::MetricsSettings {
        exporter: etl_core::metrics::Exporter::Prometheus,
        ..Default::default()
    })
    .expect("install metrics recorder")
}

fn run_one() {
    let threads = env_u64("THREADS", 1) as usize;
    let shards = env_u64("SHARDS", 4) as usize;
    let io_threads = env_u64("IO_THREADS", 4) as usize;
    let inflight = env_u64("INFLIGHT_PER_SHARD", 2);
    let batch_max_rows = env_u64("BATCH_MAX_ROWS", 262_144);
    let batch_max_mb = env_u64("BATCH_MAX_MB", 128);
    let linger_ms = env_u64("LINGER_MS", 500);
    let queue_cap = env_u64("QUEUE_CAP", 256) as usize;
    // Default inflight budget from the deployment sizing rule (docs/user-guide/
    // 05-deployment/tuning.md): `max_inflight_bytes × low_ratio` must cover the
    // fully-formed in-flight batches plus the queued 64 KiB chunks admitted to
    // the budget at chunk-seal (etl-core handoff). Use BATCH_MAX_MB (the config
    // bound) not observed batch size — the part-size arms seal on max_bytes and
    // would otherwise under-provision. low_ratio is 0.5 (config default); an
    // undersized budget self-throttles the rig below the sink's real capacity.
    const LOW_RATIO: f64 = 0.5;
    const CHUNK_MIB: f64 = 64.0 / 1024.0; // 64 KiB chunk target
    let derived_mb = (2.0
        * (shards as f64 * inflight as f64 * batch_max_mb as f64
            + shards as f64 * queue_cap as f64 * CHUNK_MIB)
        / LOW_RATIO)
        .ceil() as u64;
    let max_inflight_mb = env_u64("MAX_INFLIGHT_MB", derived_mb.max(1024));
    let format = env_str("FORMAT", "rowbinary");
    let compression = env_str("COMPRESSION", "lz4");
    let engine = env_str("ENGINE", "Null");
    let async_insert = env_u64("ASYNC_INSERT", 0);
    let max_insert_block_size = std::env::var("MAX_INSERT_BLOCK_SIZE").ok();
    let payload = env_u64("PAYLOAD", 256) as usize;
    let duration = Duration::from_secs(env_u64("DURATION_S", 30));
    let warmup = Duration::from_secs(env_u64("WARMUP_S", 5));
    let checkpoint_ms = env_u64("CHECKPOINT_INTERVAL_MS", 1000);
    // With many small MergeTree parts, acked-but-uncommitted batches pile up and
    // the framework default (1024) pauses lanes before the sink is saturated.
    // Raise it here so a saturation run isn't capped by checkpoint memory.
    let max_pending_batches = env_u64("MAX_PENDING_BATCHES", 8192);

    // Worst-case resident batch memory: every shard holds up to `inflight`
    // fully-formed batches, each capped by BATCH_MAX_ROWS or BATCH_MAX_MB —
    // whichever binds first. The large part-size arms (millions of rows) reach
    // multiple GB, so surface the bound before allocating.
    let row_bytes = payload as u64 + 40; // id + String(body ≈ payload) + overhead
    let batch_bytes = (batch_max_rows * row_bytes).min(batch_max_mb * 1024 * 1024);
    let worst_case_mb = shards as u64 * inflight * batch_bytes / (1024 * 1024);
    eprintln!(
        "budget: MAX_INFLIGHT_MB={max_inflight_mb} (derived {derived_mb}, low_ratio {LOW_RATIO}); \
         worst-case resident batches ~{worst_case_mb} MiB \
         (shards {shards} x inflight {inflight} x batch cap)"
    );

    let conn = resolve_conn();
    let sql = |q: &str| {
        docker::clickhouse_sql(&conn.host, conn.port, &conn.user, &conn.password, q)
            .expect("clickhouse")
    };

    // ── Table ────────────────────────────────────────────────────────────
    let table = "bench_sink";
    sql(&format!("DROP TABLE IF EXISTS {table}"));
    // The generator arena is periodic (4096 payloads), so at the default
    // BATCH_MAX_ROWS = 262144 = 64 arenas every batch carries byte-identical
    // content. The only thing stopping the last-N content-hash dedup window from
    // silently dropping repeated batches is the sink's unique per-batch
    // `insert_deduplication_token`; a small window keeps that behaviour cheap.
    let ddl = if engine == "MergeTree" {
        format!(
            "CREATE TABLE {table} (id UInt64, body String) ENGINE = MergeTree ORDER BY id \
             SETTINGS non_replicated_deduplication_window = 100"
        )
    } else {
        format!("CREATE TABLE {table} (id UInt64, body String) ENGINE = Null")
    };
    sql(&ddl);

    // ── Metrics + I/O runtime ────────────────────────────────────────────
    let metrics_handle = install_metrics();
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(io_threads)
        .enable_all()
        .build()
        .expect("io runtime");

    // ── Pipeline config ──────────────────────────────────────────────────
    let config = PipelineConfig::from_str(&format!(
        "pipeline: {{ name: ch-saturation, threads: {threads} }}\n\
         checkpoint: {{ interval: {checkpoint_ms}ms, max_pending_batches: {max_pending_batches} }}\n\
         backpressure: {{ max_inflight_bytes: {max_inflight_mb}MiB }}\n\
         metrics: {{ exporter: none }}\n\
         source: {{ synthetic: {{}} }}\n\
         sink: {{ clickhouse: {{}} }}\n"
    ))
    .expect("config");

    // ── ClickHouse sink from YAML ────────────────────────────────────────
    let ch_format = if format == "native" {
        "native"
    } else {
        "rowbinary"
    };
    let mut settings_pairs = vec![
        format!("async_insert: \"{async_insert}\""),
        "wait_for_async_insert: \"1\"".to_owned(),
    ];
    if let Some(mibs) = &max_insert_block_size {
        settings_pairs.push(format!("max_insert_block_size: \"{mibs}\""));
    }
    let settings = settings_pairs.join(", ");
    let shard_lines: String = (0..shards)
        .map(|_| format!("    - replicas: [{:?}]\n", conn.url))
        .collect();
    let sink_yaml = format!(
        "clickhouse:
  table: {table}
  columns: [id, body]
  format: {ch_format}
  compression: {compression}
  user: {user}
  password: {password:?}
  inflight: {{ max_per_shard: {inflight} }}
  batch: {{ max_rows: {batch_max_rows}, max_bytes: {batch_max_mb}MiB, linger: {linger_ms}ms }}
  settings: {{ {settings} }}
  shards:
{shard_lines}",
        user = conn.user,
        password = conn.password,
    );
    let section: ComponentConfig = serde_yaml::from_str(&sink_yaml).expect("sink section");
    let ch = etl_clickhouse::from_component_config(&section).expect("clickhouse sink");
    assert_eq!(ch.endpoints.len(), shards, "one endpoint per shard");

    // Native is type-driven: fetch the column schema once, before the pool
    // consumes the endpoints.
    let native_schema = if ch_format == "native" {
        Some(io.block_on(ch.native_schema()).expect("native schema"))
    } else {
        None
    };

    // ── Source ───────────────────────────────────────────────────────────
    let produced = Arc::new(AtomicU64::new(0));
    let source = SyntheticSource::new(threads, payload, Arc::clone(&produced));
    let commits = source.commits();

    // ── Sink pool (mirrors e2e_kafka_clickhouse::spawn_and_measure) ──────
    let (queues, receivers) = shard_queues(shards, queue_cap);
    let budget = Arc::new(InflightBudget::new());
    let labels = ComponentLabels::new("ch-saturation", "sink", "clickhouse");
    let metrics = (0..shards)
        .map(|s| {
            SinkShardMetrics::new(
                &labels,
                u32::try_from(s).expect("shard"),
                &[format!("replica-{s}")],
                E2eBasis::Ingest,
            )
        })
        .collect();
    let pool = SinkPool::spawn(
        Arc::new(ch.writer),
        ch.endpoints,
        receivers,
        ch.pool,
        Arc::clone(&budget),
        metrics,
        "ch-saturation",
        io.handle(),
    );
    let sink = SinkRuntime {
        queues: vec![queues.clone()],
        drain: Box::new(move |deadline| Box::pin(async move { pool.drain(deadline).await })),
        probe: None,
    };

    // ── Chain factory (one per pipeline thread) ──────────────────────────
    let chain_queues = queues;
    let chain_budget = Arc::clone(&budget);
    let chains = move |_thread: usize| -> Box<dyn RunnableChain> {
        let router = RoundRobinRouter::default();
        match &native_schema {
            Some(schema) => {
                let enc = NativeEncoder::<Owned<BenchRow>>::new(Arc::clone(schema));
                chain_owned::<Vec<u8>, _>(BytesPassthrough)
                    .with_metrics("ch-saturation", "main")
                    .map(to_row)
                    .sink(
                        enc,
                        router,
                        ChunkConfig::default(),
                        chain_queues.clone(),
                        Arc::clone(&chain_budget),
                    )
                    .build()
            }
            None => {
                let enc = ClickHouseEncoder::<Owned<BenchRow>>::new();
                chain_owned::<Vec<u8>, _>(BytesPassthrough)
                    .with_metrics("ch-saturation", "main")
                    .map(to_row)
                    .sink(
                        enc,
                        router,
                        ChunkConfig::default(),
                        chain_queues.clone(),
                        Arc::clone(&chain_budget),
                    )
                    .build()
            }
        }
    };

    let runtime =
        PipelineRuntime::new(config, source, chains, sink, budget).with_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        });
    let shutdown = runtime.shutdown_handle();
    let pipeline = std::thread::spawn(move || runtime.run());

    // ── Warm up, then measure a steady-state window ──────────────────────
    // Throughput = the delta of `etl_sink_records_total` (durable acks, summed
    // across shards) over the window; works even for Null, where the table
    // count is 0. `since` scopes the ClickHouse-side accounting to the window.
    std::thread::sleep(warmup);
    // Capture the CH-side `since` and the metrics baseline back-to-back; the few
    // ms between them is negligible against the multi-second window. `text0`
    // serves both the pause/gauge baselines and the flush-histogram delta.
    let since = chstats::now(&conn.host, conn.port, &conn.user, &conn.password);
    let text0 = metrics_handle.render();
    let sink0 = prom::value(&text0, "etl_sink_records_total", "").unwrap_or(0.0);
    let pauses0 = prom::value(&text0, "etl_backpressure_pause_events_total", "").unwrap_or(0.0);
    let paused0 = prom::value(&text0, "etl_backpressure_paused", "").unwrap_or(0.0);
    let pend0 = prom::value(&text0, "etl_checkpoint_pending_batches", "").unwrap_or(0.0);
    let c0 = produced.load(Ordering::Relaxed);
    let t0 = Instant::now();
    std::thread::sleep(duration);
    let text1 = metrics_handle.render();
    let window = t0.elapsed().as_secs_f64();
    let sink1 = prom::value(&text1, "etl_sink_records_total", "").unwrap_or(0.0);
    let pauses1 = prom::value(&text1, "etl_backpressure_pause_events_total", "").unwrap_or(0.0);
    let paused1 = prom::value(&text1, "etl_backpressure_paused", "").unwrap_or(0.0);
    let pend1 = prom::value(&text1, "etl_checkpoint_pending_batches", "").unwrap_or(0.0);
    let c1 = produced.load(Ordering::Relaxed);

    // Limiter verdict (a note token, never a variant key — reps of one arm can
    // legitimately differ, and the site groups reps by full variant identity).
    //   pauses      = windowed delta of the backpressure pause counter;
    //   paused_edge = the backpressure gauge set at either window edge
    //                 (catches a pause that spans the whole window);
    //   pend_high   = checkpoint backlog over half the pending-batch cap —
    //                 checkpoint pauses have no counter (controller only warns).
    let pauses = (pauses1 - pauses0).max(0.0);
    let paused_edge = paused0 > 0.0 || paused1 > 0.0;
    let pend_high =
        pend0 > max_pending_batches as f64 / 2.0 || pend1 > max_pending_batches as f64 / 2.0;
    let limiter = if max_inflight_mb < derived_mb {
        // A hand-set budget below the rule invalidates any ceiling claim.
        "budget"
    } else if pauses > 0.0 || paused_edge {
        "sink"
    } else if pend_high {
        "indeterminate-checkpoint"
    } else {
        "generator"
    };

    shutdown.trigger();
    let exit = pipeline.join().expect("pipeline thread").expect("run");

    // ── ClickHouse-side accounting (post-drain) ──────────────────────────
    let stats = chstats::capture(
        &conn.host,
        conn.port,
        &conn.user,
        &conn.password,
        table,
        &since,
    );
    let final_count = sql(&format!("SELECT count() FROM {table}"))
        .trim()
        .parse::<u64>()
        .unwrap_or(0);

    let sink_records = sink1 - sink0;
    let rows_per_s = if window > 0.0 {
        sink_records / window
    } else {
        0.0
    };
    let produced_delta = c1.saturating_sub(c0);

    let mut rep = Report::measurement("ch_sink_saturation")
        .variant("engine", engine.clone())
        .variant("format", ch_format)
        .variant("compression", compression.clone())
        .variant("async_insert", async_insert)
        .variant("threads", threads as u64)
        .variant("shards", shards as u64)
        .variant("io_threads", io_threads as u64)
        .variant("inflight_per_shard", inflight)
        .variant("batch_max_rows", batch_max_rows)
        .variant("batch_max_mb", batch_max_mb)
        .variant("linger_ms", linger_ms)
        .variant("queue_cap", queue_cap as u64)
        .variant("max_inflight_mb", max_inflight_mb)
        .variant("max_pending_batches", max_pending_batches)
        .variant("payload_bytes", payload as u64)
        .metric("rows_per_s", Metric::maximize(rows_per_s, "rows/s"))
        .metric(
            "rows_per_s_per_thread",
            Metric::maximize(rows_per_s / threads as f64, "rows/s"),
        )
        .metric("sink_records", Metric::maximize(sink_records, "rows"))
        .metric(
            "produced_total",
            Metric::maximize(produced.load(Ordering::Relaxed) as f64, "records"),
        )
        .metric(
            "ch_written_rows",
            Metric::maximize(stats.written_rows, "rows"),
        )
        .metric(
            "ch_cpu_us_per_row",
            Metric::minimize(stats.cpu_us_per_row(), "us"),
        )
        .metric(
            "commits",
            Metric::maximize(commits.load(Ordering::Relaxed) as f64, "commits"),
        );
    // Part shape and merges only exist for MergeTree; a Null arm would emit
    // zero-valued part metrics that read as "tiny parts", not "no parts".
    if engine == "MergeTree" {
        rep = rep
            .metric(
                "ch_parts_created",
                Metric::minimize(stats.parts_created, "parts"),
            )
            .metric(
                "ch_avg_part_rows",
                Metric::maximize(stats.avg_part_rows, "rows"),
            )
            .metric(
                "ch_avg_part_bytes",
                Metric::maximize(stats.avg_part_bytes, "bytes"),
            )
            .metric("ch_merges", Metric::minimize(stats.merges, "merges"));
    }
    if stats.async_flushes > 0.0 {
        rep = rep
            .metric(
                "ch_async_flushes",
                Metric::maximize(stats.async_flushes, "flushes"),
            )
            .metric(
                "ch_async_avg_rows",
                Metric::maximize(stats.async_avg_rows, "rows"),
            );
    }
    // Flush latency over the window only: quantile of the per-`le` bucket delta.
    if let Some(p99) =
        prom::histogram_quantile_delta(&text0, &text1, "etl_sink_flush_duration_seconds", 0.99)
    {
        rep = rep.metric("sink_flush_p99", Metric::minimize(p99, "s"));
    }
    // Windowed delta, not cumulative-since-start.
    rep = rep.metric("backpressure_pauses", Metric::minimize(pauses, "events"));
    // Local-container runs share cores with the client; record the cap so the
    // shared-host arms are distinguishable from external-server arms.
    if std::env::var("CLICKHOUSE_URL").is_err() {
        rep = rep.variant("clickhouse_cpus", env_u64("CLICKHOUSE_CPUS", 8));
    }
    rep.note(format!(
        "ceiling rig: memory source -> ClickHouse {engine}; rows_per_s = \
         etl_sink_records_total delta / window; limiter={limiter} \
         (pauses={pauses}, paused_edge={paused_edge}, pend_max={pend_max}); \
         budget_mb={max_inflight_mb} derived_mb={derived_mb}; \
         low-entropy synthetic body; window_s={window:.1}; \
         produced_delta={produced_delta}; final_count={final_count}; \
         ch_version={}; exit={:?}",
        stats.version,
        exit.state,
        pend_max = pend0.max(pend1),
    ))
    .emit();
}

fn main() {
    etl_core::telemetry::init(etl_core::telemetry::LogFormat::Pretty, "info");
    if env_u64("RUN_ONE", 0) != 0 {
        run_one();
        return;
    }

    // Create/verify the local server once in the parent so every child reuses
    // the same warm, correct-version container; children must not re-FRESH it.
    if std::env::var("CLICKHOUSE_URL").is_err() {
        let _ = docker::ensure_clickhouse();
    }

    let list = env_str("THREADS_LIST", "1,2,4");
    for threads in list.split(',').filter_map(|t| t.trim().parse::<u64>().ok()) {
        eprintln!("── ch_sink_saturation THREADS={threads} ──");
        let status = std::process::Command::new(std::env::current_exe().expect("exe"))
            .env("RUN_ONE", "1")
            .env("THREADS", threads.to_string())
            .env_remove("FRESH")
            .status()
            .expect("child run");
        assert!(status.success(), "child run failed for THREADS={threads}");
    }
}
