//! Multi-table split vs. `Null`+MV: does moving the per-type fan-out off the
//! ClickHouse server and onto the ETL tier lower server CPU at parity
//! throughput and better part shape? An in-process generator produces a
//! skewed mix of typed rows; three arms consume the *same* stream:
//!
//! - `single_null` — one `Null` table, single sink: the throughput-ceiling
//!   control (it runs the plain single-sink terminal, not a split).
//! - `null_mv` — one `Null` landing table + one `MATERIALIZED VIEW` per type
//!   into per-type MergeTree tables. ClickHouse does the fan-out.
//! - `split` — the ETL routes each row to its type's MergeTree table directly
//!   via the multi-sink split terminal. No views run.
//!
//! `null_mv` and `split` write the same rows to the same per-type tables; the
//! difference is *who* fans out. `chstats::capture_multi` reads the whole-server
//! INSERT CPU (which folds in MV execution for `null_mv`), the materialized
//! views' own CPU, and the part/merge shape across every target table. The gate
//! metric is `ch_server_cpu_us_per_row`.
//!
//! Bottleneck discipline: each arm records a `limiter` verdict; the MV-vs-split
//! comparison is only valid when both CH-writing arms are `sink`-bound (see the
//! note). Constrain `CLICKHOUSE_CPUS` and give the ETL enough of an 18-core host
//! to saturate ClickHouse.
//!
//! Usage:
//!   multi_table_split             # matrix over ARMS x TYPES_LIST x SKEWS x LINGERS
//!   RUN_ONE=1 ARM=split multi_table_split   # a single measurement
//!
//! Env: ARM (single_null|null_mv|split) TYPES (8) SKEW (skewed|uniform)
//! THREADS (6) SHARDS (2) IO_THREADS (4) INFLIGHT_PER_SHARD (2)
//! BATCH_MAX_ROWS (262144) BATCH_MAX_MB (128) LINGER_MS (1000) QUEUE_CAP (256)
//! MAX_INFLIGHT_MB (derived) MAX_PENDING_BATCHES (8192) PAYLOAD (256)
//! DURATION_S (20) WARMUP_S (5) CLICKHOUSE_CPUS (8) RESULTS (append JSONL path)
//! ARMS (single_null,null_mv,split) TYPES_LIST (8) SKEWS (skewed) LINGERS (1000)
#![allow(clippy::print_stdout, clippy::print_stderr)]

use benchmarks::report::{Metric, Report};
use benchmarks::synthetic::SyntheticSource;
use benchmarks::{chstats, docker, env_str, env_u64, prom};
use etl_clickhouse::NativeEncoder;
use etl_core::backpressure::InflightBudget;
use etl_core::config::{ComponentConfig, PipelineConfig};
use etl_core::deser::{BytesPassthrough, Owned};
use etl_core::error::ErrorPolicy;
use etl_core::metrics::{ComponentLabels, E2eBasis, MetricsHandle, SinkShardMetrics};
use etl_core::ops::{ChunkConfig, RunnableChain, SinkCtx, chain_owned};
use etl_core::pipeline::{DrainReport, PipelineRuntime, RuntimeOptions, SinkDrainFn, SinkRuntime};
use etl_core::record::RecordMeta;
use etl_core::sink::{ShardRouter, SinkPool, shard_queues};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// The per-thread chain factory the runtime drives.
type ChainFactory = Box<dyn FnMut(usize) -> Box<dyn RunnableChain> + Send>;

/// One row. Field/column order is the wire contract: `[id, kind, body]`.
#[derive(serde::Serialize)]
struct BenchRow {
    id: u64,
    kind: u16,
    body: String,
}

/// Balanced shard routing within one table (the generator is keyless).
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

/// Build the 4096-entry arena-position → kind table for the chosen skew, so the
/// per-record `kind` is an O(1) lookup. `uniform` spreads types evenly;
/// `skewed` is Zipf-ish (weight ∝ 1/(k+1)), so a couple of hot types dominate
/// and the tail trickles — the realistic shape the split targets.
fn kind_lut(types: u16, skewed: bool) -> Arc<Vec<u16>> {
    let mut lut = Vec::with_capacity(4096);
    if !skewed {
        for j in 0..4096u64 {
            lut.push((j % u64::from(types)) as u16);
        }
    } else {
        let weights: Vec<f64> = (0..types).map(|k| 1.0 / (f64::from(k) + 1.0)).collect();
        let total: f64 = weights.iter().sum();
        let mut cum = Vec::with_capacity(types as usize);
        let mut acc = 0.0;
        for w in &weights {
            acc += w / total;
            cum.push(acc);
        }
        for j in 0..4096u64 {
            let pos = j as f64 / 4096.0;
            let k = cum
                .iter()
                .position(|&c| pos < c)
                .unwrap_or(types as usize - 1);
            lut.push(k as u16);
        }
    }
    Arc::new(lut)
}

/// Turn a generated payload into a row: `id` from the arena tag (first 8 LE
/// bytes), `kind` from the skew LUT, and a same-length low-entropy ASCII body
/// (xorshift seeded from `id | 1`) so rows are neither identical nor constant.
fn to_row(mut payload: Vec<u8>, lut: &[u16]) -> BenchRow {
    let mut idb = [0u8; 8];
    let n = idb.len().min(payload.len());
    idb[..n].copy_from_slice(&payload[..n]);
    let id = u64::from_le_bytes(idb);
    let kind = lut[(id % 4096) as usize];

    let mut x = id | 1;
    for b in &mut payload {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *b = b'a' + (x % 26) as u8;
    }
    BenchRow {
        id,
        kind,
        body: String::from_utf8(payload).expect("ascii body"),
    }
}

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

/// Compose per-table drain hooks into one (concurrent, summed) — the by-hand
/// equivalent of the pipeline builder's multi-sink drain.
fn compose_drains(drains: Vec<SinkDrainFn>) -> SinkDrainFn {
    Box::new(move |deadline| {
        Box::pin(async move {
            let mut set = tokio::task::JoinSet::new();
            for drain in drains {
                set.spawn(drain(deadline));
            }
            let mut total = DrainReport::default();
            while let Some(res) = set.join_next().await {
                match res {
                    Ok(report) => {
                        total.flushed += report.flushed;
                        total.abandoned += report.abandoned;
                    }
                    Err(e) => eprintln!("sink drain task panicked: {e}"),
                }
            }
            total
        })
    })
}

/// Build a ClickHouse sink for one table plus its Native schema and a spawned
/// pool; returns the sender queues and drain hook for the runtime.
struct TablePool {
    queues: etl_core::sink::ShardQueues,
    drain: SinkDrainFn,
    schema: Arc<etl_clickhouse::NativeSchema>,
}

#[allow(clippy::too_many_arguments)]
fn spawn_table_pool(
    conn: &Conn,
    io: &tokio::runtime::Runtime,
    budget: &Arc<InflightBudget>,
    table: &str,
    shards: usize,
    queue_cap: usize,
    inflight: u64,
    batch_max_rows: u64,
    batch_max_mb: u64,
    linger_ms: u64,
) -> TablePool {
    let shard_lines: String = (0..shards)
        .map(|_| format!("    - replicas: [{:?}]\n", conn.url))
        .collect();
    let sink_yaml = format!(
        "clickhouse:
  table: {table}
  columns: [id, kind, body]
  format: native
  compression: lz4
  user: {user}
  password: {password:?}
  inflight: {{ max_per_shard: {inflight} }}
  batch: {{ max_rows: {batch_max_rows}, max_bytes: {batch_max_mb}MiB, linger: {linger_ms}ms }}
  shards:
{shard_lines}",
        user = conn.user,
        password = conn.password,
    );
    let section: ComponentConfig = serde_yaml::from_str(&sink_yaml).expect("sink section");
    let ch = etl_clickhouse::from_component_config(&section).expect("clickhouse sink");
    assert_eq!(ch.endpoints.len(), shards, "one endpoint per shard");
    let schema = io.block_on(ch.native_schema()).expect("native schema");
    let (queues, receivers) = shard_queues(shards, queue_cap);
    let labels = ComponentLabels::new("mts", table.to_owned(), "clickhouse");
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
        Arc::clone(budget),
        metrics,
        "mts",
        io.handle(),
    );
    TablePool {
        queues,
        drain: Box::new(move |deadline| Box::pin(async move { pool.drain(deadline).await })),
        schema,
    }
}

fn run_one() {
    let arm = env_str("ARM", "split");
    let types = env_u64("TYPES", 8) as u16;
    let skewed = env_str("SKEW", "skewed") != "uniform";
    let threads = env_u64("THREADS", 6) as usize;
    let shards = env_u64("SHARDS", 2) as usize;
    let io_threads = env_u64("IO_THREADS", 4) as usize;
    let inflight = env_u64("INFLIGHT_PER_SHARD", 2);
    let batch_max_rows = env_u64("BATCH_MAX_ROWS", 262_144);
    let batch_max_mb = env_u64("BATCH_MAX_MB", 128);
    let linger_ms = env_u64("LINGER_MS", 1000);
    let queue_cap = env_u64("QUEUE_CAP", 256) as usize;
    let n_tables = if arm == "single_null" {
        1
    } else {
        types as usize
    };
    // Budget sizing rule: cover the fully-formed in-flight batches across every
    // table's shards plus queued chunks, over low_ratio 0.5. With N tables the
    // in-flight footprint scales, so derive from the table count.
    const LOW_RATIO: f64 = 0.5;
    const CHUNK_MIB: f64 = 64.0 / 1024.0;
    let total_shards = (n_tables * shards) as f64;
    let derived_mb = (2.0
        * (total_shards * inflight as f64 * batch_max_mb as f64
            + total_shards * queue_cap as f64 * CHUNK_MIB)
        / LOW_RATIO)
        .ceil() as u64;
    let max_inflight_mb = env_u64("MAX_INFLIGHT_MB", derived_mb.max(1024));
    let payload = env_u64("PAYLOAD", 256) as usize;
    let duration = Duration::from_secs(env_u64("DURATION_S", 20));
    let warmup = Duration::from_secs(env_u64("WARMUP_S", 5));
    let max_pending_batches = env_u64("MAX_PENDING_BATCHES", 8192);

    let conn = resolve_conn();
    let sql = |q: &str| {
        docker::clickhouse_sql(&conn.host, conn.port, &conn.user, &conn.password, q)
            .expect("ch sql")
    };

    // ── Tables + (for null_mv) materialized views ────────────────────────
    // Drop every mts_* object, not just 0..types: a previous run with more
    // types leaves populated MergeTree tables whose background merges steal
    // capped server CPU from this run, and leftover views would stay
    // attached to the recreated Null landing table and silently execute
    // during the control arms.
    let target_tables: Vec<String> = (0..types).map(|k| format!("mts_t{k}")).collect();
    let leftovers = docker::clickhouse_sql(
        &conn.host,
        conn.port,
        &conn.user,
        &conn.password,
        "SELECT name FROM system.tables \
         WHERE database = currentDatabase() AND name LIKE 'mts%'",
    )
    .expect("ch sql");
    for table in leftovers.lines().map(str::trim).filter(|t| !t.is_empty()) {
        sql(&format!("DROP TABLE IF EXISTS {table}"));
    }
    match arm.as_str() {
        "split" => {
            for t in &target_tables {
                sql(&format!(
                    "CREATE TABLE {t} (id UInt64, kind UInt16, body String) \
                     ENGINE = MergeTree ORDER BY id \
                     SETTINGS non_replicated_deduplication_window = 100"
                ));
            }
        }
        "null_mv" => {
            sql("CREATE TABLE mts_null (id UInt64, kind UInt16, body String) ENGINE = Null");
            for (k, t) in target_tables.iter().enumerate() {
                sql(&format!(
                    "CREATE TABLE {t} (id UInt64, kind UInt16, body String) \
                     ENGINE = MergeTree ORDER BY id \
                     SETTINGS non_replicated_deduplication_window = 100"
                ));
                sql(&format!(
                    "CREATE MATERIALIZED VIEW mts_view_{k} TO {t} AS \
                     SELECT id, kind, body FROM mts_null WHERE kind = {k}"
                ));
            }
        }
        _ => {
            // single_null: the ceiling control.
            sql("CREATE TABLE mts_null (id UInt64, kind UInt16, body String) ENGINE = Null");
        }
    }

    // ── Metrics + I/O runtime ────────────────────────────────────────────
    let metrics_handle = install_metrics();
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(io_threads)
        .enable_all()
        .build()
        .expect("io runtime");

    let config = PipelineConfig::from_str(&format!(
        "pipeline: {{ name: mts, threads: {threads} }}\n\
         checkpoint: {{ interval: 1s, max_pending_batches: {max_pending_batches} }}\n\
         backpressure: {{ max_inflight_bytes: {max_inflight_mb}MiB }}\n\
         metrics: {{ exporter: none }}\n\
         source: {{ synthetic: {{}} }}\n\
         sink: {{ synthetic: {{}} }}\n"
    ))
    .expect("config");

    let budget = Arc::new(InflightBudget::new());
    let lut = kind_lut(types, skewed);

    // ── Source ───────────────────────────────────────────────────────────
    let produced = Arc::new(AtomicU64::new(0));
    let source = SyntheticSource::new(threads, payload, Arc::clone(&produced));
    let commits = source.commits();

    // ── Assembly: single sink (null arms) or N-way split ─────────────────
    let (sink, chains): (SinkRuntime, ChainFactory) = if arm == "split" {
        let pools: Vec<TablePool> = target_tables
            .iter()
            .map(|t| {
                spawn_table_pool(
                    &conn,
                    &io,
                    &budget,
                    t,
                    shards,
                    queue_cap,
                    inflight,
                    batch_max_rows,
                    batch_max_mb,
                    linger_ms,
                )
            })
            .collect();
        let all_queues: Vec<_> = pools.iter().map(|p| p.queues.clone()).collect();
        let schemas: Vec<_> = pools.iter().map(|p| Arc::clone(&p.schema)).collect();
        let sink = SinkRuntime {
            queues: all_queues.clone(),
            drain: compose_drains(pools.into_iter().map(|p| p.drain).collect()),
            probe: None,
        };
        let chain_budget = Arc::clone(&budget);
        let lut = Arc::clone(&lut);
        let chains = move |_thread: usize| -> Box<dyn RunnableChain> {
            let lut_map = Arc::clone(&lut);
            let mut split = chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .with_metrics("mts", "main")
                .map(move |p: Vec<u8>| to_row(p, &lut_map))
                .split(ErrorPolicy::Fail);
            let handles: Vec<_> = (0..types as usize)
                .map(|k| {
                    let enc = NativeEncoder::<Owned<BenchRow>>::new(Arc::clone(&schemas[k]));
                    split.add::<Owned<BenchRow>, _, _>(
                        enc,
                        RoundRobinRouter::default(),
                        SinkCtx::new(
                            format!("t{k}"),
                            all_queues[k].clone(),
                            Arc::clone(&chain_budget),
                        ),
                    )
                })
                .collect();
            split
                .route(move |row: BenchRow, out| {
                    out.emit(handles[row.kind as usize], row);
                })
                .build()
        };
        (sink, Box::new(chains))
    } else {
        // single_null / null_mv: one sink into mts_null.
        let pool = spawn_table_pool(
            &conn,
            &io,
            &budget,
            "mts_null",
            shards,
            queue_cap,
            inflight,
            batch_max_rows,
            batch_max_mb,
            linger_ms,
        );
        let schema = Arc::clone(&pool.schema);
        let queues = pool.queues.clone();
        let sink = SinkRuntime {
            queues: vec![pool.queues.clone()],
            drain: pool.drain,
            probe: None,
        };
        let chain_budget = Arc::clone(&budget);
        let lut = Arc::clone(&lut);
        let chains = move |_thread: usize| -> Box<dyn RunnableChain> {
            let lut_map = Arc::clone(&lut);
            let enc = NativeEncoder::<Owned<BenchRow>>::new(Arc::clone(&schema));
            chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .with_metrics("mts", "main")
                .map(move |p: Vec<u8>| to_row(p, &lut_map))
                .sink(
                    enc,
                    RoundRobinRouter::default(),
                    ChunkConfig::default(),
                    queues.clone(),
                    Arc::clone(&chain_budget),
                )
                .build()
        };
        (sink, Box::new(chains))
    };

    let runtime = PipelineRuntime::new(config, source, chains, sink, Arc::clone(&budget))
        .with_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        });
    let shutdown = runtime.shutdown_handle();
    let pipeline = std::thread::spawn(move || runtime.run());

    // ── Warm up, then measure a steady-state window ──────────────────────
    std::thread::sleep(warmup);
    let since = chstats::now(&conn.host, conn.port, &conn.user, &conn.password);
    let text0 = metrics_handle.render();
    let sink0 = prom::value(&text0, "etl_sink_records_total", "").unwrap_or(0.0);
    let pauses0 = prom::value(&text0, "etl_backpressure_pause_events_total", "").unwrap_or(0.0);
    let paused0 = prom::value(&text0, "etl_backpressure_paused", "").unwrap_or(0.0);
    let pend0 = prom::value(&text0, "etl_checkpoint_pending_batches", "").unwrap_or(0.0);
    let t0 = Instant::now();
    std::thread::sleep(duration);
    let text1 = metrics_handle.render();
    let window = t0.elapsed().as_secs_f64();
    let sink1 = prom::value(&text1, "etl_sink_records_total", "").unwrap_or(0.0);
    let pauses1 = prom::value(&text1, "etl_backpressure_pause_events_total", "").unwrap_or(0.0);
    let paused1 = prom::value(&text1, "etl_backpressure_paused", "").unwrap_or(0.0);
    let pend1 = prom::value(&text1, "etl_checkpoint_pending_batches", "").unwrap_or(0.0);

    let pauses = (pauses1 - pauses0).max(0.0);
    let paused_edge = paused0 > 0.0 || paused1 > 0.0;
    let pend_high =
        pend0 > max_pending_batches as f64 / 2.0 || pend1 > max_pending_batches as f64 / 2.0;
    let limiter = if max_inflight_mb < derived_mb {
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

    // ── ClickHouse-side accounting (post-drain), across target tables ────
    let stats = chstats::capture_multi(
        &conn.host,
        conn.port,
        &conn.user,
        &conn.password,
        &target_tables,
        &since,
    );

    let sink_records = sink1 - sink0;
    let rows_per_s = if window > 0.0 {
        sink_records / window
    } else {
        0.0
    };

    let mut rep = Report::measurement("multi_table_split")
        .variant("arm", arm.clone())
        .variant("types", u64::from(types))
        .variant("skew", if skewed { "skewed" } else { "uniform" })
        .variant("threads", threads as u64)
        .variant("shards", shards as u64)
        .variant("io_threads", io_threads as u64)
        .variant("inflight_per_shard", inflight)
        .variant("batch_max_rows", batch_max_rows)
        .variant("batch_max_mb", batch_max_mb)
        .variant("linger_ms", linger_ms)
        .variant("queue_cap", queue_cap as u64)
        .variant("max_inflight_mb", max_inflight_mb)
        .variant("payload_bytes", payload as u64)
        .metric("rows_per_s", Metric::maximize(rows_per_s, "rows/s"))
        .metric("sink_records", Metric::maximize(sink_records, "rows"))
        .metric(
            "produced_total",
            Metric::maximize(produced.load(Ordering::Relaxed) as f64, "records"),
        )
        .metric(
            "commits",
            Metric::maximize(commits.load(Ordering::Relaxed) as f64, "commits"),
        );

    // CH-side metrics are meaningful only for the MergeTree-writing arms.
    if arm != "single_null" {
        rep = rep
            .metric(
                "ch_server_cpu_us_per_row",
                Metric::minimize(stats.server_cpu_us_per_row(), "us"),
            )
            .metric(
                "ch_target_rows",
                Metric::maximize(stats.target_rows, "rows"),
            )
            .metric("ch_mv_cpu_us", Metric::minimize(stats.mv_cpu_us, "us"))
            .metric(
                "ch_avg_part_rows",
                Metric::maximize(stats.avg_part_rows, "rows"),
            )
            .metric(
                "ch_avg_part_bytes",
                Metric::maximize(stats.avg_part_bytes, "bytes"),
            )
            .metric(
                "ch_parts_created",
                Metric::minimize(stats.parts_created, "parts"),
            )
            .metric("ch_merges", Metric::minimize(stats.merges, "merges"))
            .metric(
                "ch_merged_rows",
                Metric::minimize(stats.merged_rows, "rows"),
            );
    }
    if std::env::var("CLICKHOUSE_URL").is_err() {
        // Default mirrors docker.rs's --cpus default so an env-less run
        // records the cap the container was actually created with.
        rep = rep.variant("clickhouse_cpus", env_u64("CLICKHOUSE_CPUS", 8));
    }
    rep.note(format!(
        "arm={arm}; rows_per_s = etl_sink_records_total delta / window; \
         limiter={limiter} (pauses={pauses}, paused_edge={paused_edge}); \
         server_insert_cpu_us={:.0} target_rows={:.0} mv_cpu_us={:.0} mv_execs={:.0}; \
         budget_mb={max_inflight_mb} derived_mb={derived_mb}; window_s={window:.1}; \
         ch_version={}; exit={:?}",
        stats.server_insert_cpu_us,
        stats.target_rows,
        stats.mv_cpu_us,
        stats.mv_executions,
        stats.version,
        exit.state,
    ))
    .emit();
}

fn main() {
    etl_core::telemetry::init(etl_core::telemetry::LogFormat::Pretty, "info");
    if env_u64("RUN_ONE", 0) != 0 {
        run_one();
        return;
    }

    if std::env::var("CLICKHOUSE_URL").is_err() {
        let _ = docker::ensure_clickhouse();
    }

    let arms = env_str("ARMS", "single_null,null_mv,split");
    let types_list = env_str("TYPES_LIST", "8");
    let skews = env_str("SKEWS", "skewed");
    let lingers = env_str("LINGERS", "1000");
    for arm in arms.split(',').map(str::trim) {
        for types in types_list.split(',').map(str::trim) {
            for skew in skews.split(',').map(str::trim) {
                for linger in lingers.split(',').map(str::trim) {
                    eprintln!(
                        "── multi_table_split ARM={arm} TYPES={types} SKEW={skew} LINGER={linger} ──"
                    );
                    let status = std::process::Command::new(std::env::current_exe().expect("exe"))
                        .env("RUN_ONE", "1")
                        .env("ARM", arm)
                        .env("TYPES", types)
                        .env("SKEW", skew)
                        .env("LINGER_MS", linger)
                        .env_remove("FRESH")
                        .status()
                        .expect("child run");
                    assert!(status.success(), "child run failed: ARM={arm}");
                }
            }
        }
    }
}
