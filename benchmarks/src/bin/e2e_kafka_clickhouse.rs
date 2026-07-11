//! Full-pipeline benchmark: Kafka → chain → sharded ClickHouse writes,
//! measured through the pipeline's own metrics plus a ClickHouse row-count
//! cross-check.
//!
//! Two payload shapes, selected by `DESER`:
//! - `DESER=none` (default): the raw baseline — `id,body` text payloads
//!   passed through byte-for-byte into a `bench_events` RowBinary table.
//! - `DESER=apache_owned|fast_owned|fast_borrowed`: an Avro *sensor batch*
//!   per message (bare datum, `raw` framing), decoded by the chosen backend,
//!   exploded with `flat_map` into one `sensor_events` row per event, and
//!   inserted as RowBinary or Native (`FORMAT`). This is the at-scale twin
//!   of the in-process `avro_pipeline` rig — the same A/B, now with a real
//!   broker and server in the loop (rig D).
//!
//! Profiles:
//! - **Local** (default): starts/reuses `etl-bench-kafka` and
//!   `etl-bench-clickhouse` containers, generates load itself.
//! - **External**: set KAFKA_BROKERS and CLICKHOUSE_URL (http://host:port)
//!   [+ CLICKHOUSE_USER/CLICKHOUSE_PASSWORD] — pure env config, runnable
//!   in Kubernetes against real clusters.
//!
//! Env: DURATION_S (60) RATE (100000 rec/s) PARTITIONS (4) THREADS (2)
//! PAYLOAD (64, raw mode) EVENTS (50, avro mode) TOPIC (bench-e2e-<pid>)
//! DESER (none) FORMAT (rowbinary) RESULTS (JSONL path)
//!
//! TOPIC defaults to a fresh per-run name (`bench-e2e-<pid>`), and the consumer
//! group is `bench-e2e-<pid>` too, so each run starts from an empty topic and
//! abandons any tail it did not drain. Pinning TOPIC to a fixed name across
//! runs makes a rerun re-consume the prior topic from offset 0 — a new per-pid
//! group with `auto.offset.reset=earliest` — into the freshly recreated table.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use benchmarks::avro_batch::{
    self, BatchFam, EventFam, SensorBatchOwned, SensorEventOwned, explode_borrowed, explode_owned,
    keep_borrowed, keep_owned,
};
use benchmarks::report::{Metric, Report};
use benchmarks::{docker, ensure_topic, env_str, env_u64, prom};
use etl_avro::{AvroDeserializerBuilder, AvroMode, AvroSettings, SchemaSource};
use etl_clickhouse::{ClickHouseEncoder, ClickHouseSink, NativeEncoder};
use etl_core::backpressure::InflightBudget;
use etl_core::config::{ComponentConfig, PipelineConfig};
use etl_core::deser::{BytesPassthrough, Owned};
use etl_core::metrics::{ComponentLabels, E2eBasis, MetricsHandle, SinkShardMetrics};
use etl_core::ops::{ChunkConfig, RunnableChain, chain, chain_owned};
use etl_core::pipeline::{PipelineRuntime, RuntimeOptions, SinkRuntime};
use etl_core::sink::{KeyHashRouter, ShardQueues, SinkPool, shard_queues};
use etl_kafka::{KafkaSource, KafkaSourceConfig};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// The row the raw (`DESER=none`) path writes. Field order == `columns`
/// order — the RowBinary wire contract.
#[derive(serde::Serialize)]
struct BenchRow {
    id: u64,
    body: String,
}

fn parse_row(payload: Vec<u8>) -> BenchRow {
    let text = String::from_utf8_lossy(&payload);
    match text.split_once(',') {
        Some((id, body)) => BenchRow {
            id: id.parse().unwrap_or(0),
            body: body.to_owned(),
        },
        None => BenchRow {
            id: 0,
            body: text.into_owned(),
        },
    }
}

/// ClickHouse connection resolved from the environment or Docker.
struct Conn {
    brokers: String,
    url: String,
    host: String,
    port: u16,
    user: String,
    password: String,
}

/// The knobs echoed into the emitted record.
struct Meta {
    deser: String,
    format: String,
    threads: usize,
    partitions: i32,
    rate: u64,
    duration: Duration,
    /// ClickHouse rows produced per Kafka message (1 raw; `EVENTS` avro).
    records_per_input: u64,
    /// Raw-mode payload size in bytes, echoed for reconciliation; unused in
    /// avro mode, where the payload dimension is `events` (== records_per_input).
    payload_bytes: u64,
    /// `SELECT version()` of the target server, recorded in the note.
    ch_version: String,
}

fn producer(brokers: &str) -> BaseProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("linger.ms", "5")
        .set("batch.size", "1048576")
        .create()
        .expect("producer")
}

/// Rate-controlled raw-text producer, running until `stop`.
fn produce_load(
    brokers: &str,
    topic: &str,
    partitions: i32,
    rate: u64,
    payload_size: usize,
    stop: &AtomicBool,
    produced: &AtomicU64,
) {
    let producer = producer(brokers);
    let body = "x".repeat(payload_size.saturating_sub(16).max(1));
    let start = Instant::now();
    let mut sent = 0u64;
    while !stop.load(Ordering::Relaxed) {
        if rate > 0 {
            let allowed = (start.elapsed().as_secs_f64() * rate as f64) as u64 + 1;
            if sent >= allowed {
                producer.poll(Duration::from_millis(1));
                continue;
            }
        }
        let payload = format!("{sent},{body}");
        let key = (sent % 1024).to_string();
        match producer.send(
            BaseRecord::to(topic)
                .partition((sent % partitions as u64) as i32)
                .key(&key)
                .payload(payload.as_bytes()),
        ) {
            Ok(()) => {
                sent += 1;
                produced.store(sent, Ordering::Relaxed);
            }
            Err(_) => {
                producer.poll(Duration::from_millis(5));
            }
        }
        if sent.is_multiple_of(4096) {
            producer.poll(Duration::ZERO);
        }
    }
    producer.flush(Duration::from_secs(30)).expect("flush");
}

/// Rate-controlled Avro sensor-batch producer (bare datum, `raw` framing).
fn produce_avro(
    brokers: &str,
    topic: &str,
    partitions: i32,
    rate: u64,
    events: u64,
    stop: &AtomicBool,
    produced: &AtomicU64,
) {
    let producer = producer(brokers);
    let datum = avro_batch::encode_batch(events);
    let start = Instant::now();
    let mut sent = 0u64;
    while !stop.load(Ordering::Relaxed) {
        if rate > 0 {
            let allowed = (start.elapsed().as_secs_f64() * rate as f64) as u64 + 1;
            if sent >= allowed {
                producer.poll(Duration::from_millis(1));
                continue;
            }
        }
        let key = (sent % 1024).to_string();
        match producer.send(
            BaseRecord::to(topic)
                .partition((sent % partitions as u64) as i32)
                .key(&key)
                .payload(&datum),
        ) {
            Ok(()) => {
                sent += 1;
                produced.store(sent, Ordering::Relaxed);
            }
            Err(_) => {
                producer.poll(Duration::from_millis(5));
            }
        }
        if sent.is_multiple_of(4096) {
            producer.poll(Duration::ZERO);
        }
    }
    producer.flush(Duration::from_secs(30)).expect("flush");
}

/// A raw-mode Avro builder over the inline sensor-batch schema.
fn avro_builder(io: &tokio::runtime::Handle) -> AvroDeserializerBuilder {
    let settings = AvroSettings {
        mode: AvroMode::Raw,
        schema: Some(SchemaSource::inline(avro_batch::BATCH_SCHEMA)),
        ..AvroSettings::default()
    };
    AvroDeserializerBuilder::from_settings(&settings, io).expect("avro builder")
}

fn kafka_source(brokers: &str, topic: &str) -> KafkaSource {
    KafkaSource::new(KafkaSourceConfig {
        brokers: brokers.to_owned(),
        topic: topic.to_owned(),
        group_id: format!("bench-e2e-{}", std::process::id()),
        commit_interval: Duration::from_secs(1),
        startup_timeout: Duration::from_secs(30),
        statistics_interval: Duration::from_secs(5),
        rdkafka: BTreeMap::from([("auto.offset.reset".to_owned(), "earliest".to_owned())]),
    })
}

fn pipeline_config(threads: usize) -> PipelineConfig {
    PipelineConfig::from_str(&format!(
        "pipeline: {{ name: bench-e2e, threads: {threads} }}\n\
         checkpoint: {{ interval: 1s }}\n\
         metrics: {{ exporter: none }}\n\
         source: {{ kafka: {{}} }}\n\
         sink: {{ clickhouse: {{}} }}\n"
    ))
    .expect("config")
}

fn install_metrics() -> MetricsHandle {
    etl_core::metrics::install(&etl_core::metrics::MetricsSettings {
        exporter: etl_core::metrics::Exporter::Prometheus,
        ..Default::default()
    })
    .expect("install metrics recorder")
}

/// Spin the pipeline, run for the window, wait for ClickHouse to drain, and
/// emit one v1 record.
#[allow(clippy::too_many_arguments)]
fn spawn_and_measure<MK>(
    config: PipelineConfig,
    source: KafkaSource,
    ch: ClickHouseSink,
    metrics_handle: MetricsHandle,
    io: tokio::runtime::Runtime,
    make_chain: MK,
    meta: &Meta,
    stop: Arc<AtomicBool>,
    produced: Arc<AtomicU64>,
    loadgen: std::thread::JoinHandle<()>,
    count_rows: impl Fn() -> u64,
) where
    MK: FnMut(ShardQueues, Arc<InflightBudget>) -> Box<dyn RunnableChain> + Send + 'static,
{
    let mut make_chain = make_chain;
    let shards = ch.endpoints.len();
    let (queues, receivers) = shard_queues(shards, 8);
    let budget = Arc::new(InflightBudget::new());
    let labels = ComponentLabels::new("bench-e2e", "sink", "clickhouse");
    let metrics = (0..shards)
        .map(|s| {
            SinkShardMetrics::new(
                &labels,
                u32::try_from(s).expect("shard"),
                &[format!("replica-{s}-0")],
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
        "bench-e2e",
        io.handle(),
    );
    let sink = SinkRuntime {
        queues: queues.clone(),
        drain: Box::new(move |deadline| Box::pin(async move { pool.drain(deadline).await })),
        probe: None,
    };

    let chain_queues = queues;
    let chain_budget = Arc::clone(&budget);
    let chains = move |_thread: usize| make_chain(chain_queues.clone(), Arc::clone(&chain_budget));

    let runtime =
        PipelineRuntime::new(config, source, chains, sink, budget).with_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        });
    let shutdown = runtime.shutdown_handle();
    let pipeline = std::thread::spawn(move || runtime.run());

    // Start the throughput clock when rows actually begin landing, so the
    // numerator (rows) and denominator (elapsed) cover the same interval:
    // Kafka group-join and pipeline warmup are excluded, and the saturated
    // drain that follows the load window is included. Bounded so a stalled
    // pipeline fails loudly instead of hanging.
    let ready = Instant::now() + Duration::from_secs(60);
    while count_rows() == 0 {
        assert!(
            Instant::now() < ready,
            "no rows landed before the ready deadline"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let consume_start = Instant::now();

    std::thread::sleep(meta.duration);
    stop.store(true, Ordering::Relaxed);
    loadgen.join().expect("loadgen");
    let sent = produced.load(Ordering::Relaxed);
    let expected = sent * meta.records_per_input;

    // Let the pipeline drain the tail: wait until the ClickHouse count is
    // stable at or above the expected rows (or a bounded grace period passes).
    // Under RATE=0 the count never reaches `expected`, so grace bounds the
    // wait; under a rate limit the pipeline keeps up and the stable-count check
    // exits early. Either way the drain stays inside the measured interval.
    let mut last = count_rows();
    let grace = Instant::now() + Duration::from_secs(30);
    loop {
        std::thread::sleep(Duration::from_secs(2));
        let now = count_rows();
        if (now == last && now >= expected) || Instant::now() > grace {
            break;
        }
        last = now;
    }

    let metrics_text = metrics_handle.render();
    shutdown.trigger();
    let exit = pipeline.join().expect("pipeline").expect("run");
    let rows = count_rows();
    let elapsed = consume_start.elapsed().as_secs_f64();

    let mut rep = Report::measurement("e2e_kafka_clickhouse")
        .variant("deser", meta.deser.clone())
        .variant("format", meta.format.clone())
        .variant("threads", meta.threads as u64)
        .variant("partitions", meta.partitions)
        .variant("target_rate", meta.rate)
        // Load-window length is configuration, so it rides in the variant.
        // The producer count is a per-rep measured quantity (under RATE=0 it
        // measures the load generator, not the pipeline): it goes in the note,
        // not in `variant` (reps of one arm must share every variant key or
        // the site cannot aggregate them) and not in `metrics` (it has no
        // "better" direction).
        .variant("window_s", meta.duration.as_secs_f64());
    // Payload dimension: records_per_input is 1 (raw) or `events` (avro).
    // Recording it lets landed rows reconcile against produced messages.
    rep = if meta.deser == "none" {
        rep.variant("payload", meta.payload_bytes)
    } else {
        rep.variant("events", meta.records_per_input)
    };
    rep = rep
        .metric("rows_in_clickhouse", Metric::maximize(rows as f64, "rows"))
        .metric(
            "rows_per_s",
            Metric::maximize(rows as f64 / elapsed, "rows/s"),
        );
    if let Some(v) = prom::histogram_quantile(&metrics_text, "etl_e2e_latency_seconds", 0.5) {
        rep = rep.metric("e2e_p50_s", Metric::minimize(v, "s"));
    }
    if let Some(v) = prom::histogram_quantile(&metrics_text, "etl_e2e_latency_seconds", 0.99) {
        rep = rep.metric("e2e_p99_s", Metric::minimize(v, "s"));
    }
    if let Some(v) =
        prom::histogram_quantile(&metrics_text, "etl_sink_flush_duration_seconds", 0.99)
    {
        rep = rep.metric("sink_flush_p99", Metric::minimize(v, "s"));
    }
    if let Some(v) = prom::value(&metrics_text, "etl_backpressure_pause_events_total", "") {
        rep = rep.metric("backpressure_pauses", Metric::minimize(v, "events"));
    }
    let measure = "rows_per_s = rows landed / elapsed from first landed row to \
                   final count (saturated throughout under RATE=0)";
    let ch_version = &meta.ch_version;
    let note = if rows >= expected {
        format!(
            "{measure}; complete; ch_version={ch_version}; exit={:?}",
            exit.state
        )
    } else {
        format!(
            "{measure}; produced {sent} messages, landed {rows}/{expected} \
             rows, tail abandoned (fresh topic/group per run); \
             ch_version={ch_version}; exit={:?}",
            exit.state
        )
    };
    rep.note(note).emit();
}

/// The raw baseline: byte passthrough → `parse_row` → RowBinary.
fn run_raw(conn: &Conn, topic: &str, partitions: i32, threads: usize, meta: Meta, payload: usize) {
    let sql = |q: &str| {
        docker::clickhouse_sql(&conn.host, conn.port, &conn.user, &conn.password, q)
            .expect("clickhouse")
    };
    sql("DROP TABLE IF EXISTS bench_events");
    sql(
        "CREATE TABLE bench_events (id UInt64, body String) ENGINE = MergeTree ORDER BY id \
         SETTINGS non_replicated_deduplication_window = 100",
    );
    ensure_topic(&conn.brokers, topic, partitions);

    let stop = Arc::new(AtomicBool::new(false));
    let produced = Arc::new(AtomicU64::new(0));
    let loadgen = {
        let (brokers, topic) = (conn.brokers.clone(), topic.to_owned());
        let (stop, produced) = (Arc::clone(&stop), Arc::clone(&produced));
        std::thread::spawn(move || {
            produce_load(
                &brokers, &topic, partitions, meta.rate, payload, &stop, &produced,
            );
        })
    };

    let metrics_handle = install_metrics();
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("io runtime");
    let config = pipeline_config(threads);
    let source = kafka_source(&conn.brokers, topic);
    // async_insert: "0" pins the synchronous insert path so the recorded server
    // behaviour is stable across server versions (26.3 defaults it to 1).
    let sink_yaml = format!(
        "clickhouse:\n  table: bench_events\n  columns: [id, body]\n  \
         user: {}\n  password: {:?}\n  settings: {{ async_insert: \"0\" }}\n  \
         shards:\n    - replicas: [{:?}]\n  \
         batch: {{ linger: 500ms, max_rows: 262144 }}\n",
        conn.user, conn.password, conn.url
    );
    let section: ComponentConfig = serde_yaml::from_str(&sink_yaml).expect("sink section");
    let ch = etl_clickhouse::from_component_config(&section).expect("clickhouse sink");
    let enc = ClickHouseEncoder::<Owned<BenchRow>>::new();
    let count_rows = || {
        sql("SELECT count() FROM bench_events")
            .trim()
            .parse::<u64>()
            .unwrap_or(0)
    };
    spawn_and_measure(
        config,
        source,
        ch,
        metrics_handle,
        io,
        move |q, b| {
            chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .with_metrics("bench-e2e", "main")
                .map(parse_row)
                .sink(enc.clone(), KeyHashRouter, ChunkConfig::default(), q, b)
                .build()
        },
        &meta,
        stop,
        produced,
        loadgen,
        count_rows,
    );
}

/// The Avro path: sensor-batch decode → `flat_map` → filter → RowBinary or
/// Native. Closes rig D.
#[allow(clippy::too_many_arguments)]
fn run_avro(
    conn: &Conn,
    topic: &str,
    partitions: i32,
    threads: usize,
    meta: Meta,
    deser_kind: &str,
    format: &str,
    events: u64,
) {
    let sql = |q: &str| {
        docker::clickhouse_sql(&conn.host, conn.port, &conn.user, &conn.password, q)
            .expect("clickhouse")
    };
    let cols_ddl = avro_batch::EVENT_COLUMNS
        .iter()
        .map(|(n, t)| format!("{n} {t}"))
        .collect::<Vec<_>>()
        .join(", ");
    let col_names = avro_batch::EVENT_COLUMNS
        .iter()
        .map(|(n, _)| *n)
        .collect::<Vec<_>>()
        .join(", ");
    sql("DROP TABLE IF EXISTS sensor_events");
    sql(&format!(
        "CREATE TABLE sensor_events ({cols_ddl}) ENGINE = MergeTree ORDER BY (sensor, batch_ts_ms)"
    ));
    ensure_topic(&conn.brokers, topic, partitions);

    let stop = Arc::new(AtomicBool::new(false));
    let produced = Arc::new(AtomicU64::new(0));
    let loadgen = {
        let (brokers, topic) = (conn.brokers.clone(), topic.to_owned());
        let (stop, produced) = (Arc::clone(&stop), Arc::clone(&produced));
        std::thread::spawn(move || {
            produce_avro(
                &brokers, &topic, partitions, meta.rate, events, &stop, &produced,
            );
        })
    };

    let metrics_handle = install_metrics();
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("io runtime");
    let config = pipeline_config(threads);
    let source = kafka_source(&conn.brokers, topic);
    let ch_format = if format == "native" {
        "native"
    } else {
        "rowbinary"
    };
    // async_insert: "0" pins the synchronous insert path (26.3 defaults it to 1).
    let sink_yaml = format!(
        "clickhouse:\n  table: sensor_events\n  columns: [{col_names}]\n  \
         format: {ch_format}\n  user: {}\n  password: {:?}\n  \
         settings: {{ async_insert: \"0\" }}\n  shards:\n    \
         - replicas: [{:?}]\n  batch: {{ linger: 500ms, max_rows: 262144 }}\n",
        conn.user, conn.password, conn.url
    );
    let section: ComponentConfig = serde_yaml::from_str(&sink_yaml).expect("sink section");
    let ch = etl_clickhouse::from_component_config(&section).expect("clickhouse sink");
    let builder = avro_builder(io.handle());
    let count_rows = || {
        sql("SELECT count() FROM sensor_events")
            .trim()
            .parse::<u64>()
            .unwrap_or(0)
    };

    // Dispatch the (decode backend × wire format) matrix. Each arm bakes the
    // deserializer and encoder into the chain factory; only the arm chosen by
    // env is ever assembled at runtime.
    match (deser_kind, format) {
        ("fast_borrowed", "native") => {
            let d = builder.build_fast::<BatchFam>().expect("fast_borrowed");
            let e = NativeEncoder::<EventFam>::new(avro_batch::native_schema());
            spawn_and_measure(
                config,
                source,
                ch,
                metrics_handle,
                io,
                move |q, b| {
                    chain::<BatchFam, _>(d.clone())
                        .with_metrics("bench-e2e", "main")
                        .flat_map::<EventFam, _>(explode_borrowed)
                        .filter(keep_borrowed)
                        .sink(e.clone(), KeyHashRouter, ChunkConfig::default(), q, b)
                        .build()
                },
                &meta,
                stop,
                produced,
                loadgen,
                count_rows,
            );
        }
        ("fast_borrowed", _) => {
            let d = builder.build_fast::<BatchFam>().expect("fast_borrowed");
            let e = ClickHouseEncoder::<EventFam>::new();
            spawn_and_measure(
                config,
                source,
                ch,
                metrics_handle,
                io,
                move |q, b| {
                    chain::<BatchFam, _>(d.clone())
                        .with_metrics("bench-e2e", "main")
                        .flat_map::<EventFam, _>(explode_borrowed)
                        .filter(keep_borrowed)
                        .sink(e.clone(), KeyHashRouter, ChunkConfig::default(), q, b)
                        .build()
                },
                &meta,
                stop,
                produced,
                loadgen,
                count_rows,
            );
        }
        ("fast_owned", "native") => {
            let d = builder
                .build_serde_fast::<SensorBatchOwned>()
                .expect("fast_owned");
            run_owned_native(
                config,
                source,
                ch,
                metrics_handle,
                io,
                d,
                &meta,
                stop,
                produced,
                loadgen,
                count_rows,
            );
        }
        ("fast_owned", _) => {
            let d = builder
                .build_serde_fast::<SensorBatchOwned>()
                .expect("fast_owned");
            run_owned_rowbinary(
                config,
                source,
                ch,
                metrics_handle,
                io,
                d,
                &meta,
                stop,
                produced,
                loadgen,
                count_rows,
            );
        }
        ("apache_owned", "native") => {
            let d = builder
                .build_serde::<SensorBatchOwned>()
                .expect("apache builder");
            run_owned_native(
                config,
                source,
                ch,
                metrics_handle,
                io,
                d,
                &meta,
                stop,
                produced,
                loadgen,
                count_rows,
            );
        }
        ("apache_owned", _) => {
            let d = builder
                .build_serde::<SensorBatchOwned>()
                .expect("apache builder");
            run_owned_rowbinary(
                config,
                source,
                ch,
                metrics_handle,
                io,
                d,
                &meta,
                stop,
                produced,
                loadgen,
                count_rows,
            );
        }
        (other, _) => {
            eprintln!("unknown DESER={other}");
            std::process::exit(2);
        }
    }
}

/// Owned decode → Native (shared by both owned backends).
#[allow(clippy::too_many_arguments)]
fn run_owned_native<D>(
    config: PipelineConfig,
    source: KafkaSource,
    ch: ClickHouseSink,
    metrics_handle: MetricsHandle,
    io: tokio::runtime::Runtime,
    deser: D,
    meta: &Meta,
    stop: Arc<AtomicBool>,
    produced: Arc<AtomicU64>,
    loadgen: std::thread::JoinHandle<()>,
    count_rows: impl Fn() -> u64,
) where
    D: etl_core::deser::Deserializer<Owned<SensorBatchOwned>> + Clone + Send + 'static,
{
    let e = NativeEncoder::<Owned<SensorEventOwned>>::new(avro_batch::native_schema());
    spawn_and_measure(
        config,
        source,
        ch,
        metrics_handle,
        io,
        move |q, b| {
            chain::<Owned<SensorBatchOwned>, _>(deser.clone())
                .with_metrics("bench-e2e", "main")
                .flat_map::<Owned<SensorEventOwned>, _>(explode_owned)
                .filter(keep_owned)
                .sink(e.clone(), KeyHashRouter, ChunkConfig::default(), q, b)
                .build()
        },
        meta,
        stop,
        produced,
        loadgen,
        count_rows,
    );
}

/// Owned decode → RowBinary (shared by both owned backends).
#[allow(clippy::too_many_arguments)]
fn run_owned_rowbinary<D>(
    config: PipelineConfig,
    source: KafkaSource,
    ch: ClickHouseSink,
    metrics_handle: MetricsHandle,
    io: tokio::runtime::Runtime,
    deser: D,
    meta: &Meta,
    stop: Arc<AtomicBool>,
    produced: Arc<AtomicU64>,
    loadgen: std::thread::JoinHandle<()>,
    count_rows: impl Fn() -> u64,
) where
    D: etl_core::deser::Deserializer<Owned<SensorBatchOwned>> + Clone + Send + 'static,
{
    let e = ClickHouseEncoder::<Owned<SensorEventOwned>>::new();
    spawn_and_measure(
        config,
        source,
        ch,
        metrics_handle,
        io,
        move |q, b| {
            chain::<Owned<SensorBatchOwned>, _>(deser.clone())
                .with_metrics("bench-e2e", "main")
                .flat_map::<Owned<SensorEventOwned>, _>(explode_owned)
                .filter(keep_owned)
                .sink(e.clone(), KeyHashRouter, ChunkConfig::default(), q, b)
                .build()
        },
        meta,
        stop,
        produced,
        loadgen,
        count_rows,
    );
}

fn main() {
    etl_core::telemetry::init(etl_core::telemetry::LogFormat::Pretty, "info");
    let duration = Duration::from_secs(env_u64("DURATION_S", 60));
    let rate = env_u64("RATE", 100_000);
    let partitions = env_u64("PARTITIONS", 4) as i32;
    let threads = env_u64("THREADS", 2) as usize;
    let payload = env_u64("PAYLOAD", 64) as usize;
    let events = env_u64("EVENTS", 50);
    let topic = env_str("TOPIC", &format!("bench-e2e-{}", std::process::id()));
    let deser_kind = env_str("DESER", "none");
    let format = env_str("FORMAT", "rowbinary");

    // ── Infrastructure ──────────────────────────────────────────────────
    // Kafka is resolved here; the ClickHouse half is shared with the other rigs.
    let external_kafka = std::env::var("KAFKA_BROKERS").ok();
    let brokers = external_kafka.unwrap_or_else(docker::ensure_kafka);
    let (url, host, port, user, password) = docker::resolve_clickhouse();
    let conn = Conn {
        brokers,
        url,
        host,
        port,
        user,
        password,
    };
    let ch_version = docker::clickhouse_sql(
        &conn.host,
        conn.port,
        &conn.user,
        &conn.password,
        "SELECT version()",
    )
    .map(|v| v.trim().to_owned())
    .unwrap_or_default();

    if deser_kind == "none" {
        let meta = Meta {
            deser: "none".to_owned(),
            format: "rowbinary".to_owned(),
            threads,
            partitions,
            rate,
            duration,
            records_per_input: 1,
            payload_bytes: payload as u64,
            ch_version: ch_version.clone(),
        };
        run_raw(&conn, &topic, partitions, threads, meta, payload);
    } else {
        let meta = Meta {
            deser: deser_kind.clone(),
            format: format.clone(),
            threads,
            partitions,
            rate,
            duration,
            records_per_input: events,
            payload_bytes: 0,
            ch_version,
        };
        run_avro(
            &conn,
            &topic,
            partitions,
            threads,
            meta,
            &deser_kind,
            &format,
            events,
        );
    }
}
