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
//! - **Local** (default): starts/reuses the bench broker (`BROKER`, default
//!   Redpanda) and `etl-bench-clickhouse` containers, generates load itself.
//! - **External**: set KAFKA_BROKERS and CLICKHOUSE_URL (http://host:port)
//!   [+ CLICKHOUSE_USER/CLICKHOUSE_PASSWORD] — pure env config, runnable
//!   in Kubernetes against real clusters.
//!
//! Env: DURATION_S (60) RATE (100000 rec/s) PARTITIONS (4) THREADS (2)
//! PAYLOAD (64, raw mode) EVENTS (50, avro mode) TOPIC (bench-e2e-<pid>)
//! DESER (none) FORMAT (rowbinary) RESULTS (JSONL path) BENCH
//!
//! Egress width — the published arms were recorded with all of this hardcoded
//! to one shard / two I/O workers, so their number measures that width and not
//! the framework. EGRESS (fixed|scaled|over, default `fixed` = the original
//! hardcoding, bit-for-bit) derives SHARDS and IO_THREADS from THREADS; both
//! are individually pinnable, as are QUEUE_CAP (8) and the in-flight budget,
//! which is now derived per arm from the DESIGN.md sizing rule rather than left
//! at the default.
//!
//! CHUNK_KIB (64) is the terminal stage's seal size — `ChunkConfig::target_bytes`.
//! The default mirrors `ChunkConfig::default()`, so an unqualified arm measures
//! the shipped path; the knob exists so the size can be swept against a real
//! deserializer and a real sink, which is what judging that default needs. It
//! feeds the in-flight budget derivation too (the queued term is `QUEUE_CAP x
//! chunk_target`), so a larger arm is provisioned rather than being charged for
//! backpressure the sizing rule was meant to prevent. It rides in the variant
//! map **only when swept off the default** — the published charts select on
//! `deser`/`format` alone, so an unconditional key would split default-chunk
//! arms from the committed records into duplicate bars.
//!
//! One consequence to know before charting: because the default arm omits the
//! key, `chunk_kib` cannot be used as a `BenchBars` **category** for this rig —
//! the 64 KiB arm has nothing to group on and renders as an `undefined` bar.
//! Sweeps that need a chunk axis should either chart the paired `queue_cap`
//! (bijective under the iso-buffer construction) or tabulate the result. The
//! synthetic rig records `chunk_kib` unconditionally and has no such
//! restriction.
//!
//! Source-bound arm — the shape in which this rig can say something about the
//! Kafka source rather than the sink. ENGINE (MergeTree|Null) takes the sink
//! out of the way; LOAD (concurrent|prefill) fills the topic up front and runs
//! with no producer, which matters because against a concurrent producer the
//! consumer is tail-following and has no backlog to prefetch — the dominant
//! source term then cannot show at all. PREFILL sizes that backlog — it must
//! outlast the window or the run exits 3 without emitting, and its default is
//! derived from DURATION_S, so a `LOAD=prefill` run at the 60 s default asks
//! for hundreds of millions of messages. `ensure_prefill` prints the size in
//! GiB before producing; set PREFILL explicitly for anything but a short
//! window. QUEUED_MIN_MESSAGES sets prefetch depth (0 — the default — means
//! set nothing, i.e. exercise the shipped path), and PER_PARTITION_DETAIL
//! enables the connector's per-partition fetch-queue and stored-lag series
//! (consumer lag itself is always per-partition and never gated).
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
    /// `MergeTree` (default) or `Null`. `Null` takes the sink out of the way so
    /// the source can be the limiter — the ~5.9M rows/s Null ceiling finally
    /// exceeds the source's, which MergeTree never does.
    engine: String,
    /// `concurrent` (producer runs alongside, today's behaviour) or `prefill`
    /// (topic filled first, no producer during the window). A source ceiling
    /// needs `prefill`: against a concurrent producer the consumer is
    /// tail-following, so there is no backlog to prefetch and the prefetch
    /// depth — the dominant source term — cannot show at all.
    load: String,
    egress: String,
    shards: usize,
    io_threads: usize,
    queue_cap: usize,
    /// Terminal-stage chunk seal size, in KiB. The framework default is 64
    /// (`ChunkConfig::default`); this rig exposes it so the size can be swept
    /// against a real deserializer and a real sink, which is what deciding the
    /// default needs — the synthetic rig has neither.
    chunk_kib: u64,
    queued_min_messages: u64,
    /// Messages the topic must hold before a `prefill` run starts.
    prefill: u64,
    /// Messages actually present when a `prefill` run started, read from the
    /// broker's watermarks. The drain check compares the source's cumulative
    /// record count against this: a run that consumed the whole backlog
    /// measured how fast a topic empties, not a ceiling. Zero when the run is
    /// not a prefill.
    prefill_backlog: u64,
    /// Enables the connector's per-partition fetch-queue and stored-lag
    /// series. Off by default because cardinality grows with the assignment.
    /// Consumer lag itself is always per-partition and never gated.
    per_partition_detail: bool,
    /// Which broker implementation served the run — identity-defining for a
    /// source measurement, so it rides in the variant map.
    broker: String,
}

impl Meta {
    /// The terminal stage's chunk tuning for this arm. `ChunkConfig` is `Copy`,
    /// so the returned value can be bound once and moved into each per-thread
    /// chain closure.
    fn chunk_config(&self) -> ChunkConfig {
        ChunkConfig {
            target_bytes: (self.chunk_kib * 1024) as usize,
            ..ChunkConfig::default()
        }
    }
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

/// `queued_min_messages` is passed through rather than left to the framework's
/// own backstop because it is the dominant term in source throughput: a
/// backlogged consumer at the framework's 1000 runs 15-76x slower than at
/// librdkafka's 100000 (see the `kafka_source_ceiling` dataset). It is
/// identity-defining for any source measurement, so it is always explicit here.
fn kafka_source(brokers: &str, topic: &str, queued_min_messages: u64) -> KafkaSource {
    KafkaSource::new(KafkaSourceConfig {
        brokers: brokers.to_owned(),
        topic: topic.to_owned(),
        group_id: format!("bench-e2e-{}", std::process::id()),
        commit_interval: Duration::from_secs(1),
        startup_timeout: Duration::from_secs(30),
        // 1s (not the 5s default) so window-edge gauge reads are fresh.
        statistics_interval: Duration::from_secs(1),
        rdkafka: {
            let mut m = BTreeMap::from([("auto.offset.reset".to_owned(), "earliest".to_owned())]);
            // 0 means "set nothing", which exercises the shipped default path
            // rather than a value that merely happens to equal it.
            if queued_min_messages > 0 {
                m.insert(
                    "queued.min.messages".to_owned(),
                    queued_min_messages.to_string(),
                );
            }
            m
        },
    })
}

/// `io_threads` is recorded in the config for self-description only: this rig
/// builds its own tokio runtime and never lets the framework build one, so the
/// config value is inert. It is set anyway so the emitted config cannot
/// disagree with the `.worker_threads(io_threads)` actually used.
fn pipeline_config(
    threads: usize,
    io_threads: usize,
    budget_mib: u64,
    per_partition_detail: bool,
) -> PipelineConfig {
    PipelineConfig::from_str(&format!(
        "pipeline: {{ name: bench-e2e, threads: {threads}, io_threads: {io_threads} }}\n\
         checkpoint: {{ interval: 1s, max_pending_batches: 8192 }}\n\
         backpressure: {{ max_inflight_bytes: {budget_mib}MiB }}\n\
         metrics: {{ exporter: none, per_partition_detail: {per_partition_detail} }}\n\
         source: {{ kafka: {{}} }}\n\
         sink: {{ clickhouse: {{}} }}\n"
    ))
    .expect("config")
}

/// Egress width derived from pipeline threads. `scaled` and `over` match
/// `pipeline_synthetic`'s derivation; `fixed` deliberately does **not** — it
/// reproduces this rig's original hardcoding (1 shard, 2 I/O workers)
/// bit-for-bit so the published arms stay reproducible, where
/// `pipeline_synthetic`'s `fixed` is (2, 2). Do not compare a `fixed` arm
/// across the two rigs.
fn derive_egress(egress: &str, threads: usize) -> (usize, usize) {
    match egress {
        "fixed" => (1, 2),
        "scaled" => (threads.max(2), (threads / 2).max(2)),
        "over" => ((threads * 2).max(2), threads.max(2)),
        other => panic!("unknown EGRESS {other} (fixed|scaled|over)"),
    }
}

/// Rows written per batch. Must track the `batch.max_rows` in the sink YAML
/// below — the budget derivation and the sink would otherwise disagree about
/// how large a batch gets.
const BATCH_MAX_ROWS: u64 = 262_144;

/// Mirrors `ChunkConfig::default().target_bytes`. An arm left at this value
/// omits the `chunk_kib` variant key so it stays identity-compatible with the
/// committed dataset; asserted against the framework at startup so a change to
/// the shipped default cannot silently desync the two.
const DEFAULT_CHUNK_KIB: u64 = 64;

/// The DESIGN.md in-flight sizing rule, applied per arm rather than left at the
/// framework default. At `EGRESS=fixed` this derives below the 256 MiB floor and
/// clamps to it — i.e. the published arms keep exactly the budget they had.
///
/// `chunk_target` is the arm's actual seal size, not a constant: the queued
/// term it feeds is `queue_cap x chunk_target`, so pinning it to 64 KiB while
/// sweeping the seal size would under-provision every larger arm and charge the
/// chunk size for backpressure the sizing rule was supposed to prevent.
fn derive_budget_mib(shards: usize, queue_cap: usize, row_bytes: u64, chunk_target: u64) -> u64 {
    const INFLIGHT_PER_SHARD: u64 = 2;
    let batch_bytes = BATCH_MAX_ROWS * row_bytes;
    let pending =
        shards as u64 * (INFLIGHT_PER_SHARD * batch_bytes + queue_cap as u64 * chunk_target);
    // DESIGN.md: budget x low_ratio >= 2 x pending. At the default low_ratio
    // of 0.5 that is budget >= 4 x pending — the 2x headroom term and the
    // division by the ratio, not 2x on its own.
    ((2 * pending) * 2 / (1024 * 1024)).max(256)
}

/// Ensures the topic holds at least `target` messages, producing only the
/// shortfall. Idempotent, so reruns across arms and reps are nearly free — the
/// consumer group is per-pid with `auto.offset.reset=earliest`, so every run
/// re-reads the same backlog from offset 0.
fn ensure_prefill(brokers: &str, topic: &str, partitions: i32, target: u64, payload: usize) -> u64 {
    use rdkafka::consumer::{BaseConsumer, Consumer};
    let probe: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("probe consumer");
    let available: u64 = (0..partitions)
        .map(|p| {
            let (low, high) = probe
                .fetch_watermarks(topic, p, Duration::from_secs(10))
                .unwrap_or((0, 0));
            (high - low).max(0) as u64
        })
        .sum();
    if available >= target {
        eprintln!("prefill: {available} messages already present (target {target})");
        return available;
    }
    let shortfall = target - available;
    eprintln!(
        "prefill: producing {shortfall} messages ({:.1} GiB on disk)",
        (shortfall * (payload as u64 + 50)) as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    benchmarks::produce(brokers, topic, partitions, shortfall, payload);
    available + shortfall
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
    loadgen: Option<std::thread::JoinHandle<()>>,
    count_rows: impl Fn() -> u64,
) where
    MK: FnMut(ShardQueues, Arc<InflightBudget>) -> Box<dyn RunnableChain> + Send + 'static,
{
    let mut make_chain = make_chain;
    let shards = ch.endpoints.len();
    // The sink YAML is generated from the same `shards` value the budget was
    // derived from; assert rather than let the two drift silently.
    assert_eq!(shards, meta.shards, "sink shard count vs derived egress");
    let (queues, receivers) = shard_queues(shards, meta.queue_cap);
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
        queues: vec![queues.clone()],
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

    // Progress is read from the pipeline's own counters rather than from
    // `SELECT count()`, because `ENGINE = Null` discards rows and would leave
    // the readiness gate spinning forever. This is a *control* signal only —
    // a readiness gate is not a measurement, so a self-reported counter is
    // fine here. The reported figures are cross-checked below against
    // ClickHouse's own server-side accounting.
    let progress = || {
        prom::value(&metrics_handle.render(), "etl_sink_records_total", "").unwrap_or(0.0) as u64
    };

    let ready = Instant::now() + Duration::from_secs(60);
    while progress() == 0 {
        assert!(
            Instant::now() < ready,
            "no rows landed before the ready deadline"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // Measure a *window* rather than a lifetime: taking both edges keeps the
    // numerator and denominator over the same interval. The original code
    // started the clock at the first landed row but took the count at the end,
    // so rows that landed before the clock started were in the numerator and
    // not the denominator.
    // Both counters are read out of the *same* render as the histograms, and
    // the clock is stopped against those same two renders. Rendering
    // separately for each quantity let the numerator span a wider interval
    // than the denominator — a systematic upward bias of roughly one render's
    // worth of records at each edge.
    let counter = |text: &str, name: &str| prom::value(text, name, "").unwrap_or(0.0) as u64;
    let text0 = metrics_handle.render();
    let window_start = Instant::now();
    let (sink0, source0) = (
        counter(&text0, "etl_sink_records_total"),
        counter(&text0, "etl_source_records_total"),
    );
    std::thread::sleep(meta.duration);
    let metrics_text = metrics_handle.render();
    let window = window_start.elapsed().as_secs_f64();
    let (sink1, source1) = (
        counter(&metrics_text, "etl_sink_records_total"),
        counter(&metrics_text, "etl_source_records_total"),
    );

    stop.store(true, Ordering::Relaxed);
    if let Some(loadgen) = loadgen {
        loadgen.join().expect("loadgen");
    }
    let sent = produced.load(Ordering::Relaxed);

    shutdown.trigger();
    let exit = pipeline.join().expect("pipeline").expect("run");
    // Whole-run absolute, for the independent cross-check. Zero under Null.
    let rows = count_rows();
    let sink_records = sink1.saturating_sub(sink0);
    let source_records = source1.saturating_sub(source0);
    let elapsed = window;

    let mut rep = Report::measurement(env_str("BENCH", "e2e_kafka_clickhouse").as_str())
        // The committed dataset predates every egress and prefetch knob below,
        // so its records carry a smaller variant key set. Without this
        // discriminator the site's aggregator — which keys on bench name plus
        // the whole variant map — would take a median across configurations
        // that are not comparable, which is worse than a duplicate bar.
        // v2 read each counter from its own render (numerator and denominator
        // over different intervals), gated drains on a lag gauge nothing
        // populated, and produced 0xAB payloads that tripled in width on the
        // way to the sink. The key stops the sets aggregating.
        .variant("harness", "v3")
        .variant("broker", meta.broker.clone())
        .variant("deser", meta.deser.clone())
        .variant("format", meta.format.clone())
        .variant("engine", meta.engine.clone())
        .variant("load", meta.load.clone())
        .variant("threads", meta.threads as u64)
        .variant("partitions", meta.partitions)
        .variant("egress", meta.egress.clone())
        .variant("shards", meta.shards as u64)
        .variant("io_threads", meta.io_threads as u64)
        .variant("queue_cap", meta.queue_cap as u64)
        .variant("queued_min_messages", meta.queued_min_messages)
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
    // Recorded only when swept away from the shipped `ChunkConfig::default()`.
    // The committed dataset predates this knob, and the published charts
    // (`docs/benchmarks/avro-fast-pipeline.mdx`) select on `deser`/`format`
    // alone — an unconditional key would give every new default-chunk arm a
    // variant identity the committed records lack, and the site would render
    // the two as duplicate bars in one category rather than aggregating them.
    // Same reasoning as `queued_min_messages`: the unqualified arm must stay
    // identity-compatible with what production actually ships.
    if meta.chunk_kib != DEFAULT_CHUNK_KIB {
        rep = rep.variant("chunk_kib", meta.chunk_kib);
    }
    rep = rep
        .metric(
            "rows_per_s",
            Metric::maximize(sink_records as f64 / elapsed, "rows/s"),
        )
        // The headline for a source-bound arm: messages pulled off Kafka, before
        // any flat_map expansion.
        .metric(
            "source_records_per_s",
            Metric::maximize(source_records as f64 / elapsed, "records/s"),
        )
        .metric(
            "sink_records",
            Metric::maximize(sink_records as f64, "rows"),
        );
    // Under Null the table discards rows, so a 0 here would read as "nothing
    // landed" rather than "not applicable". Omit it instead.
    if meta.engine != "Null" {
        rep = rep.metric("rows_in_clickhouse", Metric::maximize(rows as f64, "rows"));
    }
    // Windowed, not lifetime: the lifetime quantiles included warmup.
    if let Some(v) =
        prom::histogram_quantile_delta(&text0, &metrics_text, "etl_e2e_latency_seconds", 0.5)
    {
        rep = rep.metric("e2e_p50_s", Metric::minimize(v, "s"));
    }
    if let Some(v) =
        prom::histogram_quantile_delta(&text0, &metrics_text, "etl_e2e_latency_seconds", 0.99)
    {
        rep = rep.metric("e2e_p99_s", Metric::minimize(v, "s"));
    }
    if let Some(v) = prom::histogram_quantile_delta(
        &text0,
        &metrics_text,
        "etl_sink_flush_duration_seconds",
        0.99,
    ) {
        rep = rep.metric("sink_flush_p99", Metric::minimize(v, "s"));
    }
    let delta = |name: &str| {
        let after = prom::value(&metrics_text, name, "").unwrap_or(0.0);
        let before = prom::value(&text0, name, "").unwrap_or(0.0);
        (after - before).max(0.0)
    };
    rep = rep.metric(
        "backpressure_pauses",
        Metric::minimize(delta("etl_backpressure_pause_events_total"), "events"),
    );

    // Attribution, recorded for inspection rather than used as a gate. A fetch
    // queue at ~0 means the pipeline was draining the local queue as fast as
    // the broker could refill it — the source is broker-bound; a high, stable
    // value means the limiter is downstream. Note that a *fetch-bound* source
    // reads ~0 whether or not prefetch is generously sized, so this
    // distinguishes "source-bound" from "sink-bound", not "starved" from
    // "healthy".
    //
    // `etl_source_lag_records` is per-partition only, so `prom::value`'s sum
    // across every matching series is this member's whole backlog — directly
    // comparable with the broker's group-lag figure. `None` means no
    // partition has reported a measured lag yet; note that a *partially*
    // measured assignment sums only what it has, so a small number early in a
    // run is not evidence of a small backlog.
    let fetch_queue = prom::value(&metrics_text, "etl_kafka_source_fetch_queue_messages", "");
    let lag = prom::value(&metrics_text, "etl_source_lag_records", "");

    let measure = "rows_per_s = windowed etl_sink_records_total delta / window; \
                   source_records_per_s = windowed etl_source_records_total delta";
    let ch_version = &meta.ch_version;
    let mut note = format!(
        "{measure}; window={elapsed:.1}s; sink_records={sink_records} \
         source_records={source_records}; produced={sent}; \
         fetch_queue={fetch_queue:?} lag={lag:?}; \
         queue_full=unwired (rig assembles the runtime directly, so \
         ShardQueues::attach_metrics is never called); \
         ch_version={ch_version}; exit={:?}",
        exit.state
    );
    let oversubscribed =
        meta.threads + meta.io_threads + 1 + usize::from(meta.load == "concurrent")
            > std::thread::available_parallelism().map_or(usize::MAX, |p| p.get());
    if oversubscribed {
        note.push_str(" OVERSUBSCRIBED");
    }

    // A prefill arm that swallowed its whole backlog measured how fast the
    // topic empties, not a ceiling. Decide it against the backlog the broker
    // reported at start, compared with the source's cumulative record count —
    // not against the lag gauge, which flagged every arm of an earlier dataset
    // as a drain while 95% of the backlog was still on the broker. (In that
    // era the metrics seam was disconnected, so the gauge was registered and
    // never written and read `0`. Lag series are absent until measured now,
    // but the backlog remains the honest gate.) Invalid arms do not reach the
    // results file: a marker in a free-text note does not stop the site
    // aggregating the record into a median.
    if meta.load == "prefill" && source1 >= meta.prefill_backlog {
        eprintln!(
            "DRAINED: the source consumed {source1} of a {} message backlog — this \
             measures a drain, not a saturation. Raise PREFILL or shorten DURATION_S. \
             Nothing emitted.\n  {note}",
            meta.prefill_backlog
        );
        std::process::exit(3);
    }

    rep.note(note).emit();
}

/// The raw baseline: byte passthrough → `parse_row` → RowBinary.
fn run_raw(conn: &Conn, topic: &str, partitions: i32, threads: usize, meta: Meta, payload: usize) {
    let sql = |q: &str| {
        docker::clickhouse_sql(&conn.host, conn.port, &conn.user, &conn.password, q)
            .expect("clickhouse")
    };
    sql("DROP TABLE IF EXISTS bench_events");
    // The table name stays `bench_events` for both engines. The server-side
    // cross-check matches query_log rows by table-name substring, so an
    // engine-suffixed name would silently match two tables and double-count.
    if meta.engine == "Null" {
        sql("CREATE TABLE bench_events (id UInt64, body String) ENGINE = Null");
    } else {
        sql(
            "CREATE TABLE bench_events (id UInt64, body String) ENGINE = MergeTree ORDER BY id \
             SETTINGS non_replicated_deduplication_window = 100",
        );
    }
    ensure_topic(&conn.brokers, topic, partitions);

    let stop = Arc::new(AtomicBool::new(false));
    let produced = Arc::new(AtomicU64::new(0));
    let mut meta = meta;
    let loadgen = if meta.load == "prefill" {
        // Fill the topic first, then run with no producer at all: a saturating
        // in-process producer competes for the same cores as the thing being
        // measured, and leaves the consumer tail-following with nothing to
        // prefetch.
        let target = meta.prefill.max(1);
        meta.prefill_backlog = ensure_prefill(&conn.brokers, topic, partitions, target, payload);
        None
    } else {
        let (brokers, topic) = (conn.brokers.clone(), topic.to_owned());
        let (stop, produced) = (Arc::clone(&stop), Arc::clone(&produced));
        Some(std::thread::spawn(move || {
            produce_load(
                &brokers, &topic, partitions, meta.rate, payload, &stop, &produced,
            );
        }))
    };

    let metrics_handle = install_metrics();
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(meta.io_threads)
        .enable_all()
        .build()
        .expect("io runtime");
    // `id` (u64) plus the RowBinary-encoded `body`: a varint length and the
    // payload bytes. `produce` emits ASCII, so a byte in is a byte out — the
    // earlier 0xAB payloads went through `from_utf8_lossy` and tripled.
    let budget_mib = derive_budget_mib(
        meta.shards,
        meta.queue_cap,
        payload as u64 + 16,
        meta.chunk_kib * 1024,
    );
    let config = pipeline_config(
        threads,
        meta.io_threads,
        budget_mib,
        meta.per_partition_detail,
    );
    let source = kafka_source(&conn.brokers, topic, meta.queued_min_messages);
    // async_insert: "0" pins the synchronous insert path so the recorded server
    // behaviour is stable across server versions (26.3 defaults it to 1).
    let replicas = (0..meta.shards)
        .map(|_| format!("    - replicas: [{:?}]\n", conn.url))
        .collect::<String>();
    let sink_yaml = format!(
        "clickhouse:\n  table: bench_events\n  columns: [id, body]\n  \
         user: {}\n  password: {:?}\n  settings: {{ async_insert: \"0\" }}\n  \
         shards:\n{replicas}  \
         batch: {{ linger: 500ms, max_rows: {BATCH_MAX_ROWS} }}\n",
        conn.user, conn.password
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
    let chunk_cfg = meta.chunk_config();
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
                .sink(enc.clone(), KeyHashRouter, chunk_cfg, q, b)
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
        Some(std::thread::spawn(move || {
            produce_avro(
                &brokers, &topic, partitions, meta.rate, events, &stop, &produced,
            );
        }))
    };

    let metrics_handle = install_metrics();
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(meta.io_threads)
        .enable_all()
        .build()
        .expect("io runtime");
    let budget_mib = derive_budget_mib(meta.shards, meta.queue_cap, 48, meta.chunk_kib * 1024);
    let config = pipeline_config(
        threads,
        meta.io_threads,
        budget_mib,
        meta.per_partition_detail,
    );
    let source = kafka_source(&conn.brokers, topic, meta.queued_min_messages);
    let ch_format = if format == "native" {
        "native"
    } else {
        "rowbinary"
    };
    // async_insert: "0" pins the synchronous insert path (26.3 defaults it to 1).
    let replicas = (0..meta.shards)
        .map(|_| format!("    - replicas: [{:?}]\n", conn.url))
        .collect::<String>();
    let sink_yaml = format!(
        "clickhouse:\n  table: sensor_events\n  columns: [{col_names}]\n  \
         format: {ch_format}\n  user: {}\n  password: {:?}\n  \
         settings: {{ async_insert: \"0\" }}\n  shards:\n{replicas}  \
         batch: {{ linger: 500ms, max_rows: {BATCH_MAX_ROWS} }}\n",
        conn.user, conn.password
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
            let chunk_cfg = meta.chunk_config();
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
                        .sink(e.clone(), KeyHashRouter, chunk_cfg, q, b)
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
            let chunk_cfg = meta.chunk_config();
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
                        .sink(e.clone(), KeyHashRouter, chunk_cfg, q, b)
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
    loadgen: Option<std::thread::JoinHandle<()>>,
    count_rows: impl Fn() -> u64,
) where
    D: etl_core::deser::Deserializer<Owned<SensorBatchOwned>> + Clone + Send + 'static,
{
    let e = NativeEncoder::<Owned<SensorEventOwned>>::new(avro_batch::native_schema());
    let chunk_cfg = meta.chunk_config();
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
                .sink(e.clone(), KeyHashRouter, chunk_cfg, q, b)
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
    loadgen: Option<std::thread::JoinHandle<()>>,
    count_rows: impl Fn() -> u64,
) where
    D: etl_core::deser::Deserializer<Owned<SensorBatchOwned>> + Clone + Send + 'static,
{
    let e = ClickHouseEncoder::<Owned<SensorEventOwned>>::new();
    let chunk_cfg = meta.chunk_config();
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
                .sink(e.clone(), KeyHashRouter, chunk_cfg, q, b)
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
    let engine = env_str("ENGINE", "MergeTree");
    let load = env_str("LOAD", "concurrent");
    let egress = env_str("EGRESS", "fixed");
    let (derived_shards, derived_io) = derive_egress(&egress, threads);
    let shards = env_u64("SHARDS", derived_shards as u64) as usize;
    let io_threads = env_u64("IO_THREADS", derived_io as u64) as usize;
    let queue_cap = env_u64("QUEUE_CAP", 8) as usize;
    // An unqualified arm measures exactly what the framework ships.
    assert_eq!(
        ChunkConfig::default().target_bytes,
        (DEFAULT_CHUNK_KIB * 1024) as usize,
        "DEFAULT_CHUNK_KIB no longer mirrors ChunkConfig::default(); the \
         conditional chunk_kib variant would mislabel arms"
    );
    let chunk_kib = env_u64("CHUNK_KIB", DEFAULT_CHUNK_KIB);
    // 0 = set nothing, which is what the connector ships: `etl-kafka` pins no
    // prefetch depth, so librdkafka's default applies. Defaulting to a number
    // would make every unqualified arm measure a depth production never uses.
    // `kafka_topology` reads the same variable with the same meaning.
    let queued_min_messages = env_u64("QUEUED_MIN_MESSAGES", 0);
    let per_partition_detail = env_u64("PER_PARTITION_DETAIL", 0) != 0;
    // Enough backlog that a saturated window cannot empty the topic.
    let prefill = env_u64(
        "PREFILL",
        (env_u64("PREFILL_RATE_HINT", 6_000_000) as f64 * duration.as_secs_f64() * 1.5) as u64,
    );

    // ── Infrastructure ──────────────────────────────────────────────────
    // Kafka is resolved here; the ClickHouse half is shared with the other rigs.
    let (brokers, broker) = match std::env::var("KAFKA_BROKERS") {
        Ok(b) => (
            b,
            std::env::var("BROKER").unwrap_or_else(|_| "external".to_owned()),
        ),
        Err(_) => docker::resolve_broker(),
    };
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
            engine: engine.clone(),
            load: load.clone(),
            egress: egress.clone(),
            shards,
            io_threads,
            queue_cap,
            chunk_kib,
            queued_min_messages,
            prefill,
            prefill_backlog: 0,
            broker: broker.clone(),
            per_partition_detail,
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
            engine: engine.clone(),
            load: load.clone(),
            egress: egress.clone(),
            shards,
            io_threads,
            queue_cap,
            chunk_kib,
            queued_min_messages,
            prefill,
            prefill_backlog: 0,
            broker: broker.clone(),
            per_partition_detail,
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
