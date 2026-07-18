//! Kafka producer-sink saturation: an in-process generator source (no source
//! broker) driven through the real chain, sink pool, and runtime into the
//! Kafka producer sink at full tilt, against a local `apache/kafka:4.1.0`
//! broker. It answers three questions and controls for the one confounder the
//! ClickHouse saturation rig never had to:
//!
//! 1. **Client-path overhead** — `MODE=framework` (the full path: encode →
//!    seal → `write_batch` parse + send + delivery-report countdown) vs
//!    `MODE=raw` (a bare `ThreadedProducer` loop with the *same* forced
//!    settings, unframed) on the same broker. The framework/raw ratio is what
//!    the framing and countdown cost over librdkafka itself.
//! 2. **The default mismatch** — framework `batch.max_rows` (500k default)
//!    exceeds librdkafka `queue.buffering.max.messages` (100k default), so a
//!    full sealed batch cannot fit the producer queue and every large batch
//!    rides the writer's queue-full backoff. Sweep `BATCH_MAX_ROWS` at a fixed
//!    queue, and the queue at a fixed batch, watching for a throughput cliff.
//! 3. **Shards** — 1 vs 2 vs 4 workers over the *one* shared producer.
//!
//! **Bottleneck attribution.** Kafka has no free `Null` engine, so the broker
//! is a live confounder. Each framework arm records a two-axis verdict in its
//! note: `upstream` (`sink`/`budget`/`generator`/`checkpoint`, from the
//! framework's own backpressure + checkpoint signals — is the source being
//! pushed as hard as it can produce?) and `downstream` (`client`/`broker`,
//! from the producer-queue depth + broker RTT/latency the sink already
//! publishes as `etl_kafka_*` statistics, cross-checked against the raw
//! baseline via `RAW_BASELINE_RPS`). Only `upstream=sink, downstream=client`
//! arms back a client-path claim. The default in-flight budget is derived from
//! the sink's batch/queue sizing so the framework never self-throttles below
//! the producer queue (the sink-saturation `budget` lesson).
//!
//! Throughput is read from `etl_sink_records_total` (durable acks: the sink
//! returns `Ok` only after every delivery report under `acks=all`) over a
//! steady-state window — the client-path durable throughput.
//!
//! Usage:
//!   kafka_sink_saturation             # sweep SWEEP over SWEEP_LIST, REPS each
//!   RUN_ONE=1 kafka_sink_saturation   # a single measurement, one JSON line
//!
//! Env (scalars): MODE (framework|raw) THREADS (4) SHARDS (1) IO_THREADS (4)
//! INFLIGHT_PER_SHARD (2) BATCH_MAX_ROWS (500000) BATCH_MAX_MB (256)
//! LINGER_MS (1000) QUEUE_CAP (64) QUEUE_BUFFERING_MAX_MESSAGES (100000)
//! MAX_INFLIGHT_MB (derived) PAYLOAD (256) PARTITIONS (16)
//! TOPIC (bench-kafka-sink) DURATION_S (15) WARMUP_S (5)
//! STATISTICS_INTERVAL_MS (1000) CHECKPOINT_INTERVAL_MS (1000)
//! MAX_PENDING_BATCHES (8192) RAW_SEND_THREADS (1) KAFKA_CPUS (recorded)
//! RAW_BASELINE_RPS (optional; enables framework_over_raw) RESULTS (JSONL path)
//! Env (sweep, parent only): SWEEP (env name) SWEEP_LIST (csv) REPS (3)
//! BOOTSTRAP (external broker; skips docker) FRESH (recreate broker)
#![allow(clippy::print_stdout, clippy::print_stderr)]

use benchmarks::report::{Metric, Report};
use benchmarks::synthetic::SyntheticSource;
use benchmarks::{docker, env_str, env_u64, prom};
use etl_core::backpressure::InflightBudget;
use etl_core::config::{ComponentConfig, PipelineConfig};
use etl_core::deser::BytesPassthrough;
use etl_core::metrics::{ComponentLabels, E2eBasis, Meter, MetricsHandle, SinkShardMetrics};
use etl_core::ops::{ChunkConfig, RunnableChain, chain_owned};
use etl_core::pipeline::{PipelineRuntime, RuntimeOptions, SinkRuntime};
use etl_core::record::RecordMeta;
use etl_core::sink::{ShardRouter, ShardWriter, SinkPool, shard_queues};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::config::ClientConfig;
use rdkafka::message::DeliveryResult;
use rdkafka::producer::{BaseRecord, Producer, ProducerContext, ThreadedProducer};
use rdkafka::types::RDKafkaErrorCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Balanced shard routing. The generator emits keyless records, so the default
/// key-hash router would pin a lane to one shard; round-robin fans a thread's
/// records evenly across every shard worker. (Mirrors `ch_sink_saturation`.)
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

/// One parsed arm.
struct Cfg {
    mode: String,
    threads: usize,
    shards: usize,
    io_threads: usize,
    inflight: u64,
    batch_max_rows: u64,
    batch_max_mb: u64,
    linger_ms: u64,
    queue_cap: usize,
    queue_buffering: u64,
    payload: usize,
    partitions: i32,
    topic: String,
    duration: Duration,
    warmup: Duration,
    stat_ms: u64,
    checkpoint_ms: u64,
    max_pending_batches: u64,
    raw_send_threads: usize,
    kafka_cpus: Option<u64>,
    raw_baseline_rps: Option<f64>,
}

impl Cfg {
    fn from_env() -> Self {
        Cfg {
            mode: env_str("MODE", "framework"),
            threads: env_u64("THREADS", 4) as usize,
            shards: env_u64("SHARDS", 1) as usize,
            io_threads: env_u64("IO_THREADS", 4) as usize,
            inflight: env_u64("INFLIGHT_PER_SHARD", 2),
            batch_max_rows: env_u64("BATCH_MAX_ROWS", 500_000),
            batch_max_mb: env_u64("BATCH_MAX_MB", 256),
            linger_ms: env_u64("LINGER_MS", 1000),
            queue_cap: env_u64("QUEUE_CAP", 64) as usize,
            queue_buffering: env_u64("QUEUE_BUFFERING_MAX_MESSAGES", 100_000),
            payload: env_u64("PAYLOAD", 256) as usize,
            partitions: env_u64("PARTITIONS", 16) as i32,
            topic: env_str("TOPIC", "bench-kafka-sink"),
            duration: Duration::from_secs(env_u64("DURATION_S", 15)),
            warmup: Duration::from_secs(env_u64("WARMUP_S", 5)),
            stat_ms: env_u64("STATISTICS_INTERVAL_MS", 1000),
            checkpoint_ms: env_u64("CHECKPOINT_INTERVAL_MS", 1000),
            max_pending_batches: env_u64("MAX_PENDING_BATCHES", 8192),
            raw_send_threads: env_u64("RAW_SEND_THREADS", 1) as usize,
            kafka_cpus: std::env::var("KAFKA_CPUS")
                .ok()
                .and_then(|v| v.parse().ok()),
            raw_baseline_rps: std::env::var("RAW_BASELINE_RPS")
                .ok()
                .and_then(|v| v.parse().ok()),
        }
    }

    /// Common variant dimensions shared by both modes.
    fn base_variants(&self, rep: Report) -> Report {
        let mut rep = rep
            .variant("mode", self.mode.clone())
            .variant("payload_bytes", self.payload as u64)
            .variant("partitions", i64::from(self.partitions))
            .variant("shards", self.shards as u64)
            .variant("batch_max_rows", self.batch_max_rows)
            .variant("queue_buffering_max_messages", self.queue_buffering);
        if let Some(c) = self.kafka_cpus {
            rep = rep.variant("kafka_cpus", c);
        }
        rep
    }
}

/// `retention.ms`/`retention.bytes` are set aggressively so a run that produces
/// tens of millions of messages per arm across the whole matrix never fills the
/// broker's disk — Kafka purges old segments continuously. Producer throughput
/// writes to the active segment and is unaffected; the light background purge is
/// noted as a caveat on the results page. Idempotent: an existing topic (from a
/// prior arm) already carries these configs.
fn ensure_bench_topic(bootstrap: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("admin client");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1))
        .set("retention.ms", "10000")
        .set("retention.bytes", "134217728") // 128 MiB/partition steady-state cap
        .set("segment.bytes", "134217728")
        .set("segment.ms", "5000");
    let results = rt
        .block_on(admin.create_topics(&[new_topic], &AdminOptions::new()))
        .expect("create_topics call");
    for result in results {
        match result {
            Ok(_) => {}
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((name, code)) => panic!("failed to create topic {name}: {code}"),
        }
    }
}

fn install_metrics() -> MetricsHandle {
    etl_core::metrics::install(&etl_core::metrics::MetricsSettings {
        exporter: etl_core::metrics::Exporter::Prometheus,
        ..Default::default()
    })
    .expect("install metrics recorder")
}

const PIPELINE_NAME: &str = "kafka-sink-sat";

/// The framework path: generator → chain → Kafka sink pool, measured over a
/// steady-state window with the two-axis bottleneck verdict.
fn run_framework(bootstrap: &str, cfg: &Cfg) {
    // Derived in-flight budget from the deployment sizing rule (DESIGN.md
    // § Backpressure): `max_inflight_bytes × low_ratio` must cover the
    // fully-formed in-flight batches plus queued chunks, or the framework pauses
    // the source before the *producer queue* is the bottleneck — masking the
    // very queue-full cliff we are hunting as a budget artifact. Use BATCH_MAX_MB
    // (the config bound), not observed batch size.
    const LOW_RATIO: f64 = 0.5;
    const CHUNK_MIB: f64 = 64.0 / 1024.0; // 64 KiB chunk target
    let derived_mb = (2.0
        * (cfg.shards as f64 * cfg.inflight as f64 * cfg.batch_max_mb as f64
            + cfg.shards as f64 * cfg.queue_cap as f64 * CHUNK_MIB)
        / LOW_RATIO)
        .ceil() as u64;
    let max_inflight_mb = env_u64("MAX_INFLIGHT_MB", derived_mb.max(1024));

    // Worst-case resident batch memory, surfaced before allocating.
    let row_bytes = cfg.payload as u64 + 16; // payload + framing overhead
    let batch_bytes = (cfg.batch_max_rows * row_bytes).min(cfg.batch_max_mb * 1024 * 1024);
    let worst_case_mb = cfg.shards as u64 * cfg.inflight * batch_bytes / (1024 * 1024);
    eprintln!(
        "budget: MAX_INFLIGHT_MB={max_inflight_mb} (derived {derived_mb}); \
         worst-case resident batches ~{worst_case_mb} MiB; queue_buffering={}",
        cfg.queue_buffering
    );

    let metrics_handle = install_metrics();
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.io_threads)
        .enable_all()
        .build()
        .expect("io runtime");

    let config = PipelineConfig::from_str(&format!(
        "pipeline: {{ name: {PIPELINE_NAME}, threads: {threads} }}\n\
         checkpoint: {{ interval: {ckpt}ms, max_pending_batches: {maxpend} }}\n\
         backpressure: {{ max_inflight_bytes: {budget}MiB }}\n\
         metrics: {{ exporter: none }}\n\
         source: {{ synthetic: {{}} }}\n\
         sink: {{ kafka: {{}} }}\n",
        threads = cfg.threads,
        ckpt = cfg.checkpoint_ms,
        maxpend = cfg.max_pending_batches,
        budget = max_inflight_mb,
    ))
    .expect("config");

    // Kafka sink from YAML. queue.buffering.max.messages is the one legitimate
    // way to set the producer-queue depth (not owned/denied by the sink).
    let sink_yaml = format!(
        "kafka:\n  brokers: {bootstrap}\n  topic: {topic}\n  shards: {shards}\n  \
         statistics_interval: {stat}ms\n  inflight: {{ max_per_shard: {inflight} }}\n  \
         batch: {{ max_rows: {rows}, max_bytes: {mb}MiB, linger: {linger}ms }}\n  \
         rdkafka: {{ \"queue.buffering.max.messages\": \"{qbm}\" }}\n",
        topic = cfg.topic,
        shards = cfg.shards,
        stat = cfg.stat_ms,
        inflight = cfg.inflight,
        rows = cfg.batch_max_rows,
        mb = cfg.batch_max_mb,
        linger = cfg.linger_ms,
        qbm = cfg.queue_buffering,
    );
    let section: ComponentConfig = serde_yaml::from_str(&sink_yaml).expect("sink section");
    let k = etl_kafka::sink::from_component_config(&section).expect("kafka sink");
    assert_eq!(k.endpoints.len(), cfg.shards, "one endpoint per shard");
    // Encoder must be built while the sink bundle is whole (borrows &self).
    let enc = k.encoder_bytes();
    let mut writer = k.writer;
    let endpoints = k.endpoints;
    let pool_cfg = k.pool;

    // Wire the sink's librdkafka statistics into the global recorder. The manual
    // SinkPool::spawn path does not call attach_metrics (the pipeline builder
    // does), so we do it here — names render as etl_kafka_* (production's
    // role-scoped scope would be etl_kafka_sink_*; the translation is identical).
    let stats_meter = Meter::with_namespace("kafka", PIPELINE_NAME, "sink", "kafka");
    writer.attach_metrics(Some(stats_meter));

    // Source + sink pool (mirrors ch_sink_saturation / e2e_kafka_clickhouse).
    let produced = Arc::new(AtomicU64::new(0));
    let source = SyntheticSource::new(cfg.threads, cfg.payload, Arc::clone(&produced));
    let commits = source.commits();

    let (queues, receivers) = shard_queues(cfg.shards, cfg.queue_cap);
    let budget = Arc::new(InflightBudget::new());
    let labels = ComponentLabels::new(PIPELINE_NAME, "sink", "kafka");
    let metrics = (0..cfg.shards)
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
        Arc::new(writer),
        endpoints,
        receivers,
        pool_cfg,
        Arc::clone(&budget),
        metrics,
        PIPELINE_NAME,
        io.handle(),
    );
    let sink = SinkRuntime {
        queues: vec![queues.clone()],
        drain: Box::new(move |deadline| Box::pin(async move { pool.drain(deadline).await })),
        probe: None,
    };

    let chain_queues = queues;
    let chain_budget = Arc::clone(&budget);
    let chains = move |_thread: usize| -> Box<dyn RunnableChain> {
        chain_owned::<Vec<u8>, _>(BytesPassthrough)
            .with_metrics(PIPELINE_NAME, "main")
            .sink(
                enc.clone(),
                RoundRobinRouter::default(),
                ChunkConfig::default(),
                chain_queues.clone(),
                Arc::clone(&chain_budget),
            )
            .build()
    };

    let runtime =
        PipelineRuntime::new(config, source, chains, sink, budget).with_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        });
    let shutdown = runtime.shutdown_handle();
    let pipeline = std::thread::spawn(move || runtime.run());

    // Warm up, then measure a steady-state window.
    std::thread::sleep(cfg.warmup);
    let text0 = metrics_handle.render();
    let sink0 = prom::value(&text0, "etl_sink_records_total", "").unwrap_or(0.0);
    let pauses0 = prom::value(&text0, "etl_backpressure_pause_events_total", "").unwrap_or(0.0);
    let paused0 = prom::value(&text0, "etl_backpressure_paused", "").unwrap_or(0.0);
    let pend0 = prom::value(&text0, "etl_checkpoint_pending_batches", "").unwrap_or(0.0);
    let queue0 = prom::value(&text0, "etl_kafka_produce_queue_messages", "").unwrap_or(0.0);
    let c0 = produced.load(Ordering::Relaxed);
    let t0 = Instant::now();
    std::thread::sleep(cfg.duration);
    let text1 = metrics_handle.render();
    let window = t0.elapsed().as_secs_f64();
    let sink1 = prom::value(&text1, "etl_sink_records_total", "").unwrap_or(0.0);
    let pauses1 = prom::value(&text1, "etl_backpressure_pause_events_total", "").unwrap_or(0.0);
    let paused1 = prom::value(&text1, "etl_backpressure_paused", "").unwrap_or(0.0);
    let pend1 = prom::value(&text1, "etl_checkpoint_pending_batches", "").unwrap_or(0.0);
    let queue1 = prom::value(&text1, "etl_kafka_produce_queue_messages", "").unwrap_or(0.0);
    let c1 = produced.load(Ordering::Relaxed);

    // Producer/broker telemetry at window end (gauges reflect the last interval).
    let queue_bytes = prom::value(&text1, "etl_kafka_produce_queue_bytes", "").unwrap_or(0.0);
    let rtt_p99 = prom::value(&text1, "etl_kafka_broker_rtt_p99_seconds", "");
    let outbuf_p99 = prom::value(&text1, "etl_kafka_broker_outbuf_latency_p99_seconds", "");
    let int_p99 = prom::value(&text1, "etl_kafka_broker_int_latency_p99_seconds", "");
    let tx_messages = prom::value(&text1, "etl_kafka_tx_messages_total", "").unwrap_or(0.0);
    let queue_peak = queue0.max(queue1);
    let queue_full_ratio = if cfg.queue_buffering > 0 {
        queue_peak / cfg.queue_buffering as f64
    } else {
        0.0
    };

    // ── Upstream verdict (is the framework pushed as hard as it can produce?) ──
    let pauses = (pauses1 - pauses0).max(0.0);
    let paused_edge = paused0 > 0.0 || paused1 > 0.0;
    let pend_high = pend0 > cfg.max_pending_batches as f64 / 2.0
        || pend1 > cfg.max_pending_batches as f64 / 2.0;
    let upstream = if max_inflight_mb < derived_mb {
        "budget"
    } else if pauses > 0.0 || paused_edge {
        "sink"
    } else if pend_high {
        "checkpoint"
    } else {
        "generator"
    };

    let sink_records = sink1 - sink0;
    let rps = if window > 0.0 {
        sink_records / window
    } else {
        0.0
    };
    let byte_rate = rps * cfg.payload as f64;

    // ── Downstream verdict (client path vs broker) ──
    // Primary: the raw baseline is the broker-headroom oracle. ratio ≥ 0.9 ⇒ the
    // framework matches raw ⇒ the broker is the shared ceiling. Fallback (no raw
    // baseline supplied): the queue-full + broker-latency mechanism read.
    const OUTBUF_HIGH_S: f64 = 0.05; // 50 ms in-transit ⇒ broker draining slowly
    let framework_over_raw = cfg
        .raw_baseline_rps
        .and_then(|raw| if raw > 0.0 { Some(rps / raw) } else { None });
    let downstream = match framework_over_raw {
        Some(ratio) => {
            if ratio >= 0.9 {
                "broker"
            } else {
                "client"
            }
        }
        None => {
            let broker_slow = outbuf_p99.is_some_and(|v| v >= OUTBUF_HIGH_S);
            if queue_full_ratio >= 0.8 && !broker_slow {
                "client"
            } else if queue_full_ratio >= 0.8 {
                "broker"
            } else {
                "client"
            }
        }
    };

    shutdown.trigger();
    let exit = pipeline.join().expect("pipeline thread").expect("run");
    let produced_delta = c1.saturating_sub(c0);

    let mut rep = cfg
        .base_variants(Report::measurement("kafka_sink_saturation"))
        .variant("threads", cfg.threads as u64)
        .variant("io_threads", cfg.io_threads as u64)
        .variant("inflight_per_shard", cfg.inflight)
        .variant("batch_max_mb", cfg.batch_max_mb)
        .variant("linger_ms", cfg.linger_ms)
        .variant("queue_cap", cfg.queue_cap as u64)
        .variant("max_inflight_mb", max_inflight_mb)
        .variant("max_pending_batches", cfg.max_pending_batches)
        .metric("records_per_s", Metric::maximize(rps, "records/s"))
        .metric(
            "records_per_s_per_thread",
            Metric::maximize(rps / cfg.threads as f64, "records/s"),
        )
        .metric("mb_per_s", Metric::bytes_per_s(byte_rate))
        .metric("sink_records", Metric::maximize(sink_records, "records"))
        .metric(
            "produced_total",
            Metric::maximize(produced.load(Ordering::Relaxed) as f64, "records"),
        )
        .metric(
            "commits",
            Metric::maximize(commits.load(Ordering::Relaxed) as f64, "commits"),
        )
        .metric(
            "produce_queue_messages",
            Metric::maximize(queue_peak, "messages"),
        )
        .metric(
            "produce_queue_bytes",
            Metric::maximize(queue_bytes, "bytes"),
        )
        .metric("tx_messages", Metric::maximize(tx_messages, "messages"))
        .metric("backpressure_pauses", Metric::minimize(pauses, "events"));
    if let Some(ratio) = framework_over_raw {
        rep = rep.metric("framework_over_raw", Metric::maximize(ratio, "ratio"));
    }
    if let Some(v) = rtt_p99 {
        rep = rep.metric("broker_rtt_p99", Metric::minimize(v, "s"));
    }
    if let Some(v) = outbuf_p99 {
        rep = rep.metric("broker_outbuf_latency_p99", Metric::minimize(v, "s"));
    }
    if let Some(v) = int_p99 {
        rep = rep.metric("broker_int_latency_p99", Metric::minimize(v, "s"));
    }
    if let Some(p50) =
        prom::histogram_quantile_delta(&text0, &text1, "etl_e2e_latency_seconds", 0.5)
    {
        rep = rep.metric("e2e_p50", Metric::minimize(p50, "s"));
    }
    if let Some(p99) =
        prom::histogram_quantile_delta(&text0, &text1, "etl_e2e_latency_seconds", 0.99)
    {
        rep = rep.metric("e2e_p99", Metric::minimize(p99, "s"));
    }
    rep.note(format!(
        "producer-sink saturation: memory source -> Kafka; rps = \
         etl_sink_records_total delta / window; upstream={upstream} \
         (pauses={pauses}, paused_edge={paused_edge}, pend_max={pend_max}); \
         downstream={downstream} (queue_peak={queue_peak:.0}/{qbm} \
         ratio={queue_full_ratio:.2}, outbuf_p99={outbuf:?}, over_raw={over:?}); \
         budget_mb={max_inflight_mb} derived_mb={derived_mb}; window_s={window:.1}; \
         produced_delta={produced_delta}; exit={:?}",
        exit.state,
        pend_max = pend0.max(pend1),
        qbm = cfg.queue_buffering,
        outbuf = outbuf_p99,
        over = framework_over_raw,
    ))
    .emit();
}

/// Delivery-counting context for the raw baseline: counts only durable
/// (successful) delivery reports, so the raw throughput is directly comparable
/// to the framework's `Ok`-after-every-report durable throughput.
struct RawCtx {
    delivered: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
}

impl ClientContext for RawCtx {}

impl ProducerContext for RawCtx {
    type DeliveryOpaque = ();
    fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
        match result {
            Ok(_) => {
                self.delivered.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// The raw baseline: a bare `ThreadedProducer` with the *same forced settings*
/// as the sink (`acks=all`, `enable.idempotence`, matched message/timeout/queue
/// caps), producing the same-size payloads unframed and gating on delivery
/// reports. This is the broker-headroom oracle and the Q1 overhead denominator.
fn run_raw(bootstrap: &str, cfg: &Cfg) {
    let delivered = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let producer: ThreadedProducer<RawCtx> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("message.max.bytes", "1000000")
        .set("message.timeout.ms", "30000")
        .set(
            "queue.buffering.max.messages",
            cfg.queue_buffering.to_string(),
        )
        .create_with_context(RawCtx {
            delivered: Arc::clone(&delivered),
            errors: Arc::clone(&errors),
        })
        .expect("raw producer");

    let stop = Arc::new(AtomicBool::new(false));
    let payload = Arc::new(vec![0xa5u8; cfg.payload]);
    let topic = Arc::new(cfg.topic.clone());
    let senders: Vec<_> = (0..cfg.raw_send_threads.max(1))
        .map(|_| {
            let (p, stop, payload, topic) = (
                producer.clone(),
                Arc::clone(&stop),
                Arc::clone(&payload),
                Arc::clone(&topic),
            );
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    // Keyless, no explicit partition — librdkafka's partitioner
                    // spreads across partitions, matching the framework's keyless
                    // records.
                    match p.send(BaseRecord::<(), [u8]>::to(&topic).payload(&payload)) {
                        Ok(()) => {}
                        Err((e, _))
                            if e.rdkafka_error_code() == Some(RDKafkaErrorCode::QueueFull) =>
                        {
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        Err(_) => std::thread::sleep(Duration::from_millis(1)),
                    }
                }
            })
        })
        .collect();

    std::thread::sleep(cfg.warmup);
    let d0 = delivered.load(Ordering::Relaxed);
    let t0 = Instant::now();
    std::thread::sleep(cfg.duration);
    let d1 = delivered.load(Ordering::Relaxed);
    let window = t0.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    for h in senders {
        h.join().expect("raw sender");
    }
    producer.flush(Duration::from_secs(30)).expect("flush");

    let delivered_delta = d1.saturating_sub(d0) as f64;
    let rps = if window > 0.0 {
        delivered_delta / window
    } else {
        0.0
    };
    let byte_rate = rps * cfg.payload as f64;

    cfg.base_variants(Report::measurement("kafka_sink_saturation"))
        .variant("threads", cfg.raw_send_threads as u64)
        .metric("records_per_s", Metric::maximize(rps, "records/s"))
        .metric("mb_per_s", Metric::bytes_per_s(byte_rate))
        .metric("sink_records", Metric::maximize(delivered_delta, "records"))
        .note(format!(
            "raw rdkafka baseline (acks=all + idempotence, unframed): rps = \
             durable delivery reports / window; broker-headroom oracle + Q1 \
             overhead denominator; send_threads={}; errors={}; \
             queue_buffering={}; window_s={window:.1}",
            cfg.raw_send_threads,
            errors.load(Ordering::Relaxed),
            cfg.queue_buffering,
        ))
        .emit();
}

fn run_one(bootstrap: &str) {
    let cfg = Cfg::from_env();
    ensure_bench_topic(bootstrap, &cfg.topic, cfg.partitions);
    match cfg.mode.as_str() {
        "raw" => run_raw(bootstrap, &cfg),
        "framework" => run_framework(bootstrap, &cfg),
        other => {
            eprintln!("unknown MODE={other} (expected framework|raw)");
            std::process::exit(2);
        }
    }
}

fn main() {
    etl_core::telemetry::init(etl_core::telemetry::LogFormat::Pretty, "info");

    // Resolve the broker: external BOOTSTRAP, else the local bench container
    // (honouring scalar KAFKA_CPUS + FRESH). The parent owns the broker; children
    // reuse it via BOOTSTRAP so only one process manages docker.
    let bootstrap = std::env::var("BOOTSTRAP").unwrap_or_else(|_| docker::ensure_kafka());

    if env_u64("RUN_ONE", 0) != 0 {
        run_one(&bootstrap);
        return;
    }

    let topic = env_str("TOPIC", "bench-kafka-sink");
    let partitions = env_u64("PARTITIONS", 16) as i32;
    ensure_bench_topic(&bootstrap, &topic, partitions);

    // Sweep exactly one env dimension (SWEEP over SWEEP_LIST); other knobs are
    // held from the scalar env. KAFKA_CPUS is varied across separate invocations
    // (a shell loop with FRESH=1), per the methodology doc — not here.
    let sweep = env_str("SWEEP", "");
    let list = env_str("SWEEP_LIST", "");
    let reps = env_u64("REPS", 3);
    let values: Vec<String> = if list.is_empty() {
        vec![String::new()]
    } else {
        list.split(',').map(|s| s.trim().to_owned()).collect()
    };

    for value in &values {
        for rep in 0..reps {
            if sweep.is_empty() {
                eprintln!("── kafka_sink_saturation rep {rep} ──");
            } else {
                eprintln!("── kafka_sink_saturation {sweep}={value} rep {rep} ──");
            }
            let mut cmd = std::process::Command::new(std::env::current_exe().expect("exe"));
            cmd.env("RUN_ONE", "1")
                .env("BOOTSTRAP", &bootstrap)
                .env_remove("SWEEP")
                .env_remove("SWEEP_LIST")
                .env_remove("FRESH");
            if !sweep.is_empty() {
                cmd.env(&sweep, value);
            }
            let status = cmd.status().expect("child run");
            assert!(status.success(), "child failed for {sweep}={value}");
        }
    }
}
