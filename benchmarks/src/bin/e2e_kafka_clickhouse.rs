//! Full-pipeline benchmark: Kafka → chain → sharded ClickHouse writes,
//! measured through the pipeline's own metrics plus a ClickHouse row-count
//! cross-check.
//!
//! Profiles:
//! - **Local** (default): starts/reuses `etl-bench-kafka` and
//!   `etl-bench-clickhouse` containers, generates load itself.
//! - **External**: set KAFKA_BROKERS and CLICKHOUSE_URL (http://host:port)
//!   [+ CLICKHOUSE_USER/CLICKHOUSE_PASSWORD] — pure env config, runnable
//!   in Kubernetes against real clusters.
//!
//! Env: DURATION_S (60) RATE (100000 rec/s) PARTITIONS (4) THREADS (2)
//! PAYLOAD (64) TOPIC (bench-e2e) METRICS_PORT (19096) RESULTS (JSONL path)
#![allow(clippy::print_stdout, clippy::print_stderr)]

use benchmarks::{docker, ensure_topic, env_str, env_u64, prom, report};
use etl_core::backpressure::InflightBudget;
use etl_core::config::{ComponentConfig, PipelineConfig};
use etl_core::deser::BytesPassthrough;
use etl_core::metrics::{ComponentLabels, SinkShardMetrics};
use etl_core::ops::{ChunkConfig, chain_owned};
use etl_core::pipeline::{DrainReport, PipelineRuntime, RuntimeOptions, SinkRuntime};
use etl_core::sink::{KeyHashRouter, SinkPool, shard_queues};
use etl_kafka::{KafkaSource, KafkaSourceConfig};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// The row this pipeline writes. Field order == `columns` order — the
/// RowBinary wire contract.
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

/// Rate-controlled raw producer, running until `stop`.
fn produce_load(
    bootstrap: &str,
    topic: &str,
    partitions: i32,
    rate: u64,
    payload_size: usize,
    stop: &AtomicBool,
    produced: &AtomicU64,
) {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("linger.ms", "5")
        .set("batch.size", "1048576")
        .create()
        .expect("producer");
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

fn main() {
    etl_core::telemetry::init(etl_core::telemetry::LogFormat::Pretty, "info");
    let duration = Duration::from_secs(env_u64("DURATION_S", 60));
    let rate = env_u64("RATE", 100_000);
    let partitions = env_u64("PARTITIONS", 4) as i32;
    let threads = env_u64("THREADS", 2) as usize;
    let payload_size = env_u64("PAYLOAD", 64) as usize;
    let topic = env_str("TOPIC", &format!("bench-e2e-{}", std::process::id()));

    // ── Infrastructure ──────────────────────────────────────────────────
    let external_kafka = std::env::var("KAFKA_BROKERS").ok();
    let external_ch = std::env::var("CLICKHOUSE_URL").ok();
    let brokers = external_kafka.unwrap_or_else(docker::ensure_kafka);
    let (ch_url, ch_host, ch_port, ch_user, ch_password) = match external_ch {
        Some(url) => {
            let hp = url.trim_start_matches("http://").to_owned();
            let (h, p) = hp.split_once(':').expect("CLICKHOUSE_URL host:port");
            (
                url.clone(),
                h.to_owned(),
                p.parse::<u16>().expect("port"),
                env_str("CLICKHOUSE_USER", "default"),
                env_str("CLICKHOUSE_PASSWORD", ""),
            )
        }
        None => {
            let (h, p, u, pw) = docker::ensure_clickhouse();
            (format!("http://{h}:{p}"), h, p, u, pw)
        }
    };

    let sql = |q: &str| {
        docker::clickhouse_sql(&ch_host, ch_port, &ch_user, &ch_password, q).expect("clickhouse")
    };
    sql("DROP TABLE IF EXISTS bench_events");
    sql(
        "CREATE TABLE bench_events (id UInt64, body String) ENGINE = MergeTree ORDER BY id \
         SETTINGS non_replicated_deduplication_window = 100",
    );
    ensure_topic(&brokers, &topic, partitions);

    // ── Load ────────────────────────────────────────────────────────────
    let stop = Arc::new(AtomicBool::new(false));
    let produced = Arc::new(AtomicU64::new(0));
    let loadgen = {
        let (brokers, topic) = (brokers.clone(), topic.clone());
        let (stop, produced) = (Arc::clone(&stop), Arc::clone(&produced));
        std::thread::spawn(move || {
            produce_load(
                &brokers,
                &topic,
                partitions,
                rate,
                payload_size,
                &stop,
                &produced,
            );
        })
    };

    // ── Pipeline ────────────────────────────────────────────────────────
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
pipeline: {{ name: bench-e2e, threads: {threads} }}
checkpoint: {{ interval: 1s }}
metrics: {{ exporter: none }}
source: {{ kafka: {{}} }}
sink: {{ clickhouse: {{}} }}
"
    ))
    .expect("config");

    let source = KafkaSource::new(KafkaSourceConfig {
        brokers: brokers.clone(),
        topic: topic.clone(),
        group_id: format!("bench-e2e-{}", std::process::id()),
        commit_interval: Duration::from_secs(1),
        startup_timeout: Duration::from_secs(30),
        statistics_interval: Duration::from_secs(5),
        rdkafka: BTreeMap::from([("auto.offset.reset".to_owned(), "earliest".to_owned())]),
    });

    let sink_yaml = format!(
        r"
clickhouse:
  table: bench_events
  columns: [id, body]
  user: {ch_user}
  password: {ch_password:?}
  shards:
    - replicas: [{ch_url:?}]
  batch: {{ linger: 500ms, max_rows: 262144 }}
"
    );
    let section: ComponentConfig = serde_yaml::from_str(&sink_yaml).expect("sink section");
    let ch = etl_clickhouse::from_component_config(&section).expect("clickhouse sink");
    let shards = ch.endpoints.len();

    let (queues, receivers) = shard_queues(shards, 8);
    let budget = Arc::new(InflightBudget::new());
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("io runtime");
    let labels = ComponentLabels::new("bench-e2e", "sink", "clickhouse");
    let metrics = (0..shards)
        .map(|s| {
            SinkShardMetrics::new(
                &labels,
                u32::try_from(s).expect("shard"),
                &[format!("replica-{s}-0")],
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
        drain: Box::new(move |deadline| {
            Box::pin(async move {
                let r = pool.drain(deadline).await;
                DrainReport {
                    flushed_batches: r.flushed,
                    abandoned_batches: r.abandoned,
                }
            })
        }),
        probe: None,
    };

    let chain_queues = queues;
    let chain_budget = Arc::clone(&budget);
    let chains = move |_thread: usize| {
        chain_owned::<Vec<u8>, _>(BytesPassthrough)
            .with_metrics("bench-e2e", "main")
            .map(parse_row)
            .sink(
                etl_clickhouse::ClickHouseEncoder::<BenchRow>::new(),
                KeyHashRouter,
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

    // ── Measure ─────────────────────────────────────────────────────────
    std::thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);
    loadgen.join().expect("loadgen");
    let sent = produced.load(Ordering::Relaxed);

    // Let the pipeline drain the tail: wait until the ClickHouse count is
    // stable (or a bounded grace period passes).
    let count = |raw: &str| raw.trim().parse::<u64>().unwrap_or(0);
    let mut last = count(&sql("SELECT count() FROM bench_events"));
    let grace = Instant::now() + Duration::from_secs(30);
    loop {
        std::thread::sleep(Duration::from_secs(2));
        let now = count(&sql("SELECT count() FROM bench_events"));
        if (now == last && now >= sent) || Instant::now() > grace {
            last = now;
            break;
        }
        last = now;
    }

    let metrics_text = metrics_handle.render();
    shutdown.trigger();
    let exit = pipeline.join().expect("pipeline").expect("run");

    let rows = count(&sql("SELECT count() FROM bench_events"));
    report(&serde_json::json!({
        "bench": "e2e_kafka_clickhouse",
        "threads": threads,
        "partitions": partitions,
        "target_rate": rate,
        "window_s": duration.as_secs(),
        "records_produced": sent,
        "rows_in_clickhouse": rows,
        "rows_per_s": rows as f64 / duration.as_secs_f64(),
        "e2e_p50_s": prom::histogram_quantile(&metrics_text, "etl_e2e_latency_seconds", 0.5),
        "e2e_p99_s": prom::histogram_quantile(&metrics_text, "etl_e2e_latency_seconds", 0.99),
        "sink_flush_p99_s":
            prom::histogram_quantile(&metrics_text, "etl_sink_flush_duration_seconds", 0.99),
        "backpressure_pauses":
            prom::value(&metrics_text, "etl_backpressure_pause_events_total", ""),
        "exit": format!("{:?}", exit.state),
        "note": if rows >= sent { "complete" } else { "tail replayed on next run (at-least-once)" },
    }));
    let _ = last;
}
