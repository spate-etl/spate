//! Shared harness for the Docker-backed end-to-end suite.
//!
//! Each scenario lives in its **own test binary** (`tests/e2e_*.rs`): the
//! metrics recorder is process-global, so any scenario asserting on
//! `/metrics` needs a fresh process — a shared binary would leave every
//! pipeline after the first with a no-op exporter. Containers are started
//! per binary and shared across that binary's tests.
//!
//! Run the whole suite (Docker required):
//!
//! ```sh
//! cargo test -p etl -- --ignored --test-threads=1
//! ```

// Each binary uses the slice of the harness it needs; `pub` here is
// module-internal API for the scenario binaries.
#![allow(dead_code, unreachable_pub)]

use apache_avro::Schema;
use apache_avro::to_avro_datum;
use etl::avro::AvroDeserializerBuilder;
use etl::backpressure::InflightBudget;
use etl::clickhouse::ClickHouseEncoder;
use etl::config::PipelineConfig;
use etl::error::ErrorPolicy;
use etl::kafka::KafkaSource;
use etl::metrics::{ComponentLabels, E2eBasis, SinkShardMetrics};
use etl::ops::{ChunkConfig, chain_owned};
use etl::pipeline::{
    ExitReport, PipelineRuntime, RuntimeOptions, ShutdownHandle, SinkProbeFn, SinkRuntime,
    StartError,
};
use etl::sink::{KeyHashRouter, ShardWriter, SinkPool, shard_queues};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::process::Command;
use std::time::{Duration, Instant};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

/// The record that travels end to end: Avro in (field names), RowBinary
/// out (field order == the `columns` list in the pipeline config).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Event {
    pub id: i64,
    pub name: String,
}

pub const SCHEMA_ID: u32 = 42;
pub const SCHEMA_JSON: &str = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;
pub const CH_PASSWORD: &str = "e2e";

/// Globally-unique record id: partition-millions + sequence. With a fresh
/// topic and a single ordered producer per partition, `seq` equals the
/// Kafka offset, which the drain/restart scenario relies on.
pub fn event_id(partition: i32, seq: i64) -> i64 {
    i64::from(partition) * 1_000_000 + seq
}

pub struct Harness {
    /// Runtime for the stub registry, ClickHouse queries, and Kafka admin.
    pub rt: tokio::runtime::Runtime,
    kafka: Container<Kafka>,
    ch: Container<GenericImage>,
    pub brokers: String,
    pub ch_url: String,
    pub registry_url: String,
    schema: Schema,
}

impl Harness {
    /// Start Kafka + ClickHouse + the stub schema registry. Slow on first
    /// use (image pulls); panics with context on any failure.
    pub fn up() -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("harness runtime");

        let kafka = Kafka::default().start().expect(
            "start Kafka container (is Docker running? first run pulls apache/kafka — slow)",
        );
        let kafka_port = kafka.get_host_port_ipv4(KAFKA_PORT).expect("kafka port");
        let brokers = format!("127.0.0.1:{kafka_port}");

        // Pinned modern ClickHouse with a password — the stock module image
        // (23.3-alpine, no auth) is unrepresentative of production.
        let ch = GenericImage::new("clickhouse/clickhouse-server", "25.6")
            .with_env_var("CLICKHOUSE_PASSWORD", CH_PASSWORD)
            .start()
            .expect(
                "start ClickHouse container (first run pulls clickhouse/clickhouse-server:25.6)",
            );
        let ch_port = ch.get_host_port_ipv4(8123).expect("clickhouse port");
        let ch_url = format!("http://127.0.0.1:{ch_port}");
        // GenericImage has no ready condition; /ping is unauthenticated
        // and turns 200 once the server accepts connections.
        let ping: SocketAddr = format!("127.0.0.1:{ch_port}").parse().expect("addr");
        wait_until(Duration::from_secs(60), "clickhouse /ping", || {
            std::panic::catch_unwind(|| http_get(ping, "/ping").0 == 200).unwrap_or(false)
        });

        let registry_addr = rt.block_on(serve_stub_registry());
        let registry_url = format!("http://{registry_addr}");

        Harness {
            rt,
            kafka,
            ch,
            brokers,
            ch_url,
            registry_url,
            schema: Schema::parse_str(SCHEMA_JSON).expect("schema"),
        }
    }

    // ── ClickHouse ─────────────────────────────────────────────────────

    pub fn ch_client(&self) -> clickhouse::Client {
        clickhouse::Client::default()
            .with_url(&self.ch_url)
            .with_user("default")
            .with_password(CH_PASSWORD)
    }

    /// Create the target table with a deduplication window (without it,
    /// retry idempotency silently does nothing on plain MergeTree).
    pub fn create_table(&self, table: &str) {
        let ddl = format!(
            "CREATE TABLE {table} (id Int64, name String) \
             ENGINE = MergeTree ORDER BY id \
             SETTINGS non_replicated_deduplication_window = 1000"
        );
        self.rt
            .block_on(self.ch_client().query(&ddl).execute())
            .expect("create table");
    }

    pub fn count(&self, table: &str) -> u64 {
        self.scalar(&format!("SELECT count() FROM {table}"))
    }

    /// Distinct record ids — the row-conservation measure (duplicates from
    /// at-least-once replay inflate `count`, never `uniq`).
    pub fn uniq(&self, table: &str) -> u64 {
        self.scalar(&format!("SELECT uniqExact(id) FROM {table}"))
    }

    pub fn scalar(&self, sql: &str) -> u64 {
        self.rt
            .block_on(self.ch_client().query(sql).fetch_one::<u64>())
            .expect("scalar query")
    }

    /// Pause/unpause the ClickHouse container (simulated sink outage).
    pub fn pause_clickhouse(&self) {
        docker(&["pause", self.ch.id()]);
    }

    pub fn unpause_clickhouse(&self) {
        docker(&["unpause", self.ch.id()]);
    }

    // ── Kafka ──────────────────────────────────────────────────────────

    pub fn create_topic(&self, topic: &str, partitions: i32) {
        let admin: AdminClient<_> = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .create()
            .expect("admin client");
        self.rt
            .block_on(admin.create_topics(
                &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            ))
            .expect("create topic")
            .into_iter()
            .for_each(|r| {
                r.expect("topic created");
            });
    }

    /// Produce Confluent-framed Avro events, keyed by record id (the key
    /// drives shard routing). `events` are (partition, Event) pairs.
    pub fn produce(&self, topic: &str, events: &[(i32, Event)]) {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .create()
            .expect("producer");
        for (partition, event) in events {
            let payload = self.confluent_frame(event);
            let key = event.id.to_string();
            loop {
                match producer.send(
                    BaseRecord::to(topic)
                        .partition(*partition)
                        .payload(&payload)
                        .key(key.as_bytes()),
                ) {
                    Ok(()) => break,
                    // Local queue full: give librdkafka a beat to flush.
                    Err((e, _)) => {
                        producer.poll(Duration::from_millis(50));
                        let _ = e;
                    }
                }
            }
            producer.poll(Duration::ZERO);
        }
        producer
            .flush(Duration::from_secs(30))
            .expect("producer flush");
    }

    /// A payload that fails Avro decoding (valid Confluent header, torn
    /// datum) — exercises the deserializer's Skip policy.
    pub fn produce_poison(&self, topic: &str, partition: i32, n: usize) {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .create()
            .expect("producer");
        let mut frame = Vec::new();
        frame.push(0u8);
        frame.extend_from_slice(&SCHEMA_ID.to_be_bytes());
        frame.extend_from_slice(&[0x0B]); // torn varint string: length 5, no bytes
        for _ in 0..n {
            producer
                .send(
                    BaseRecord::to(topic)
                        .partition(partition)
                        .payload(&frame)
                        .key(b"poison"),
                )
                .map_err(|(e, _)| e)
                .expect("send poison");
            producer.poll(Duration::ZERO);
        }
        producer.flush(Duration::from_secs(30)).expect("flush");
    }

    pub fn confluent_frame(&self, event: &Event) -> Vec<u8> {
        let mut rec = apache_avro::types::Record::new(&self.schema).expect("record");
        rec.put("id", event.id);
        rec.put("name", event.name.as_str());
        let datum = to_avro_datum(&self.schema, rec).expect("avro datum");
        let mut frame = Vec::with_capacity(5 + datum.len());
        frame.push(0u8);
        frame.extend_from_slice(&SCHEMA_ID.to_be_bytes());
        frame.extend_from_slice(&datum);
        frame
    }

    /// Committed offset per partition for a group (0 when none).
    pub fn committed(&self, topic: &str, group: &str, partitions: i32) -> Vec<i64> {
        let probe: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", group)
            .create()
            .expect("probe consumer");
        let mut tpl = rdkafka::TopicPartitionList::new();
        for p in 0..partitions {
            tpl.add_partition(topic, p);
        }
        let committed = probe
            .committed_offsets(tpl, Duration::from_secs(10))
            .expect("committed offsets");
        (0..partitions)
            .map(|p| {
                committed
                    .find_partition(topic, p)
                    .map(|e| match e.offset() {
                        rdkafka::Offset::Offset(o) => o,
                        _ => 0,
                    })
                    .unwrap_or(0)
            })
            .collect()
    }

    // ── Pipeline assembly (mirrors the flagship example) ───────────────

    pub fn spawn_pipeline(&self, params: &PipelineParams) -> RunningPipeline {
        let config = PipelineConfig::from_str(&params.yaml(self)).expect("pipeline config parses");
        // Exporter before handles: the sink metrics below must bind to the
        // real recorder (install is idempotent across scenarios).
        let _metrics = etl::metrics::install(&etl::pipeline::metrics_settings(&config))
            .expect("metrics install");
        let admin: SocketAddr = config.metrics.listen;
        let pipeline_name = config.pipeline.name.clone();

        let io = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("e2e-connectors")
            .enable_all()
            .build()
            .expect("io runtime");

        let source = KafkaSource::from_component_config(&config.source).expect("kafka source");
        let deser_section = config.deserializer.as_ref().expect("deserializer section");
        let avro =
            AvroDeserializerBuilder::from_component(deser_section, io.handle()).expect("avro");
        let deserializer = avro.build_serde::<Event>();

        let sink = etl::clickhouse::config::from_component_config(&config.sink).expect("sink");
        let num_shards = sink.endpoints.len();
        let (queues, receivers) = shard_queues(num_shards, params.queue_capacity);
        let budget = std::sync::Arc::new(InflightBudget::new());

        let sink_labels = ComponentLabels::new(pipeline_name.clone(), "sink", "clickhouse");
        let shard_metrics = sink
            .endpoints
            .iter()
            .enumerate()
            .map(|(shard, replicas)| {
                let urls: Vec<String> = replicas.iter().map(|e| e.url().to_string()).collect();
                SinkShardMetrics::new(
                    &sink_labels,
                    u32::try_from(shard).unwrap_or(0),
                    &urls,
                    E2eBasis::Ingest,
                )
            })
            .collect();

        let pool = SinkPool::spawn(
            std::sync::Arc::new(sink.writer),
            sink.endpoints,
            receivers,
            sink.pool,
            std::sync::Arc::clone(&budget),
            shard_metrics,
            &pipeline_name,
            io.handle(),
        );

        let probe: SinkProbeFn = {
            let probe_sink =
                etl::clickhouse::config::from_component_config(&config.sink).expect("probe sink");
            let writer = std::sync::Arc::new(probe_sink.writer);
            let endpoints = std::sync::Arc::new(probe_sink.endpoints);
            Box::new(move || {
                let writer = std::sync::Arc::clone(&writer);
                let endpoints = std::sync::Arc::clone(&endpoints);
                Box::pin(async move {
                    for shard in endpoints.iter() {
                        for endpoint in shard {
                            writer.probe(endpoint).await?;
                        }
                    }
                    Ok(())
                })
            })
        };

        let sink_runtime = SinkRuntime {
            queues: queues.clone(),
            drain: Box::new(move |deadline| Box::pin(async move { pool.drain(deadline).await })),
            probe: Some(probe),
        };

        let chunk_bytes = params.chunk_bytes;
        let chains = {
            let queues = queues;
            let budget = std::sync::Arc::clone(&budget);
            let name = pipeline_name.clone();
            move |_thread: usize| {
                chain_owned::<Event, _>(deserializer.clone())
                    .with_metrics(name.clone(), "main")
                    .try_map(Ok::<Event, &str>, ErrorPolicy::Skip)
                    .sink(
                        ClickHouseEncoder::<Event>::new(),
                        KeyHashRouter,
                        ChunkConfig {
                            target_bytes: chunk_bytes,
                            ..ChunkConfig::default()
                        },
                        queues.clone(),
                        std::sync::Arc::clone(&budget),
                    )
                    .build()
            }
        };

        let runtime = PipelineRuntime::new(config, source, chains, sink_runtime, budget)
            .with_options(RuntimeOptions {
                handle_signals: false,
                ..RuntimeOptions::default()
            });
        let shutdown = runtime.shutdown_handle();
        let join = std::thread::Builder::new()
            .name("e2e-pipeline".into())
            .spawn(move || runtime.run())
            .expect("spawn pipeline thread");

        RunningPipeline {
            shutdown,
            join,
            admin,
            _io: io,
        }
    }
}

/// Everything scenario-specific about one pipeline instance.
pub struct PipelineParams {
    pub name: &'static str,
    pub topic: String,
    pub group: String,
    pub table: String,
    pub admin_port: u16,
    pub threads: usize,
    pub shards: usize,
    pub queue_capacity: usize,
    pub chunk_bytes: usize,
    pub batch_max_rows: u64,
    pub linger: &'static str,
    pub commit_interval: &'static str,
    pub drain_timeout: &'static str,
}

impl PipelineParams {
    pub fn defaults(name: &'static str, admin_port: u16) -> Self {
        PipelineParams {
            name,
            topic: format!("{name}-topic"),
            group: format!("{name}-group"),
            table: format!("{}_rows", name.replace('-', "_")),
            admin_port,
            threads: 2,
            shards: 2,
            queue_capacity: 8,
            chunk_bytes: 16 * 1024,
            batch_max_rows: 2_000,
            linger: "200ms",
            commit_interval: "500ms",
            drain_timeout: "20s",
        }
    }

    fn yaml(&self, h: &Harness) -> String {
        let shard_lines: String = (0..self.shards)
            .map(|_| format!("      - replicas: [\"{}\"]\n", h.ch_url))
            .collect();
        format!(
            r#"
pipeline:
  name: {name}
  threads: {threads}
  io_threads: 2
checkpoint:
  interval: {commit}
  max_pending_batches: 1024
  drain_timeout: {drain}
backpressure:
  max_inflight_bytes: 64MiB
metrics:
  exporter: prometheus
  listen: 127.0.0.1:{port}
source:
  kafka:
    brokers: {brokers}
    topic: {topic}
    group_id: {group}
    commit_interval: {commit}
    rdkafka:
      auto.offset.reset: earliest
deserializer:
  avro:
    mode: confluent
    registry:
      url: {registry}
sink:
  clickhouse:
    table: {table}
    columns: [id, name]
    shards:
{shards_yaml}    user: default
    password: {password}
    batch:
      max_rows: {max_rows}
      max_bytes: 8MiB
      linger: {linger}
    inflight:
      max_per_shard: 2
"#,
            name = self.name,
            threads = self.threads,
            commit = self.commit_interval,
            drain = self.drain_timeout,
            port = self.admin_port,
            brokers = h.brokers,
            topic = self.topic,
            group = self.group,
            registry = h.registry_url,
            table = self.table,
            shards_yaml = shard_lines,
            password = CH_PASSWORD,
            max_rows = self.batch_max_rows,
            linger = self.linger,
        )
    }
}

pub struct RunningPipeline {
    pub shutdown: ShutdownHandle,
    pub join: std::thread::JoinHandle<Result<ExitReport, StartError>>,
    pub admin: SocketAddr,
    _io: tokio::runtime::Runtime,
}

impl RunningPipeline {
    /// Trigger the drain and wait for the exit report.
    pub fn stop(self) -> ExitReport {
        self.shutdown.trigger();
        self.join
            .join()
            .expect("pipeline thread panicked")
            .expect("pipeline start failed")
    }
}

// ── Plumbing ───────────────────────────────────────────────────────────

fn docker(args: &[&str]) {
    let out = Command::new("docker")
        .args(args)
        .output()
        .expect("run docker CLI");
    assert!(
        out.status.success(),
        "docker {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Minimal Confluent-compatible registry: serves `SCHEMA_JSON` for
/// `/schemas/ids/{SCHEMA_ID}`, 404 elsewhere.
async fn serve_stub_registry() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stub registry");
    let addr = listener.local_addr().expect("stub addr");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let io = hyper_util::rt::TokioIo::new(stream);
                let service = service_fn(|req: Request<hyper::body::Incoming>| async move {
                    let known = format!("/schemas/ids/{SCHEMA_ID}");
                    let (status, body) = if req.uri().path() == known {
                        (
                            StatusCode::OK,
                            serde_json::json!({ "schema": SCHEMA_JSON }).to_string(),
                        )
                    } else {
                        (
                            StatusCode::NOT_FOUND,
                            r#"{"error_code":40403,"message":"Schema not found"}"#.to_string(),
                        )
                    };
                    Ok::<_, std::convert::Infallible>(
                        Response::builder()
                            .status(status)
                            .header("content-type", "application/vnd.schemaregistry.v1+json")
                            .body(Full::new(Bytes::from(body)))
                            .expect("response"),
                    )
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await;
            });
        }
    });
    addr
}

/// Blocking HTTP GET against the pipeline's admin server.
pub fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut stream = std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .expect("connect admin server");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: e2e\r\nConnection: close\r\n\r\n"
    )
    .expect("write request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");
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

/// Sum every sample of a metric family in a Prometheus exposition body
/// (label sets differ; the family total is what scenarios assert on).
pub fn metric_sum(body: &str, family: &str) -> f64 {
    body.lines()
        .filter(|l| {
            !l.starts_with('#')
                && (l.starts_with(&format!("{family}{{")) || l.starts_with(&format!("{family} ")))
        })
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

/// Poll `check` until it returns true or `timeout` elapses.
pub fn wait_until(timeout: Duration, what: &str, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check() {
            return;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    panic!("timed out after {timeout:?} waiting for: {what}");
}

/// Events for `total` records spread round-robin over `partitions`, with
/// per-partition sequence numbers encoded into the id.
pub fn events(partitions: i32, total: usize) -> Vec<(i32, Event)> {
    let mut seqs = vec![0i64; usize::try_from(partitions).expect("partitions")];
    (0..total)
        .map(|i| {
            let p = i32::try_from(i % usize::try_from(partitions).unwrap()).unwrap();
            let seq = seqs[usize::try_from(p).unwrap()];
            seqs[usize::try_from(p).unwrap()] += 1;
            (
                p,
                Event {
                    id: event_id(p, seq),
                    name: format!("evt-{p}-{seq}"),
                },
            )
        })
        .collect()
}
