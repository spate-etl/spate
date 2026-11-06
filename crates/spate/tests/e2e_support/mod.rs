//! Shared harness for the Docker-backed end-to-end suite.
//!
//! Each scenario lives in its **own test binary** (`tests/e2e_*.rs`): the
//! metrics recorder is process-global, so any scenario asserting on
//! `/metrics` needs a fresh process. A shared binary would leave every
//! pipeline after the first with a no-op exporter. Containers are started
//! per binary and shared across that binary's tests.
//!
//! [`Harness::spawn_pipeline`] starts a [`Sampler`] with each pipeline, so every
//! scenario checks INV-1 and the pending-batch limit for its whole run.
//!
//! Run the whole suite (Docker required):
//!
//! ```sh
//! cargo test -p spate -- --ignored --test-threads=1
//! ```

#![expect(deprecated, reason = "fixtures call the datum free functions directly")]
// Each binary uses the slice of the harness it needs; `pub` here is
// module-internal API for the scenario binaries.
#![allow(dead_code, unreachable_pub)]
// What a run sampled and how it drained go to the test log by design.
#![allow(clippy::print_stdout)]

use apache_avro::Schema;
use apache_avro::to_avro_datum;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use serde::{Deserialize, Serialize};
use spate::avro::AvroDeserializerBuilder;
use spate::clickhouse::{ClickHouseEncoder, config};
use spate::config::PipelineConfig;
use spate::error::ErrorPolicy;
use spate::hash::FastMap;
use spate::kafka::KafkaSource;
use spate::ops::{ChunkConfig, chain_owned};
use spate::pipeline::{
    ExitReport, Pipeline, RuntimeOptions, ShutdownHandle, SinkOptions, StartError,
};
use spate::sink::KeyHashRouter;
use spate_test::{metric_series, metric_value};
use spate_test_support::{container_image, http};
use std::net::SocketAddr;
use std::ops::Range;
use std::process::Command;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

/// The record that travels end to end: Avro in (field names), RowBinary out
/// (field order is `#[derive(ClickHouseRow)]`'s generated column list).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, spate::clickhouse::ClickHouseRow)]
pub struct Event {
    pub id: i64,
    pub name: String,
}

pub const SCHEMA_ID: u32 = 42;
pub const SCHEMA_JSON: &str = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;
pub const CH_PASSWORD: &str = "e2e";

/// Schemas the stub registry serves, by id. Shared with the server task so a
/// scenario whose row shape is not [`Event`] can publish its own.
type Schemas = Arc<Mutex<FastMap<u32, String>>>;

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
    schemas: Schemas,
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

        // The broker `ci/kafka/` pins, fetched by digest and re-tagged so this
        // starts the pinned bytes.
        let (kafka_image, kafka_tag) = container_image(&["--pull", "kafka"]);
        let kafka = Kafka::default()
            .with_name(&kafka_image)
            .with_tag(&kafka_tag)
            .start()
            .unwrap_or_else(|e| panic!("start Kafka container {kafka_image}:{kafka_tag}: {e}"));
        let kafka_port = kafka.get_host_port_ipv4(KAFKA_PORT).expect("kafka port");
        let brokers = format!("127.0.0.1:{kafka_port}");

        // The same pinned server the ClickHouse suite runs, fetched by digest
        // and re-tagged so this starts the pinned bytes.
        let (ch_image, ch_tag) = container_image(&["--pull", "clickhouse"]);
        let ch = GenericImage::new(&ch_image, &ch_tag)
            .with_env_var("CLICKHOUSE_PASSWORD", CH_PASSWORD)
            .start()
            .unwrap_or_else(|e| panic!("start ClickHouse container {ch_image}:{ch_tag}: {e}"));
        let ch_port = ch.get_host_port_ipv4(8123).expect("clickhouse port");
        let ch_url = format!("http://127.0.0.1:{ch_port}");
        // GenericImage has no ready condition. `/ping` answers before the
        // entrypoint has necessarily applied `CLICKHOUSE_PASSWORD`, so wait for
        // a query as the user the pipeline connects as.
        let ch_addr = SocketAddr::from(([127, 0, 0, 1], ch_port));
        let query = format!("/?user=default&password={CH_PASSWORD}&query=SELECT%201");
        wait_until(
            Duration::from_secs(60),
            "clickhouse to answer a query",
            || http(ch_addr, "GET", &query).is_ok_and(|(status, _)| status == 200),
        );

        let schemas: Schemas = Arc::new(Mutex::new(FastMap::from_iter([(
            SCHEMA_ID,
            SCHEMA_JSON.to_string(),
        )])));
        let registry_addr = rt.block_on(serve_stub_registry(Arc::clone(&schemas)));
        let registry_url = format!("http://{registry_addr}");

        Harness {
            rt,
            kafka,
            ch,
            brokers,
            ch_url,
            registry_url,
            schema: Schema::parse_str(SCHEMA_JSON).expect("schema"),
            schemas,
        }
    }

    /// Publish one more schema in the stub registry, so a scenario whose
    /// record shape is not [`Event`] can frame payloads against its own id.
    pub fn register_schema(&self, id: u32, json: &str) {
        self.schemas
            .lock()
            .expect("registry schemas")
            .insert(id, json.to_string());
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

    /// Distinct record ids, the row-conservation measure (duplicates from
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

    #[expect(
        clippy::disallowed_methods,
        reason = "the harness administers the broker outside the connector's client factory"
    )]
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
    #[expect(
        clippy::disallowed_methods,
        reason = "the harness seeds the topic outside the connector's client factory"
    )]
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
    /// datum), exercising the deserializer's Skip policy.
    #[expect(
        clippy::disallowed_methods,
        reason = "the harness seeds the topic outside the connector's client factory"
    )]
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
        read_committed(
            &probe_consumer(&self.brokers, group),
            topic,
            partitions,
            Duration::from_secs(10),
        )
        .expect("committed offsets")
    }

    // ── Pipeline assembly (mirrors the flagship example) ───────────────

    pub fn spawn_pipeline(&self, params: &PipelineParams) -> RunningPipeline {
        let config = PipelineConfig::from_str(&params.yaml(self)).expect("pipeline config parses");
        let partitions = partition_count(&probe_consumer(&self.brokers, &params.group), params);
        let admin: SocketAddr = config
            .admin
            .listen
            .expect("the scenario names an admin address");

        // The builder owns init: exporter before any handle (idempotent
        // across scenarios), the shared I/O runtime, queues, pool, probe.
        let pipeline = Pipeline::from_config(config).expect("pipeline builder");
        let source =
            KafkaSource::from_component_config(&pipeline.config().source).expect("kafka source");
        let deser_section = pipeline
            .config()
            .deserializer
            .as_ref()
            .expect("deserializer section");
        let deserializer =
            AvroDeserializerBuilder::from_component(deser_section, &pipeline.io_handle())
                .expect("avro")
                .build_serde::<Event>()
                .expect("apache builder");
        let sink = pipeline.block_on(
            config::from_component_config(
                pipeline
                    .config()
                    .sink_config("default")
                    .expect("sink config"),
            )
            .expect("sink")
            .with_row::<spate::deser::Owned<Event>>(),
        );
        let sink = sink.expect("schema fetch");
        let schema = sink.schema();

        let chunk_bytes = params.chunk_bytes;
        let runtime = pipeline
            .sink_with(
                sink,
                SinkOptions::default().with_queue_capacity(params.queue_capacity),
            )
            .expect("sink assembly")
            .chains(move |ctx| {
                chain_owned::<Event, _>(deserializer.clone())
                    .with_metrics(ctx.pipeline, "main")
                    .try_map(Ok::<Event, &str>, ErrorPolicy::Skip)
                    .sink(
                        ClickHouseEncoder::<spate::deser::Owned<Event>>::with_schema(
                            schema.clone(),
                        ),
                        KeyHashRouter,
                        ChunkConfig {
                            target_bytes: chunk_bytes,
                            ..ChunkConfig::default()
                        },
                        ctx.queues,
                        ctx.budget,
                    )
                    .build()
            })
            .runtime_options(RuntimeOptions {
                handle_signals: false,
                ..RuntimeOptions::default()
            })
            .into_runtime(source)
            .expect("into_runtime");
        let shutdown = runtime.shutdown_handle();
        #[expect(
            clippy::disallowed_methods,
            reason = "`RunningPipeline` joins the pipeline thread"
        )]
        let join = std::thread::Builder::new()
            .name("e2e-pipeline".into())
            .spawn(move || runtime.run())
            .expect("spawn pipeline thread");
        let sampler = Sampler::start(SampleTarget {
            pipeline: params.name,
            brokers: self.brokers.clone(),
            ch_client: self.ch_client(),
            admin,
            topic: params.topic.clone(),
            group: params.group.clone(),
            table: params.table.clone(),
            partitions,
            holes: params.holes.clone(),
            max_pending_batches: params.max_pending_batches,
        });

        RunningPipeline {
            shutdown,
            join,
            admin,
            name: params.name,
            topic: params.topic.clone(),
            group: params.group.clone(),
            table: params.table.clone(),
            partitions,
            sampler,
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
    /// Concurrent sealed batches per shard. Lowering it to 1 makes a sink
    /// outage saturate the in-flight window immediately, which is what the
    /// drain-during-outage scenario needs to set up deterministically.
    pub inflight_max_per_shard: usize,
    /// `source.kafka.consumers`: the Kafka consumers the instance runs.
    pub kafka_consumers: usize,
    /// `checkpoint.max_pending_batches`, the limit the [`Sampler`] holds every
    /// pending reading to.
    pub max_pending_batches: usize,
    /// `(partition, offsets)` that write no row, such as a skipped record. A
    /// record's id otherwise equals [`event_id`] of its offset.
    pub holes: Vec<(i32, Range<i64>)>,
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
            inflight_max_per_shard: 2,
            kafka_consumers: 1,
            max_pending_batches: 1024,
            holes: Vec::new(),
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
admin:
  listen: 127.0.0.1:{port}
checkpoint:
  interval: {commit}
  max_pending_batches: {max_pending}
  drain_timeout: {drain}
backpressure:
  max_inflight_bytes: 64MiB
metrics:
  exporter: prometheus
source:
  kafka:
    brokers: {brokers}
    topic: {topic}
    group_id: {group}
    consumers: {consumers}
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
    shards:
{shards_yaml}    user: default
    password: {password}
    batch:
      max_rows: {max_rows}
      max_bytes: 8MiB
      linger: {linger}
    inflight:
      max_per_shard: {inflight}
"#,
            name = self.name,
            threads = self.threads,
            commit = self.commit_interval,
            max_pending = self.max_pending_batches,
            drain = self.drain_timeout,
            port = self.admin_port,
            brokers = h.brokers,
            topic = self.topic,
            group = self.group,
            consumers = self.kafka_consumers,
            registry = h.registry_url,
            table = self.table,
            shards_yaml = shard_lines,
            password = CH_PASSWORD,
            max_rows = self.batch_max_rows,
            linger = self.linger,
            inflight = self.inflight_max_per_shard,
        )
    }
}

/// Whether a drained check reads the lag gauge.
#[derive(Clone, Copy)]
enum Lag {
    Read,
    Ignored,
}

pub struct RunningPipeline {
    pub shutdown: ShutdownHandle,
    pub join: JoinHandle<Result<ExitReport, StartError>>,
    pub admin: SocketAddr,
    name: &'static str,
    topic: String,
    group: String,
    table: String,
    partitions: i32,
    /// A path that takes this out of the pipeline owes it [`Sampler::finish`].
    pub sampler: Sampler,
}

impl RunningPipeline {
    /// Trigger the drain, wait for the exit report, and finish the sampler.
    ///
    /// # Panics
    ///
    /// Panics on anything [`Sampler::finish`] panics on.
    pub fn stop(self) -> ExitReport {
        self.stop_sampled().0
    }

    /// [`Self::stop`], also returning what the sampler saw.
    pub fn stop_sampled(self) -> (ExitReport, SampleReport) {
        self.shutdown.trigger();
        let report = self
            .join
            .join()
            .expect("pipeline thread panicked")
            .expect("pipeline start failed");
        (report, self.sampler.finish())
    }

    /// Wait until the run has nothing left to do: `expected` distinct rows, lag
    /// reading zero on every partition, and every partition committed to its end.
    ///
    /// A partition this member lost also reads zero lag, which the committed
    /// offsets rule out. A partition whose lag was never measured has no series,
    /// and fails.
    ///
    /// # Panics
    ///
    /// Panics when that state does not arrive under a generous deadline.
    pub fn assert_drained(&self, h: &Harness, expected: u64) {
        self.wait_drained(h, expected, Lag::Read);
    }

    /// [`Self::assert_drained`] with no lag reading, for a scenario whose faults
    /// leave the gauge above zero on a committed partition (#1774).
    pub fn assert_drained_ignoring_lag(&self, h: &Harness, expected: u64) {
        self.wait_drained(h, expected, Lag::Ignored);
    }

    fn wait_drained(&self, h: &Harness, expected: u64, lag: Lag) {
        let probe = probe_consumer(&h.brokers, &self.group);
        let mut seen = String::new();
        wait_until(Duration::from_secs(180), "the run to drain", || {
            let rows = h.uniq(&self.table);
            if rows != expected {
                return false;
            }
            let lags = match lag {
                Lag::Read => self.lags(),
                Lag::Ignored => Vec::new(),
            };
            let committed = read_committed(&probe, &self.topic, self.partitions, PROBE_TIMEOUT)
                .expect("committed offsets");
            let ends = read_ends(&probe, &self.topic, self.partitions);
            let state = format!("lag {lags:?}, committed {committed:?}, end {ends:?}");
            if state != seen {
                println!("all {rows} rows written: {state}");
                seen = state;
            }
            lags.iter().all(|lag| *lag == Some(0.0)) && committed == ends
        });
    }

    /// The lag gauge per partition, `None` where it has no series.
    fn lags(&self) -> Vec<Option<f64>> {
        let (_, body) = http_get(self.admin, "/metrics");
        (0..self.partitions)
            .map(|p| {
                let partition = p.to_string();
                metric_value(
                    &body,
                    "spate_source_lag_records",
                    &[("pipeline", self.name), ("partition", &partition)],
                )
            })
            .collect()
    }
}

// ── Sampling ───────────────────────────────────────────────────────────

/// How long one probe waits on a server that may be paused.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// The gap between samples. No outcome depends on it, and [`Sampler::finish`]
/// ends the wait at once.
const SAMPLE_CADENCE: Duration = Duration::from_millis(250);

/// How long the final sample may wait for a server to recover from a fault.
const FINAL_SAMPLE_DEADLINE: Duration = Duration::from_secs(120);

/// What one pipeline's [`Sampler`] reads.
struct SampleTarget {
    pipeline: &'static str,
    brokers: String,
    ch_client: clickhouse::Client,
    admin: SocketAddr,
    topic: String,
    group: String,
    table: String,
    partitions: i32,
    holes: Vec<(i32, Range<i64>)>,
    max_pending_batches: usize,
}

/// What a [`Sampler`] saw over a run with no violation.
#[derive(Clone, Copy, Debug)]
pub struct SampleReport {
    /// Samples that compared committed offsets with rows.
    pub samples: u64,
    /// Samples discarded because a probe of the broker or ClickHouse failed.
    pub discarded: u64,
    /// The highest pending-batch reading.
    pub peak_pending: f64,
}

/// A thread reading committed offsets, rows and pending work while a pipeline
/// runs, and failing the test on any reading that breaks INV-1 or the
/// pending-batch limit.
///
/// A sampler must reach [`Sampler::finish`], which the checks end in. Dropping an
/// unfinished one panics, so a path that skips it fails.
pub struct Sampler {
    stop: mpsc::Sender<()>,
    thread: Option<JoinHandle<Sampled>>,
}

#[derive(Default)]
struct Sampled {
    report: Option<SampleReport>,
    violations: Vec<String>,
    pending_readings: u64,
}

impl Sampler {
    #[expect(
        clippy::disallowed_methods,
        reason = "`finish` joins the thread, and dropping the sender ends it"
    )]
    fn start(target: SampleTarget) -> Self {
        let (stop, stopped) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("e2e-sampler".into())
            .spawn(move || target.run(&stopped))
            .expect("spawn sampler thread");
        Sampler {
            stop,
            thread: Some(thread),
        }
    }

    /// Stop sampling, take a final sample, and return what the run showed.
    ///
    /// Call it once the pipeline has exited and any server a scenario paused is
    /// back, so the final sample judges what the run left committed.
    ///
    /// # Panics
    ///
    /// Panics on any violation, when no sample compared committed offsets with
    /// rows, or when no pending reading arrived.
    pub fn finish(mut self) -> SampleReport {
        // A failed send means the thread panicked, which the join reports.
        let _ = self.stop.send(());
        let thread = self.thread.take().expect("finish takes the sampler");
        let sampled = thread
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        assert!(
            sampled.violations.is_empty(),
            "the run broke a sampled check: {:#?}",
            sampled.violations
        );
        let report = sampled.report.expect("a finished sampler has a report");
        assert!(report.samples > 0, "no sample compared commits with rows");
        assert!(sampled.pending_readings > 0, "no pending reading arrived");
        println!("sampled: {report:?}");
        report
    }
}

impl Drop for Sampler {
    fn drop(&mut self) {
        if self.thread.is_some() && !std::thread::panicking() {
            panic!("a pipeline's sampler was dropped without `Sampler::finish`");
        }
    }
}

impl SampleTarget {
    /// Sample until told to stop, then take the final sample. A dropped sender
    /// means the test is unwinding, so nothing is left to judge.
    fn run(self, stopped: &mpsc::Receiver<()>) -> Sampled {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("sampler runtime");
        let probe = probe_consumer(&self.brokers, &self.group);
        let mut sampled = Sampled::default();
        let mut report = SampleReport {
            samples: 0,
            discarded: 0,
            peak_pending: 0.0,
        };
        loop {
            self.sample(&rt, &probe, &mut sampled, &mut report);
            match stopped.recv_timeout(SAMPLE_CADENCE) {
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return sampled,
                Ok(()) => break,
            }
        }
        wait_until(FINAL_SAMPLE_DEADLINE, "a final sample", || {
            self.sample(&rt, &probe, &mut sampled, &mut report)
        });
        sampled.report = Some(report);
        sampled
    }

    /// One sample. Returns whether it compared committed offsets with rows.
    fn sample(
        &self,
        rt: &tokio::runtime::Runtime,
        probe: &BaseConsumer,
        sampled: &mut Sampled,
        report: &mut SampleReport,
    ) -> bool {
        // A stopped pipeline's admin server is gone, and that leaves the
        // comparison below to run.
        if let Ok((200, body)) = http(self.admin, "GET", "/metrics") {
            let readings = metric_series(
                &body,
                "spate_checkpoint_pending_batches",
                &[("pipeline", self.pipeline)],
            );
            for reading in readings {
                sampled.pending_readings += 1;
                report.peak_pending = report.peak_pending.max(reading.value);
                if reading.value > self.max_pending_batches as f64 {
                    sampled.violations.push(format!(
                        "pending reading {} over the limit {}",
                        reading.value, self.max_pending_batches
                    ));
                }
            }
        }
        match self.compare(rt, probe) {
            Ok(found) => {
                report.samples += 1;
                sampled.violations.extend(found);
                true
            }
            Err(_) => {
                report.discarded += 1;
                false
            }
        }
    }

    /// Compare committed offsets with rows, returning what breaks INV-1.
    ///
    /// Committed is read first and rows only grow, so a commit that was backed by
    /// rows when it was made always compares as backed.
    fn compare(
        &self,
        rt: &tokio::runtime::Runtime,
        probe: &BaseConsumer,
    ) -> Result<Vec<String>, String> {
        let committed = read_committed(probe, &self.topic, self.partitions, PROBE_TIMEOUT)
            .map_err(|e| format!("committed offsets: {e}"))?;
        let mut found = Vec::new();
        for (partition, offset) in (0..self.partitions).zip(committed) {
            let holes: Vec<Range<i64>> = self
                .holes
                .iter()
                .filter(|(p, _)| *p == partition)
                .map(|(_, range)| range.clone())
                .collect();
            let lo = event_id(partition, 0);
            let present = self.scalar(
                rt,
                &format!(
                    "SELECT uniqExact(id) FROM {} WHERE id >= {lo} AND id < {}",
                    self.table,
                    lo + offset
                ),
            )?;
            let in_holes: i64 = holes
                .iter()
                .map(|range| range.end.min(offset) - range.start.min(offset))
                .sum();
            let expected = u64::try_from(offset - in_holes).expect("offsets past the holes");
            if present != expected {
                found.push(format!(
                    "partition {partition}: committed to {offset}, {present} of {expected} rows \
                     below it are durable"
                ));
            }
            for range in &holes {
                let written = self.scalar(
                    rt,
                    &format!(
                        "SELECT count() FROM {} WHERE id >= {} AND id < {}",
                        self.table,
                        lo + range.start,
                        lo + range.end
                    ),
                )?;
                if written != 0 {
                    found.push(format!(
                        "partition {partition}: {written} rows in offsets {range:?}, which write \
                         none"
                    ));
                }
            }
        }
        Ok(found)
    }

    /// One `u64` from ClickHouse. The client has no timeout of its own, and a
    /// paused server leaves a query waiting.
    fn scalar(&self, rt: &tokio::runtime::Runtime, sql: &str) -> Result<u64, String> {
        rt.block_on(async {
            tokio::time::timeout(PROBE_TIMEOUT, self.ch_client.query(sql).fetch_one::<u64>()).await
        })
        .map_err(|_| format!("timed out: {sql}"))?
        .map_err(|e| format!("{sql}: {e}"))
    }
}

/// A consumer in `group` that reads the group's offsets and joins nothing.
#[expect(
    clippy::disallowed_methods,
    reason = "the harness reads the group's offsets outside the connector's client factory"
)]
fn probe_consumer(brokers: &str, group: &str) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .create()
        .expect("probe consumer")
}

/// The partitions of the scenario's topic, which exists before its pipeline.
fn partition_count(probe: &BaseConsumer, params: &PipelineParams) -> i32 {
    let metadata = probe
        .fetch_metadata(Some(&params.topic), Duration::from_secs(10))
        .expect("topic metadata");
    let count = metadata.topics()[0].partitions().len();
    assert!(count > 0, "{} has no partitions", params.topic);
    i32::try_from(count).expect("partition count")
}

/// Committed offset per partition (0 when none).
fn read_committed(
    probe: &BaseConsumer,
    topic: &str,
    partitions: i32,
    timeout: Duration,
) -> rdkafka::error::KafkaResult<Vec<i64>> {
    let mut tpl = rdkafka::TopicPartitionList::new();
    for p in 0..partitions {
        tpl.add_partition(topic, p);
    }
    let committed = probe.committed_offsets(tpl, timeout)?;
    Ok((0..partitions)
        .map(|p| {
            committed
                .find_partition(topic, p)
                .map(|e| match e.offset() {
                    rdkafka::Offset::Offset(o) => o,
                    _ => 0,
                })
                .unwrap_or(0)
        })
        .collect())
}

/// The offset after each partition's last record.
fn read_ends(probe: &BaseConsumer, topic: &str, partitions: i32) -> Vec<i64> {
    (0..partitions)
        .map(|p| {
            let (_, high) = probe
                .fetch_watermarks(topic, p, PROBE_TIMEOUT)
                .expect("watermarks");
            high
        })
        .collect()
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

/// Minimal Confluent-compatible registry: serves the registered schemas from
/// `/schemas/ids/{id}`, with `SCHEMA_JSON` under `SCHEMA_ID` out of the box,
/// and 404s everything else, including the subject lookups a `prewarm_subjects`
/// list makes (a pre-warm miss is logged, never fatal).
#[expect(
    clippy::disallowed_methods,
    reason = "the stub server's tasks end when the test's runtime drops"
)]
async fn serve_stub_registry(schemas: Schemas) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stub registry");
    let addr = listener.local_addr().expect("stub addr");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let schemas = Arc::clone(&schemas);
            tokio::spawn(async move {
                let io = hyper_util::rt::TokioIo::new(stream);
                let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                    let schemas = Arc::clone(&schemas);
                    async move {
                        let found = req
                            .uri()
                            .path()
                            .strip_prefix("/schemas/ids/")
                            .and_then(|rest| rest.split('/').next())
                            .and_then(|id| id.parse::<u32>().ok())
                            .and_then(|id| {
                                schemas.lock().expect("registry schemas").get(&id).cloned()
                            });
                        let (status, body) = if let Some(schema) = found {
                            (
                                StatusCode::OK,
                                serde_json::json!({ "schema": schema }).to_string(),
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
                    }
                });
                let _ = http1::Builder::new().serve_connection(io, service).await;
            });
        }
    });
    addr
}

/// Blocking HTTP GET against the pipeline's admin server.
pub fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    http(addr, "GET", path).unwrap_or_else(|e| panic!("GET {path} from {addr}: {e}"))
}

/// Poll a condition until it holds or a timeout elapses; shared with the
/// `spate-test` crate so scenarios and framework users use one helper.
pub use spate_test::wait_until;

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
