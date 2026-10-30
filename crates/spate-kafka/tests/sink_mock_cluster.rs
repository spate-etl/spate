//! Kafka sink integration tests against rdkafka's in-process MockCluster
//! (no Docker).
//!
//! Coverage: the produce path round-trips keys/headers/payloads/tombstones;
//! a full pipeline acks (commits) only after delivery reports confirm;
//! shutdown with in-flight data exits cleanly within its deadline and
//! never commits past what was delivered; a downed broker maps to a
//! retryable batch error; readiness probes fail fast on unknown topics;
//! and enabled statistics populate the fixed `spate_kafka_sink_*` families.
//!
//! Statistics note: per-broker series register lazily on the producer's
//! poll thread, which cannot see a test-local recorder
//! (`metrics::with_local_recorder` is thread-local), so the statistics test
//! asserts fixed families only; per-broker translation is
//! covered by the unit tests in `src/sink/metrics.rs`.

mod support;

use support::Raw as _;

use bytes::BytesMut;
use rdkafka::message::{Headers, Message};
use rdkafka::mocking::MockCluster;
use rdkafka::types::RDKafkaRespErr;
use spate_core::clock::SystemClock;
use spate_core::clock::tokio::{Clock as IoClock, Sleep, TestClock as IoTestClock};
use spate_core::config::PipelineConfig;
use spate_core::deser::{BytesPassthrough, Owned};
use spate_core::error::{ErrorClass, SinkError};
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::record::{PartitionId, Record};
use spate_core::sink::{KeyHashRouter, RowEncoder, SealedBatch, ShardWriter};
use spate_core::source::LaneId;
use spate_kafka::sink::{KafkaEncoder, KafkaMessage, KafkaSink, KafkaSinkConfig, MessageEncoder};
use spate_test::{PipelineRun, memory_source, record, unique_name, wait_until};
use std::panic::RefUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use support::consume;

const TOPIC: &str = "orders-out";

fn sink_config(brokers: &str, extra: &str) -> KafkaSinkConfig {
    // Statistics default off for determinism; a test opting in supplies
    // its own `statistics_interval` line through `extra`.
    let stats = if extra.contains("statistics_interval") {
        ""
    } else {
        "statistics_interval: 0s\n"
    };
    let yaml = format!("brokers: {brokers}\ntopic: {TOPIC}\n{stats}{extra}");
    serde_yaml::from_str(&yaml).expect("sink config")
}

fn sink(brokers: &str, extra: &str) -> KafkaSink {
    spate_kafka::sink::build(sink_config(brokers, extra)).expect("sink build")
}

/// One consumed message: key, payload (`None` = tombstone), headers.
type Consumed = (Option<Vec<u8>>, Option<Vec<u8>>, Vec<(String, Vec<u8>)>);

/// Read `n` messages back from the mock cluster (order within the single
/// partition is the produce order).
fn consume_all(brokers: &str, n: usize) -> Vec<Consumed> {
    consume(
        brokers,
        TOPIC,
        "verify",
        n,
        Duration::from_secs(30),
        |message| {
            let headers = message
                .headers()
                .map(|hs| {
                    hs.iter()
                        .map(|h| (h.key.to_string(), h.value.unwrap_or(&[]).to_vec()))
                        .collect()
                })
                .unwrap_or_default();
            Some((
                message.key().map(<[u8]>::to_vec),
                message.payload().map(<[u8]>::to_vec),
                headers,
            ))
        },
    )
}

/// Test encoder over `key|header|payload` structured payloads, exercising
/// keys, headers, and tombstones through the public `MessageEncoder` seam.
#[derive(Clone)]
struct StructuredEncoder;

impl MessageEncoder<Owned<Vec<u8>>> for StructuredEncoder {
    fn encode<'buf>(
        &mut self,
        rec: &Record<Vec<u8>>,
        msg: &mut KafkaMessage,
    ) -> Result<(), SinkError> {
        let mut parts = rec.payload.splitn(3, |b| *b == b'|');
        let key = parts.next().expect("key part");
        let header = parts.next().expect("header part");
        let payload = parts.next().expect("payload part");
        if !key.is_empty() {
            msg.set_key(key);
        }
        if !header.is_empty() {
            msg.add_header("tag", header);
        }
        if payload == b"__tombstone__" {
            msg.set_tombstone();
        } else {
            msg.set_payload(payload);
        }
        Ok(())
    }
}

#[tokio::test]
async fn write_batch_round_trips_keys_headers_payloads() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    let sink = sink(&brokers, "");

    let mut encoder = KafkaEncoder::new(StructuredEncoder);
    let mut frame = BytesMut::new();
    let inputs: &[&[u8]] = &[
        b"user-1|trace-a|first payload",
        b"|  |second, keyless",
        b"user-2||__tombstone__",
    ];
    for input in inputs {
        encoder
            .encode(&record(input.to_vec()), &mut frame)
            .expect("encode");
    }
    let batch = SealedBatch {
        rows: inputs.len() as u64,
        bytes: frame.len() as u64,
        frames: vec![frame.freeze()],
        dedup_token: "unused-by-kafka".to_string(),
    };

    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("write_batch");

    let consumed = consume_all(&brokers, 3);
    assert_eq!(
        consumed[0],
        (
            Some(b"user-1".to_vec()),
            Some(b"first payload".to_vec()),
            vec![("tag".to_string(), b"trace-a".to_vec())]
        )
    );
    assert_eq!(
        consumed[1],
        (
            None,
            Some(b"second, keyless".to_vec()),
            vec![("tag".to_string(), b"  ".to_vec())]
        )
    );
    assert_eq!(
        consumed[2],
        (Some(b"user-2".to_vec()), None, vec![]),
        "tombstone arrives as a null payload"
    );
}

/// One pipeline config per spawn, with a distinct pipeline name.
fn pipeline_config() -> String {
    let name = unique_name("kafka-sink-test");
    format!(
        r#"
pipeline: {{ name: {name}, threads: 1, io_threads: 1 }}
admin: {{ listen: none }}
metrics: {{ exporter: none }}
source: {{ memory: {{}} }}
sink: {{ kafka: {{}} }}
"#
    )
}

/// Spawn a memory→kafka pipeline over `sink`; returns the source handle,
/// the shutdown trigger, and the bounded-join handle.
fn spawn_pipeline(
    sink: KafkaSink,
) -> (
    spate_test::SourceHandle,
    spate_core::pipeline::ShutdownHandle,
    PipelineRun,
) {
    let (source, handle) = memory_source();
    let encoder = sink.encoder_bytes();
    let runtime =
        Pipeline::from_config(PipelineConfig::from_str(&pipeline_config()).expect("config"))
            .expect("builder")
            .sink(sink)
            .expect("sink")
            .chains(move |ctx| {
                let chunk_cfg = ctx.chunk();
                chain_owned::<Vec<u8>, _>(BytesPassthrough)
                    .with_metrics(ctx.pipeline, "main")
                    .sink(
                        encoder.clone(),
                        KeyHashRouter,
                        chunk_cfg,
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
    let run = PipelineRun::spawn(move || runtime.run());
    (handle, shutdown, run)
}

/// The at-least-once core: the source watermark advances only after every
/// delivery report of the covering batch confirmed.
#[test]
fn pipeline_commits_only_after_delivery_reports() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    // Short linger so steady-state batches seal quickly.
    let (handle, shutdown, run) = spawn_pipeline(sink(&brokers, "batch:\n  linger: 100ms\n"));

    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);
    let mut last = 0;
    for payload in [&b"alpha"[..], b"beta", b"gamma"] {
        last = handle.push(p, Some(b"key"), payload);
    }
    assert!(
        handle.wait_committed(p, last + 1, Duration::from_secs(20)),
        "watermark must advance once delivery reports confirmed (last \
         committed: {:?})",
        handle.last_committed(p)
    );

    shutdown.trigger();
    let report = run
        .wait_exit(Duration::from_secs(30))
        .expect("pipeline exits within the drain deadline")
        .expect("run");
    assert_eq!(report.exit_code(), 0, "clean drain: {report:?}");

    let consumed = consume_all(&brokers, 3);
    let payloads: Vec<_> = consumed.iter().map(|(_, p, _)| p.clone()).collect();
    assert_eq!(
        payloads,
        vec![
            Some(b"alpha".to_vec()),
            Some(b"beta".to_vec()),
            Some(b"gamma".to_vec())
        ],
        "every committed record is durably on the topic, in order"
    );
}

/// Shutdown with data still in flight: the pipeline must exit cleanly
/// within its deadline (no producer-teardown hang), and the watermark must
/// never run past what was delivered, the at-least-once boundary the drain
/// machinery guarantees (undelivered records replay after a restart; they
/// are never silently committed).
#[test]
fn shutdown_with_inflight_data_commits_only_delivered() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    // Linger far beyond the test: any durable write after the trigger can
    // only come from the drain's force-seal.
    let (handle, shutdown, run) = spawn_pipeline(sink(&brokers, "batch:\n  linger: 60s\n"));

    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);
    for payload in [&b"one"[..], b"two", b"three", b"four", b"five"] {
        handle.push(p, None, payload);
    }
    shutdown.trigger();
    let report = run
        .wait_exit(Duration::from_secs(30))
        .expect(
            "pipeline exits within the drain deadline — a hang here is \
                 the producer-teardown footgun",
        )
        .expect("run");
    assert_eq!(report.exit_code(), 0, "clean drain: {report:?}");

    // Committed ⊆ delivered: everything the watermark covers must be on
    // the topic (consume as many as were committed; ordering is the push
    // order within the single partition).
    let committed = handle.last_committed(p).unwrap_or(0);
    let expected: &[&[u8]] = &[b"one", b"two", b"three", b"four", b"five"];
    let consumed = consume_all(&brokers, usize::try_from(committed).expect("count"));
    for (i, (_, payload, _)) in consumed.iter().enumerate() {
        assert_eq!(
            payload.as_deref(),
            Some(expected[i]),
            "committed offset {i} must be backed by its message"
        );
    }
}

/// A write to a missing topic fails. With `topic.metadata.propagation.max.ms`
/// below `delivery_timeout`, the broker's answer fails the first write as
/// fatal, and the next fails locally as retryable. At the defaults, 30 s each,
/// the first write times out as retryable instead.
#[tokio::test]
async fn a_write_to_a_missing_topic_fails() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    let brokers = cluster.bootstrap_servers();
    let sink = sink(
        &brokers,
        "delivery_timeout: 10s\nrdkafka:\n  topic.metadata.propagation.max.ms: \"0\"\n",
    );
    let mut encoder = sink.encoder_bytes();
    let mut frame = BytesMut::new();
    encoder
        .encode(&record(b"nowhere".to_vec()), &mut frame)
        .expect("encode");
    let batch = SealedBatch {
        rows: 1,
        bytes: frame.len() as u64,
        frames: vec![frame.freeze()],
        dedup_token: "unused".to_string(),
    };
    // The broker's answer is fatal and names the missing topic. librdkafka
    // then knows the topic is missing and fails the next write locally,
    // which stays retryable. The readiness probe reports the missing topic
    // as fatal whatever the timeouts.
    for (attempt, expected, hint) in [
        ("first", ErrorClass::Fatal, "does not exist"),
        ("second", ErrorClass::Retryable, "Unknown topic"),
    ] {
        let err = tokio::time::timeout(
            Duration::from_secs(60),
            sink.writer.write_batch(&sink.endpoints[0][0], &batch),
        )
        .await
        .expect("the write settles")
        .expect_err("a missing topic fails the write");
        match err {
            SinkError::Client { class, reason } => {
                assert_eq!(class, expected, "{attempt}: {reason}");
                assert!(reason.contains(hint), "{attempt}: {reason}");
            }
            other => panic!("{attempt}: unexpected error shape: {other:?}"),
        }
    }
}

#[tokio::test]
async fn delivery_failure_maps_to_retryable() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    // Short delivery timeout so the timed-out reports arrive quickly.
    let sink = sink(&brokers, "delivery_timeout: 2s\n");
    cluster.broker_down(1).expect("broker down");

    let mut encoder = sink.encoder_bytes();
    let mut frame = BytesMut::new();
    encoder
        .encode(&record(b"doomed".to_vec()), &mut frame)
        .expect("encode");
    let batch = SealedBatch {
        rows: 1,
        bytes: frame.len() as u64,
        frames: vec![frame.freeze()],
        dedup_token: "unused".to_string(),
    };

    let err = sink
        .writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect_err("no broker → delivery must fail");
    match err {
        SinkError::Client { class, reason } => {
            assert_eq!(
                class,
                ErrorClass::Retryable,
                "a downed broker is transient; the framework retries the \
                 batch (reason: {reason})"
            );
        }
        other => panic!("unexpected error shape: {other:?}"),
    }
}

#[tokio::test]
async fn probe_verifies_topic_and_fails_fast_on_unknown_topic() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    let sink = sink(&brokers, "");
    let probe = sink.probe_fn();

    probe().await.expect("existing topic probes healthy");

    cluster
        .topic_error(
            TOPIC,
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART,
        )
        .expect("inject topic error");
    let err = probe()
        .await
        .expect_err("unknown topic must fail the probe");
    match err {
        SinkError::Client { class, reason } => {
            assert_eq!(class, ErrorClass::Fatal, "unknown topic is fatal");
            assert!(reason.contains("topic"), "actionable: {reason}");
        }
        other => panic!("unexpected error shape: {other:?}"),
    }
}

/// Pins that a probe whose metadata read panics on its blocking thread
/// returns a retryable error instead of unwinding into the caller:
/// librdkafka refuses a topic name over 512 bytes, and rdkafka panics on
/// the refusal.
#[tokio::test]
async fn a_probe_with_an_overlong_topic_fails_without_panicking() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    let brokers = cluster.bootstrap_servers();
    let topic = "t".repeat(513);
    let yaml = format!("brokers: {brokers}\ntopic: {topic}\nstatistics_interval: 0s\n");
    let config: KafkaSinkConfig = serde_yaml::from_str(&yaml).expect("sink config");
    let sink = spate_kafka::sink::build(config).expect("sink build");

    let err = sink.probe_fn()()
        .await
        .expect_err("an overlong topic must fail the probe");
    match err {
        SinkError::Client { class, reason } => {
            assert_eq!(class, ErrorClass::Retryable, "{reason}");
            assert!(reason.starts_with("probe task failed"), "{reason}");
        }
        other => panic!("unexpected error shape: {other:?}"),
    }
}

/// The readiness probe reports a missing topic as fatal and leaves it
/// missing: its metadata read never asks the broker to create the topic.
#[tokio::test]
async fn the_probe_does_not_create_a_missing_topic() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    let brokers = cluster.bootstrap_servers();
    let sink = sink(&brokers, "");

    let err = sink.probe_fn()()
        .await
        .expect_err("a missing topic must fail the probe");
    match err {
        SinkError::Client { class, reason } => {
            assert_eq!(
                class,
                ErrorClass::Fatal,
                "a missing topic is fatal: {reason}"
            );
            assert!(reason.contains("does not exist"), "actionable: {reason}");
        }
        other => panic!("unexpected error shape: {other:?}"),
    }

    let consumer: rdkafka::consumer::BaseConsumer = support::raw_config()
        .set("bootstrap.servers", &brokers)
        .build()
        .expect("consumer");
    let metadata = rdkafka::consumer::Consumer::fetch_metadata(
        &consumer,
        Some(TOPIC),
        Duration::from_secs(10),
    )
    .expect("metadata");
    let topic = &metadata.topics()[0];
    assert_eq!(
        topic.error(),
        Some(RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART),
        "the topic stays missing"
    );
}

#[test]
fn statistics_populate_kafka_sink_fixed_families() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    let mut sink = sink(&brokers, "statistics_interval: 200ms\n");

    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let prom = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        sink.writer
            .attach_metrics(Some(spate_core::metrics::Meter::with_namespace(
                "kafka",
                "orders",
                "orders_out",
                "kafka",
            )));
    });

    // The poll thread publishes into the handles resolved above once per
    // interval; totals may legitimately be zero, so presence is the assert.
    wait_until(Duration::from_secs(10), "fixed families rendered", || {
        prom.run_upkeep();
        let rendered = prom.render();
        rendered.contains("spate_kafka_tx_requests_total")
            && rendered.contains("spate_kafka_produce_queue_messages")
    });
}

/// A tokio-timeline test clock that counts how often the writer reads it.
#[derive(Debug)]
struct CountingClock {
    inner: Arc<IoTestClock>,
    reads: AtomicUsize,
}

impl IoClock for CountingClock {
    fn now(&self) -> tokio::time::Instant {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.now()
    }

    fn sleep_until(&self, deadline: tokio::time::Instant) -> Sleep {
        self.inner.sleep_until(deadline)
    }
}

/// The writer's grace past `delivery_timeout`, `DELIVERY_GRACE` in the
/// writer.
const DELIVERY_GRACE: Duration = Duration::from_secs(5);

/// A sink whose writer runs its deadlines on `io_clock`.
fn sink_on(brokers: &str, extra: &str, io_clock: Arc<dyn IoClock + RefUnwindSafe>) -> KafkaSink {
    spate_kafka::sink::build_on_clocks(sink_config(brokers, extra), Arc::new(SystemClock), io_clock)
        .expect("sink build")
}

/// A batch of `rows` one-byte payloads.
fn rows_batch(sink: &KafkaSink, rows: u64) -> SealedBatch {
    let mut encoder = sink.encoder_bytes();
    let mut frame = BytesMut::new();
    for _ in 0..rows {
        encoder
            .encode(&record(b"x".to_vec()), &mut frame)
            .expect("encode");
    }
    SealedBatch {
        rows,
        bytes: frame.len() as u64,
        frames: vec![frame.freeze()],
        dedup_token: "unused".to_string(),
    }
}

/// Runs `write_batch` for `batch` on `io` and returns its handle.
fn spawn_write(
    io: &tokio::runtime::Runtime,
    sink: &KafkaSink,
    batch: SealedBatch,
) -> tokio::task::JoinHandle<Result<(), SinkError>> {
    let (writer, endpoint) = (sink.writer.clone(), sink.endpoints[0][0].clone());
    io.spawn(async move { writer.write_batch(&endpoint, &batch).await })
}

/// The retryable reason `result` carries.
fn retryable_reason(result: Result<(), SinkError>) -> String {
    match result {
        Err(SinkError::Client {
            class: ErrorClass::Retryable,
            reason,
        }) => reason,
        other => panic!("expected a retryable error, got {other:?}"),
    }
}

/// A producer queue that stays full fails the batch once the writer's clock
/// reaches the send deadline, and a queue that drains one nanosecond before
/// it delivers the batch.
#[test]
fn a_full_queue_fails_the_batch_at_the_send_deadline() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    let io = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let test_clock = io.block_on(async { IoTestClock::frozen() });
    let clock = Arc::new(CountingClock {
        inner: Arc::clone(&test_clock),
        reads: AtomicUsize::new(0),
    });
    let delivery_timeout = Duration::from_secs(300);
    let send_window = delivery_timeout + DELIVERY_GRACE;
    let sink = sink_on(
        &brokers,
        "delivery_timeout: 300s\nrdkafka:\n  queue.buffering.max.messages: \"1\"\n",
        Arc::clone(&clock) as _,
    );

    // The first send reads the deadline and fills the queue, so every
    // further read is a queue-full retry.
    let in_backoff = |from: usize| {
        wait_until(Duration::from_secs(30), "a queue-full retry", || {
            clock.reads.load(Ordering::SeqCst) >= from + 2
        });
    };

    cluster.broker_down(1).expect("broker down");
    let write = spawn_write(&io, &sink, rows_batch(&sink, 2));
    in_backoff(0);
    test_clock.advance(send_window - Duration::from_nanos(1));
    let reads = clock.reads.load(Ordering::SeqCst);
    wait_until(Duration::from_secs(30), "a retry after the advance", || {
        clock.reads.load(Ordering::SeqCst) > reads
    });
    cluster.broker_up(1).expect("broker up");
    io.block_on(write)
        .expect("write task")
        .expect("the queue drained before the deadline");

    cluster.broker_down(1).expect("broker down");
    let reads = clock.reads.load(Ordering::SeqCst);
    let write = spawn_write(&io, &sink, rows_batch(&sink, 2));
    in_backoff(reads);
    test_clock.advance(send_window);
    let result = io
        .block_on(async { tokio::time::timeout(Duration::from_secs(30), write).await })
        .expect("the write ends at the send deadline")
        .expect("write task");
    let reason = retryable_reason(result);
    assert!(
        reason.starts_with("producer queue stayed full past the delivery deadline"),
        "{reason}"
    );
    cluster.broker_up(1).expect("broker up");
}

/// A delivery report still missing when the writer's clock reaches the
/// report deadline fails the batch, and one that arrives one nanosecond
/// before it delivers the batch.
#[test]
fn missing_reports_fail_the_batch_at_the_wait_deadline() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    let io = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let clock = io.block_on(async { IoTestClock::frozen() });
    let wait_window = Duration::from_secs(300) + DELIVERY_GRACE;
    let sink = sink_on(
        &brokers,
        "delivery_timeout: 300s\n",
        Arc::clone(&clock) as _,
    );
    let armed = |deadline| {
        wait_until(Duration::from_secs(30), "the report wait", || {
            clock.has_slept_until(deadline)
        });
    };

    cluster.broker_down(1).expect("broker down");
    let deadline = clock.now() + wait_window;
    let write = spawn_write(&io, &sink, rows_batch(&sink, 1));
    armed(deadline);
    clock.advance(wait_window - Duration::from_nanos(1));
    cluster.broker_up(1).expect("broker up");
    io.block_on(write)
        .expect("write task")
        .expect("the report arrived before the deadline");

    cluster.broker_down(1).expect("broker down");
    let deadline = clock.now() + wait_window;
    let write = spawn_write(&io, &sink, rows_batch(&sink, 1));
    armed(deadline);
    clock.advance(wait_window);
    let reason = retryable_reason(io.block_on(write).expect("write task"));
    assert!(
        reason.starts_with("delivery reports missing past the deadline"),
        "{reason}"
    );
    cluster.broker_up(1).expect("broker up");
}
