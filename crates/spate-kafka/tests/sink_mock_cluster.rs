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
//! (`metrics::with_local_recorder` is thread-local) — the statistics test
//! therefore asserts fixed families only; per-broker translation is
//! covered by the unit tests in `src/sink/metrics.rs`.

use bytes::BytesMut;
use rdkafka::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::mocking::MockCluster;
use rdkafka::types::RDKafkaRespErr;
use spate_core::checkpoint::AckRef;
use spate_core::config::PipelineConfig;
use spate_core::deser::{BytesPassthrough, Owned};
use spate_core::error::{ErrorClass, SinkError};
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::record::{PartitionId, Record, RecordMeta};
use spate_core::sink::{KeyHashRouter, RowEncoder, SealedBatch, ShardWriter};
use spate_core::source::LaneId;
use spate_kafka::sink::{KafkaEncoder, KafkaMessage, KafkaSink, KafkaSinkConfig, MessageEncoder};
use spate_test::{PipelineRun, memory_source, wait_until};
use std::time::{Duration, Instant};

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
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "verify")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Alert)
        .create()
        .expect("consumer");
    consumer.subscribe(&[TOPIC]).expect("subscribe");
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut out = Vec::new();
    while out.len() < n {
        assert!(
            Instant::now() < deadline,
            "consumed only {} of {n} messages within the deadline",
            out.len()
        );
        let Some(message) = consumer.poll(Duration::from_millis(250)) else {
            continue;
        };
        let message = message.expect("message");
        let headers = message
            .headers()
            .map(|hs| {
                hs.iter()
                    .map(|h| (h.key.to_string(), h.value.unwrap_or(&[]).to_vec()))
                    .collect()
            })
            .unwrap_or_default();
        out.push((
            message.key().map(<[u8]>::to_vec),
            message.payload().map(<[u8]>::to_vec),
            headers,
        ));
    }
    out
}

fn record(payload: &[u8]) -> Record<Vec<u8>> {
    let (ack, _rx) = AckRef::test_pair();
    Record {
        payload: payload.to_vec(),
        meta: RecordMeta {
            partition: PartitionId(0),
            offset: 0,
            event_time_ms: 0,
            key_hash: None,
        },
        ack,
    }
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
        encoder.encode(&record(input), &mut frame).expect("encode");
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
///
/// Metric gauge series have a single live owner per process and the pipeline
/// name is part of every key, so two pipelines called `kafka-sink-test` alive
/// at once — which is what `cargo test` does with the tests in this file —
/// are a collision the builder refuses. In production these would be separate
/// processes.
fn pipeline_config() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!(
        r#"
pipeline: {{ name: kafka-sink-test-{n}, threads: 1, io_threads: 1 }}
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
/// never run past what was actually delivered — the at-least-once boundary
/// the drain machinery guarantees (undelivered records replay after a
/// restart; they are never silently committed).
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
        .encode(&record(b"doomed"), &mut frame)
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
    // interval; totals may legitimately be zero — presence is the assert.
    wait_until(Duration::from_secs(10), "fixed families rendered", || {
        prom.run_upkeep();
        let rendered = prom.render();
        rendered.contains("spate_kafka_tx_requests_total")
            && rendered.contains("spate_kafka_produce_queue_messages")
    });
}
