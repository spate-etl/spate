//! Kafka→Kafka end-to-end against a real broker (Docker via
//! testcontainers): source topic → operator chain → **split** → two sink
//! topics, the producer-sink target use case. Run explicitly:
//!
//! ```sh
//! cargo test -p spate-kafka --test sink_kafka_broker -- --ignored
//! ```

use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use spate_core::config::PipelineConfig;
use spate_core::deser::{BytesPassthrough, Owned};
use spate_core::error::ErrorPolicy;
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::sink::KeyHashRouter;
use spate_kafka::sink::KafkaSinkConfig;
use spate_kafka::{KafkaSource, KafkaSourceConfig};
use spate_test::{PipelineRun, wait_until};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use testcontainers::runners::SyncRunner;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

const IN_TOPIC: &str = "orders";
const A_TOPIC: &str = "orders-a";
const B_TOPIC: &str = "orders-b";
const GROUP: &str = "kafka-to-kafka";
const TOTAL: usize = 100;

const PIPELINE_CONFIG: &str = r#"
pipeline: { name: kafka-to-kafka, threads: 1, io_threads: 1 }
admin: { listen: none }
metrics: { exporter: none }
source: { kafka: {} }
sinks:
  a: { kafka: {} }
  b: { kafka: {} }
"#;

fn sink_config(brokers: &str, topic: &str) -> KafkaSinkConfig {
    let yaml = format!(
        "brokers: {brokers}\ntopic: {topic}\nstatistics_interval: 0s\n\
         batch:\n  linger: 100ms\n"
    );
    serde_yaml::from_str(&yaml).expect("sink config")
}

/// Produce `payloads` to `topic`, creating it as a side effect (the test
/// image auto-creates topics) so the pipeline's sinks and probes never see
/// a not-yet-created topic.
fn produce(brokers: &str, topic: &str, payloads: &[Vec<u8>]) {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "30000")
        .create()
        .expect("producer");
    for payload in payloads {
        producer
            .send(
                BaseRecord::<[u8], [u8]>::to(topic)
                    .payload(payload)
                    .partition(0),
            )
            .expect("enqueue");
    }
    producer.flush(Duration::from_secs(30)).expect("flush");
}

/// Consume from `topic` until `deadline` yields nothing new for a second,
/// returning payloads (excluding the warmup marker).
fn consume_payloads(brokers: &str, topic: &str, expect: usize) -> Vec<Vec<u8>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("verify-{topic}"))
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut out = Vec::new();
    while out.len() < expect {
        assert!(
            Instant::now() < deadline,
            "{topic}: consumed only {} of {expect} within the deadline",
            out.len()
        );
        if let Some(message) = consumer.poll(Duration::from_millis(250)) {
            let message = message.expect("message");
            let payload = message.payload().unwrap_or_default().to_vec();
            if payload != b"warmup" {
                out.push(payload);
            }
        }
    }
    out
}

#[test]
#[ignore = "requires Docker"]
fn kafka_to_kafka_split_round_trip() {
    let container = Kafka::default().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");

    // Input: alternating `a-<i>` / `b-<i>` payloads. Warmup singles create
    // the output topics up front.
    let inputs: Vec<Vec<u8>> = (0..TOTAL)
        .map(|i| {
            let side = if i % 2 == 0 { "a" } else { "b" };
            format!("{side}-{i}").into_bytes()
        })
        .collect();
    produce(&brokers, IN_TOPIC, &inputs);
    produce(&brokers, A_TOPIC, &[b"warmup".to_vec()]);
    produce(&brokers, B_TOPIC, &[b"warmup".to_vec()]);

    let source = KafkaSource::new(KafkaSourceConfig {
        brokers: brokers.clone(),
        topic: IN_TOPIC.to_string(),
        group_id: GROUP.to_string(),
        commit_interval: Duration::from_millis(200),
        startup_timeout: Duration::from_secs(60),
        statistics_interval: Duration::ZERO,
        rdkafka: BTreeMap::from([("auto.offset.reset".to_string(), "earliest".to_string())]),
    });
    let sink_a = spate_kafka::sink::build(sink_config(&brokers, A_TOPIC)).expect("sink a");
    let sink_b = spate_kafka::sink::build(sink_config(&brokers, B_TOPIC)).expect("sink b");
    let enc_a = sink_a.encoder_bytes();
    let enc_b = sink_b.encoder_bytes();

    let runtime = Pipeline::from_config(PipelineConfig::from_str(PIPELINE_CONFIG).expect("config"))
        .expect("builder")
        .add_sink("a", sink_a)
        .expect("add sink a")
        .add_sink("b", sink_b)
        .expect("add sink b")
        .chains(move |ctx| {
            let mut split = chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .with_metrics(ctx.pipeline.clone(), "main")
                .split(ErrorPolicy::Skip);
            let a = split.add::<Owned<Vec<u8>>, _, _>(enc_a.clone(), KeyHashRouter, ctx.sink("a"));
            let b = split.add::<Owned<Vec<u8>>, _, _>(enc_b.clone(), KeyHashRouter, ctx.sink("b"));
            split
                .route(move |payload: Vec<u8>, out| match payload.first() {
                    Some(b'a') => out.emit(a, payload),
                    Some(b'b') => out.emit(b, payload),
                    _ => {}
                })
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

    // Every input lands on its side, in partition order.
    let got_a = consume_payloads(&brokers, A_TOPIC, TOTAL / 2);
    let got_b = consume_payloads(&brokers, B_TOPIC, TOTAL / 2);
    let want_a: Vec<Vec<u8>> = inputs.iter().filter(|p| p[0] == b'a').cloned().collect();
    let want_b: Vec<Vec<u8>> = inputs.iter().filter(|p| p[0] == b'b').cloned().collect();
    assert_eq!(got_a, want_a, "topic a receives exactly the a-records");
    assert_eq!(got_b, want_b, "topic b receives exactly the b-records");

    // The source group commits the full input once both destinations have
    // durably written (worst-status ack merge across the split).
    let probe: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", GROUP)
        .create()
        .expect("probe");
    wait_until(
        Duration::from_secs(30),
        "source group committed all",
        || {
            let mut tpl = rdkafka::TopicPartitionList::new();
            tpl.add_partition(IN_TOPIC, 0);
            let committed = probe
                .committed_offsets(tpl, Duration::from_secs(10))
                .expect("committed");
            committed.elements()[0].offset() == rdkafka::Offset::Offset(TOTAL as i64)
        },
    );

    shutdown.trigger();
    let report = run
        .wait_exit(Duration::from_secs(60))
        .expect("pipeline exits within the drain deadline")
        .expect("run");
    assert_eq!(report.exit_code(), 0, "clean drain: {report:?}");
}
