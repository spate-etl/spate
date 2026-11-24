//! A broker that speaks TLS on its host listener, and a source and sink run
//! against it.

use super::Raw as _;
use super::sink::sealed;
use super::{broker, drain_lane};
use rdkafka::ClientConfig;
use rdkafka::producer::{BaseProducer, Producer as _};
use spate_core::checkpoint::Checkpointer;
use spate_core::sink::ShardWriter;
use spate_core::source::{Source, SourceCtx, SourceEvent};
use spate_core::tls::TlsSection;
use spate_kafka::sink::KafkaSinkConfig;
use spate_kafka::{KafkaSink, KafkaSource, KafkaSourceConfig};
use spate_test::HANG;
use spate_test_support::{SingleBroker, TestCa, pem};
use std::collections::BTreeMap;
use std::time::Duration;
use testcontainers::ImageExt as _;

/// The records [`connector_clients`] writes and reads back.
pub(crate) const RECORDS: usize = 10;

/// Boots a TLS broker signed by a fresh CA, and returns the CA as PEM, the
/// container and its address.
pub(crate) fn tls_broker() -> (String, SingleBroker, String) {
    let ca = TestCa::new("spate-kafka-test CA");
    let (chain, key) = ca.leaf(&["127.0.0.1", "localhost"]);
    // Kafka reads these as properties, where `\n` is a newline.
    let inline = |pem: String| pem.replace('\n', "\\n");
    let kafka = SingleBroker::start(
        broker()
            .with_env_var(
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "BROKER:PLAINTEXT,PLAINTEXT:SSL,CONTROLLER:PLAINTEXT",
            )
            .with_env_var("KAFKA_SSL_KEYSTORE_TYPE", "PEM")
            .with_env_var(
                "KAFKA_SSL_KEYSTORE_KEY",
                inline(pem("PRIVATE KEY", key.secret_pkcs8_der())),
            )
            .with_env_var(
                "KAFKA_SSL_KEYSTORE_CERTIFICATE_CHAIN",
                inline(pem("CERTIFICATE", &chain)),
            ),
    )
    .expect("start kafka container");
    let brokers = kafka.bootstrap().to_owned();
    (pem("CERTIFICATE", &ca.der()), kafka, brokers)
}

/// Runs a sink, its readiness probe and a source over TLS on `topic`, each
/// with `rdkafka` as its passthrough and `tls` as its typed section.
pub(crate) fn connector_clients(
    brokers: &str,
    topic: &str,
    rdkafka: &BTreeMap<String, String>,
    tls: &TlsSection,
) {
    let mut sink_cfg = KafkaSinkConfig::new(brokers, topic);
    sink_cfg.statistics_interval = Duration::ZERO;
    sink_cfg.rdkafka = rdkafka.clone();
    sink_cfg.tls = tls.clone();
    let sink = KafkaSink::new(sink_cfg).expect("sink build");
    let batch = sealed(
        &mut sink.encoder_bytes(),
        (0..RECORDS).map(|i| format!("tls-{i}")),
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(sink.writer.write_batch(&sink.endpoints[0][0], &batch))
        .expect("write_batch over TLS");
    rt.block_on(sink.probe_fn()())
        .expect("readiness probe over TLS");

    let mut source = tls_source(brokers, topic, rdkafka, tls);
    let cp = Checkpointer::new();
    source.open(SourceCtx::new(cp.handle())).expect("open");
    let mut lanes = None;
    spate_test::wait_until(HANG, "partition assignment", || {
        if let SourceEvent::LanesAssigned(assigned) = source
            .poll_events(Duration::from_millis(200))
            .expect("poll_events")
        {
            lanes = Some(assigned);
        }
        lanes.is_some()
    });
    let mut lanes = lanes.expect("assigned");
    let rows = drain_lane(&mut source, &mut lanes[0], RECORDS, HANG);
    assert_eq!(rows.len(), RECORDS);
}

/// A source of `topic` from its start, in a group named for the topic, with
/// `rdkafka` as its passthrough and `tls` as its typed section.
pub(crate) fn tls_source(
    brokers: &str,
    topic: &str,
    rdkafka: &BTreeMap<String, String>,
    tls: &TlsSection,
) -> KafkaSource {
    let mut source_cfg = KafkaSourceConfig::new(brokers, topic, topic);
    source_cfg.startup_timeout = Duration::from_secs(60);
    source_cfg.rdkafka = rdkafka.clone();
    source_cfg
        .rdkafka
        .insert("auto.offset.reset".to_string(), "earliest".to_string());
    source_cfg.tls = tls.clone();
    KafkaSource::new(source_cfg).expect("the config validates")
}

/// Create `topic`. The sink never creates a topic, so a producer's metadata
/// read on `cc` does it, under librdkafka's auto-create default.
pub(crate) fn create_topic(cc: &ClientConfig, topic: &str) {
    let producer: BaseProducer = cc.build().expect("raw producer");
    spate_test::wait_until(HANG, "the topic exists", || {
        producer
            .client()
            .fetch_metadata(Some(topic), Duration::from_secs(10))
            .is_ok_and(|m| m.topics().iter().all(|t| t.error().is_none()))
    });
}

/// A raw TLS client configuration for `brokers`, with no CA setting.
pub(crate) fn raw(brokers: &str) -> ClientConfig {
    let mut cc = super::raw_config();
    cc.set("bootstrap.servers", brokers)
        .set("security.protocol", "ssl")
        .set("group.id", "tls-raw");
    cc
}
