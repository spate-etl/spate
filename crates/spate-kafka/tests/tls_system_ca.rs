//! TLS against a broker whose CA exists only in the system trust store.
//!
//! The clients run as this test binary inside a Debian container whose system
//! bundle holds the test CA, so the host's trust store and environment play no
//! part.
#![cfg(all(feature = "tls", target_os = "linux"))]

mod support;

use base64::Engine as _;
use bytes::BytesMut;
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
use rdkafka::ClientConfig;
use rdkafka::client::ClientContext;
use rdkafka::consumer::{BaseConsumer, ConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::message::DeliveryResult;
use rdkafka::producer::{BaseProducer, ProducerContext};
use spate_core::checkpoint::{AckRef, Checkpointer};
use spate_core::record::{PartitionId, Record, RecordMeta};
use spate_core::sink::{RowEncoder, SealedBatch, ShardWriter};
use spate_core::source::{Source, SourceCtx, SourceEvent};
use spate_kafka::sink::KafkaSinkConfig;
use spate_kafka::{KafkaSource, KafkaSourceConfig};
use spate_test_support::container_image;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::drain_lane;
use testcontainers::core::WaitFor;
use testcontainers::core::wait::ExitWaitStrategy;
use testcontainers::runners::SyncRunner;
use testcontainers::{CopyTargetOptions, GenericImage, ImageExt};
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

const NAME: &str = "probe_ca_default_completes_a_tls_handshake";
const ARM: &str = "SPATE_TEST_PROBE_ARM";
const BROKERS: &str = "SPATE_TEST_PROBE_BROKERS";
const TOPIC: &str = "tls-system-ca";
const RECORDS: usize = 10;
const CLIENT_BINARY: &str = "/spate/tls_system_ca";

/// A source, a sink and its readiness probe with the connector's CA default
/// complete a TLS handshake against a broker signed by a CA only the system
/// store holds, and raw clients with `ssl.ca.location` unset fail
/// verification against it.
#[test]
#[ignore = "requires Docker"]
fn probe_ca_default_completes_a_tls_handshake() {
    if let Ok(arm) = std::env::var(ARM) {
        let brokers = std::env::var(BROKERS).expect("broker address");
        match arm.as_str() {
            "probe" => probe_arm(&brokers),
            "unset" => unset_arm(&brokers),
            other => panic!("unknown arm {other}"),
        }
        return;
    }

    let (ca_pem, broker_key, broker_chain) = certificates();
    // Kafka reads these as properties, where `\n` is a newline.
    let inline = |pem: &str| pem.replace('\n', "\\n");
    let kafka = Kafka::default()
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "BROKER:PLAINTEXT,PLAINTEXT:SSL,CONTROLLER:PLAINTEXT",
        )
        .with_env_var("KAFKA_SSL_KEYSTORE_TYPE", "PEM")
        .with_env_var("KAFKA_SSL_KEYSTORE_KEY", inline(&broker_key))
        .with_env_var(
            "KAFKA_SSL_KEYSTORE_CERTIFICATE_CHAIN",
            inline(&broker_chain),
        )
        .start()
        .expect("start kafka container");
    let port = kafka.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");

    // `probe` first: it shares the broker certificate, so a certificate
    // defect fails there and cannot pass as a verification failure below.
    // `ci/debian/` pins the image, pulled by digest.
    let (image, tag) = container_image(&["--pull", "debian"]);
    for arm in ["probe", "unset"] {
        let client = GenericImage::new(&image, &tag)
            .with_wait_for(WaitFor::exit(ExitWaitStrategy::new()))
            .with_entrypoint(CLIENT_BINARY)
            // Past the child's own deadlines, so the child reports why it failed.
            .with_startup_timeout(Duration::from_secs(300))
            .with_network("host")
            .with_copy_to(
                CopyTargetOptions::new(CLIENT_BINARY).with_mode(0o755),
                std::env::current_exe().expect("test binary"),
            )
            // The first bundle `probe` finds in this image.
            .with_copy_to(
                "/etc/ssl/certs/ca-certificates.crt",
                ca_pem.clone().into_bytes(),
            )
            .with_cmd(["--exact", NAME, "--ignored", "--nocapture"])
            .with_env_var(ARM, arm)
            .with_env_var(BROKERS, &brokers)
            .start()
            .expect("run the client container");
        let code = client.exit_code().expect("exit code");
        let stdout = String::from_utf8_lossy(&client.stdout_to_vec().expect("stdout")).into_owned();
        // A filter that matches nothing also exits 0.
        assert!(
            code == Some(0) && stdout.contains("1 passed"),
            "{arm} arm exited {code:?}: {stdout}{}",
            String::from_utf8_lossy(&client.stderr_to_vec().expect("stderr"))
        );
    }
}

/// A CA certificate, and a broker key and chain it signs for `127.0.0.1`, as PEM.
fn certificates() -> (String, String, String) {
    let mut ca = CertificateParams::new(Vec::<String>::new()).expect("CA params");
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.distinguished_name
        .push(DnType::CommonName, "spate-kafka-test CA");
    let ca_key = KeyPair::generate().expect("CA key");
    let ca_cert = ca.self_signed(&ca_key).expect("CA cert");
    let issuer = Issuer::new(ca, ca_key);

    let broker_key = KeyPair::generate().expect("broker key");
    let broker = CertificateParams::new(vec!["127.0.0.1".to_string(), "localhost".to_string()])
        .expect("broker params")
        .signed_by(&broker_key, &issuer)
        .expect("broker cert");
    (
        pem("CERTIFICATE", ca_cert.der()),
        pem("PRIVATE KEY", &broker_key.serialize_der()),
        pem("CERTIFICATE", broker.der()),
    )
}

fn pem(label: &str, der: &[u8]) -> String {
    let body = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for line in body.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).expect("base64 is ASCII"));
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

fn probe_arm(brokers: &str) {
    let tls = BTreeMap::from([("security.protocol".to_string(), "ssl".to_string())]);

    let mut sink_cfg = KafkaSinkConfig::new(brokers, TOPIC);
    sink_cfg.statistics_interval = Duration::ZERO;
    sink_cfg.rdkafka = tls.clone();
    let sink = spate_kafka::sink::build(sink_cfg).expect("sink build");
    let mut encoder = sink.encoder_bytes();
    let mut frames = Vec::new();
    for i in 0..RECORDS {
        let (ack, _rx) = AckRef::test_pair();
        let record = Record {
            payload: format!("tls-{i}").into_bytes(),
            meta: RecordMeta {
                partition: PartitionId(0),
                offset: i as i64,
                event_time_ms: 0,
                key_hash: None,
            },
            ack,
        };
        let mut frame = BytesMut::new();
        encoder.encode(&record, &mut frame).expect("encode");
        frames.push(frame.freeze());
    }
    let batch = SealedBatch {
        rows: RECORDS as u64,
        bytes: frames.iter().map(|f| f.len() as u64).sum(),
        frames,
        dedup_token: "unused-by-kafka".to_string(),
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(sink.writer.write_batch(&sink.endpoints[0][0], &batch))
        .expect("write_batch over TLS");
    rt.block_on(sink.probe_fn()())
        .expect("readiness probe over TLS");

    let mut source_cfg = KafkaSourceConfig::new(brokers, TOPIC, "tls-system-ca");
    source_cfg.startup_timeout = Duration::from_secs(60);
    source_cfg.rdkafka = tls;
    source_cfg
        .rdkafka
        .insert("auto.offset.reset".to_string(), "earliest".to_string());
    let cp = Checkpointer::new();
    let mut source = KafkaSource::new(source_cfg);
    source.open(SourceCtx::new(cp.handle())).expect("open");
    let mut lanes = None;
    spate_test::wait_until(Duration::from_secs(60), "partition assignment", || {
        if let SourceEvent::LanesAssigned(assigned) = source
            .poll_events(Duration::from_millis(200))
            .expect("poll_events")
        {
            lanes = Some(assigned);
        }
        lanes.is_some()
    });
    let mut lanes = lanes.expect("assigned");
    let rows = drain_lane(&mut source, &mut lanes[0], RECORDS, Duration::from_secs(60));
    assert_eq!(rows.len(), RECORDS);
}

fn unset_arm(brokers: &str) {
    let (producer_errors, consumer_errors) = (Errors::default(), Errors::default());
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", brokers)
        .set("security.protocol", "ssl")
        .set("group.id", "tls-system-ca-unset");
    let producer: BaseProducer<Errors> = cc
        .create_with_context(producer_errors.clone())
        .expect("raw producer");
    let consumer: BaseConsumer<Errors> = cc
        .create_with_context(consumer_errors.clone())
        .expect("raw consumer");
    spate_test::wait_until(
        Duration::from_secs(60),
        "a certificate verification failure from both clients with ssl.ca.location unset; \
         a handshake that succeeds means this build reads the system store without `probe` \
         (rdkafka-sys defines WITH_STATIC_LIB_libcrypto, or /usr/local/ssl holds the test CA)",
        || {
            producer.poll(Duration::ZERO);
            let _ = consumer.poll(Duration::ZERO);
            producer_errors.verify_failed() && consumer_errors.verify_failed()
        },
    );
}

/// A client's error callback text.
#[derive(Clone, Default)]
struct Errors(Arc<Mutex<Vec<String>>>);

impl Errors {
    fn verify_failed(&self) -> bool {
        self.0
            .lock()
            .expect("errors")
            .iter()
            .any(|e| e.contains("certificate verify failed"))
    }
}

impl ClientContext for Errors {
    fn error(&self, error: KafkaError, reason: &str) {
        self.0
            .lock()
            .expect("errors")
            .push(format!("{error}: {reason}"));
    }
}

impl ConsumerContext for Errors {}

impl ProducerContext for Errors {
    type DeliveryOpaque = ();

    fn delivery(&self, _: &DeliveryResult<'_>, _: Self::DeliveryOpaque) {}
}
