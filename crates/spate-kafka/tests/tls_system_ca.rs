//! TLS against a broker whose CA exists only in a trust store the client
//! container holds.
//!
//! The clients run as a test binary inside a pinned container, so the host's
//! trust store and environment play no part. One test runs this binary, built
//! against vendored OpenSSL, with the CA in the system bundle. The other builds
//! the binary against the system OpenSSL and puts the CA only in the hashed
//! certificate directory.
#![cfg(all(feature = "tls", target_os = "linux"))]

mod support;

use bytes::BytesMut;
use rdkafka::ClientConfig;
use rdkafka::client::ClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, ConsumerContext};
use rdkafka::error::KafkaError;
use spate_core::checkpoint::{AckRef, Checkpointer};
use spate_core::record::{PartitionId, Record, RecordMeta};
use spate_core::sink::{RowEncoder, SealedBatch, ShardWriter};
use spate_core::source::{Source, SourceCtx, SourceEvent};
use spate_kafka::sink::KafkaSinkConfig;
use spate_kafka::{KafkaSource, KafkaSourceConfig};
use spate_test_support::{TestCa, container_image, pem};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{collections::BTreeMap, fs};
use support::{broker, drain_lane};
use testcontainers::core::wait::ExitWaitStrategy;
use testcontainers::core::{AccessMode, Mount, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ContainerRequest, CopyTargetOptions, GenericImage, ImageExt};
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

const BUNDLE: &str = "probe_ca_default_completes_a_tls_handshake";
const HASHED: &str = "system_openssl_default_preserves_hashed_directory_trust";
const ARM: &str = "SPATE_TEST_PROBE_ARM";
const BROKERS: &str = "SPATE_TEST_PROBE_BROKERS";
const TOPIC: &str = "tls-system-ca";
const RECORDS: usize = 10;
const CLIENT_BINARY: &str = "/spate/tls_system_ca";
const HASHED_CA: &str = "/etc/ssl/certs/spate-test-ca.pem";

/// A source, a sink and its readiness probe with the connector's CA default
/// complete a TLS handshake against a broker signed by a CA only the system
/// bundle holds, and a client naming an unrelated CA fails verification.
#[test]
#[ignore = "requires Docker"]
fn probe_ca_default_completes_a_tls_handshake() {
    if client_arm() {
        return;
    }
    let (ca_pem, _kafka, brokers) = tls_broker();
    // `probe` first: it shares the broker certificate, so a certificate
    // defect fails there and cannot pass as a verification failure below.
    // `ci/debian/` pins the image, pulled by digest.
    let (image, tag) = container_image(&["--pull", "debian"]);
    for arm in ["probe", "untrusted"] {
        let client = GenericImage::new(&image, &tag)
            .with_wait_for(WaitFor::exit(ExitWaitStrategy::new()))
            .with_entrypoint(CLIENT_BINARY);
        let client = client_request(
            client,
            &std::env::current_exe().expect("test binary"),
            arm,
            &brokers,
        )
        // The first bundle `probe` finds in this image.
        .with_copy_to(
            "/etc/ssl/certs/ca-certificates.crt",
            ca_pem.clone().into_bytes(),
        )
        .with_cmd(["--exact", BUNDLE, "--ignored", "--nocapture"])
        .start()
        .expect("run the client container");
        passed(&client, arm);
    }
}

/// Built against the system OpenSSL, a source, a sink and its readiness probe
/// with the connector's CA default trust a CA found only in the hashed
/// certificate directory, which explicit `probe` does not read.
///
/// Regression for #825.
#[test]
#[ignore = "requires Docker"]
fn system_openssl_default_preserves_hashed_directory_trust() {
    if client_arm() {
        return;
    }
    // `ci/rust/` pins the image, pulled by digest. It builds the client and
    // runs it, with the system libssl the build links.
    let (image, tag) = container_image(&["--pull", "rust"]);
    let out = Scratch::new(Path::new(env!("CARGO_TARGET_TMPDIR")).join("tls_system_ca"));
    let binary = build_system_linked(&image, &tag, &out.0);
    let (ca_pem, _kafka, brokers) = tls_broker();

    let client = GenericImage::new(&image, &tag)
        .with_wait_for(WaitFor::exit(ExitWaitStrategy::new()))
        .with_entrypoint("sh");
    let script = format!(
        "ldd {CLIENT_BINARY} && openssl rehash /etc/ssl/certs && \
         exec {CLIENT_BINARY} --exact {HASHED} --ignored --nocapture"
    );
    let client = client_request(client, &binary, "system", &brokers)
        .with_copy_to(HASHED_CA, ca_pem.into_bytes())
        .with_cmd(["-c", &script])
        .start()
        .expect("run the client container");
    let stdout = passed(&client, "system");
    for lib in ["libssl.so.3 =>", "libcrypto.so.3 =>"] {
        assert!(
            stdout.contains(lib),
            "the client does not link {lib}: {stdout}"
        );
    }
}

/// Boots a TLS broker signed by a fresh CA, and returns the CA as PEM, the
/// container and its address.
fn tls_broker() -> (String, Container<Kafka>, String) {
    let ca = TestCa::new("spate-kafka-test CA");
    let (chain, key) = ca.leaf(&["127.0.0.1", "localhost"]);
    // Kafka reads these as properties, where `\n` is a newline.
    let inline = |pem: String| pem.replace('\n', "\\n");
    let kafka = broker()
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
        )
        .start()
        .expect("start kafka container");
    let port = kafka.get_host_port_ipv4(KAFKA_PORT).expect("port");
    (
        pem("CERTIFICATE", &ca.der()),
        kafka,
        format!("127.0.0.1:{port}"),
    )
}

fn connector_clients(brokers: &str) {
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

/// Runs the client arm named by the environment, if any, and reports whether it did.
fn client_arm() -> bool {
    let Ok(arm) = std::env::var(ARM) else {
        return false;
    };
    let brokers = std::env::var(BROKERS).expect("broker address");
    match arm.as_str() {
        "probe" => connector_clients(&brokers),
        "untrusted" => {
            let unrelated = TestCa::new("spate-kafka-test unrelated CA");
            let pem = pem("CERTIFICATE", &unrelated.der());
            assert!(
                !verifies(raw(&brokers).set("ssl.ca.pem", pem)),
                "a client naming an unrelated CA verified the broker"
            );
        }
        "system" => system_arm(&brokers),
        other => panic!("unknown arm {other}"),
    }
    true
}

fn system_arm(brokers: &str) {
    for var in ["SSL_CERT_FILE", "SSL_CERT_DIR"] {
        assert!(std::env::var_os(var).is_none(), "{var} is set");
    }
    let ca = fs::read_to_string(HASHED_CA).expect("test CA");
    assert!(
        verifies(&raw(brokers)),
        "raw default must trust the hashed directory"
    );
    assert!(
        verifies(raw(brokers).set("ssl.ca.location", "/etc/ssl/certs")),
        "explicit directory must verify"
    );
    assert!(
        verifies(raw(brokers).set("ssl.ca.pem", ca)),
        "explicit PEM must verify"
    );
    // The image's bundle is the first `probe` finds, and it lacks the CA.
    assert!(
        !verifies(raw(brokers).set("ssl.ca.location", "probe")),
        "explicit probe verified with the CA outside the bundle"
    );
    connector_clients(brokers);
}

/// A client container on the host network that runs `binary` as `arm`.
fn client_request(
    image: GenericImage,
    binary: &Path,
    arm: &str,
    brokers: &str,
) -> ContainerRequest<GenericImage> {
    image
        // Past the child's own deadlines, so the child reports why it failed.
        .with_startup_timeout(Duration::from_secs(300))
        .with_network("host")
        .with_copy_to(
            CopyTargetOptions::new(CLIENT_BINARY).with_mode(0o755),
            binary.to_path_buf(),
        )
        .with_env_var(ARM, arm)
        .with_env_var(BROKERS, brokers)
}

/// Asserts that the client ran exactly one test and passed, and returns its stdout.
fn passed(client: &Container<GenericImage>, arm: &str) -> String {
    let code = client.exit_code().expect("exit code");
    let stdout = String::from_utf8_lossy(&client.stdout_to_vec().expect("stdout")).into_owned();
    // A filter that matches nothing also exits 0.
    assert!(
        code == Some(0)
            && stdout.lines().any(|l| l == "running 1 test")
            && stdout.contains("1 passed"),
        "{arm} arm exited {code:?}: {stdout}{}",
        String::from_utf8_lossy(&client.stderr_to_vec().expect("stderr"))
    );
    stdout
}

/// Builds this test binary against the system OpenSSL in `image`, offline and
/// from the host's cargo registry, and returns its path under `out`.
///
/// The build runs as the owner of `out`, so the test can remove what it writes.
fn build_system_linked(image: &str, tag: &str, out: &Path) -> PathBuf {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root");
    let cargo_home = std::env::var_os("CARGO_HOME").map_or_else(
        || Path::new(&std::env::var_os("HOME").expect("HOME")).join(".cargo"),
        PathBuf::from,
    );
    let read_only = |host: &Path, container: &str| {
        Mount::bind_mount(host.display().to_string(), container)
            .with_access_mode(AccessMode::ReadOnly)
    };
    let owner = fs::metadata(out).expect("output directory");
    let script = "set -eu
        mkdir -p \"$CARGO_HOME/registry\"
        cp -R /registry/index /registry/cache \"$CARGO_HOME/registry/\"
        cargo test -p spate-kafka --features tls --test tls_system_ca --no-run --offline --locked
        cp \"$(find /tmp/target/debug/deps -maxdepth 1 -type f -name 'tls_system_ca-*' ! -name '*.d')\" /out/tls_system_ca";
    let builder = GenericImage::new(image, tag)
        .with_wait_for(WaitFor::exit(ExitWaitStrategy::new()))
        .with_entrypoint("sh")
        .with_cmd(["-c", script])
        // A cold build of librdkafka and every dev-dependency.
        .with_startup_timeout(Duration::from_secs(1800))
        .with_network("none")
        .with_user(format!("{}:{}", owner.uid(), owner.gid()))
        .with_mount(read_only(&workspace, "/workspace"))
        .with_mount(read_only(
            &cargo_home.join("registry/index"),
            "/registry/index",
        ))
        .with_mount(read_only(
            &cargo_home.join("registry/cache"),
            "/registry/cache",
        ))
        .with_mount(Mount::bind_mount(out.display().to_string(), "/out"))
        .with_working_dir("/workspace")
        .with_env_var("OPENSSL_NO_VENDOR", "1")
        .with_env_var("CARGO_HOME", "/tmp/cargo")
        .with_env_var("HOME", "/tmp")
        .with_env_var("CARGO_TARGET_DIR", "/tmp/target")
        .with_env_var("CARGO_TERM_COLOR", "never")
        .start()
        .expect("run the builder container");
    let code = builder.exit_code().expect("exit code");
    assert_eq!(
        code,
        Some(0),
        "system OpenSSL build failed: {}",
        String::from_utf8_lossy(&builder.stderr_to_vec().expect("stderr"))
    );
    out.join("tls_system_ca")
}

/// A directory, emptied on creation and removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new(path: PathBuf) -> Self {
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("create scratch directory");
        Scratch(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// A raw TLS client configuration for `brokers`, with no CA setting.
fn raw(brokers: &str) -> ClientConfig {
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", brokers)
        .set("security.protocol", "ssl")
        .set("group.id", "tls-system-ca-raw");
    cc
}

/// Whether a consumer on `cc` verifies the broker's certificate, read from
/// librdkafka's security debug log.
fn verifies(cc: &ClientConfig) -> bool {
    let lines = Lines::default();
    let consumer: BaseConsumer<Lines> = cc
        .clone()
        .set("debug", "security")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(lines.clone())
        .expect("raw consumer");
    let mut verified = None;
    spate_test::wait_until(
        Duration::from_secs(60),
        "a certificate verification outcome",
        || {
            let _ = consumer.poll(Duration::from_millis(100));
            let lines = lines.0.lock().expect("lines");
            let seen = |text| lines.iter().any(|l| l.contains(text));
            verified = if seen("Broker SSL certificate verified") {
                Some(true)
            } else if seen("certificate verify failed") {
                Some(false)
            } else {
                None
            };
            verified.is_some()
        },
    );
    verified.expect("outcome")
}

/// A client's log and error lines.
#[derive(Clone, Default)]
struct Lines(Arc<Mutex<Vec<String>>>);

impl ClientContext for Lines {
    fn log(&self, _: RDKafkaLogLevel, facility: &str, message: &str) {
        self.0
            .lock()
            .expect("lines")
            .push(format!("{facility}: {message}"));
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.0
            .lock()
            .expect("lines")
            .push(format!("{error}: {reason}"));
    }
}

impl ConsumerContext for Lines {}
