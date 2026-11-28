//! `tls.root_ca` against a broker whose CA the client trusts only through it,
//! only through the system roots, through both, or through neither.
//!
//! Each arm runs in a child process of this binary, whose system roots are
//! the file `SSL_CERT_FILE` names. `SSL_CERT_DIR` is removed, since the
//! store reads it too and cargo sets it on Linux.
#![cfg(feature = "tls")]

mod support;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use spate_core::checkpoint::Checkpointer;
use spate_core::error::ErrorClass;
use spate_core::feed::{Feed as _, Unit};
use spate_core::source::{Source as _, SourceCtx};
use spate_core::tls::TlsSection;
use spate_test::HANG;
use spate_test_support::{TestCa, pem};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use support::Raw as _;
use support::feed::{ON_BROKER, direct_with, publisher_on};
use support::tls::{connector_clients, create_topic, raw, tls_broker, tls_source};

const NAME: &str = "root_ca_adds_to_the_system_roots";
const ARM: &str = "SPATE_TEST_ROOT_CA_ARM";
const BROKERS: &str = "SPATE_TEST_ROOT_CA_BROKERS";
const ROOT_CA: &str = "SPATE_TEST_ROOT_CA_FILE";

/// A source, a sink, its readiness probe, a feed publisher and a feed worker
/// complete a TLS handshake when the broker's CA is in `tls.root_ca`, in the
/// system roots, or in both. With it in neither, the source fails on the
/// rejected handshake.
#[test]
#[ignore = "requires Docker"]
fn root_ca_adds_to_the_system_roots() {
    if client_arm() {
        return;
    }
    let (ca_pem, _kafka, brokers) = tls_broker();
    let dir = tempfile::tempdir().unwrap();
    let broker_ca = dir.path().join("broker-ca.pem");
    std::fs::write(&broker_ca, &ca_pem).unwrap();
    let unrelated = dir.path().join("unrelated-ca.pem");
    let other = TestCa::new("spate-kafka-test unrelated CA");
    std::fs::write(&unrelated, pem("CERTIFICATE", &other.der())).unwrap();

    let trusted = || {
        let mut cc = raw(&brokers);
        cc.set("ssl.ca.pem", &ca_pem);
        cc
    };
    // (arm, system roots, `tls.root_ca`)
    for (arm, system, root_ca) in [
        ("root-ca-only", &unrelated, &broker_ca),
        ("system-only", &broker_ca, &unrelated),
        ("both", &broker_ca, &broker_ca),
        ("neither", &unrelated, &unrelated),
    ] {
        create_topic(&trusted(), &records(arm));
        create_feed_topics(&trusted(), arm);
        spate_test_support::run_in_child(NAME, |child| {
            child
                .args(["--ignored", "--nocapture"])
                .env(ARM, arm)
                .env(BROKERS, &brokers)
                .env(ROOT_CA, root_ca)
                .env("SSL_CERT_FILE", system)
                .env_remove("SSL_CERT_DIR")
        });
    }
}

/// Runs the client arm named by the environment, if any, and reports whether
/// it did.
fn client_arm() -> bool {
    let Ok(arm) = std::env::var(ARM) else {
        return false;
    };
    let brokers = std::env::var(BROKERS).expect("broker address");
    let root_ca = PathBuf::from(std::env::var_os(ROOT_CA).expect("root_ca"));
    let rdkafka = BTreeMap::from([("security.protocol".to_owned(), "ssl".to_owned())]);
    let mut tls = TlsSection::default();
    tls.root_ca = Some(root_ca.clone());
    if arm == "neither" {
        verification_fails(&brokers, &records(&arm), &rdkafka, &tls);
    } else {
        connector_clients(&brokers, &records(&arm), &rdkafka, &tls);
        feed_round_trip(&brokers, &arm, &root_ca);
    }
    true
}

/// The topic an arm's source and sink use.
fn records(arm: &str) -> String {
    format!("{arm}-records")
}

/// A publisher connects and publishes a unit, and a worker connects and
/// receives it, on the arm's feed topics.
fn feed_round_trip(brokers: &str, arm: &str, root_ca: &Path) {
    let yaml = format!(
        "feed:\n  kafka:\n    brokers: \"{brokers}\"\n    group_id: {arm}-workers\n    \
         topic: {arm}-units\n    retry_topic: {arm}-units-retry\n    \
         dead_letter_topic: {arm}-units-dead\n    attempt_topic: {arm}-units-attempts\n    \
         statistics_interval: 0s\n    rdkafka:\n      security.protocol: ssl\n    \
         tls:\n      root_ca: \"{}\"\n",
        root_ca.display()
    );
    let (publisher, _io) = publisher_on(&yaml);
    let unit = Unit::new(format!("{arm}-unit"), b"body".to_vec());
    spate_test_s3_feed::publish(&publisher.expect("publisher connects"), unit.clone());
    let worker = direct_with(ON_BROKER, &yaml);
    let delivery = worker.take("the published unit");
    assert_eq!(delivery.unit, unit);
    worker.feed.acknowledge(delivery.receipt).unwrap();
}

/// The source fails on the rejected handshake, with
/// `certificate verify failed` in its error.
fn verification_fails(
    brokers: &str,
    topic: &str,
    rdkafka: &BTreeMap<String, String>,
    tls: &TlsSection,
) {
    let mut source = tls_source(brokers, topic, rdkafka, tls);
    let cp = Checkpointer::new();
    source.open(SourceCtx::new(cp.handle())).expect("open");
    let mut failed = None;
    spate_test::wait_until(HANG, "the source fails", || {
        failed = source.poll_events(Duration::from_millis(200)).err();
        failed.is_some()
    });
    let err = failed.expect("failed");
    assert_eq!(err.class(), ErrorClass::Fatal, "{err}");
    assert!(
        err.to_string().contains("certificate verify failed"),
        "{err}"
    );
}

/// Create the arm's feed topics, as the feed suites create theirs, and wait
/// until the broker lists them.
fn create_feed_topics(cc: &rdkafka::ClientConfig, arm: &str) {
    let names = [
        format!("{arm}-units"),
        format!("{arm}-units-retry"),
        format!("{arm}-units-dead"),
        format!("{arm}-units-attempts"),
    ];
    let topics = [
        NewTopic::new(&names[0], 1, TopicReplication::Fixed(1))
            .set("message.timestamp.type", "LogAppendTime"),
        NewTopic::new(&names[1], 1, TopicReplication::Fixed(1)),
        NewTopic::new(&names[2], 1, TopicReplication::Fixed(1)),
        NewTopic::new(&names[3], 1, TopicReplication::Fixed(1)).set("cleanup.policy", "compact"),
    ];
    let admin: AdminClient<DefaultClientContext> = cc.build().expect("admin client");
    let created = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(admin.create_topics(&topics, &AdminOptions::new()))
        .expect("create topics");
    for result in created {
        result.unwrap_or_else(|(topic, code)| panic!("create {topic}: {code}"));
    }
    for name in &names {
        create_topic(cc, name);
    }
}
