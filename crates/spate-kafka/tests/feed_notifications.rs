//! Storage notifications through the Kafka feed: a RustFS store publishes
//! each created object's notification to the work topic, and an S3 source
//! on the feed reads the object it names.
//!
//! Every test needs Docker and is `#[ignore]`d. Run them with:
//!
//! ```sh
//! cargo nextest run --profile docker -p spate-kafka --all-features --locked \
//!   --run-ignored ignored-only --test feed_notifications
//! ```

mod support;

use object_store::ObjectStoreExt as _;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use spate_core::pipeline::ExitState;
use spate_kafka::feed::KafkaFeed;
use spate_test::wait_until;
use spate_test_support::{ACCESS_KEY, REGION, Rustfs, S3_PORT, SECRET_KEY};
use support::broker;
use support::feed::{
    DEADLINE, WORK, committed, create_topics, feed_yaml, launch_source, recs, written,
};
use testcontainers::ImageExt;
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::SyncRunner;
use testcontainers_modules::kafka::apache::KAFKA_PORT;

const BUCKET: &str = "notified";

/// Pins the read of a notified object end to end: RustFS publishes the
/// notification for a key with a space, `#`, `[ ]`, `%` and a non-ASCII
/// character through Kafka, the source decodes the key the event encodes
/// and reads the object under it, every record reaches the sink, and the
/// work topic commits past the notification.
#[test]
#[ignore = "requires Docker"]
fn a_key_with_reserved_and_non_ascii_bytes_reads_its_object() {
    // The broker maps RustFS's port too: RustFS runs in its network
    // namespace, so it reaches the broker on the listener the broker
    // advertises to other containers. Declared first, it is dropped last.
    let kafka = broker()
        .with_mapped_port(0, KAFKA_PORT)
        .with_mapped_port(0, S3_PORT.tcp())
        .start()
        .expect("start kafka container");
    let brokers = format!(
        "127.0.0.1:{}",
        kafka.get_host_port_ipv4(KAFKA_PORT).expect("broker port")
    );
    let s3_port = kafka.get_host_port_ipv4(S3_PORT).expect("S3 port");
    create_topics(&brokers);
    let rustfs = Rustfs::start_notifying(kafka.id(), s3_port, WORK).expect("start RustFS");
    rustfs.create_bucket(BUCKET).expect("create bucket");
    rustfs
        .put_bucket_notification(BUCKET)
        .expect("configure notifications");

    let key = "dir/é x#y[1]%.ndjson";
    let rows = recs("notified", 5);
    let mut body = Vec::new();
    for row in &rows {
        body.extend_from_slice(row.as_bytes());
        body.push(b'\n');
    }
    let store = AmazonS3Builder::new()
        .with_endpoint(rustfs.endpoint())
        .with_allow_http(true)
        .with_bucket_name(BUCKET)
        .with_region(REGION)
        .with_access_key_id(ACCESS_KEY)
        .with_secret_access_key(SECRET_KEY)
        .build()
        .expect("S3 client");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(store.put(&Path::parse(key).unwrap(), body.into()))
        .expect("put the object");

    let group = spate_test::unique_name("notified");
    let source = format!(
        "  s3:\n    url: \"s3://{BUCKET}/\"\n    store:\n      endpoint: \"{}\"\n      \
         allow_http: \"true\"\n      access_key_id: \"{ACCESS_KEY}\"\n      \
         secret_access_key: \"{SECRET_KEY}\"\n      region: \"{REGION}\"\n",
        rustfs.endpoint()
    );
    let worker = launch_source(
        KafkaFeed::connect,
        &source,
        &feed_yaml(&brokers, &group, "", ""),
        |_| {},
    );

    let mut want = rows.clone();
    want.sort();
    wait_until(
        DEADLINE,
        "every record of the notified object is written",
        || written(&worker.script) == want,
    );
    wait_until(DEADLINE, "the notification's offset is committed", || {
        committed(&brokers, &group, WORK) == Some(1)
    });
    assert_eq!(worker.stop(), ExitState::Completed);
    drop(rustfs);
}
