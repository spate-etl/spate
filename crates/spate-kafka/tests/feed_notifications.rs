//! Storage notifications through the Kafka feed: a store whose lane declares
//! `notifications` publishes each created object's notification to the work topic, and an S3 source
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
use spate_test_support::{ACCESS_KEY, REGION, S3Store, SECRET_KEY, Store};
use support::broker;
use support::feed::{
    DEADLINE, WORK, committed, create_topics, feed_yaml, high_watermark, launch_source, recs,
    written,
};
use testcontainers::ImageExt;
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::SyncRunner;
use testcontainers_modules::kafka::apache::KAFKA_PORT;

const BUCKET: &str = "notified";

/// Pins the read of a notified object end to end: the store publishes the
/// notification for a key with a space, `#`, `[ ]`, `%` and a non-ASCII
/// character through Kafka, the source decodes the key the event encodes
/// and reads the object under it, every record reaches the sink, and the
/// work topic commits past the notification.
#[test]
#[ignore = "requires Docker"]
fn a_key_with_reserved_and_non_ascii_bytes_reads_its_object() {
    // The broker maps the store's port too: the store runs in its network
    // namespace, so it reaches the broker on the listener the broker
    // advertises to other containers. Declared first, it is dropped last.
    let s3 = Store::selected().expect("the selected S3 lane");
    let kafka = broker()
        .with_mapped_port(0, KAFKA_PORT)
        .with_mapped_port(0, s3.container_port().tcp())
        .start()
        .expect("start kafka container");
    let brokers = format!(
        "127.0.0.1:{}",
        kafka.get_host_port_ipv4(KAFKA_PORT).expect("broker port")
    );
    let s3_port = kafka
        .get_host_port_ipv4(s3.container_port())
        .expect("S3 port");
    create_topics(&brokers);
    let s3_store = S3Store::start_notifying(kafka.id(), s3_port, WORK).expect("start the S3 store");
    s3_store.create_bucket(BUCKET).expect("create bucket");
    s3_store
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
        .with_endpoint(s3_store.endpoint())
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
        s3_store.endpoint()
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
    drop(s3_store);
}

/// Pins the overwrite path against a store: an object written twice before
/// the worker starts sends two notifications, the first one's read fails its
/// precondition, the worker re-enqueues the key at its current ETag through
/// the work topic, and the sink holds the second version's rows once for each
/// of the two units that name it and none of the first's.
#[test]
#[ignore = "requires Docker"]
fn an_object_overwritten_before_its_read_is_read_at_its_new_etag() {
    // Declared first, the broker is dropped last; see the test above.
    let s3 = Store::selected().expect("the selected S3 lane");
    let kafka = broker()
        .with_mapped_port(0, KAFKA_PORT)
        .with_mapped_port(0, s3.container_port().tcp())
        .start()
        .expect("start kafka container");
    let brokers = format!(
        "127.0.0.1:{}",
        kafka.get_host_port_ipv4(KAFKA_PORT).expect("broker port")
    );
    let s3_port = kafka
        .get_host_port_ipv4(s3.container_port())
        .expect("S3 port");
    create_topics(&brokers);
    let s3_store = S3Store::start_notifying(kafka.id(), s3_port, WORK).expect("start the S3 store");
    s3_store.create_bucket(BUCKET).expect("create bucket");
    s3_store
        .put_bucket_notification(BUCKET)
        .expect("configure notifications");

    let store = AmazonS3Builder::new()
        .with_endpoint(s3_store.endpoint())
        .with_allow_http(true)
        .with_bucket_name(BUCKET)
        .with_region(REGION)
        .with_access_key_id(ACCESS_KEY)
        .with_secret_access_key(SECRET_KEY)
        .build()
        .expect("S3 client");
    let lines = |rows: &[String]| {
        let mut body = Vec::new();
        for row in rows {
            body.extend_from_slice(row.as_bytes());
            body.push(b'\n');
        }
        body
    };
    let first = recs("first", 3);
    let second = recs("second", 5);
    let path = Path::parse("dir/overwritten.ndjson").unwrap();
    let io = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    io.block_on(store.put(&path, lines(&first).into()))
        .expect("put the first version");
    io.block_on(store.put(&path, lines(&second).into()))
        .expect("put the second version");
    wait_until(DEADLINE, "both notifications reach the work topic", || {
        high_watermark(&brokers, WORK) == 2
    });

    let group = spate_test::unique_name("overwritten");
    let source = format!(
        "  s3:\n    url: \"s3://{BUCKET}/\"\n    store:\n      endpoint: \"{}\"\n      \
         allow_http: \"true\"\n      access_key_id: \"{ACCESS_KEY}\"\n      \
         secret_access_key: \"{SECRET_KEY}\"\n      region: \"{REGION}\"\n",
        s3_store.endpoint()
    );
    let worker = launch_source(
        KafkaFeed::connect,
        &source,
        &feed_yaml(&brokers, &group, "", ""),
        |_| {},
    );

    let mut want: Vec<String> = second.iter().chain(&second).cloned().collect();
    want.sort();
    wait_until(
        DEADLINE,
        "the second version is written once per unit naming it",
        || written(&worker.script) == want,
    );
    wait_until(
        DEADLINE,
        "the notifications and the re-enqueue are committed",
        || committed(&brokers, &group, WORK) == Some(3),
    );
    assert_eq!(written(&worker.script), want);
    assert_eq!(high_watermark(&brokers, WORK), 3);
    assert_eq!(worker.stop(), ExitState::Completed);
    drop(s3_store);
}
