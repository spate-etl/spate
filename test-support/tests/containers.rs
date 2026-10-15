//! Smoke tests for the container helpers, against the images `ci/` pins.
//!
//! Ignored by default; run with Docker available:
//!
//! ```sh
//! cargo nextest run --profile docker -p spate-test-support --all-features \
//!   --locked --run-ignored ignored-only
//! ```

use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::TryStreamExt as _;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore as _, ObjectStoreExt as _, PutPayload};
use spate_test_support::{ACCESS_KEY, ElasticMq, Message, REGION, Rustfs, SECRET_KEY};

/// A bucket created through the helper takes a signed put, get, list and
/// delete, and the container joins the network it was given.
#[test]
#[ignore = "requires Docker"]
fn rustfs_round_trips_an_object() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let network = format!("spate-test-support-{}-{nanos}", std::process::id());
    let rustfs = Rustfs::start(Some(&network)).expect("start RustFS");
    assert_eq!(
        rustfs.container_name(),
        Some(format!("{network}-rustfs").as_str())
    );
    let joined = std::process::Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}",
        ])
        .arg(rustfs.container_name().unwrap())
        .output()
        .expect("docker inspect");
    assert!(joined.status.success(), "{joined:?}");
    assert!(
        String::from_utf8_lossy(&joined.stdout)
            .split_whitespace()
            .any(|n| n == network),
        "{joined:?}"
    );
    rustfs.create_bucket("smoke").expect("create the bucket");

    let store = AmazonS3Builder::new()
        .with_endpoint(rustfs.endpoint())
        .with_allow_http(true)
        .with_bucket_name("smoke")
        .with_region(REGION)
        .with_access_key_id(ACCESS_KEY)
        .with_secret_access_key(SECRET_KEY)
        .build()
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let key = StorePath::from("data/one.ndjson");
        store
            .put(&key, PutPayload::from_static(b"{\"k\":1}\n"))
            .await
            .unwrap();
        let body = store.get(&key).await.unwrap().bytes().await.unwrap();
        assert_eq!(&body[..], b"{\"k\":1}\n");
        let listed: Vec<String> = store
            .list(None)
            .map_ok(|meta| meta.location.to_string())
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed, ["data/one.ndjson"]);
        store.delete(&key).await.unwrap();
        let listed: Vec<_> = store.list(None).try_collect().await.unwrap();
        assert!(listed.is_empty(), "{listed:?}");
    });
}

/// A message sent to a queue the helper created is received once and, once
/// deleted, leaves the queue empty.
#[test]
#[ignore = "requires Docker"]
fn elasticmq_sends_receives_and_deletes() {
    let elasticmq = ElasticMq::start().expect("start ElasticMQ");
    let queue = elasticmq.create_queue("smoke").unwrap();
    assert_eq!(queue, "/000000000000/smoke");
    let body = "a & b <c> / #d\r\n";
    elasticmq.send_message(&queue, body).unwrap();

    let received = elasticmq.receive_messages(&queue).unwrap();
    let [
        Message {
            body: got,
            receipt_handle,
        },
    ] = received.as_slice()
    else {
        panic!("one message expected: {received:?}");
    };
    assert_eq!(got, body);
    assert_eq!(elasticmq.approximate_counts(&queue).unwrap(), (0, 1));
    elasticmq.delete_message(&queue, receipt_handle).unwrap();
    assert_eq!(elasticmq.approximate_counts(&queue).unwrap(), (0, 0));
}

/// One receive returns every visible message, up to ten.
#[test]
#[ignore = "requires Docker"]
fn elasticmq_one_receive_returns_every_message() {
    let elasticmq = ElasticMq::start().expect("start ElasticMQ");
    let queue = elasticmq.create_queue("several").unwrap();
    for body in ["one", "two", "three"] {
        elasticmq.send_message(&queue, body).unwrap();
    }
    let mut bodies: Vec<String> = elasticmq
        .receive_messages(&queue)
        .unwrap()
        .into_iter()
        .map(|m| m.body)
        .collect();
    bodies.sort();
    assert_eq!(bodies, ["one", "three", "two"]);
}

/// A queue created with a redrive policy reports it, naming its dead-letter
/// queue by ARN, and a deleted queue no longer answers.
#[test]
#[ignore = "requires Docker"]
fn elasticmq_reports_a_redrive_policy() {
    let elasticmq = ElasticMq::start().expect("start ElasticMQ");
    let dead = elasticmq.create_queue("smoke-dlq").unwrap();
    let arn = elasticmq.queue_arn(&dead).unwrap();
    assert!(arn.ends_with(":smoke-dlq"), "{arn}");
    let policy = format!(r#"{{"deadLetterTargetArn":"{arn}","maxReceiveCount":"5"}}"#);
    let queue = elasticmq
        .create_queue_with_attributes("smoke-src", &[("RedrivePolicy", &policy)])
        .unwrap();
    let reported = elasticmq
        .queue_attribute(&queue, "RedrivePolicy")
        .unwrap()
        .expect("the queue reports its redrive policy");
    assert!(reported.contains(&arn), "{reported}");
    assert!(reported.contains("\"maxReceiveCount\":5"), "{reported}");

    elasticmq.delete_queue(&dead).unwrap();
    assert!(
        elasticmq.queue_arn(&dead).is_err(),
        "a deleted queue answers"
    );
}
