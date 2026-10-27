//! Storage notifications through the Kafka feed: a store whose lane declares
//! `notifications` publishes each created object's notification to the work
//! topic, and an S3 source on the feed reads the object it names.
//!
//! Every test needs Docker and is `#[ignore]`d. Run them with:
//!
//! ```sh
//! cargo nextest run --profile docker -p spate-kafka --all-features --locked \
//!   --run-ignored ignored-only --test feed_notifications
//! ```

mod support;

use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{Attribute, ObjectStore as _, ObjectStoreExt as _, PutOptions, TagSet};
use serde_json::Value;
use spate_core::pipeline::ExitState;
use spate_kafka::feed::KafkaFeed;
use spate_test::wait_until;
use spate_test_support::{ACCESS_KEY, REGION, RGW_OPAQUE_DATA, S3Store, SECRET_KEY, Store};
use support::broker;
use support::feed::{
    DEAD, DEADLINE, RETRY, WORK, Worker, committed, create_topics, feed_yaml, high_watermark,
    launch_source, read_topic, recs, written,
};
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ImageExt};
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

const BUCKET: &str = "notified";

/// Pins the read of a notified object end to end: the store publishes the
/// notification for a key with a space, `#`, `[ ]`, `%` and a non-ASCII
/// character through Kafka, the source reads the key the event carries,
/// form-decoded or as written by its `eventSource`, and reads the object
/// under it, every record reaches the sink, and the work topic commits past
/// the notification.
#[test]
#[ignore = "requires Docker"]
fn a_key_with_reserved_and_non_ascii_bytes_reads_its_object() {
    let n = notifying();
    let rows = recs("notified", 5);
    n.put(
        &Path::parse("dir/é x#y[1]%.ndjson").unwrap(),
        &rows,
        PutOptions::default(),
    );

    let worker = n.worker("notified");
    let mut want = rows.clone();
    want.sort();
    wait_until(
        DEADLINE,
        "every record of the notified object is written",
        || written(&worker.script) == want,
    );
    wait_until(DEADLINE, "the notification's offset is committed", || {
        committed(&n.brokers, &worker.group, WORK) == Some(1)
    });
    assert_eq!(worker.stop(), ExitState::Completed);
}

/// Pins the overwrite path against a store: an object written twice before
/// the worker starts sends two notifications, the first one's read fails its
/// precondition, the worker re-enqueues the key at its current ETag through
/// the work topic under the stale unit's id, and the sink holds the second
/// version's rows once for each of the two units that name it and none of
/// the first's. The key holds `+` and `%25`, so a key the store sends as
/// written goes through the form-encoded re-enqueue and back.
#[test]
#[ignore = "requires Docker"]
fn an_object_overwritten_before_its_read_is_read_at_its_new_etag() {
    let n = notifying();
    let first = recs("first", 3);
    let second = recs("second", 5);
    let path = Path::parse("dir/over+written c%25.ndjson").unwrap();
    n.put(&path, &first, PutOptions::default());
    n.put(&path, &second, PutOptions::default());
    wait_until(DEADLINE, "both notifications reach the work topic", || {
        high_watermark(&n.brokers, WORK) == 2
    });

    let worker = n.worker("overwritten");
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
        || committed(&n.brokers, &worker.group, WORK) == Some(3),
    );
    assert_eq!(written(&worker.script), want);
    assert_eq!(high_watermark(&n.brokers, WORK), 3);

    let current = n.io.block_on(n.client.head(&path)).expect("head").e_tag;
    let current = unquoted(current.as_deref().expect("an ETag"));
    let messages = read_topic(&n.brokers, WORK, 3);
    let stale = (0..2)
        .find(|&i| record(&messages[i])["s3"]["object"]["eTag"].as_str() != Some(current))
        .expect("one notification names the first version");
    let stale_id = messages[stale].key.clone().map_or_else(
        || format!("{WORK}/0/{stale}"),
        |key| String::from_utf8(key).expect("a UTF-8 key"),
    );
    let re_enqueued = messages[2].key.as_deref().map(String::from_utf8_lossy);
    assert_eq!(
        re_enqueued.as_deref(),
        Some(stale_id.as_str()),
        "the re-enqueue keeps the stale unit's id"
    );
    assert_eq!(worker.stop(), ExitState::Completed);
}

/// Pins what each store announces and that the source reads every object
/// under its own name: one object per key holding a space, `+` and `%25`,
/// a bare `%`, a literal `%2F` beside a raw `/`, the pairs `p+q` and `p q`,
/// `%41` and `A`, and three- and four-byte UTF-8. Each notification carries
/// the key, `eventSource`, Kafka key and `eventName` the store's arm states,
/// and an RFC 3339 `eventTime`. One object carries user metadata and a tag,
/// and each store's event carries both, with Ceph RGW's topic `OpaqueData`,
/// as the S3 page states. Every object's rows reach the sink once, with no
/// retry, dead letter or re-enqueue.
#[test]
#[ignore = "requires Docker"]
fn every_notified_key_reads_its_own_object() {
    let n = notifying();
    let keys = [
        "p/a space.ndjson",
        "p/a+b c%25.ndjson",
        "p/bare%.ndjson",
        "p/a%2Fb.ndjson",
        "p/a/b.ndjson",
        "p/p+q.ndjson",
        "p/p q.ndjson",
        "p/%41.ndjson",
        "p/A.ndjson",
        "p/中𝄞.ndjson",
    ];
    let mut want = Vec::new();
    for (i, key) in keys.iter().enumerate() {
        let rows = recs(&format!("o{i}"), 3);
        let mut options = PutOptions::default();
        if i == 0 {
            options
                .attributes
                .insert(Attribute::Metadata("note".into()), "hello".into());
            let mut tags = TagSet::default();
            tags.push("tagk", "tagv");
            options.tags = tags;
        }
        n.put(&Path::parse(key).unwrap(), &rows, options);
        want.extend(rows);
    }
    want.sort();
    let mut listed = Vec::new();
    let mut prefixes = vec![Path::from("p")];
    while let Some(prefix) = prefixes.pop() {
        let page =
            n.io.block_on(n.client.list_with_delimiter(Some(&prefix)))
                .expect("list");
        listed.extend(
            page.objects
                .into_iter()
                .map(|meta| meta.location.to_string()),
        );
        prefixes.extend(page.common_prefixes);
    }
    listed.sort_unstable();
    let mut written_keys: Vec<String> = keys.iter().map(|k| (*k).to_owned()).collect();
    written_keys.sort_unstable();
    assert_eq!(listed, written_keys, "the store holds each key as written");

    let total = keys.len();
    wait_until(
        DEADLINE,
        "every notification reaches the work topic",
        || high_watermark(&n.brokers, WORK) == i64::try_from(total).unwrap(),
    );
    let store = n.s3.store();
    let messages = read_topic(&n.brokers, WORK, total);
    for key in keys {
        let expect = announced(store, key, "Put");
        let message = messages
            .iter()
            .find(|m| record(m)["s3"]["object"]["key"].as_str() == Some(expect.key.as_str()))
            .unwrap_or_else(|| panic!("no notification announces {key:?} as {:?}", expect.key));
        let record = record(message);
        let payload = message.payload.as_deref().map_or(0, <[u8]>::len);
        assert_eq!(
            message
                .key
                .as_deref()
                .map(String::from_utf8_lossy)
                .as_deref(),
            expect.kafka_key.as_deref(),
            "the Kafka key for {key:?}"
        );
        assert_eq!(record["eventSource"], expect.source, "{key:?}");
        assert_eq!(record["eventName"], expect.event_name, "{key:?}");
        let time = record["eventTime"].as_str().unwrap_or_default();
        assert!(rfc3339_utc(time), "{key:?}: eventTime {time:?}");
        carries_store_fields(store, &record, key == keys[0], payload);
    }

    let worker = n.worker("every-key");
    wait_until(DEADLINE, "every object's rows are written once", || {
        written(&worker.script) == want
    });
    wait_until(DEADLINE, "every notification is committed", || {
        committed(&n.brokers, &worker.group, WORK) == Some(i64::try_from(total).unwrap())
    });
    assert_eq!(
        high_watermark(&n.brokers, WORK),
        i64::try_from(total).unwrap(),
        "nothing was re-enqueued"
    );
    assert_eq!(high_watermark(&n.brokers, RETRY), 0, "nothing was retried");
    assert_eq!(
        high_watermark(&n.brokers, DEAD),
        0,
        "nothing was dead-lettered"
    );
    assert_eq!(worker.stop(), ExitState::Completed);
}

/// Pins that a multipart object is announced at its stored size and read to
/// its last record: two parts, the first 5 MiB, one `CompleteMultipartUpload`
/// notification whose size equals the object's and whose ETag counts the
/// parts. The read pins that ETag through `If-Match`. The bucket has no
/// compression or encryption, under which the announced size is the stored
/// size.
#[test]
#[ignore = "requires Docker"]
fn a_multipart_object_is_announced_at_its_stored_size_and_read() {
    let n = notifying();
    let path = Path::parse("multi/parts.ndjson").unwrap();
    // Large rows keep the object over one 5 MiB part with few records for
    // the sink to write.
    let pad = "p".repeat(60_000);
    let rows: Vec<String> = (0..100)
        .map(|i| format!("{{\"k\":\"multi-{i}\",\"pad\":\"{pad}\"}}"))
        .collect();
    let mut body = Vec::new();
    for row in &rows {
        body.extend_from_slice(row.as_bytes());
        body.push(b'\n');
    }
    let cut = 5 * 1024 * 1024;
    assert!(body.len() > cut, "the second part is not empty");
    let mut upload =
        n.io.block_on(n.client.put_multipart(&path))
            .expect("start the upload");
    n.io.block_on(upload.put_part(body[..cut].to_vec().into()))
        .expect("the first part");
    n.io.block_on(upload.put_part(body[cut..].to_vec().into()))
        .expect("the second part");
    n.io.block_on(upload.complete())
        .expect("complete the upload");
    let stored = n.io.block_on(n.client.head(&path)).expect("head").size;

    wait_until(DEADLINE, "the notification reaches the work topic", || {
        high_watermark(&n.brokers, WORK) >= 1
    });
    let messages = read_topic(&n.brokers, WORK, 1);
    let record = record(&messages[0]);
    let expect = announced(n.s3.store(), path.as_ref(), "CompleteMultipartUpload");
    assert_eq!(record["eventName"], expect.event_name);
    assert_eq!(
        record["s3"]["object"]["size"].as_u64(),
        Some(stored),
        "the announced size against the stored size"
    );
    let etag = record["s3"]["object"]["eTag"].as_str().unwrap_or_default();
    assert!(etag.ends_with("-2"), "a two-part ETag: {etag:?}");

    let worker = n.worker("multipart");
    let mut want = rows.clone();
    want.sort();
    wait_until(DEADLINE, "every record of the object is written", || {
        written(&worker.script) == want
    });
    wait_until(DEADLINE, "the notification is committed", || {
        committed(&n.brokers, &worker.group, WORK) == Some(1)
    });
    assert_eq!(
        high_watermark(&n.brokers, WORK),
        1,
        "nothing was re-enqueued"
    );
    assert_eq!(high_watermark(&n.brokers, RETRY), 0, "nothing was retried");
    assert_eq!(
        high_watermark(&n.brokers, DEAD),
        0,
        "nothing was dead-lettered"
    );
    assert_eq!(worker.stop(), ExitState::Completed);
}

/// What a store announces for an object written at `key` by an
/// `ObjectCreated:{event}` request.
struct Announced {
    /// The record's `s3.object.key`.
    key: String,
    /// The Kafka message's key.
    kafka_key: Option<String>,
    source: &'static str,
    event_name: String,
}

fn announced(store: Store, key: &str, event: &str) -> Announced {
    match store {
        Store::Rustfs => {
            let encoded = rustfs_encoded(key);
            Announced {
                kafka_key: Some(format!("{BUCKET}/{encoded}")),
                key: encoded,
                source: "rustfs:s3",
                event_name: format!("s3:ObjectCreated:{event}"),
            }
        }
        Store::Rgw => Announced {
            key: key.to_owned(),
            kafka_key: None,
            source: "ceph:s3",
            event_name: format!("ObjectCreated:{event}"),
        },
        Store::Garage => unreachable!("Garage sends no bucket notifications"),
    }
}

/// Asserts the fields beyond the key each store's event carries: an object
/// written with `note: hello` metadata and the tag `tagk=tagv` carries both
/// when `tagged`, and Ceph RGW's carries the topic's `OpaqueData` and an
/// empty `versionId`. `payload` is the message's length, for the failure
/// message.
fn carries_store_fields(store: Store, record: &Value, tagged: bool, payload: usize) {
    let object = &record["s3"]["object"];
    match store {
        Store::Rustfs => {
            if tagged {
                let meta = &object["userMetadata"];
                assert_eq!(meta["note"], "hello", "{payload} bytes: {record}");
                assert_eq!(
                    meta["x-amz-tagging"], "tagk=tagv",
                    "{payload} bytes: {record}"
                );
            }
        }
        Store::Rgw => {
            assert_eq!(object["versionId"], "", "{payload} bytes: {record}");
            assert_eq!(
                record["opaqueData"], RGW_OPAQUE_DATA,
                "{payload} bytes: {record}"
            );
            let (metadata, tags) = if tagged {
                (
                    serde_json::json!([{"key": "x-amz-meta-note", "val": "hello"}]),
                    serde_json::json!([{"key": "tagk", "val": "tagv"}]),
                )
            } else {
                (serde_json::json!([]), serde_json::json!([]))
            };
            assert_eq!(object["metadata"], metadata, "{payload} bytes: {record}");
            assert_eq!(object["tags"], tags, "{payload} bytes: {record}");
        }
        Store::Garage => unreachable!("Garage sends no bucket notifications"),
    }
}

/// `key` as RustFS writes it in an event: every byte other than an ASCII
/// letter, a digit, `-`, `.`, `_` and `~` as `%HH`, and a space as `+`.
fn rustfs_encoded(key: &str) -> String {
    let mut out = String::new();
    for b in key.bytes() {
        match b {
            b' ' => out.push('+'),
            b if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') => {
                out.push(char::from(b));
            }
            b => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// The one record of a notification message, inside RustFS's envelope or
/// not.
fn record(message: &support::feed::Read) -> Value {
    let body: Value =
        serde_json::from_slice(message.payload.as_deref().expect("a payload")).expect("JSON");
    body["Records"][0].clone()
}

/// Whether `time` is an RFC 3339 UTC timestamp, `YYYY-MM-DDTHH:MM:SS`, an
/// optional fraction, then `Z`, as the source parses an event time.
fn rfc3339_utc(time: &str) -> bool {
    let b = time.as_bytes();
    let digits =
        |r: std::ops::Range<usize>| b.get(r).is_some_and(|d| d.iter().all(u8::is_ascii_digit));
    b.len() >= 20
        && digits(0..4)
        && b[4] == b'-'
        && digits(5..7)
        && b[7] == b'-'
        && digits(8..10)
        && b[10] == b'T'
        && digits(11..13)
        && b[13] == b':'
        && digits(14..16)
        && b[16] == b':'
        && digits(17..19)
        && b.last() == Some(&b'Z')
        && (b.len() == 20 || (b[19] == b'.' && digits(20..b.len() - 1)))
}

/// `etag` without its surrounding quotes.
fn unquoted(etag: &str) -> &str {
    etag.trim_matches('"')
}

/// A broker, a notifying store in its network namespace with [`BUCKET`]
/// configured, and a client for the bucket. The store is declared first, so
/// it is dropped before the broker whose namespace it runs in.
struct Notifying {
    s3: S3Store,
    client: object_store::aws::AmazonS3,
    io: tokio::runtime::Runtime,
    brokers: String,
    _kafka: Container<Kafka>,
}

/// A worker reading the bucket through the Kafka feed, in a group of its own.
struct Reading {
    worker: Worker,
    group: String,
}

impl std::ops::Deref for Reading {
    type Target = Worker;

    fn deref(&self) -> &Worker {
        &self.worker
    }
}

impl Notifying {
    /// Writes `rows`, one per line, at `path` with `options`.
    fn put(&self, path: &Path, rows: &[String], options: PutOptions) {
        let mut body = Vec::new();
        for row in rows {
            body.extend_from_slice(row.as_bytes());
            body.push(b'\n');
        }
        self.io
            .block_on(self.client.put_opts(path, body.into(), options))
            .unwrap_or_else(|e| panic!("put {path}: {e}"));
    }

    /// Starts a worker over the whole bucket in a group named for `name`.
    fn worker(&self, name: &str) -> Reading {
        let group = spate_test::unique_name(name);
        let source = format!(
            "  s3:\n    url: \"s3://{BUCKET}/\"\n    store:\n      endpoint: \"{}\"\n      \
             allow_http: \"true\"\n      access_key_id: \"{ACCESS_KEY}\"\n      \
             secret_access_key: \"{SECRET_KEY}\"\n      region: \"{REGION}\"\n",
            self.s3.endpoint()
        );
        let worker = launch_source(
            KafkaFeed::connect,
            &source,
            &feed_yaml(&self.brokers, &group, "", ""),
            |_| {},
        );
        Reading { worker, group }
    }
}

/// Starts the broker and the selected store in its network namespace, with
/// [`BUCKET`] sending its notifications to the work topic. The broker maps
/// the store's port: the store reaches the broker on the listener it
/// advertises to other containers.
fn notifying() -> Notifying {
    let store = Store::selected().expect("the selected S3 lane");
    let kafka = broker()
        .with_mapped_port(0, KAFKA_PORT)
        .with_mapped_port(0, store.container_port().tcp())
        .start()
        .expect("start kafka container");
    let brokers = format!(
        "127.0.0.1:{}",
        kafka.get_host_port_ipv4(KAFKA_PORT).expect("broker port")
    );
    let s3_port = kafka
        .get_host_port_ipv4(store.container_port())
        .expect("S3 port");
    create_topics(&brokers);
    let s3 = S3Store::start_notifying(kafka.id(), s3_port, WORK).expect("start the S3 store");
    s3.create_bucket(BUCKET).expect("create bucket");
    // A store's refusal reaches the caller: a bucket that does not exist has
    // no notification configuration to set.
    let refused = s3.put_bucket_notification("absent");
    assert!(refused.is_err(), "{refused:?}");
    s3.put_bucket_notification(BUCKET)
        .expect("configure notifications");
    let client = AmazonS3Builder::new()
        .with_endpoint(s3.endpoint())
        .with_allow_http(true)
        .with_bucket_name(BUCKET)
        .with_region(REGION)
        .with_access_key_id(ACCESS_KEY)
        .with_secret_access_key(SECRET_KEY)
        .build()
        .expect("S3 client");
    let io = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    Notifying {
        s3,
        client,
        io,
        brokers,
        _kafka: kafka,
    }
}
