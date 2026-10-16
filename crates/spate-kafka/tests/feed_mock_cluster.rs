//! The Kafka feed against rdkafka's in-process MockCluster: the S3 source
//! reads units through a `feed: { kafka: … }` section, and the group's
//! committed offsets and the retry and dead-letter topics show when a unit
//! is finished, retried or dead-lettered.
//!
//! The mock answers a group's rebalance after `session.timeout.ms - 1000`,
//! so the members run at a 6 s session timeout.

mod support;

use rdkafka::mocking::{MockCluster, MockCoordinator};
use rdkafka::types::RDKafkaRespErr;
use spate_core::feed::{Feed, FeedErrorKind, Receipt, Unit};
use spate_core::pipeline::ExitState;
use spate_kafka::feed::KafkaFeedError;
use spate_test::{WriteOutcome, wait_until};
use std::io::{Read as _, Write as _};
use std::net::TcpStream;
use std::time::{Duration, Instant};
use support::feed::{
    COMMIT_PERIOD, DEAD, DEADLINE, ON_MOCK, RETRY, WORK, committed, data_dir, direct, exists,
    feed_yaml, high_watermark, launch, produce_raw, publish, read_topic, recs, sorted, stage,
    written,
};

/// A cluster of `brokers` with the work, retry and dead-letter topics, one
/// partition each.
fn cluster(brokers: i32) -> MockCluster<'static, rdkafka::producer::DefaultProducerContext> {
    let cluster = MockCluster::new(brokers).expect("mock cluster");
    for topic in [WORK, RETRY, DEAD] {
        cluster.create_topic(topic, 1, 1).expect("create topic");
    }
    cluster
}

/// Pins INV-1 on Kafka: while the unit's last write is held over several
/// commit periods, the group never commits past the unit's offset; once the
/// write succeeds the offset is committed with every row written.
#[test]
fn a_unit_is_deleted_only_after_its_last_record_is_acknowledged() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("deleted");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("d", 3);
    let unit = stage(&data, "d.ndjson", &rows);

    let worker = launch(&data, &feed_yaml(&brokers, &group, "", ""), |sink| {
        sink.enqueue_global(WriteOutcome::ok());
        sink.enqueue_global(WriteOutcome::ok());
        sink.enqueue_global(WriteOutcome::ok().held());
    });
    publish(&worker.feed, unit);
    wait_until(DEADLINE, "the last write is held", || {
        worker.script.held_writes() == 1
    });

    // Elapsed time is the property: the hold outlasts several commits.
    for _ in 0..8 {
        std::thread::sleep(COMMIT_PERIOD / 2);
        let at = committed(&brokers, &group, WORK);
        assert!(
            at.is_none_or(|at| at <= 0),
            "the group committed {at:?} while the unit's last write was held"
        );
    }
    worker.script.release_held();
    wait_until(DEADLINE, "the unit's offset is committed", || {
        committed(&brokers, &group, WORK) == Some(1)
    });
    assert_eq!(written(&worker.script), sorted(rows));
    assert_eq!(worker.recording.seen.lock().unwrap().as_slice(), [1]);
    assert_eq!(worker.stop(), ExitState::Completed);
}

/// Pins that a failed unit's offset completes only once the write to the
/// retry topic is confirmed: with the retry topic's leader down, the work
/// partition's committed offset holds at the unit over several commit
/// periods, and passes it once the leader is back.
#[test]
fn the_offset_completes_only_after_the_retry_write_is_confirmed() {
    let cluster = cluster(2);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("confirmed");
    cluster.partition_leader(WORK, 0, Some(1)).unwrap();
    cluster.partition_leader(DEAD, 0, Some(1)).unwrap();
    cluster.partition_leader(RETRY, 0, Some(2)).unwrap();
    cluster
        .coordinator(MockCoordinator::Group(group.clone()), 1)
        .unwrap();
    cluster.broker_down(2).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);

    let worker = launch(
        &data,
        &feed_yaml(&brokers, &group, "  attempt_cap: 2\n", ""),
        |_| {},
    );
    // A body that is no split descriptor: the reader fails the unit at once.
    publish(
        &worker.feed,
        Unit::new("unreadable", b"not a descriptor".to_vec()),
    );
    wait_until(DEADLINE, "the reader takes the unit", || {
        !worker.recording.seen.lock().unwrap().is_empty()
    });

    // Elapsed time is the property: the report is held over several
    // commits.
    for _ in 0..8 {
        std::thread::sleep(COMMIT_PERIOD / 2);
        let at = committed(&brokers, &group, WORK);
        assert!(
            at.is_none_or(|at| at <= 0),
            "the group committed {at:?} before the retry write was confirmed"
        );
    }
    cluster.broker_up(2).unwrap();
    wait_until(DEADLINE, "the unit's offset is committed", || {
        committed(&brokers, &group, WORK) == Some(1)
    });
    assert_eq!(worker.stop(), ExitState::Completed);
}

/// Pins the cap on the reported count: a unit failed on each delivery is
/// read with counts 1, 2 and 3, written to the retry topic with the header
/// at 1 and 2, and on its third failure, at `attempt_cap: 3`, written to
/// the dead-letter topic with the header at 3. Every offset is committed.
#[test]
fn a_unit_that_reports_failure_reaches_the_dead_letter_topic_at_the_cap() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("capped");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);

    let worker = launch(
        &data,
        &feed_yaml(&brokers, &group, "  attempt_cap: 3\n", ""),
        |_| {},
    );
    publish(
        &worker.feed,
        Unit::new("poison", b"not a descriptor".to_vec()),
    );

    let dead = read_topic(&brokers, DEAD, 1);
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].key.as_deref(), Some(&b"poison"[..]));
    assert_eq!(dead[0].payload.as_deref(), Some(&b"not a descriptor"[..]));
    assert_eq!(dead[0].header("spate.deliveries"), Some("3"));

    let retried = read_topic(&brokers, RETRY, 2);
    let counts: Vec<_> = retried
        .iter()
        .map(|m| m.header("spate.deliveries"))
        .collect();
    assert_eq!(counts, [Some("1"), Some("2")]);
    assert_eq!(worker.recording.seen.lock().unwrap().as_slice(), [1, 2, 3]);

    wait_until(DEADLINE, "every offset is committed", || {
        committed(&brokers, &group, WORK) == Some(1)
            && committed(&brokers, &group, RETRY) == Some(2)
    });
    assert_eq!(high_watermark(&brokers, DEAD), 1, "dead-lettered once");
    assert_eq!(worker.stop(), ExitState::Completed);
}

/// Pins that a message with no key, and one whose key is not UTF-8, go to
/// the dead-letter topic as read, with no retry, and that their offsets are
/// committed once those writes are confirmed. No reader takes them.
#[test]
fn a_message_with_no_unit_goes_to_the_dead_letter_topic_at_once() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("no-unit");
    produce_raw(
        &brokers,
        WORK,
        &[(None, b"keyless"), (Some(&[0xff, 0xfe]), b"not utf-8")],
    );
    let worker = direct(&feed_yaml(&brokers, &group, "", ""));

    let dead = read_topic(&brokers, DEAD, 2);
    assert_eq!(dead[0].key, None);
    assert_eq!(dead[0].payload.as_deref(), Some(&b"keyless"[..]));
    assert_eq!(dead[1].key.as_deref(), Some(&[0xff, 0xfe][..]));
    assert_eq!(dead[1].payload.as_deref(), Some(&b"not utf-8"[..]));
    wait_until(DEADLINE, "both offsets are committed", || {
        committed(&brokers, &group, WORK) == Some(2)
    });
    assert_eq!(high_watermark(&brokers, RETRY), 0, "nothing was retried");
    assert!(worker.feed.receive(8).unwrap().is_empty());
}

/// Pins that a retry write still queued when librdkafka marks its topic
/// missing stops the feed: librdkafka fails it with the metadata
/// response's `UnknownTopicOrPartition`, and the next `receive` and every
/// later call return Fatal, naming the write. The retry topic goes missing
/// after the feed connects, as a topic deleted while the feed runs does. A
/// propagation window shorter than the delivery timeout keeps the write
/// queued until then.
#[test]
fn a_failed_write_to_a_missing_retry_topic_makes_the_feed_fatal() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("missing-retry");
    let worker = direct(&feed_yaml(
        &brokers,
        &group,
        "",
        // librdkafka waits this long before it calls a topic missing.
        "      topic.metadata.propagation.max.ms: \"1000\"\n",
    ));
    cluster
        .topic_error(
            RETRY,
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART,
        )
        .unwrap();
    publish(&worker.feed, Unit::new("u", b"body".to_vec()));
    let delivery = worker.take("the unit");
    worker.feed.fail(delivery.receipt, "test").unwrap();

    let mut error = None;
    wait_until(DEADLINE, "the feed stops", || {
        error = worker.feed.receive(1).err();
        error
            .as_ref()
            .is_some_and(|e| e.kind == FeedErrorKind::Fatal)
    });
    let error = error.unwrap();
    assert!(
        error
            .reason
            .contains(&format!("writing a unit to `{RETRY}`")),
        "{error}"
    );
    assert_eq!(
        worker.feed.acknowledge(Receipt(1)).unwrap_err().kind,
        FeedErrorKind::Fatal
    );
    let mut refused = worker.feed.publish(Unit::new("v", Vec::new()));
    assert_eq!(
        refused.try_outcome().unwrap().unwrap_err().kind,
        FeedErrorKind::Fatal
    );
}

/// Pins that a dead-letter topic one metadata response calls missing does
/// not stop the feed: librdkafka marks the topic missing for a window, a
/// write in that window is sent again, and it lands once the window ends.
#[test]
fn a_topic_reported_missing_once_does_not_stop_the_feed() {
    use rdkafka::types::RDKafkaRespErr;
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("transient-missing");
    let worker = direct(&feed_yaml(
        &brokers,
        &group,
        "",
        "      topic.metadata.propagation.max.ms: \"1000\"\n      \
         topic.metadata.refresh.interval.ms: \"100\"\n",
    ));
    produce_raw(&brokers, WORK, &[(None, &b"first"[..])]);
    wait_until(DEADLINE, "the first message is dead-lettered", || {
        high_watermark(&brokers, DEAD) == 1
    });
    // Elapsed time is the property: librdkafka marks a topic missing on one
    // response only once the topic is older than the propagation window,
    // and a few metadata refreshes must see the error.
    std::thread::sleep(Duration::from_millis(1500));
    cluster
        .topic_error(
            DEAD,
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART,
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(500));
    cluster
        .topic_error(DEAD, RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR)
        .unwrap();
    produce_raw(&brokers, WORK, &[(None, &b"second"[..])]);
    let mut fatal = None;
    wait_until(
        DEADLINE,
        "the second message is dead-lettered or the feed stops",
        || {
            if let Err(e) = worker.feed.receive(1)
                && e.kind == FeedErrorKind::Fatal
            {
                fatal = Some(e);
            }
            fatal.is_some() || high_watermark(&brokers, DEAD) == 2
        },
    );
    assert!(
        fatal.is_none(),
        "the feed stopped though the topic exists: {fatal:?}"
    );
}

/// Pins that a committed-offset lookup the coordinator keeps answering
/// with "load in progress" does not hold the feed's thread out of `poll`
/// past `max.poll.interval.ms`: over 20 s of a rebalance, neither member
/// reports a poll-interval error and neither stops.
#[test]
fn a_stalled_offset_fetch_does_not_exceed_the_max_poll_interval() {
    use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("max-poll");
    let body = "      max.poll.interval.ms: \"6000\"\n";
    let a = direct(&feed_yaml(&brokers, &group, "", body));
    publish(&a.feed, Unit::new("u1", b"x".to_vec()));
    let _ = a.take("the first delivery");
    cluster.request_errors(
        RDKafkaApiKey::OffsetFetch,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS; 200],
    );
    // A second member forces a rebalance, so A looks up its offsets again.
    let b = direct(&feed_yaml(&brokers, &group, "", body));
    let start = Instant::now();
    let mut errors = Vec::new();
    // Elapsed time is the property: the members must outlast several
    // poll intervals.
    wait_until(DEADLINE, "20 s pass", || {
        for (who, feed) in [("a", &a.feed), ("b", &b.feed)] {
            if let Err(e) = feed.receive(1) {
                errors.push(format!("{who} +{:?}: {e}", start.elapsed()));
                assert_ne!(e.kind, FeedErrorKind::Fatal, "{errors:#?}");
            }
        }
        start.elapsed() >= Duration::from_secs(20)
    });
    assert!(
        !errors
            .iter()
            .any(|e| e.to_lowercase().contains("poll interval")),
        "a member exceeded max.poll.interval.ms: {errors:#?}"
    );
}

fn zigzag(buf: &mut Vec<u8>, v: i64) {
    let mut z = ((v << 1) ^ (v >> 63)) as u64;
    loop {
        let b = (z & 0x7f) as u8;
        z >>= 7;
        if z == 0 {
            buf.push(b);
            break;
        }
        buf.push(b | 0x80);
    }
}

/// One magic-2 record batch holding one keyed record, with a CRC that does
/// not match its contents.
fn corrupt_batch(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut rec = vec![0u8];
    zigzag(&mut rec, 0);
    zigzag(&mut rec, 0);
    zigzag(&mut rec, key.len() as i64);
    rec.extend_from_slice(key);
    zigzag(&mut rec, value.len() as i64);
    rec.extend_from_slice(value);
    zigzag(&mut rec, 0);
    let mut records = Vec::new();
    zigzag(&mut records, rec.len() as i64);
    records.extend_from_slice(&rec);
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let mut tail = Vec::new();
    tail.extend_from_slice(&0i16.to_be_bytes());
    tail.extend_from_slice(&0i32.to_be_bytes());
    tail.extend_from_slice(&now.to_be_bytes());
    tail.extend_from_slice(&now.to_be_bytes());
    tail.extend_from_slice(&(-1i64).to_be_bytes());
    tail.extend_from_slice(&(-1i16).to_be_bytes());
    tail.extend_from_slice(&(-1i32).to_be_bytes());
    tail.extend_from_slice(&1i32.to_be_bytes());
    tail.extend_from_slice(&records);
    let mut batch = Vec::new();
    batch.extend_from_slice(&0i64.to_be_bytes());
    batch.extend_from_slice(&((4 + 1 + 4 + tail.len()) as i32).to_be_bytes());
    batch.extend_from_slice(&(-1i32).to_be_bytes());
    batch.push(2);
    batch.extend_from_slice(&0xdead_beefu32.to_be_bytes());
    batch.extend_from_slice(&tail);
    batch
}

/// Send `batch` to partition 0 of `topic` as a version 3 Produce request
/// and return the partition's error code. A client library would compute
/// the batch's CRC itself; the mock stores the batch as sent.
fn produce_batch(broker: &str, topic: &str, batch: &[u8]) -> i16 {
    let mut b = Vec::new();
    b.extend_from_slice(&0i16.to_be_bytes());
    b.extend_from_slice(&3i16.to_be_bytes());
    b.extend_from_slice(&7i32.to_be_bytes());
    b.extend_from_slice(&5i16.to_be_bytes());
    b.extend_from_slice(b"spate");
    b.extend_from_slice(&(-1i16).to_be_bytes());
    b.extend_from_slice(&1i16.to_be_bytes());
    b.extend_from_slice(&10_000i32.to_be_bytes());
    b.extend_from_slice(&1i32.to_be_bytes());
    b.extend_from_slice(&(topic.len() as i16).to_be_bytes());
    b.extend_from_slice(topic.as_bytes());
    b.extend_from_slice(&1i32.to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.extend_from_slice(&(batch.len() as i32).to_be_bytes());
    b.extend_from_slice(batch);
    let mut s = TcpStream::connect(broker).unwrap();
    s.write_all(&(b.len() as i32).to_be_bytes()).unwrap();
    s.write_all(&b).unwrap();
    let mut len = [0u8; 4];
    s.read_exact(&mut len).unwrap();
    let mut resp = vec![0u8; usize::try_from(i32::from_be_bytes(len)).unwrap()];
    s.read_exact(&mut resp).unwrap();
    let at = 4 + 4 + 2 + topic.len() + 4 + 4;
    i16::from_be_bytes([resp[at], resp[at + 1]])
}

/// Pins that a record batch librdkafka reports as corrupt stops the feed,
/// with the reason naming the bad message.
#[test]
fn a_corrupt_batch_stops_the_feed() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let first = brokers.split(',').next().unwrap().to_owned();
    assert_eq!(
        produce_batch(&first, WORK, &corrupt_batch(b"lost", b"body")),
        0
    );
    produce_raw(&brokers, WORK, &[(Some(&b"after"[..]), &b"body"[..])]);
    let group = spate_test::unique_name("corrupt");
    let worker = direct(&feed_yaml(
        &brokers,
        &group,
        "",
        "      check.crcs: \"true\"\n",
    ));
    let mut error = None;
    wait_until(DEADLINE, "the feed stops on the corrupt batch", || {
        error = worker.feed.receive(1).err();
        error
            .as_ref()
            .is_some_and(|e| e.kind == FeedErrorKind::Fatal)
    });
    let error = error.unwrap();
    assert!(error.reason.contains("Bad message"), "{error}");
}

/// Pins that connecting refuses a missing retry topic and leaves it
/// missing. A metadata read of a missing topic by a client that may create
/// topics creates it with the broker's defaults, so a check that read it
/// that way would pass and keep the topic.
#[test]
fn the_startup_check_does_not_create_a_missing_topic() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(WORK, 1, 1).unwrap();
    cluster.create_topic(DEAD, 1, 1).unwrap();
    let brokers = cluster.bootstrap_servers();
    let feed = support::feed::section(&feed_yaml(
        &brokers,
        &spate_test::unique_name("uncreated"),
        "",
        "",
    ));
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    match ON_MOCK(&feed, io.handle()) {
        Err(KafkaFeedError::Refused(message)) => assert!(
            message.contains(&format!("the retry topic `{RETRY}` does not exist")),
            "{message}"
        ),
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert!(
        !exists(&brokers, RETRY),
        "connecting created the retry topic"
    );
    assert!(exists(&brokers, WORK), "the probe sees an existing topic");
}

/// One startup attempt on `yaml`, the result's error.
fn connect_error(yaml: &str) -> KafkaFeedError {
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    match ON_MOCK(&support::feed::section(yaml), io.handle()) {
        Err(e) => e,
        Ok(feed) => panic!("expected an error, got {feed:?}"),
    }
}

/// Pins that a startup read that fails in the client keeps its own error
/// and names the read, and is not taken for a missing topic. A topic name
/// with a NUL byte cannot reach librdkafka.
#[test]
fn a_failed_startup_read_is_not_a_missing_topic() {
    let cluster = cluster(1);
    let yaml = feed_yaml(
        &cluster.bootstrap_servers(),
        &spate_test::unique_name("nul"),
        "",
        "",
    )
    .replace("topic: units\n", "topic: \"units\\0\"\n");
    let message = match connect_error(&yaml) {
        KafkaFeedError::Refused(m) | KafkaFeedError::Unavailable(m) => m,
        other => panic!("expected a failed read, got {other:?}"),
    };
    assert!(
        message.contains("reading the metadata of the work topic") && message.contains("nul"),
        "{message}"
    );
    assert!(!message.contains("does not exist"), "{message}");
}

/// Pins that an `rdkafka:` value librdkafka does not accept refuses
/// connecting, before any read of the topics.
#[test]
fn a_property_librdkafka_rejects_is_refused() {
    let cluster = cluster(1);
    let yaml = feed_yaml(
        &cluster.bootstrap_servers(),
        &spate_test::unique_name("rejected"),
        "",
        "      socket.timeout.ms: \"soon\"\n",
    );
    match connect_error(&yaml) {
        KafkaFeedError::Refused(message) => assert!(
            message.contains("creating the admin client") && message.contains("socket.timeout.ms"),
            "{message}"
        ),
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// Pins that a topic the metadata response reports unauthorized refuses
/// connecting, naming the read, and is not taken for a missing topic.
#[test]
fn an_unauthorized_topic_is_refused() {
    let cluster = cluster(1);
    cluster
        .topic_error(
            RETRY,
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,
        )
        .unwrap();
    let yaml = feed_yaml(
        &cluster.bootstrap_servers(),
        &spate_test::unique_name("unauthorized"),
        "",
        "",
    );
    match connect_error(&yaml) {
        KafkaFeedError::Refused(message) => assert!(
            message.contains(&format!(
                "reading the metadata of the retry topic `{RETRY}`"
            )),
            "{message}"
        ),
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// Pins that a work topic name longer than librdkafka's 512-byte limit is
/// an invalid topic name, and connecting refuses it, naming the read.
#[test]
fn an_overlong_topic_name_is_refused() {
    let cluster = cluster(1);
    let long = "u".repeat(513);
    let yaml = feed_yaml(
        &cluster.bootstrap_servers(),
        &spate_test::unique_name("overlong"),
        "",
        "",
    )
    .replace("topic: units\n", &format!("topic: {long}\n"));
    assert!(yaml.contains(&long));
    match connect_error(&yaml) {
        KafkaFeedError::Refused(message) => assert!(
            message.contains("reading the metadata of the work topic")
                && message.contains("longer than 512 bytes"),
            "{message}"
        ),
        other => panic!("expected a refusal, got {other:?}"),
    }
}
