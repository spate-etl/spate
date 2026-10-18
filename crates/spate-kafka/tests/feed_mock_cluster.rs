//! The Kafka feed against rdkafka's in-process MockCluster: the S3 source
//! reads units through a `feed: { kafka: … }` section, and the group's
//! committed offsets and the retry, dead-letter and attempt topics show
//! when a unit is counted, finished, retried or dead-lettered.
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
use std::time::{Duration, Instant};
use support::feed::{
    ATTEMPT, ATTEMPT_PARTITIONS, COMMIT_PERIOD, DEAD, DEADLINE, ON_MOCK, RETRY, WORK, committed,
    data_dir, direct, exists, feed_yaml, high_watermark, launch, produce_raw, publish,
    read_attempts, read_topic, recs, sorted, stage, written,
};
use support::{corrupt_batch, produce_batch};

/// A cluster of `brokers` with the work, retry and dead-letter topics, one
/// partition each, and the attempt topic with two.
fn cluster(brokers: i32) -> MockCluster<'static, rdkafka::producer::DefaultProducerContext> {
    let cluster = MockCluster::new(brokers).expect("mock cluster");
    for topic in [WORK, RETRY, DEAD] {
        cluster.create_topic(topic, 1, 1).expect("create topic");
    }
    cluster
        .create_topic(ATTEMPT, ATTEMPT_PARTITIONS, 1)
        .expect("create topic");
    cluster
}

/// Place every partition of the attempt topic on `broker`.
fn attempts_on(
    cluster: &MockCluster<'static, rdkafka::producer::DefaultProducerContext>,
    broker: i32,
) {
    for partition in 0..ATTEMPT_PARTITIONS {
        cluster
            .partition_leader(ATTEMPT, partition, Some(broker))
            .unwrap();
    }
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
    attempts_on(&cluster, 1);
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

/// Pins the cap on the recorded count: a unit failed on each delivery is
/// read with counts 1, 2 and 3, written to the retry topic with the count
/// header at 1 and 2 and its publish identity in `spate.origin`, and on its
/// third failure, at `attempt_cap: 3`, written to the dead-letter topic.
/// Its attempt record holds 1, 2 and 3, then a tombstone, all on the
/// partition its identity hashes to. Every offset is committed.
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
    assert_eq!(dead[0].header("spate.origin"), Some("units/0/0"));

    let retried = read_topic(&brokers, RETRY, 2);
    let counts: Vec<_> = retried
        .iter()
        .map(|m| (m.header("spate.deliveries"), m.header("spate.origin")))
        .collect();
    assert_eq!(
        counts,
        [
            (Some("1"), Some("units/0/0")),
            (Some("2"), Some("units/0/0"))
        ]
    );
    assert_eq!(worker.recording.seen.lock().unwrap().as_slice(), [1, 2, 3]);

    wait_until(DEADLINE, "every offset is committed", || {
        committed(&brokers, &group, WORK) == Some(1)
            && committed(&brokers, &group, RETRY) == Some(2)
    });
    // `units/0/0` hashes to partition 1 of 2.
    let id = "units/0/0".to_owned();
    assert_eq!(
        read_attempts(&brokers, ATTEMPT_PARTITIONS, 4),
        [
            (1, id.clone(), Some(1)),
            (1, id.clone(), Some(2)),
            (1, id.clone(), Some(3)),
            (1, id, None),
        ]
    );
    assert_eq!(high_watermark(&brokers, DEAD), 1, "dead-lettered once");
    assert_eq!(worker.stop(), ExitState::Completed);
}

/// Pins that a worker counts from the attempt record a worker before it
/// wrote: worker A takes the unit and stops gracefully without settling
/// it, and worker B, in the same group, reads it with 2.
#[test]
fn a_restarted_worker_counts_from_the_latest_attempt_record() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("restarted");
    let yaml = feed_yaml(&brokers, &group, "", "");
    let a = direct(&yaml);
    publish(&a.feed, Unit::new("u", b"body".to_vec()));
    assert_eq!(a.take("A reads the unit").deliveries, 1);
    drop(a);

    let b = direct(&yaml);
    assert_eq!(b.take("B reads the unit").deliveries, 2);
}

/// Pins that an acknowledged unit's attempt record is tombstoned: the
/// unit read again, by a group that never read it, counts from zero.
#[test]
fn a_completed_unit_read_again_counts_from_zero() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("completed");
    let a = direct(&feed_yaml(&brokers, &group, "", ""));
    publish(&a.feed, Unit::new("u", b"body".to_vec()));
    let delivery = a.take("A reads the unit");
    assert_eq!(delivery.deliveries, 1);
    a.feed.acknowledge(delivery.receipt).unwrap();
    wait_until(DEADLINE, "the unit's offset is committed", || {
        committed(&brokers, &group, WORK) == Some(1)
    });

    let again = direct(&feed_yaml(
        &brokers,
        &spate_test::unique_name("again"),
        "",
        "",
    ));
    assert_eq!(again.take("the unit is read again").deliveries, 1);
}

/// Pins that a unit whose recorded attempts have reached the cap goes to
/// the dead-letter topic without being read. The record is seeded by a
/// librdkafka producer set to `partitioner=murmur2`, so librdkafka places
/// it. While the dead-letter topic's leader is down, the reader gets
/// nothing and the unit's offset stays uncommitted over several commit
/// periods; once the leader is back, the offset is committed and the
/// record is tombstoned on the partition the seed landed on. The window
/// opens once a unit behind it on the partition is read, which the gate
/// admits after the capped unit, so it covers commits after the move is
/// sent.
#[test]
fn a_unit_at_the_cap_goes_to_the_dead_letter_topic_unread() {
    let cluster = cluster(2);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("unread");
    for topic in [WORK, RETRY] {
        cluster.partition_leader(topic, 0, Some(1)).unwrap();
    }
    attempts_on(&cluster, 1);
    cluster.partition_leader(DEAD, 0, Some(2)).unwrap();
    cluster
        .coordinator(MockCoordinator::Group(group.clone()), 1)
        .unwrap();
    produce_raw(
        &brokers,
        WORK,
        &[
            (Some(&b"capped"[..]), &b"body"[..]),
            (Some(&b"behind"[..]), &b"body"[..]),
        ],
    );
    let seeder: rdkafka::producer::BaseProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("partitioner", "murmur2")
        .create()
        .unwrap();
    seeder
        .send(
            rdkafka::producer::BaseRecord::to(ATTEMPT)
                .key("units/0/0")
                .payload(r#"{"v":1,"attempts":3}"#),
        )
        .map_err(|(e, _)| e)
        .unwrap();
    rdkafka::producer::Producer::flush(&seeder, Duration::from_secs(10)).unwrap();
    let seeded = read_attempts(&brokers, ATTEMPT_PARTITIONS, 1);
    cluster.broker_down(2).unwrap();

    let worker = direct(&feed_yaml(&brokers, &group, "  attempt_cap: 3\n", ""));
    let behind = worker.take("the unit behind the capped one");
    assert_eq!(behind.unit.id, "behind");
    assert_eq!(behind.deliveries, 1);
    // Elapsed time is the property: the move is held over several commits.
    for _ in 0..8 {
        std::thread::sleep(COMMIT_PERIOD / 2);
        assert!(
            worker.feed.receive(8).map_or_else(
                |e| {
                    assert_eq!(e.kind, FeedErrorKind::Retryable, "{e}");
                    true
                },
                |got| got.is_empty()
            ),
            "the unit was read"
        );
        let at = committed(&brokers, &group, WORK);
        assert!(
            at.is_none_or(|at| at <= 0),
            "the group committed {at:?} before the dead-letter write was confirmed"
        );
    }
    cluster.broker_up(2).unwrap();
    wait_until(DEADLINE, "the unit's offset is committed", || {
        committed(&brokers, &group, WORK) == Some(1)
    });
    let dead = read_topic(&brokers, DEAD, 1);
    assert_eq!(dead[0].key.as_deref(), Some(&b"capped"[..]));
    assert_eq!(dead[0].header("spate.origin"), Some("units/0/0"));
    let partition = seeded[0].0;
    let capped: Vec<_> = read_attempts(&brokers, ATTEMPT_PARTITIONS, 3)
        .into_iter()
        .filter(|(_, id, _)| id == "units/0/0")
        .collect();
    assert_eq!(
        capped,
        [
            (partition, "units/0/0".to_owned(), Some(3)),
            (partition, "units/0/0".to_owned(), None),
        ]
    );
    assert!(
        worker.feed.receive(8).map_or_else(
            |e| {
                assert_eq!(e.kind, FeedErrorKind::Retryable, "{e}");
                true
            },
            |got| got.is_empty()
        ),
        "the unit was read"
    );
}

/// Pins that each publish starts its own count, whatever the unit's id:
/// the same unit published twice is delivered twice with 1. The second
/// publish follows the first copy's read, so the first copy's attempt
/// record is on the attempt topic, unsettled, when the second is counted.
#[test]
fn a_unit_published_twice_counts_each_publish_from_one() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let worker = direct(&feed_yaml(
        &brokers,
        &spate_test::unique_name("twice"),
        "",
        "",
    ));
    publish(&worker.feed, Unit::new("same", b"first".to_vec()));
    let first = worker.take("the first copy");
    publish(&worker.feed, Unit::new("same", b"second".to_vec()));
    let second = worker.take("the second copy");
    assert_eq!(first.unit.body, b"first");
    assert_eq!(
        (first.deliveries, second.deliveries),
        (1, 1),
        "the second publish counted the first's read"
    );
}

/// Pins that a unit published again after its earlier publish was
/// dead-lettered is read: at `attempt_cap: 1` the first publish fails to
/// the dead-letter topic, and the second is read with 1.
#[test]
fn a_dead_lettered_unit_published_again_is_read() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("again-after-dead");
    let worker = direct(&feed_yaml(&brokers, &group, "  attempt_cap: 1\n", ""));
    publish(&worker.feed, Unit::new("u", b"first".to_vec()));
    let first = worker.take("the first publish");
    worker.feed.fail(first.receipt, "test").unwrap();
    wait_until(DEADLINE, "the first publish is dead-lettered", || {
        committed(&brokers, &group, WORK) == Some(1)
    });
    publish(&worker.feed, Unit::new("u", b"second".to_vec()));
    let second = worker.take("the second publish");
    assert_eq!(second.unit.body, b"second");
    assert_eq!(second.deliveries, 1);
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
/// with "load in progress" does not hold the feed's consumer thread out of
/// `poll` past `max.poll.interval.ms`: over 20 s of a rebalance, neither
/// member reports a poll-interval error and neither stops.
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

/// Pins that a batch whose codec id this build does not know stops the feed,
/// with the reason naming the codec and the cause.
#[test]
fn an_unknown_codec_stops_the_feed_and_names_it() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let first = brokers.split(',').next().unwrap().to_owned();
    let mut batch = corrupt_batch(b"k", b"body");
    // The attributes field: codec id 5, which no librdkafka build knows.
    batch[21..23].copy_from_slice(&5i16.to_be_bytes());
    assert_eq!(produce_batch(&first, WORK, &batch), 0);
    let group = spate_test::unique_name("codec");
    let worker = direct(&feed_yaml(&brokers, &group, "", ""));
    let mut error = None;
    wait_until(DEADLINE, "the feed stops on the unknown codec", || {
        error = worker.feed.receive(1).err();
        error
            .as_ref()
            .is_some_and(|e| e.kind == FeedErrorKind::Fatal)
    });
    let error = error.unwrap();
    assert!(error.reason.contains("codec 0x5"), "{error}");
    assert!(error.reason.contains("cannot read"), "{error}");
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
