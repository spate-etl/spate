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
use spate_core::clock::TestClock as StopClock;
use spate_core::clock::tokio::TestClock;
use spate_core::feed::{Depth, DepthOf, Feed, FeedError, FeedErrorKind, Receipt, Unit};
use spate_core::pipeline::ExitState;
use spate_kafka::feed::KafkaFeedError;
use spate_test::{WriteOutcome, wait_until};
use std::time::{Duration, Instant};
use support::feed::{
    ATTEMPT, ATTEMPT_PARTITIONS, COMMIT_PERIOD, DEAD, DEADLINE, ON_MOCK, RETRY, WORK,
    bounded_section, committed, data_dir, direct, exists, feed_yaml, high_watermark,
    high_watermarks, launch, launch_bounded, launch_customized, launch_stopping, list_once,
    produce_raw, publish, publish_to, publisher_on_mock, read_attempts, read_topic, recs,
    run_to_exit, sorted, stage, written,
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

/// Pins that a message whose key is not UTF-8 goes to the dead-letter topic
/// as read, with no retry, and that its offset is committed once that write
/// is confirmed. No reader takes it.
#[test]
fn a_message_with_no_unit_goes_to_the_dead_letter_topic_at_once() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("no-unit");
    produce_raw(&brokers, WORK, &[(Some(&[0xff, 0xfe]), b"not utf-8")]);
    let worker = direct(&feed_yaml(&brokers, &group, "", ""));

    let dead = read_topic(&brokers, DEAD, 1);
    assert_eq!(dead[0].key.as_deref(), Some(&[0xff, 0xfe][..]));
    assert_eq!(dead[0].payload.as_deref(), Some(&b"not utf-8"[..]));
    wait_until(DEADLINE, "its offset is committed", || {
        committed(&brokers, &group, WORK) == Some(1)
    });
    assert_eq!(high_watermark(&brokers, RETRY), 0, "nothing was retried");
    assert!(worker.feed.receive(8).unwrap().is_empty());
}

/// Pins that a message with no key is a unit named by its position, read,
/// counted and committed like a keyed one, and that a message with an empty
/// key keeps the empty key as its id.
#[test]
fn a_message_without_a_key_takes_its_position_as_its_id() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("keyless");
    produce_raw(
        &brokers,
        WORK,
        &[(Some(&b""[..]), &b"empty"[..]), (None, &b"keyless"[..])],
    );
    let worker = direct(&feed_yaml(&brokers, &group, "", ""));

    let empty = worker.take("the empty-keyed message");
    let keyless = worker.take("the keyless message");
    assert_eq!(
        [&*empty.unit.id, &*keyless.unit.id],
        ["", "units/0/1"],
        "the ids"
    );
    assert_eq!(empty.unit.body, b"empty");
    assert_eq!(keyless.unit.body, b"keyless");
    assert_eq!((empty.deliveries, keyless.deliveries), (1, 1));
    worker.feed.acknowledge(empty.receipt).unwrap();
    worker.feed.acknowledge(keyless.receipt).unwrap();
    wait_until(DEADLINE, "both offsets are committed", || {
        committed(&brokers, &group, WORK) == Some(2)
    });
    assert_eq!(
        high_watermark(&brokers, DEAD),
        0,
        "nothing was dead-lettered"
    );
    assert_eq!(high_watermark(&brokers, RETRY), 0, "nothing was retried");
    let counted: Vec<_> = read_attempts(&brokers, ATTEMPT_PARTITIONS, 4)
        .into_iter()
        .filter(|(_, id, _)| id == "units/0/1")
        .map(|(_, _, count)| count)
        .collect();
    assert_eq!(counted, [Some(1), None]);
}

/// Pins that a keyless unit keeps the id its position gave it through its
/// retries: each retry and dead-letter copy is keyed by that id and names it
/// as its origin, and the unit's attempts count under it to the cap.
#[test]
fn a_keyless_unit_keeps_its_position_through_retries() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("keyless-retries");
    produce_raw(
        &brokers,
        WORK,
        &[(Some(&b"before"[..]), &b"x"[..]), (None, &b"keyless"[..])],
    );
    let worker = direct(&feed_yaml(&brokers, &group, "  attempt_cap: 2\n", ""));

    let before = worker.take("the keyed message");
    assert_eq!(before.unit.id, "before");
    worker.feed.acknowledge(before.receipt).unwrap();
    let first = worker.take("the keyless message");
    assert_eq!((&*first.unit.id, first.deliveries), ("units/0/1", 1));
    worker.feed.fail(first.receipt, "test").unwrap();
    let retry = read_topic(&brokers, RETRY, 1);
    assert_eq!(retry[0].key.as_deref(), Some(&b"units/0/1"[..]));
    assert_eq!(retry[0].header("spate.origin"), Some("units/0/1"));
    assert_eq!(retry[0].header("spate.deliveries"), Some("1"));
    let second = worker.take("the retry copy");
    assert_eq!((&*second.unit.id, second.deliveries), ("units/0/1", 2));
    worker.feed.fail(second.receipt, "test").unwrap();
    let dead = read_topic(&brokers, DEAD, 1);
    assert_eq!(dead[0].key.as_deref(), Some(&b"units/0/1"[..]));
    assert_eq!(dead[0].header("spate.origin"), Some("units/0/1"));
    assert_eq!(dead[0].header("spate.deliveries"), Some("2"));
    wait_until(DEADLINE, "both topics' offsets are committed", || {
        committed(&brokers, &group, WORK) == Some(2)
            && committed(&brokers, &group, RETRY) == Some(1)
    });
    let counted: Vec<_> = read_attempts(&brokers, ATTEMPT_PARTITIONS, 5)
        .into_iter()
        .filter(|(_, id, _)| id == "units/0/1")
        .map(|(_, _, count)| count)
        .collect();
    assert_eq!(counted, [Some(1), Some(2), None]);
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
    produce_raw(&brokers, WORK, &[(Some(&[0xff, 0xfe][..]), &b"first"[..])]);
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
    produce_raw(&brokers, WORK, &[(Some(&[0xff, 0xfe][..]), &b"second"[..])]);
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

/// Pins the publisher end to end: a publish is confirmed and writes the
/// unit to the work topic, key the unit's id, value its body and no header,
/// and writes nothing to the retry or dead-letter topics.
#[test]
fn a_publisher_writes_the_unit_to_the_work_topic() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let (publisher, _io) = publisher_on_mock(&feed_yaml(
        &brokers,
        &spate_test::unique_name("publisher"),
        "",
        "",
    ));
    let publisher = publisher.unwrap();
    let unit = Unit::new("u", b"body".to_vec());
    publish_to(&publisher, unit.clone());
    let read = read_topic(&brokers, WORK, 1);
    assert_eq!(read[0].key.as_deref(), Some(unit.id.as_bytes()));
    assert_eq!(read[0].payload.as_deref(), Some(&unit.body[..]));
    assert!(read[0].headers.is_empty(), "{:?}", read[0].headers);
    assert_eq!(high_watermark(&brokers, RETRY), 0);
    assert_eq!(high_watermark(&brokers, DEAD), 0);
}

/// Pins that a publish-only handle on the workers' group charges no unit an
/// attempt: a worker on that group reads each published unit on its first
/// delivery, and the attempt topic holds one count and one tombstone per
/// unit, the worker's.
#[test]
fn a_publish_only_handle_charges_no_attempt() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("publish-only");
    let yaml = feed_yaml(&brokers, &group, "", "");
    let (publisher, _io) = publisher_on_mock(&yaml);
    let publisher = publisher.unwrap();
    for i in 0..3 {
        publish_to(&publisher, Unit::new(format!("u{i}"), b"body".to_vec()));
    }
    let worker = direct(&yaml);
    for _ in 0..3 {
        let delivery = worker.take("a published unit");
        assert_eq!(delivery.deliveries, 1, "{}", delivery.unit.id);
        worker.feed.acknowledge(delivery.receipt).unwrap();
    }
    wait_until(DEADLINE, "every unit is committed", || {
        committed(&brokers, &group, WORK) == Some(3)
    });
    wait_until(DEADLINE, "a count and a tombstone per unit", || {
        high_watermarks(&brokers, ATTEMPT, ATTEMPT_PARTITIONS) == 6
    });
    drop(publisher);
}

/// Pins that the publisher runs the workers' startup check and refuses a
/// missing retry topic, naming it.
#[test]
fn a_publisher_refuses_a_missing_retry_topic() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(WORK, 1, 1).unwrap();
    cluster.create_topic(DEAD, 1, 1).unwrap();
    let (publisher, _io) = publisher_on_mock(&feed_yaml(
        &cluster.bootstrap_servers(),
        &spate_test::unique_name("publisher-missing"),
        "",
        "",
    ));
    match publisher {
        Err(KafkaFeedError::Refused(message)) => assert!(
            message.contains(&format!("the retry topic `{RETRY}` does not exist")),
            "{message}"
        ),
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// Pins that the publisher refuses a work topic the cluster reports
/// unauthorized, naming the read.
#[test]
fn a_publisher_refuses_an_unauthorized_work_topic() {
    let cluster = cluster(1);
    cluster
        .topic_error(
            WORK,
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,
        )
        .unwrap();
    let (publisher, _io) = publisher_on_mock(&feed_yaml(
        &cluster.bootstrap_servers(),
        &spate_test::unique_name("publisher-unauthorized"),
        "",
        "",
    ));
    match publisher {
        Err(KafkaFeedError::Refused(message)) => assert!(
            message.contains(&format!("reading the metadata of the work topic `{WORK}`")),
            "{message}"
        ),
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// Pins that the publish in flight when the cluster refuses the
/// publisher's idempotent producer id fails with the refusal's cause.
#[test]
fn the_publish_in_flight_at_a_fatal_producer_error_names_its_cause() {
    let cluster = cluster(1);
    cluster.request_errors(
        rdkafka::types::RDKafkaApiKey::InitProducerId,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED],
    );
    let (publisher, io) = publisher_on_mock(&feed_yaml(
        &cluster.bootstrap_servers(),
        &spate_test::unique_name("publisher-in-flight-fatal"),
        "",
        "",
    ));
    let publisher = publisher.unwrap();
    let err = io
        .block_on(publisher.publish(Unit::new("a", b"body".to_vec())))
        .unwrap_err();
    assert_eq!(err.kind, FeedErrorKind::Fatal, "{}", err.reason);
    assert!(
        err.reason.contains("Cluster authorization failed"),
        "{}",
        err.reason
    );
}

/// Pins that a bounded job on Kafka ends `Completed` once its unit is read
/// and committed and the group's lag stays zero over the window, read
/// through a consumer that never joins the group.
#[test]
fn a_bounded_job_on_kafka_ends_completed() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("bounded");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("b", 2);
    let _unit = stage(&data, "b.ndjson", &rows);
    let section = bounded_section(&brokers, &group, &dir);
    list_once(publisher_on_mock(&section), &data, &section);

    let clock = TestClock::frozen();
    let worker = launch_bounded(ON_MOCK, &data, &section, &clock);
    wait_until(DEADLINE, "the unit is read and committed", || {
        written(&worker.script) == sorted(rows.clone())
            && committed(&brokers, &group, WORK) == Some(1)
    });
    let report = run_to_exit(&worker, &clock);
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(report.exit_code(), 0);
}

/// Pins that a bounded job whose lister published nothing ends
/// `Completed`: partitions with no commit read from their log start.
#[test]
fn a_bounded_job_that_published_nothing_ends_completed() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("empty");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let section = bounded_section(&brokers, &group, &dir);
    list_once(publisher_on_mock(&section), &data, &section);

    let clock = TestClock::frozen();
    let worker = launch_bounded(ON_MOCK, &data, &section, &clock);
    let report = run_to_exit(&worker, &clock);
    assert_eq!(report.state, ExitState::Completed);
}

/// Pins that a message with no unit, which goes to the dead-letter topic at
/// once, ends a bounded job `Stalled`, naming the dead-letter topic.
#[test]
fn a_message_with_no_unit_ends_a_bounded_job_stalled() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("no-unit-bounded");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let section = bounded_section(&brokers, &group, &dir);
    list_once(publisher_on_mock(&section), &data, &section);
    produce_raw(
        &brokers,
        WORK,
        &[(Some(&[0xff, 0xfe][..]), &b"not utf-8"[..])],
    );

    let clock = TestClock::frozen();
    let worker = launch_bounded(ON_MOCK, &data, &section, &clock);
    wait_until(
        DEADLINE,
        "the message is dead-lettered and committed",
        || high_watermark(&brokers, DEAD) == 1 && committed(&brokers, &group, WORK) == Some(1),
    );
    let report = run_to_exit(&worker, &clock);
    assert_eq!(report.exit_code(), 1);
    let ExitState::Failed(failure) = &report.state else {
        panic!("expected a failed run, got {:?}", report.state);
    };
    assert!(
        failure.reason.contains("bounded feed job stalled") && failure.reason.contains(DEAD),
        "{}",
        failure.reason
    );
}

/// Read `of` from `feed` and wait for the answer.
fn read_depth(feed: &dyn Feed, of: DepthOf) -> Result<Depth, FeedError> {
    let mut reading = feed.read_depth(of).expect("the Kafka feed counts");
    let mut outcome = None;
    wait_until(DEADLINE, "the depth read is answered", || {
        outcome = reading.try_outcome();
        outcome.is_some()
    });
    outcome.expect("answered")
}

/// Pins that a broker refusing to describe the dead-letter topic, which it
/// answers as the topic's own error in an answered metadata request, fails
/// the dead-letter read Fatal, so a bounded job fails where it would read
/// the topic again every 15 s.
#[test]
fn a_dead_letter_topic_authorization_failure_is_fatal() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    for topic in [WORK, RETRY, DEAD] {
        cluster.create_topic(topic, 1, 1).expect("create topic");
    }
    cluster
        .create_topic(ATTEMPT, ATTEMPT_PARTITIONS, 1)
        .expect("create topic");
    let group = spate_test::unique_name("dead-letter-denied");
    let d = direct(&feed_yaml(&cluster.bootstrap_servers(), &group, "", ""));
    assert!(
        read_depth(&d.feed, DepthOf::DeadLetters)
            .unwrap()
            .is_empty()
    );
    cluster
        .topic_error(
            DEAD,
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,
        )
        .unwrap();
    let e = read_depth(&d.feed, DepthOf::DeadLetters).unwrap_err();
    assert_eq!(e.kind, FeedErrorKind::Fatal, "{e}");
    assert!(e.reason.contains(DEAD), "{e}");
}

/// Pins that startup refuses a `checkpoint.drain_timeout` within the stop's
/// reserve on the Kafka feed, naming the key and the feed.
#[test]
fn startup_refuses_a_drain_timeout_within_the_stop_reserve() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("reserve");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let source = format!("  s3:\n    url: \"file://{}/\"\n", data.display());
    let worker = launch_customized(
        ON_MOCK,
        &source,
        &feed_yaml(&brokers, &group, "", ""),
        |_| {},
        |source| source,
        "{ interval: 100ms, drain_timeout: 5s }",
        None,
    );
    let e = worker
        .run
        .wait_exit(DEADLINE)
        .expect("the run exits")
        .unwrap_err();
    let message = e.to_string();
    assert!(
        message.contains("checkpoint.drain_timeout") && message.contains("`kafka`"),
        "{message}"
    );
}

/// Pins that a stopping worker holding no unit stops receiving and closes
/// its feed without reading on: the run completes with the stop clock never
/// moved toward the reserve.
#[test]
fn a_worker_holding_nothing_stops_without_reading_on() {
    let cluster = cluster(1);
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("idle-stop");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let clock = StopClock::frozen();
    let worker = launch_stopping(
        ON_MOCK,
        &data,
        "",
        &feed_yaml(&brokers, &group, "", ""),
        &clock,
        |_| {},
    );
    worker.shutdown.trigger();
    let report = worker
        .run
        .wait_exit(DEADLINE)
        .expect("the run exits")
        .unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(worker.recording.stopped_receiving(), 1);
    assert_eq!(worker.recording.closes().len(), 1);
}
