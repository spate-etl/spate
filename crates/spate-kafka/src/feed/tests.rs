//! Unit tests of the backend's pure parts: the cap route, reading a
//! message, classifying a failed write, admitting a unit at the gate,
//! settling a write against the tracker, and the lock discipline around
//! commits.

use super::attempt::{self, Entry, PublishId, decode, partition_of};
use super::config::KafkaFeedConfig;
use super::consumer::{
    Read, commit_moved, commit_revoked, committed_lookup_timeout, read, take_due,
};
use super::events::Cause;
use super::gate::admission;
use super::inflight::Tp;
use super::producer::FeedProducerContext;
use super::write::Producer;
use super::write::{
    self, Attempt, DELIVERIES_HEADER, Destination, Failure, Finish, Move, Outgoing, Tombstone,
    Write, WriteClass, classify, route,
};
use super::{Core, Gating, KafkaFeed, KafkaFeedError, KafkaFeedPublisher, Ready};
use rdkafka::config::FromClientConfigAndContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode as C};
use spate_core::config::FeedSection;
use spate_core::feed::{Feed, FeedError, FeedErrorKind, Receipt, Unit, confirmation, wake_channel};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// The attempt topic's partition count in these tests.
const TWO: NonZeroU32 = NonZeroU32::new(2).expect("non-zero");

fn core(cap: u32) -> Core {
    let section: FeedSection = serde_yaml::from_str(&format!(
        "attempt_cap: {cap}\nkafka:\n  brokers: localhost:9092\n  group_id: workers\n  \
         topic: units\n  retry_topic: units-retry\n  dead_letter_topic: units-dead\n  \
         attempt_topic: units-attempts\n"
    ))
    .unwrap();
    Core::new(&KafkaFeedConfig::from_section(&section).unwrap(), TWO)
}

fn tp(partition: i32) -> Tp {
    Tp::new(&Arc::from("units"), partition)
}

fn cap(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

/// The publish identity of the work message at `offset` of partition 0.
fn identity(core: &Core, offset: i64) -> PublishId {
    PublishId::of_message("units", 0, offset, &[], &core.topics)
}

fn attempt_partition(core: &Core, identity: &PublishId) -> i32 {
    partition_of(identity.as_bytes(), core.attempt_partitions)
}

/// Deliver `offset` of `tp(0)` and return it as the gate takes it, with
/// `admitting` raised as the gate's take raises it.
fn gated(core: &Core, offset: i64) -> Gating {
    let mut st = core.lock();
    let receipt = st.tracker.deliver(&tp(0), offset).unwrap();
    st.admitting += 1;
    Gating {
        receipt,
        unit: Unit::new(format!("u{offset}"), b"body".to_vec()),
        identity: identity(core, offset),
    }
}

/// Record `count` attempts for `identity` in `core`'s ledger, as the
/// follower would on reading them.
fn recorded(core: &Core, identity: &PublishId, count: u32) {
    let partition = attempt_partition(core, identity);
    attempt::lock(&core.ledger).apply(partition, 0, identity.as_bytes(), Entry::Count(count));
}

/// Pins the cap boundary on a delivery's count: a unit failed on delivery
/// `cap - 1` goes to the retry topic, one failed on delivery `cap`, or past
/// it as after the cap was lowered, to the dead-letter topic.
#[test]
fn a_unit_fails_to_the_dead_letter_topic_from_the_cap() {
    assert_eq!(route(4, cap(5)), Destination::Retry);
    for (deliveries, cap) in [(5, cap(5)), (9, cap(5)), (1, NonZeroU32::MIN)] {
        assert_eq!(
            route(deliveries, cap),
            Destination::DeadLetter(Cause::AttemptCap),
            "{deliveries} under {cap}"
        );
    }
}

/// Pins what a message names: a UTF-8 key is the unit's id and the value
/// its body, and its attempts count under its publish identity: a retry
/// copy's `spate.origin`, a work message's own position. A missing or
/// non-UTF-8 key names no unit, and the message is kept as read for the
/// dead-letter topic.
#[test]
fn a_message_names_a_unit_by_its_key() {
    let topics = &core(10).topics;
    let headers = vec![
        ("other".to_owned(), Some(b"9".to_vec())),
        ("spate.origin".to_owned(), Some(b"units/3/4".to_vec())),
    ];
    match read(
        ("units-retry", 0, 7),
        Some(b"day-1"),
        Some(b"body"),
        headers,
        topics,
    ) {
        Read::Unit { unit, identity } => {
            assert_eq!(unit, Unit::new("day-1", b"body".to_vec()));
            assert_eq!(identity.to_string(), "units/3/4");
        }
        other => panic!("{other:?}"),
    }
    match read(("units", 1, 5), Some(b"empty"), None, Vec::new(), topics) {
        Read::Unit { unit, identity } => {
            assert!(unit.body.is_empty());
            assert_eq!(identity.to_string(), "units/1/5");
        }
        other => panic!("{other:?}"),
    }
    for key in [None, Some(&[0xff, 0xfe][..])] {
        let headers = vec![("h".to_owned(), None)];
        match read(
            ("units-retry", 0, 1),
            key,
            Some(b"raw"),
            headers.clone(),
            topics,
        ) {
            Read::NoUnit(out) => {
                assert_eq!(&*out.topic, "units-retry");
                assert_eq!(out.partition, None);
                assert_eq!(out.key.as_deref(), key);
                assert_eq!(out.payload.as_deref(), Some(&b"raw"[..]));
                assert_eq!(out.headers, headers);
            }
            other => panic!("{other:?}"),
        }
    }
}

/// Pins the write classes: an over-size message is refused, the codes the
/// sink's table calls fatal are fatal, and a timeout, a full queue, a topic
/// librdkafka has marked missing or no code is retried.
#[test]
fn a_failed_write_is_classified_by_the_sink_table() {
    assert_eq!(classify(Some(C::MessageSizeTooLarge)), WriteClass::Refused);
    for fatal in [
        C::UnknownTopicOrPartition,
        C::TopicAuthorizationFailed,
        C::InvalidProducerEpoch,
        C::Fatal,
    ] {
        assert_eq!(classify(Some(fatal)), WriteClass::Fatal, "{fatal:?}");
    }
    for retryable in [
        C::MessageTimedOut,
        C::QueueFull,
        C::BrokerTransportFailure,
        C::PurgeQueue,
        C::UnknownTopic,
    ] {
        assert_eq!(
            classify(Some(retryable)),
            WriteClass::Retryable,
            "{retryable:?}"
        );
    }
    assert_eq!(classify(None), WriteClass::Retryable);
}

/// Pins the bound on the committed-offset read: a quarter of
/// `max.poll.interval.ms`, at most 10 s, which librdkafka's 300 s default
/// reaches.
#[test]
fn the_committed_offset_read_is_bounded_by_the_poll_interval() {
    let mut cc = rdkafka::ClientConfig::new();
    assert_eq!(committed_lookup_timeout(&cc), Duration::from_secs(10));
    cc.set("max.poll.interval.ms", "6000");
    assert_eq!(committed_lookup_timeout(&cc), Duration::from_millis(1500));
    cc.set("max.poll.interval.ms", "600000");
    assert_eq!(committed_lookup_timeout(&cc), Duration::from_secs(10));
}

/// A move of `receipt`'s delivery to the retry topic.
fn moving(core: &Core, receipt: Receipt) -> Box<Write> {
    let position = core.lock().tracker.release(receipt).unwrap();
    let unit = Unit::new("u", b"b".to_vec());
    Box::new(Write::Move(Move::new(
        Outgoing::unit(Arc::clone(&core.topics.retry), &unit, 1, None),
        position,
        Destination::Retry,
        Some(unit.id),
        1,
        Finish::Complete,
    )))
}

fn failure(code: C) -> Failure {
    Failure {
        code: Some(code),
        reason: format!("{code:?}"),
    }
}

/// Pins that a move finishes its offset only on a confirmed report: a
/// retryable failure queues it to be sent again with its offset open, and
/// the confirmed resend finishes it.
#[test]
fn a_move_finishes_its_offset_only_when_confirmed() {
    let core = core(10);
    let receipt = {
        let mut st = core.lock();
        st.tracker.assign([tp(0)]);
        st.tracker.deliver(&tp(0), 7).unwrap()
    };
    let write = moving(&core, receipt);
    core.settle(*write, Err(failure(C::MessageTimedOut)));
    let resend = {
        let mut st = core.lock();
        assert_eq!(st.tracker.take_commits(), vec![(tp(0), 7)], "7 is open");
        assert_eq!(st.resends.len(), 1, "queued to be sent again");
        Box::new(st.resends.pop().unwrap().1)
    };
    core.settle(*resend, Ok(()));
    assert_eq!(core.lock().tracker.take_commits(), vec![(tp(0), 8)]);
    assert!(core.lock().fatal.is_none());
}

/// Pins that a resend is taken once due, and that a move or a tombstone
/// whose partition was given up since is dropped, its offset left for the
/// next owner, whose attempt record a late tombstone would clear.
#[test]
fn a_resend_for_a_partition_given_up_is_dropped() {
    let core = core(10);
    let (kept, dropped, acknowledged) = {
        let mut st = core.lock();
        st.tracker.assign([tp(0), tp(1)]);
        (
            st.tracker.deliver(&tp(0), 1).unwrap(),
            st.tracker.deliver(&tp(1), 1).unwrap(),
            st.tracker.deliver(&tp(1), 2).unwrap(),
        )
    };
    for receipt in [kept, dropped] {
        let write = moving(&core, receipt);
        core.settle(*write, Err(failure(C::MessageTimedOut)));
    }
    let position = core.lock().tracker.release(acknowledged).unwrap();
    let tombstone = Tombstone::new(&core.topics.attempt, &identity(&core, 2), 0, position);
    core.settle(
        Write::Tombstone(tombstone),
        Err(failure(C::MessageTimedOut)),
    );
    let mut st = core.lock();
    st.tracker.revoke(&[tp(1)]);
    let soon = Instant::now();
    assert!(take_due(&mut st, soon).is_empty(), "not due yet");
    let due = take_due(&mut st, soon + Duration::from_secs(1));
    assert_eq!(due.len(), 1);
    match &due[0] {
        Write::Move(moving) => assert_eq!(moving.position.tp, tp(0)),
        other => panic!("{other:?}"),
    }
    assert!(st.resends.is_empty());
}

/// Pins that a fatal write report stops the feed, drops the writes queued
/// to be sent again, and leaves the offset open.
#[test]
fn a_fatal_write_report_stops_the_feed() {
    let core = core(10);
    let (first, second) = {
        let mut st = core.lock();
        st.tracker.assign([tp(0)]);
        (
            st.tracker.deliver(&tp(0), 1).unwrap(),
            st.tracker.deliver(&tp(0), 2).unwrap(),
        )
    };
    let queued = moving(&core, first);
    core.settle(*queued, Err(failure(C::QueueFull)));
    let fatal = moving(&core, second);
    core.settle(*fatal, Err(failure(C::UnknownTopicOrPartition)));
    let mut st = core.lock();
    assert_eq!(st.fatal.as_ref().unwrap().kind, FeedErrorKind::Fatal);
    assert!(st.resends.is_empty());
    assert_eq!(st.tracker.take_commits(), vec![(tp(0), 1)]);
}

/// Pins how a publish settles: confirmed, refused for an over-size message
/// with the feed still running, retryable, or fatal with the feed stopped.
#[test]
fn a_publish_settles_by_its_report() {
    let core = core(10);
    let settle = |outcome| {
        let (confirmer, mut confirmation) = confirmation();
        let message = Outgoing::unit(
            Arc::clone(&core.topics.work),
            &Unit::new("u", Vec::new()),
            0,
            None,
        );
        core.settle(Write::Publish { message, confirmer }, outcome);
        confirmation.try_outcome().unwrap()
    };
    assert!(settle(Ok(())).is_ok());
    let refused = settle(Err(failure(C::MessageSizeTooLarge))).unwrap_err();
    assert_eq!(refused.kind, FeedErrorKind::Refused);
    assert!(core.lock().fatal.is_none());
    let retry = settle(Err(failure(C::MessageTimedOut))).unwrap_err();
    assert_eq!(retry.kind, FeedErrorKind::Retryable);
    let fatal = settle(Err(failure(C::TopicAuthorizationFailed))).unwrap_err();
    assert_eq!(fatal.kind, FeedErrorKind::Fatal);
    assert!(core.lock().fatal.is_some());
}

/// A unit of `tp(0)` at `offset`, buffered for the reader with `receipt`.
fn ready(core: &Core, receipt: Receipt, offset: i64) -> Ready {
    let identity = identity(core, offset);
    Ready {
        receipt,
        unit: Unit::new("u", Vec::new()),
        deliveries: 1,
        attempt_partition: attempt_partition(core, &identity),
        identity,
    }
}

/// Pins that a revocation's commit runs with the state lock released, and
/// that the revoked partition's buffered units leave the buffer first.
#[test]
fn a_revocation_commits_with_the_lock_released() {
    let core = core(10);
    {
        let mut st = core.lock();
        st.tracker.assign([tp(0), tp(1)]);
        for (partition, offset) in [(0, 4), (1, 9)] {
            let receipt = st.tracker.deliver(&tp(partition), offset).unwrap();
            let ready = ready(&core, receipt, offset);
            st.ready.push_back(ready);
        }
    }
    let mut committed = None;
    commit_revoked(&core, &[tp(0)], |points| {
        assert!(
            core.state.try_lock().is_ok(),
            "the lock is held across the commit"
        );
        committed = Some(points.to_vec());
    });
    assert_eq!(committed, Some(vec![(tp(0), 4)]));
    let st = core.lock();
    assert_eq!(st.ready.len(), 1);
    assert_eq!(st.ready[0].receipt, Receipt(2));
}

/// Pins that the periodic commit runs with the state lock released, and
/// that a failed commit leaves its points to be taken again.
#[test]
fn a_failed_periodic_commit_is_taken_again() {
    let core = core(10);
    {
        let mut st = core.lock();
        st.tracker.assign([tp(0)]);
        let receipt = st.tracker.deliver(&tp(0), 3).unwrap();
        assert!(st.tracker.acknowledge(receipt));
    }
    commit_moved(&core, |points| {
        assert!(
            core.state.try_lock().is_ok(),
            "the lock is held across the commit"
        );
        assert_eq!(points, [(tp(0), 4)]);
        false
    });
    let mut again = None;
    commit_moved(&core, |points| {
        again = Some(points.to_vec());
        true
    });
    assert_eq!(again, Some(vec![(tp(0), 4)]));
    commit_moved(&core, |points| panic!("nothing moved: {points:?}"));
}

/// Pins the headers a unit is written with: none on a publish, and on a
/// moved copy the count of the delivery moved and the unit's publish
/// identity.
#[test]
fn a_unit_is_written_with_its_count() {
    let unit = Unit::new("day-1", b"body".to_vec());
    let first = Outgoing::unit(Arc::from("units"), &unit, 0, None);
    assert_eq!(first.key.as_deref(), Some(&b"day-1"[..]));
    assert_eq!(first.payload.as_deref(), Some(&b"body"[..]));
    assert!(first.headers.is_empty());
    let origin = identity(&core(10), 7);
    let retried = Outgoing::unit(Arc::from("units-retry"), &unit, 3, Some(&origin));
    assert_eq!(
        retried.headers,
        vec![
            (DELIVERIES_HEADER.to_owned(), Some(b"3".to_vec())),
            ("spate.origin".to_owned(), Some(b"units/0/7".to_vec())),
        ]
    );
    assert_eq!(
        write::next_backoff(std::time::Duration::from_secs(4)).as_secs(),
        5
    );
}

/// Pins that the feed producer's context logs each error event once when it
/// arrives as the identical pair of callbacks rdkafka makes.
#[test]
fn a_producer_error_event_is_logged_once() {
    use rdkafka::client::ClientContext;
    const DOWN: &str = "1/1 brokers are down";
    let context = FeedProducerContext::new(Arc::new(core(10)));
    let lines = spate_test::capture_logs(tracing::Level::WARN, || {
        for _ in 0..4 {
            context.error(KafkaError::Global(C::AllBrokersDown), DOWN);
        }
    });
    let logged = lines.iter().filter(|line| line.contains(DOWN)).count();
    assert_eq!(logged, 2, "two events, one line each: {lines:#?}");
}

/// The section of a feed whose clients reach no broker.
fn unreachable_config(extra: &str) -> KafkaFeedConfig {
    let section: FeedSection = serde_yaml::from_str(&format!(
        "kafka:\n  brokers: 127.0.0.1:1\n  group_id: workers\n  topic: units\n  \
         retry_topic: units-retry\n  dead_letter_topic: units-dead\n  \
         attempt_topic: units-attempts\n  statistics_interval: 0s\n{extra}"
    ))
    .unwrap();
    KafkaFeedConfig::from_section(&section).unwrap()
}

/// A feed with no threads, whose producer reaches no broker.
fn idle_feed() -> KafkaFeed {
    let config = unreachable_config("");
    let core = Arc::new(Core::new(&config, TWO));
    let producer = Producer::from_config_and_context(
        &config.producer_config(false),
        FeedProducerContext::new(Arc::clone(&core)),
    )
    .unwrap();
    KafkaFeed {
        core,
        producer,
        thread: None,
        gate: None,
        follower: None,
        depth: None,
    }
}

/// A publisher whose producer reaches no broker, over `config`.
fn idle_publisher_over(config: &KafkaFeedConfig) -> KafkaFeedPublisher {
    let core = Arc::new(Core::new(config, TWO));
    let producer = Producer::from_config_and_context(
        &config.producer_config(false),
        FeedProducerContext::new(Arc::clone(&core)),
    )
    .unwrap();
    KafkaFeedPublisher::from_parts(core, producer)
}

fn idle_publisher() -> KafkaFeedPublisher {
    idle_publisher_over(&unreachable_config(""))
}

/// Pins that the publish-only handle reports no depth, since it reads
/// nothing, so a bounded job refuses it at startup.
#[test]
fn a_publish_only_handle_reports_no_depth() {
    use spate_core::feed::DepthOf;
    let publisher = idle_publisher();
    assert!(publisher.read_depth(DepthOf::Feed).is_none());
    assert!(publisher.read_depth(DepthOf::DeadLetters).is_none());
}

/// Pins the publisher's receive side: it names the `kafka` backend, does
/// not hand back, refuses a read as Fatal naming the worker's constructor,
/// names no delivery for a receipt, and keeps no waker.
#[test]
fn a_publisher_answers_the_receive_side() {
    let publisher = idle_publisher();
    assert_eq!(publisher.backend(), "kafka");
    assert!(!publisher.hands_back());
    let err = publisher.receive(8).unwrap_err();
    assert_eq!(err.kind, FeedErrorKind::Fatal);
    assert!(err.reason.contains("KafkaFeed::connect"), "{}", err.reason);
    for outcome in [
        publisher.renew(Receipt(1)),
        publisher.acknowledge(Receipt(1)),
        publisher.fail(Receipt(1), "why"),
    ] {
        assert_eq!(outcome.unwrap_err().kind, FeedErrorKind::Gone);
    }
    let (waker, park) = wake_channel();
    publisher.set_waker(waker);
    assert!(!park.park(Duration::ZERO), "woken");
    publisher.core().latch("x");
    assert!(!park.park(Duration::ZERO), "a latch woke the waker");
}

/// Pins that a publisher whose feed latched refuses every later publish
/// at once with the latched error.
#[test]
fn a_latched_publisher_refuses_every_later_publish() {
    let publisher = idle_publisher();
    publisher.core().latch("x");
    let mut confirmation = publisher.publish(Unit::new("u", b"body".to_vec()));
    let err = confirmation.try_outcome().expect("settled").unwrap_err();
    assert_eq!(err.kind, FeedErrorKind::Fatal);
    assert_eq!(err.reason, "x");
}

/// Pins that a publisher's `Debug` prints no passthrough value.
#[cfg(feature = "tls")]
#[test]
fn a_publishers_debug_prints_no_rdkafka_value() {
    let publisher = idle_publisher_over(&unreachable_config(
        "  rdkafka:\n    security.protocol: sasl_plaintext\n    sasl.mechanism: PLAIN\n    \
         sasl.username: u\n    sasl.password: hunter2\n",
    ));
    let shown = format!("{publisher:?}");
    assert!(!shown.contains("hunter2"), "{shown}");
    assert!(shown.contains("units"), "{shown}");
}

/// Hand the producer context of `publisher` one error callback, as
/// rdkafka's poll does.
fn report_error(publisher: &KafkaFeedPublisher, code: C, text: &str) {
    use rdkafka::client::ClientContext as _;
    use rdkafka::producer::Producer as _;
    publisher
        .producer()
        .context()
        .error(KafkaError::Global(code), text);
}

/// Pins that a rejected credential or TLS handshake the producer reports
/// after connect stops the feed, and a later publish fails Fatal with the
/// rejection's text.
#[test]
fn a_rejection_the_feed_producer_reports_latches_the_feed() {
    for (code, text) in [
        (
            C::Authentication,
            "SASL authentication error: Authentication failed: Invalid username or password",
        ),
        (
            C::SaslAuthenticationFailed,
            "Broker: SASL Authentication failed: Invalid username or password",
        ),
        (
            C::SSL,
            "ssl://127.0.0.1:9093/bootstrap: SSL handshake failed: ssl/tls alert handshake \
             failure: SSL alert number 40 (after 0ms in state SSL_HANDSHAKE)",
        ),
    ] {
        let publisher = idle_publisher();
        report_error(&publisher, code, text);
        assert!(publisher.core().latched(), "{code:?}");
        let err = publisher
            .publish(Unit::new("u", b"body".to_vec()))
            .try_outcome()
            .expect("settled")
            .unwrap_err();
        assert_eq!(err.kind, FeedErrorKind::Fatal);
        assert!(err.reason.ends_with(text), "{}", err.reason);
    }
}

/// Pins that a SASL mechanism handshake that failed on a local error, such
/// as a dropped connection, does not stop the feed.
#[test]
fn a_local_sasl_handshake_failure_does_not_latch_the_feed() {
    let publisher = idle_publisher();
    report_error(
        &publisher,
        C::Authentication,
        "SASL mechanism handshake failed: Local: Broker transport failure",
    );
    assert!(!publisher.core().latched());
}

/// Pins that a fatal producer error stops the feed with its cause, so a
/// later publish fails Fatal naming it, for librdkafka's fatal code and for
/// the cause's code rdkafka reports it under.
#[test]
fn a_fatal_producer_error_latches_the_feed_with_its_cause() {
    let text = "Failed to acquire idempotence PID from broker 127.0.0.1:9092/1: Broker: \
                Cluster authorization failed";
    for code in [
        C::Fatal,
        C::ClusterAuthorizationFailed,
        C::OutOfOrderSequenceNumber,
        C::InvalidProducerEpoch,
        C::InvalidProducerIdMapping,
        C::UnknownProducerId,
        C::Fenced,
        C::Inconsistent,
        C::GaplessGuarantee,
    ] {
        let publisher = idle_publisher();
        report_error(&publisher, code, text);
        let err = publisher
            .publish(Unit::new("u", b"body".to_vec()))
            .try_outcome()
            .expect("settled")
            .unwrap_err();
        assert_eq!(err.kind, FeedErrorKind::Fatal, "{code:?}");
        assert!(
            err.reason.contains("Cluster authorization failed"),
            "{}",
            err.reason
        );
    }
    let publisher = idle_publisher();
    report_error(
        &publisher,
        C::BrokerTransportFailure,
        "127.0.0.1:9092/1: Disconnected",
    );
    assert!(
        !publisher.core().latched(),
        "a transport failure stopped the feed"
    );
}

/// Pins that a publish whose enqueue librdkafka refuses inline as fatal
/// settles Fatal and latches the core it was published through, so the
/// publish path holds no state lock across the send.
#[test]
fn an_inline_fatal_enqueue_settles_without_deadlock() {
    use rdkafka::producer::Producer as _;
    use rdkafka::types::RDKafkaRespErr;
    let config = unreachable_config("");
    let owner = Arc::new(Core::new(&config, TWO));
    let producer = Arc::new(
        Producer::from_config_and_context(
            &config.producer_config(false),
            FeedProducerContext::new(Arc::clone(&owner)),
        )
        .unwrap(),
    );
    // SAFETY: the pointer is a live client owned by `producer`, and the
    // reason is a NUL-terminated literal.
    let raised = unsafe {
        rdkafka::bindings::rd_kafka_test_fatal_error(
            producer.client().native_ptr(),
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED,
            c"by test".as_ptr(),
        )
    };
    assert_eq!(raised, RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR);
    // A core no callback has latched yet, as between librdkafka raising the
    // fatal error and the poll thread reporting it.
    let fresh = Arc::new(Core::new(&config, TWO));
    let (core, sender) = (Arc::clone(&fresh), Arc::clone(&producer));
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut confirmation =
            super::publish_unit(&core, &sender, &Unit::new("u", b"body".to_vec()));
        let _ = tx.send(confirmation.try_outcome());
    });
    let outcome = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("publish_unit did not return: the state lock is held across the send");
    let err = outcome.expect("settled inline").unwrap_err();
    assert_eq!(err.kind, FeedErrorKind::Fatal, "{}", err.reason);
    assert!(fresh.latched());
}
/// Pins that the publisher refuses a section a worker refuses, with the
/// worker's error, before it builds any client.
#[test]
fn a_publisher_refuses_the_section_a_worker_refuses() {
    let section: FeedSection = serde_yaml::from_str(
        "kafka:\n  brokers: 127.0.0.1:1\n  group_id: workers\n  topic: units\n  \
         retry_topic: units\n  dead_letter_topic: units-dead\n  \
         attempt_topic: units-attempts\n",
    )
    .unwrap();
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let worker = KafkaFeed::connect(&section, io.handle()).unwrap_err();
    let publisher = KafkaFeedPublisher::connect(&section, io.handle()).unwrap_err();
    assert!(matches!(worker, KafkaFeedError::Config(_)), "{worker}");
    assert!(
        matches!(publisher, KafkaFeedError::Config(_)),
        "{publisher}"
    );
    assert_eq!(worker.to_string(), publisher.to_string());
}

/// Buffer one unit of partition 0 for `feed`'s reader.
fn buffer_one(feed: &KafkaFeed) {
    let mut st = feed.core.lock();
    st.tracker.assign([tp(0)]);
    let receipt = st.tracker.deliver(&tp(0), 0).unwrap();
    let ready = ready(&feed.core, receipt, 0);
    st.ready.push_back(ready);
}

/// Pins that a transient consumer error is returned by the next `receive`
/// alone, ahead of the units buffered, and that the call after it returns
/// those units.
#[test]
fn a_transient_consumer_error_is_returned_once() {
    let feed = idle_feed();
    buffer_one(&feed);
    feed.core.lock().retryable = Some(FeedError::new(FeedErrorKind::Retryable, "broker down"));
    let error = feed.receive(8).unwrap_err();
    assert_eq!(error.kind, FeedErrorKind::Retryable);
    assert_eq!(feed.receive(8).unwrap().len(), 1);
}

/// Pins that a waker set while units are buffered is woken at once.
#[test]
fn a_waker_set_with_units_buffered_wakes_at_once() {
    let feed = idle_feed();
    buffer_one(&feed);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    assert!(park.park(std::time::Duration::ZERO), "not woken");
}

/// Pins the gate's two answers: a unit recorded below the cap is admitted
/// with an attempt write of the recorded count plus one, to its attempt
/// partition, its receipt still held; a unit recorded at the cap moves to
/// the dead-letter topic, unread, with the recorded count in its
/// `spate.deliveries` header, its receipt released, and its offset
/// finishes on a tombstone. Both offsets stay unfinished.
#[test]
fn a_unit_is_admitted_below_the_cap_and_dead_lettered_at_it() {
    let core = core(3);
    core.lock().tracker.assign([tp(0)]);
    let below = gated(&core, 0);
    let at = gated(&core, 1);
    recorded(&core, &below.identity, 2);
    recorded(&core, &at.identity, 3);
    let (below_receipt, at_receipt) = (below.receipt, at.receipt);
    let (below_id, at_id) = (below.identity.clone(), at.identity.clone());

    match admission(&core, below) {
        Some(Write::Attempt(attempt)) => {
            assert_eq!(attempt.attempts, 3);
            assert_eq!(&*attempt.message.topic, "units-attempts");
            assert_eq!(
                attempt.message.partition,
                Some(attempt_partition(&core, &below_id))
            );
            assert_eq!(attempt.message.key.as_deref(), Some(below_id.as_bytes()));
            assert_eq!(
                decode("units-attempts", 0, 0, attempt.message.payload.as_deref()),
                Ok(Entry::Count(3))
            );
        }
        other => panic!("{other:?}"),
    }
    assert!(core.lock().tracker.held(below_receipt).is_some());
    assert_eq!(core.lock().admitting, 2, "the attempt write is in flight");

    match admission(&core, at) {
        Some(Write::Move(moving)) => {
            assert_eq!(moving.to, Destination::DeadLetter(Cause::AttemptCap));
            assert_eq!(moving.finish, Finish::Tombstone(at_id.clone()));
            assert_eq!(&*moving.message.topic, "units-dead");
            assert!(
                moving
                    .message
                    .headers
                    .contains(&("spate.origin".to_owned(), Some(at_id.as_bytes().to_vec())))
            );
            assert!(
                moving
                    .message
                    .headers
                    .contains(&(DELIVERIES_HEADER.to_owned(), Some(b"3".to_vec()))),
                "the copy carries the recorded count"
            );
        }
        other => panic!("{other:?}"),
    }
    let mut st = core.lock();
    assert!(st.tracker.held(at_receipt).is_none());
    assert_eq!(st.admitting, 1);
    assert_eq!(st.tracker.commit_point(&tp(0)), Some(0));
}

/// Pins that acknowledging a unit leaves its offset unfinished until the
/// tombstone of its attempt record is confirmed.
#[test]
fn an_acknowledged_unit_finishes_only_once_its_tombstone_is_confirmed() {
    let feed = idle_feed();
    buffer_one(&feed);
    let delivery = feed.receive(1).unwrap().pop().unwrap();
    let position = feed.core.lock().tracker.held(delivery.receipt).unwrap();
    feed.acknowledge(delivery.receipt).unwrap();
    assert_eq!(feed.core.lock().tracker.commit_point(&tp(0)), Some(0));

    let identity = identity(&feed.core, 0);
    let partition = attempt_partition(&feed.core, &identity);
    let tombstone = Tombstone::new(&feed.core.topics.attempt, &identity, partition, position);
    assert_eq!(tombstone.message.payload, None);
    assert_eq!(tombstone.message.partition, Some(partition));
    feed.core.settle(Write::Tombstone(tombstone), Ok(()));
    assert_eq!(feed.core.lock().tracker.commit_point(&tp(0)), Some(1));
}

/// Pins that acknowledging a receipt the feed does not hold, or one whose
/// partition was given up since it was handed out, answers Gone and sends
/// no tombstone.
#[test]
fn acknowledging_a_receipt_no_longer_held_is_gone() {
    let feed = idle_feed();
    assert_eq!(
        feed.acknowledge(Receipt(99)).unwrap_err().kind,
        FeedErrorKind::Gone
    );
    buffer_one(&feed);
    let delivery = feed.receive(1).unwrap().pop().unwrap();
    feed.core.lock().tracker.revoke(&[tp(0)]);
    assert_eq!(
        feed.acknowledge(delivery.receipt).unwrap_err().kind,
        FeedErrorKind::Gone
    );
}

/// A move of the unit at `offset` of `tp(0)` to the dead-letter topic,
/// finished by a tombstone.
fn dead_lettering(core: &Core, offset: i64) -> Write {
    let position = {
        let mut st = core.lock();
        let receipt = st.tracker.deliver(&tp(0), offset).unwrap();
        st.tracker.release(receipt).unwrap()
    };
    let unit = Unit::new("u", b"b".to_vec());
    let identity = identity(core, offset);
    Write::Move(Move::new(
        Outgoing::unit(
            Arc::clone(&core.topics.dead_letter),
            &unit,
            3,
            Some(&identity),
        ),
        position,
        Destination::DeadLetter(Cause::AttemptCap),
        Some(unit.id),
        3,
        Finish::Tombstone(identity),
    ))
}

/// Pins that a confirmed dead-letter write queues the tombstone of the
/// unit's attempt record for the consumer thread to send, and finishes
/// nothing itself; and that a dead-letter write confirmed on a stopped
/// feed queues nothing.
#[test]
fn a_dead_lettered_unit_finishes_only_once_its_tombstone_is_confirmed() {
    let core = core(3);
    core.lock().tracker.assign([tp(0)]);
    let write = dead_lettering(&core, 4);
    core.settle(write, Ok(()));
    let tombstone = {
        let mut st = core.lock();
        assert_eq!(st.tracker.commit_point(&tp(0)), Some(4));
        assert_eq!(st.resends.len(), 1);
        let (at, write) = st.resends.pop().unwrap();
        assert!(at <= Instant::now(), "due at once");
        write
    };
    let identity = identity(&core, 4);
    match &tombstone {
        Write::Tombstone(t) => {
            assert_eq!(t.message.key.as_deref(), Some(identity.as_bytes()));
            assert_eq!(t.message.payload, None);
            assert_eq!(
                t.message.partition,
                Some(attempt_partition(&core, &identity))
            );
        }
        other => panic!("{other:?}"),
    }
    core.settle(tombstone, Ok(()));
    assert_eq!(core.lock().tracker.commit_point(&tp(0)), Some(5));

    let write = dead_lettering(&core, 5);
    core.stop.store(true, Ordering::Release);
    core.settle(write, Ok(()));
    let st = core.lock();
    assert!(st.resends.is_empty(), "a stopped feed queued a tombstone");
    assert_eq!(st.tracker.commit_point(&tp(0)), Some(5));
}

/// Pins that a confirmed retry write finishes its offset at once and leaves
/// the attempt record in place: no tombstone is queued.
#[test]
fn a_retried_unit_keeps_its_attempt_record() {
    let core = core(3);
    let receipt = {
        let mut st = core.lock();
        st.tracker.assign([tp(0)]);
        st.tracker.deliver(&tp(0), 2).unwrap()
    };
    let write = moving(&core, receipt);
    core.settle(*write, Ok(()));
    let st = core.lock();
    assert!(st.resends.is_empty());
    assert_eq!(st.tracker.commit_point(&tp(0)), Some(3));
}

/// The attempt write that admits `entry`, as the gate builds it.
fn attempt_write(core: &Core, entry: Gating) -> Write {
    let partition = attempt_partition(core, &entry.identity);
    Write::Attempt(Attempt::new(
        &core.topics.attempt,
        entry.identity,
        partition,
        1,
        entry.receipt,
        entry.unit,
    ))
}

/// Pins that an attempt confirmed after its partition was given up buffers
/// nothing for the reader, and gives back its share of `admitting`.
#[test]
fn an_attempt_confirmed_after_its_partition_was_given_up_is_dropped() {
    let core = core(10);
    core.lock().tracker.assign([tp(0)]);
    let entry = gated(&core, 0);
    let write = attempt_write(&core, entry);
    core.lock().tracker.revoke(&[tp(0)]);
    core.settle(write, Ok(()));
    let st = core.lock();
    assert!(st.ready.is_empty());
    assert_eq!(st.admitting, 0);
}

/// Pins that a delivery whose partition is given up while the gate holds
/// it is skipped, below the cap and at it, and gives back its share of
/// `admitting`.
#[test]
fn a_receipt_released_during_a_gate_pass_is_skipped() {
    let core = core(3);
    core.lock().tracker.assign([tp(0)]);
    let below = gated(&core, 0);
    let at = gated(&core, 1);
    recorded(&core, &at.identity, 3);
    core.lock().tracker.revoke(&[tp(0)]);
    assert!(admission(&core, below).is_none());
    assert!(admission(&core, at).is_none());
    assert_eq!(core.lock().admitting, 0);
}

/// Pins that an attempt write queued to be sent again is dropped once its
/// partition is given up, and gives back its share of `admitting`.
#[test]
fn a_resent_attempt_for_a_revoked_partition_is_dropped() {
    let core = core(10);
    core.lock().tracker.assign([tp(0)]);
    let entry = gated(&core, 0);
    let write = attempt_write(&core, entry);
    let mut st = core.lock();
    st.resends.push((Instant::now(), write));
    st.tracker.revoke(&[tp(0)]);
    assert!(take_due(&mut st, Instant::now() + Duration::from_secs(1)).is_empty());
    assert_eq!(st.admitting, 0);
}

/// Pins that an attempt write that fails transiently is sent again while
/// its partition is owned, and that its confirmation hands the unit to the
/// reader with its offset still unfinished.
#[test]
fn a_failed_attempt_write_is_sent_again_and_delivers_the_unit() {
    let core = core(10);
    core.lock().tracker.assign([tp(0)]);
    let entry = gated(&core, 7);
    let write = admission(&core, entry).expect("admitted below the cap");
    core.settle(write, Err(failure(C::MessageTimedOut)));
    let mut due = take_due(&mut core.lock(), Instant::now() + Duration::from_secs(1));
    assert_eq!(due.len(), 1);
    core.settle(due.pop().unwrap(), Ok(()));
    {
        let st = core.lock();
        assert_eq!(st.ready.len(), 1);
        assert_eq!(st.ready[0].deliveries, 1);
        assert_eq!(st.admitting, 0);
        assert_eq!(st.tracker.commit_point(&tp(0)), Some(7));
    }

    // One that cannot be sent again stops the feed, naming the write.
    let entry = gated(&core, 8);
    let write = admission(&core, entry).expect("admitted below the cap");
    core.settle(write, Err(failure(C::TopicAuthorizationFailed)));
    let reason = core.lock().fatal.clone().expect("the feed stopped").reason;
    assert!(
        reason.contains("writing an attempt record to `units-attempts`"),
        "{reason}"
    );
}

/// Pins that a confirmed attempt write wakes the reader.
#[test]
fn an_admitted_unit_wakes_the_reader() {
    let feed = idle_feed();
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    feed.core.lock().tracker.assign([tp(0)]);
    let entry = gated(&feed.core, 0);
    let write = attempt_write(&feed.core, entry);
    feed.core.settle(write, Ok(()));
    assert!(park.park(Duration::ZERO), "not woken");
}

/// Pins that a tombstone write that fails transiently is queued to be sent
/// again, and that a fatal report for one stops the feed with its offset
/// open.
#[test]
fn a_fatal_tombstone_report_stops_the_feed() {
    let core = core(10);
    let position = {
        let mut st = core.lock();
        st.tracker.assign([tp(0)]);
        let receipt = st.tracker.deliver(&tp(0), 0).unwrap();
        st.tracker.release(receipt).unwrap()
    };
    let identity = identity(&core, 0);
    let tombstone =
        |position| Write::Tombstone(Tombstone::new(&core.topics.attempt, &identity, 0, position));
    core.settle(
        tombstone(position.clone()),
        Err(failure(C::MessageTimedOut)),
    );
    assert_eq!(core.lock().resends.len(), 1);
    assert!(core.lock().fatal.is_none());
    core.settle(
        tombstone(position),
        Err(failure(C::TopicAuthorizationFailed)),
    );
    let mut st = core.lock();
    let reason = &st.fatal.as_ref().expect("the feed stopped").reason;
    assert!(reason.contains("tombstone"), "{reason}");
    assert!(st.resends.is_empty());
    assert_eq!(st.tracker.take_commits(), vec![(tp(0), 0)]);
}

/// Pins that an attempt write and a tombstone write the broker refuses for
/// their content, as a message over the topic's size limit, each stop the
/// feed naming the write and queue nothing to send again.
#[test]
fn a_refused_attempt_or_tombstone_write_stops_the_feed() {
    let core_a = core(10);
    core_a.lock().tracker.assign([tp(0)]);
    let entry = gated(&core_a, 0);
    let write = admission(&core_a, entry).expect("admitted below the cap");
    core_a.settle(write, Err(failure(C::MessageSizeTooLarge)));
    let st = core_a.lock();
    let reason = &st.fatal.as_ref().expect("the feed stopped").reason;
    assert!(
        reason.contains("writing an attempt record to `units-attempts`"),
        "{reason}"
    );
    assert!(st.resends.is_empty());
    drop(st);

    let core_t = core(10);
    let position = {
        let mut st = core_t.lock();
        st.tracker.assign([tp(0)]);
        let receipt = st.tracker.deliver(&tp(0), 0).unwrap();
        st.tracker.release(receipt).unwrap()
    };
    let tombstone = Tombstone::new(&core_t.topics.attempt, &identity(&core_t, 0), 0, position);
    core_t.settle(
        Write::Tombstone(tombstone),
        Err(failure(C::MessageSizeTooLarge)),
    );
    let st = core_t.lock();
    let reason = &st.fatal.as_ref().expect("the feed stopped").reason;
    assert!(
        reason.contains("writing an attempt record's tombstone to `units-attempts`"),
        "{reason}"
    );
    assert!(st.resends.is_empty());
}

/// Pins that a write failing again waits longer each time: a tombstone
/// failed three times is queued with a next wait of 400 ms, 800 ms, then
/// 1.6 s.
#[test]
fn a_write_failing_again_backs_off_further_each_time() {
    let core = core(10);
    let position = {
        let mut st = core.lock();
        st.tracker.assign([tp(0)]);
        let receipt = st.tracker.deliver(&tp(0), 0).unwrap();
        st.tracker.release(receipt).unwrap()
    };
    let mut write = Write::Tombstone(Tombstone::new(
        &core.topics.attempt,
        &identity(&core, 0),
        0,
        position,
    ));
    for millis in [400, 800, 1600] {
        core.settle(write, Err(failure(C::MessageTimedOut)));
        let (_, mut queued) = core.lock().resends.pop().expect("queued to send again");
        assert_eq!(
            queued.backoff_mut().copied(),
            Some(Duration::from_millis(millis))
        );
        write = queued;
    }
}

/// Pins that a copy at the gate replaced by a redelivery leaves the buffer
/// count, which decides when the partitions pause.
#[test]
fn a_replaced_copy_at_the_gate_leaves_the_buffer_count() {
    let core = core(10);
    let mut st = core.lock();
    st.tracker.assign([tp(0)]);
    let first = st.tracker.deliver(&tp(0), 3).unwrap();
    st.gating.push_back(Gating {
        receipt: first,
        unit: Unit::new("u", Vec::new()),
        identity: identity(&core, 3),
    });
    let replaced = st.tracker.replace_copies(&tp(0), 3);
    st.forget(&replaced);
    let second = st.tracker.deliver(&tp(0), 3).unwrap();
    st.gating.push_back(Gating {
        receipt: second,
        unit: Unit::new("u", Vec::new()),
        identity: identity(&core, 3),
    });
    assert_eq!(st.held_for_reader(), 1);
}

/// Run `f` on a thread of its own and report whether it returned within
/// `limit`.
fn returns_within(limit: Duration, f: impl FnOnce() + Send + 'static) -> bool {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        f();
        let _ = tx.send(());
    });
    rx.recv_timeout(limit).is_ok()
}

/// Pins that a gate pass whose every watermark read fails puts back each
/// delivery in its order, with `admitting` lowered and the feed still
/// running: a pass over four attempt partitions on an unreachable broker
/// admits nothing and returns.
#[test]
fn a_gate_pass_puts_back_the_deliveries_of_each_failed_watermark_read() {
    const STEP: Duration = Duration::from_millis(100);
    let config = unreachable_config("  delivery_timeout: 1h\n");
    let core = Arc::new(Core::new(&config, NonZeroU32::new(4).unwrap()));
    let producer = Producer::from_config_and_context(
        &config.producer_config(false),
        FeedProducerContext::new(Arc::clone(&core)),
    )
    .unwrap();
    let mut seen = std::collections::BTreeSet::new();
    let mut offsets = Vec::new();
    for offset in 0.. {
        let id = identity(&core, offset);
        if seen.insert(attempt_partition(&core, &id)) {
            offsets.push(offset);
        }
        if seen.len() == 4 {
            break;
        }
    }
    {
        let mut st = core.lock();
        st.tracker.assign([tp(0)]);
        for &offset in &offsets {
            let receipt = st.tracker.deliver(&tp(0), offset).unwrap();
            st.gating.push_back(Gating {
                receipt,
                unit: Unit::new("u", Vec::new()),
                identity: identity(&core, offset),
            });
        }
    }
    let gate = super::gate::Gate::new(Arc::clone(&core), producer, config.delivery_timeout)
        .with_step(STEP);
    assert!(
        returns_within(Duration::from_secs(10), move || gate.pass()),
        "the pass did not return"
    );
    let st = core.lock();
    assert!(st.fatal.is_none(), "a timed-out read stopped the feed");
    let back: Vec<String> = st.gating.iter().map(|g| g.identity.to_string()).collect();
    let expected: Vec<String> = offsets
        .iter()
        .map(|&o| identity(&core, o).to_string())
        .collect();
    assert_eq!(back, expected);
    assert_eq!(st.admitting, 0);
}

/// Pins that a feed whose gate waits on a watermark read of an unreachable
/// attempt partition drops within a few steps: each read is bounded by the
/// gate's step.
#[test]
fn a_gate_waiting_on_an_unreachable_attempt_partition_lets_the_feed_drop() {
    let mut feed = idle_feed();
    let gate = super::gate::Gate::new(
        Arc::clone(&feed.core),
        feed.producer.clone(),
        Duration::from_secs(3600),
    );
    feed.gate = Some(gate.spawn().unwrap());
    {
        let mut st = feed.core.lock();
        st.tracker.assign([tp(0)]);
        let receipt = st.tracker.deliver(&tp(0), 0).unwrap();
        st.gating.push_back(Gating {
            receipt,
            unit: Unit::new("u", Vec::new()),
            identity: identity(&feed.core, 0),
        });
        feed.core.gate_wake.notify_all();
    }
    let core = Arc::clone(&feed.core);
    spate_test::wait_until(Duration::from_secs(30), "the gate takes the unit", || {
        core.lock().admitting == 1
    });
    assert!(
        returns_within(Duration::from_secs(10), move || drop(feed)),
        "dropping the feed did not return"
    );
}

/// Pins that a pass whose watermark read fails does not put back a
/// delivery whose partition was revoked while the pass ran.
#[test]
fn a_delivery_revoked_during_a_pass_is_not_put_back() {
    let config = unreachable_config("  delivery_timeout: 1h\n");
    let core = Arc::new(Core::new(&config, TWO));
    let producer = Producer::from_config_and_context(
        &config.producer_config(false),
        FeedProducerContext::new(Arc::clone(&core)),
    )
    .unwrap();
    {
        let mut st = core.lock();
        st.tracker.assign([tp(0)]);
        let receipt = st.tracker.deliver(&tp(0), 0).unwrap();
        st.gating.push_back(Gating {
            receipt,
            unit: Unit::new("u", Vec::new()),
            identity: identity(&core, 0),
        });
    }
    let gate = super::gate::Gate::new(Arc::clone(&core), producer, config.delivery_timeout)
        .with_step(Duration::from_secs(1));
    let pass = std::thread::spawn(move || gate.pass());
    spate_test::wait_until(
        Duration::from_secs(10),
        "the pass takes the delivery",
        || core.lock().admitting == 1,
    );
    super::consumer::commit_revoked(&core, &[tp(0)], |_| {});
    assert!(
        core.lock().gating.is_empty(),
        "the revocation left the queue"
    );
    pass.join().unwrap();
    let st = core.lock();
    assert!(
        st.gating.is_empty(),
        "a delivery revoked during the pass went back to the gate: {} entries, held_for_reader {}",
        st.gating.len(),
        st.held_for_reader()
    );
}

/// Pins that a latched feed's gate thread ends without waiting for a stop.
#[test]
fn a_latched_feed_ends_its_gate_thread() {
    let config = unreachable_config("");
    let core = Arc::new(Core::new(&config, TWO));
    let producer = Producer::from_config_and_context(
        &config.producer_config(false),
        FeedProducerContext::new(Arc::clone(&core)),
    )
    .unwrap();
    let handle = super::gate::Gate::new(Arc::clone(&core), producer, config.delivery_timeout)
        .with_step(Duration::from_millis(10))
        .spawn()
        .unwrap();
    core.latch("probe");
    spate_test::wait_until(Duration::from_secs(10), "the gate thread ends", || {
        handle.is_finished()
    });
    super::stop(&core);
    handle.join().unwrap();
}

/// Pins that a feed that stops or latches while a pass reads its
/// watermarks ends the pass before its next read: over four attempt
/// partitions on an unreachable broker, the pass begins the one read it
/// was in when the feed ended, and no other.
#[test]
fn a_feed_that_ends_during_a_pass_ends_it_before_the_next_read() {
    // The stop or latch has to land while the first read waits out its
    // step, so the step is long against the 5 ms poll.
    const STEP: Duration = Duration::from_secs(2);
    let config = unreachable_config("  delivery_timeout: 1h\n");
    for how in ["stopped", "latched"] {
        let core = Arc::new(Core::new(&config, NonZeroU32::new(4).unwrap()));
        let producer = Producer::from_config_and_context(
            &config.producer_config(false),
            FeedProducerContext::new(Arc::clone(&core)),
        )
        .unwrap();
        let mut seen = std::collections::BTreeSet::new();
        let mut offsets = Vec::new();
        for offset in 0.. {
            if seen.insert(attempt_partition(&core, &identity(&core, offset))) {
                offsets.push(offset);
            }
            if seen.len() == 4 {
                break;
            }
        }
        {
            let mut st = core.lock();
            st.tracker.assign([tp(0)]);
            for &offset in &offsets {
                let receipt = st.tracker.deliver(&tp(0), offset).unwrap();
                st.gating.push_back(Gating {
                    receipt,
                    unit: Unit::new("u", Vec::new()),
                    identity: identity(&core, offset),
                });
            }
        }
        let gate = super::gate::Gate::new(Arc::clone(&core), producer, config.delivery_timeout)
            .with_step(STEP);
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            gate.pass();
            let _ = tx.send(());
        });
        spate_test::wait_until(Duration::from_secs(10), "the pass begins a read", || {
            core.gate_reads.load(Ordering::SeqCst) >= 1
        });
        if how == "stopped" {
            super::stop(&core);
        } else {
            drop(core.latch("probe"));
        }
        assert!(
            rx.recv_timeout(Duration::from_secs(30)).is_ok(),
            "the pass of a {how} feed did not return"
        );
        assert_eq!(
            core.gate_reads.load(Ordering::SeqCst),
            1,
            "the pass of a {how} feed began a read after the feed ended"
        );
    }
}

/// Pins that the feed answers `hands_back` with `true`, so a stopping
/// worker reads to the stop reserve and startup checks its drain timeout.
#[test]
fn a_kafka_feed_hands_back() {
    assert!(idle_feed().hands_back());
}

/// Pins the close's flush share: half of what the close is given, so the
/// commit and the leave keep the other half.
#[test]
fn the_close_flush_keeps_half_of_its_time_for_the_commit() {
    for (within, flush) in [(0, 0), (500, 250), (1000, 500), (10_000, 5000)] {
        assert_eq!(
            super::consumer::close_flush(Duration::from_millis(within)),
            Duration::from_millis(flush)
        );
    }
}

/// Pins that a gate pass begun after the feed stops receiving takes no
/// delivery: the unit stays at the gate, the pass begins no watermark read,
/// and no attempt record is written for it.
#[test]
fn a_unit_at_the_gate_when_the_stop_begins_gets_no_attempt_record() {
    let feed = idle_feed();
    {
        let mut st = feed.core.lock();
        st.tracker.assign([tp(0)]);
        let receipt = st.tracker.deliver(&tp(0), 0).unwrap();
        st.gating.push_back(Gating {
            receipt,
            unit: Unit::new("u0", b"body".to_vec()),
            identity: identity(&feed.core, 0),
        });
    }
    feed.stop_receiving();
    let gate = super::gate::Gate::new(
        Arc::clone(&feed.core),
        feed.producer.clone(),
        Duration::from_secs(3600),
    )
    .with_step(Duration::from_millis(10));
    assert!(
        returns_within(Duration::from_secs(10), move || gate.pass()),
        "the pass did not return"
    );
    let st = feed.core.lock();
    assert_eq!(st.gating.len(), 1, "the unit left the gate");
    assert_eq!(st.admitting, 0);
    assert_eq!(feed.core.gate_reads.load(Ordering::SeqCst), 0);
}

/// Pins that stopping a latched feed logs no stop line.
#[test]
fn stopping_a_latched_feed_logs_nothing() {
    let feed = idle_feed();
    feed.core.latch("a test latch");
    let lines = spate_test::capture_logs(tracing::Level::INFO, || feed.stop_receiving());
    assert!(
        !lines.iter().any(|l| l.contains("stopped receiving")),
        "{lines:#?}"
    );
}

/// Pins that a gate pass with nothing it may take waits a step before it
/// returns, so the gate thread does not spin: on an idle feed, and on a
/// stopping feed with a delivery at the gate. A step of an hour that has
/// not ended after 500 ms is the property.
#[test]
fn a_gate_pass_with_nothing_to_take_waits_a_step() {
    for stopping in [false, true] {
        let feed = idle_feed();
        if stopping {
            {
                let mut st = feed.core.lock();
                st.tracker.assign([tp(0)]);
                let receipt = st.tracker.deliver(&tp(0), 0).unwrap();
                st.gating.push_back(Gating {
                    receipt,
                    unit: Unit::new("u0", b"body".to_vec()),
                    identity: identity(&feed.core, 0),
                });
            }
            feed.stop_receiving();
        }
        let gate = super::gate::Gate::new(
            Arc::clone(&feed.core),
            feed.producer.clone(),
            Duration::from_secs(3600),
        )
        .with_step(Duration::from_secs(3600));
        assert!(
            !returns_within(Duration::from_millis(500), move || gate.pass()),
            "a pass with nothing to take returned at once (stopping: {stopping})"
        );
        super::stop(&feed.core);
    }
}

/// Pins that the stop line counts every unit the feed holds for the
/// reader: buffered, at the gate and with its attempt write in flight.
#[test]
fn the_stop_line_counts_every_unit_held_for_the_reader() {
    let feed = idle_feed();
    {
        let mut st = feed.core.lock();
        st.tracker.assign([tp(0)]);
        let buffered = st.tracker.deliver(&tp(0), 1).unwrap();
        let unit = ready(&feed.core, buffered, 1);
        st.ready.push_back(unit);
        let at_gate = st.tracker.deliver(&tp(0), 2).unwrap();
        st.gating.push_back(Gating {
            receipt: at_gate,
            unit: Unit::new("u2", Vec::new()),
            identity: identity(&feed.core, 2),
        });
    }
    // The gate has taken offset 3 and its attempt write is in flight.
    let _admitting = gated(&feed.core, 3);
    let lines = spate_test::capture_logs(tracing::Level::INFO, || feed.stop_receiving());
    let stop = lines
        .iter()
        .find(|l| l.contains("the Kafka feed stopped receiving"))
        .unwrap_or_else(|| panic!("no stop line: {lines:#?}"));
    assert!(stop.contains("buffered=3"), "{stop}");
}

/// Pins that a latch rejects a close the consumer thread has not taken:
/// with no consumer thread, only the latch can settle it.
#[test]
fn a_latch_settles_a_close_the_consumer_thread_has_not_taken() {
    let feed = idle_feed();
    let mut closing = feed.close(Duration::from_secs(10)).expect("a close");
    assert!(closing.try_outcome().is_none(), "settled before the latch");
    feed.core.latch("a test latch");
    let err = closing.try_outcome().expect("settled").unwrap_err();
    assert_eq!(err.kind, FeedErrorKind::Fatal);
    assert_eq!(err.reason, "a test latch");
}

/// Pins that `close` returns at once while no broker answers: it only
/// stores the close and stops the feed's threads.
#[test]
fn a_close_returns_while_the_broker_is_down() {
    let feed = idle_feed();
    assert!(
        returns_within(Duration::from_secs(10), move || {
            drop(feed.close(Duration::from_secs(1)));
        }),
        "the close did not return"
    );
}

mod cluster {
    //! The feed against rdkafka's in-process MockCluster, where a test reads
    //! the feed's buffer and ownership directly.

    use super::super::attempt::{self, Follower};
    use super::super::config::KafkaFeedConfig;
    use super::super::consumer::{self, Driver, POLL_BATCH};
    use super::super::gate::Gate;
    use super::super::inflight::Tp;
    use super::super::producer::FeedProducerContext;
    use super::super::startup::{CONNECT_BOUND, Scope};
    use super::super::write::Producer;
    use super::super::{Core, Gating, KafkaFeed, READY_CAPACITY};
    use super::{TWO, returns_within};
    use crate::context::Intent;
    use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer as _};
    use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
    use rdkafka::{ClientConfig, Offset, TopicPartitionList};
    use spate_core::config::FeedSection;
    use spate_core::feed::{Feed, FeedErrorKind, Unit};
    use spate_test::wait_until;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::thread::JoinHandle;
    use std::time::Duration;

    const DEADLINE: Duration = Duration::from_secs(60);

    /// The gate's step in the tests that run it beside the consumer.
    const STEP: Duration = Duration::from_millis(10);

    const ATTEMPTS: &str = "units-attempts";

    /// Units produced to each of the work topic's two partitions, more than
    /// the buffer holds.
    const PER_PARTITION: usize = 2 * READY_CAPACITY;

    pub(super) fn section(brokers: &str, group: &str) -> FeedSection {
        section_with(brokers, group, "")
    }

    /// [`section`] with `rdkafka` appended to its passthrough.
    fn section_with(brokers: &str, group: &str, rdkafka: &str) -> FeedSection {
        serde_yaml::from_str(&format!(
            "kafka:\n  brokers: \"{brokers}\"\n  group_id: {group}\n  topic: units\n  \
             retry_topic: units-retry\n  dead_letter_topic: units-dead\n  \
             attempt_topic: {ATTEMPTS}\n  statistics_interval: 0s\n  rdkafka:\n    \
             session.timeout.ms: \"6000\"\n    heartbeat.interval.ms: \"2000\"\n{rdkafka}"
        ))
        .unwrap()
    }

    fn connect(brokers: &str, group: &str, io: &tokio::runtime::Runtime) -> KafkaFeed {
        // The MockCluster answers no request for a topic's configuration.
        KafkaFeed::connect_with(
            &section(brokers, group),
            io.handle(),
            Scope::Existence,
            CONNECT_BOUND,
        )
        .unwrap()
    }

    /// The work-topic partitions `feed` owns.
    fn owned(feed: &KafkaFeed) -> usize {
        feed.core
            .lock()
            .tracker
            .owned()
            .filter(|tp| &*tp.topic == "units")
            .count()
    }

    fn buffered(feed: &KafkaFeed) -> usize {
        feed.core.lock().ready.len()
    }

    /// Pins that a partition paused on a full buffer is fetched again after
    /// a rebalance hands it back. Worker A fills its buffer and pauses both
    /// partitions; worker B joins, and A must read the partition it is then
    /// given; B leaves, and A reads every unit of both partitions.
    #[test]
    fn a_paused_partition_resumes_after_it_is_reassigned() {
        let cluster = MockCluster::new(1).expect("mock cluster");
        cluster.create_topic("units", 2, 1).unwrap();
        cluster.create_topic("units-retry", 1, 1).unwrap();
        cluster.create_topic("units-dead", 1, 1).unwrap();
        cluster.create_topic(ATTEMPTS, 2, 1).unwrap();
        let brokers = cluster.bootstrap_servers();
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .unwrap();
        let mut ids = BTreeSet::new();
        for p in 0..2 {
            for i in 0..PER_PARTITION {
                let id = format!("p{p}-{i}");
                producer
                    .send(
                        BaseRecord::to("units")
                            .partition(p)
                            .key(id.as_bytes())
                            .payload(b"body"),
                    )
                    .map_err(|(e, _)| e)
                    .unwrap();
                ids.insert(id);
            }
        }
        producer.flush(Duration::from_secs(30)).unwrap();
        let group = spate_test::unique_name("paused");
        let io = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let a = connect(&brokers, &group, &io);
        wait_until(
            DEADLINE,
            "A owns both partitions with a full buffer",
            || owned(&a) == 2 && buffered(&a) >= READY_CAPACITY,
        );
        let b = connect(&brokers, &group, &io);
        wait_until(DEADLINE, "the members split the partitions", || {
            owned(&a) == 1 && owned(&b) == 1
        });
        wait_until(DEADLINE, "A reads the partition it was given", || {
            buffered(&a) > 0
        });
        drop(b);
        wait_until(DEADLINE, "A owns both partitions again", || owned(&a) == 2);

        let mut read = BTreeSet::new();
        wait_until(DEADLINE, "A reads every unit", || {
            for delivery in a.receive(64).unwrap_or_default() {
                read.insert(delivery.unit.id);
                a.acknowledge(delivery.receipt).unwrap();
            }
            read == ids
        });
    }

    /// A cluster with one partition each of the work, retry and
    /// dead-letter topics, two of the attempt topic, and `n` messages keyed
    /// `u0`, `u1`, … on the work topic.
    fn cluster_with(n: usize) -> MockCluster<'static, rdkafka::producer::DefaultProducerContext> {
        let cluster = MockCluster::new(1).expect("mock cluster");
        for topic in ["units", "units-retry", "units-dead"] {
            cluster.create_topic(topic, 1, 1).unwrap();
        }
        cluster.create_topic(ATTEMPTS, 2, 1).unwrap();
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", cluster.bootstrap_servers())
            .create()
            .unwrap();
        for i in 0..n {
            let id = format!("u{i}");
            producer
                .send(BaseRecord::to("units").key(id.as_bytes()).payload(b"body"))
                .map_err(|(e, _)| e)
                .unwrap();
        }
        producer.flush(Duration::from_secs(30)).unwrap();
        cluster
    }

    /// The feed's parts for `group` on a two-partition attempt topic, with
    /// the consumer subscribed and no thread started.
    pub(super) fn parts_for(
        brokers: &str,
        group: &str,
    ) -> (KafkaFeedConfig, Arc<Core>, Producer, Driver) {
        let config = KafkaFeedConfig::from_section(&section(brokers, group)).unwrap();
        let (core, producer, d) = super::super::parts(&config, TWO).unwrap();
        (config, core, producer, d)
    }

    /// The feed's follower and gate, each on its own thread as `connect`
    /// starts them. Stopped and joined by [`Admission::stop`], or on drop.
    pub(super) struct Admission {
        core: Arc<Core>,
        threads: Vec<JoinHandle<()>>,
    }

    /// Start the follower and the gate, the gate at `step`, beside a
    /// consumer the test drives.
    pub(super) fn admission(
        config: &KafkaFeedConfig,
        core: &Arc<Core>,
        producer: &Producer,
        step: Duration,
    ) -> Admission {
        let follower = Follower::new(config, Arc::clone(core))
            .unwrap()
            .spawn()
            .unwrap();
        let gate = Gate::new(Arc::clone(core), producer.clone(), config.delivery_timeout)
            .with_step(step)
            .spawn()
            .unwrap();
        Admission {
            core: Arc::clone(core),
            threads: vec![follower, gate],
        }
    }

    impl Admission {
        /// Stop the feed as dropping it does, and join both threads.
        pub(super) fn stop(mut self) {
            self.join();
        }

        fn join(&mut self) {
            super::super::stop(&self.core);
            for thread in self.threads.drain(..) {
                thread.join().expect("the thread ends");
            }
        }
    }

    impl Drop for Admission {
        fn drop(&mut self) {
            if !self.threads.is_empty() {
                self.join();
            }
        }
    }

    /// Poll and serve rebalances until `done` holds.
    pub(super) fn drive(d: &mut Driver, what: &str, mut done: impl FnMut(&Driver) -> bool) {
        wait_until(DEADLINE, what, || {
            d.poll();
            d.serve_intents();
            done(d)
        });
    }

    pub(super) fn owns(d: &Driver) -> usize {
        d.core.lock().tracker.owned().count()
    }

    /// Acknowledge the oldest buffered unit.
    pub(super) fn acknowledge_first(d: &Driver) {
        let mut st = d.core.lock();
        let ready = st.ready.pop_front().expect("a buffered unit");
        assert!(st.tracker.acknowledge(ready.receipt));
    }

    /// The offset `group` has committed for the work topic's partition.
    pub(super) fn committed(brokers: &str, group: &str) -> Option<i64> {
        let probe: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group)
            .create()
            .unwrap();
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition("units", 0);
        let committed = probe
            .committed_offsets(tpl, Duration::from_secs(10))
            .unwrap();
        match committed.elements()[0].offset() {
            Offset::Offset(offset) => Some(offset),
            _ => None,
        }
    }

    /// Pins that a panic on the feed's thread while it holds partitions
    /// stops the feed with a Fatal error naming the consumer thread, and
    /// that dropping the feed then returns: the unwind closes the consumer
    /// as a normal stop does.
    #[test]
    fn a_panic_on_the_feed_thread_stops_the_feed_and_its_drop_returns() {
        let cluster = cluster_with(0);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("panic");
        let (_, core, producer, mut d) = parts_for(&brokers, &group);
        drive(&mut d, "the driver owns its partitions", |d| owns(d) == 2);
        let context = Arc::clone(d.consumer.context());
        let thread = std::thread::spawn(move || consumer::run(d));
        // A poisoned intent lock makes the thread's next look at it panic.
        let _ = std::thread::spawn(move || {
            let _held = context.intents.lock().unwrap();
            panic!("poisoning the intent lock");
        })
        .join();
        let feed = KafkaFeed {
            core,
            producer,
            thread: Some(thread),
            gate: None,
            follower: None,
            depth: None,
        };
        let mut reason = None;
        wait_until(DEADLINE, "the feed stops", || match feed.receive(1) {
            Err(e) if e.kind == FeedErrorKind::Fatal => {
                reason = Some(e.reason);
                true
            }
            _ => false,
        });
        let dropped = returns_within(Duration::from_secs(30), move || drop(feed));
        if !dropped {
            // Its clients are still connected; dropping it would wait on them.
            std::mem::forget(cluster);
        }
        assert!(dropped, "dropping the feed did not return");
        assert_eq!(
            reason.as_deref(),
            Some("the Kafka feed's consumer thread panicked")
        );
    }

    /// Pins that stopping the feed waits for a tombstone in flight and
    /// commits the offset it finishes. The broker's round trip is slowed so
    /// the tombstone is confirmed hundreds of milliseconds after it is sent,
    /// well inside the 1 s wait; a stop that commits at once commits the
    /// unit's own offset. No test bounds the wait from above.
    #[test]
    fn stopping_commits_what_finished() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("stop-commit");
        let (config, core, producer, mut d) = parts_for(&brokers, &group);
        let feed = KafkaFeed {
            core: Arc::clone(&core),
            producer: producer.clone(),
            thread: None,
            gate: None,
            follower: None,
            depth: None,
        };
        let admission = admission(&config, &core, &producer, STEP);
        drive(&mut d, "the unit is buffered", |d| {
            !d.core.lock().ready.is_empty()
        });
        let delivery = feed.receive(1).unwrap().pop().unwrap();
        admission.stop();
        cluster
            .broker_round_trip_time(1, Duration::from_millis(300))
            .unwrap();
        feed.acknowledge(delivery.receipt).unwrap();
        consumer::run(d);
        assert_eq!(committed(&brokers, &group), Some(1));
    }

    /// Pins that a revocation commits the offsets its partitions finished,
    /// with no periodic commit run. The revocation is queued as the
    /// consumer context records one: the mock refuses every commit while a
    /// second member's join is in progress.
    #[test]
    fn a_revocation_commits_what_finished() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("revoke-commit");
        let (config, core, producer, mut d) = parts_for(&brokers, &group);
        let admission = admission(&config, &core, &producer, STEP);
        drive(&mut d, "the unit is buffered", |d| {
            !d.core.lock().ready.is_empty()
        });
        acknowledge_first(&d);
        let assigned = d.consumer.assignment().unwrap();
        d.consumer
            .context()
            .intents
            .lock()
            .unwrap()
            .push_back(Intent::Revoke(assigned));
        d.serve_intents();
        assert_eq!(owns(&d), 0);
        assert_eq!(committed(&brokers, &group), Some(1));
        admission.stop();
    }

    /// Pins that a rebalance error resumes the partitions the thread paused:
    /// once the partitions are assigned again, they are fetched.
    #[test]
    fn a_rebalance_error_resumes_what_was_paused() {
        let cluster = cluster_with(2 * READY_CAPACITY);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("lost-paused");
        let (config, core, producer, mut d) = parts_for(&brokers, &group);
        let admission = admission(&config, &core, &producer, STEP);
        wait_until(DEADLINE, "the buffer fills and the thread pauses", || {
            d.poll();
            d.serve_intents();
            d.backpressure();
            !d.paused.is_empty()
        });
        let assigned = d.consumer.assignment().unwrap();
        // The error arm of the rebalance callback, as librdkafka calls it.
        d.consumer.context().rebalance(
            &d.consumer,
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
            &mut TopicPartitionList::new(),
        );
        d.consumer
            .context()
            .intents
            .lock()
            .unwrap()
            .push_back(Intent::Assign(assigned));
        d.serve_intents();
        {
            let st = d.core.lock();
            assert!(st.ready.is_empty(), "the buffer was dropped");
            assert!(st.gating.is_empty(), "the gate's queue was dropped");
        }
        // Attempt writes still in flight count against the buffer until they
        // settle, so the new assignment can start paused, as the consumer
        // thread's own loop resumes it.
        wait_until(DEADLINE, "the reassigned partitions are fetched", || {
            d.poll();
            d.serve_intents();
            d.backpressure();
            !d.core.lock().ready.is_empty()
        });
        admission.stop();
    }

    /// Pins that a thread started on a latched feed ends without committing
    /// an offset finished before the latch.
    #[test]
    fn a_latched_feed_commits_nothing_more() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("latched");
        let (config, core, producer, mut d) = parts_for(&brokers, &group);
        let admission = admission(&config, &core, &producer, STEP);
        drive(&mut d, "the unit is buffered", |d| {
            !d.core.lock().ready.is_empty()
        });
        acknowledge_first(&d);
        d.core.latch("a test latch");
        let thread = std::thread::spawn(move || consumer::run(d));
        let mut at = None;
        wait_until(DEADLINE, "the thread ends or commits", || {
            at = committed(&brokers, &group);
            at.is_some() || thread.is_finished()
        });
        assert_eq!(at, None, "the latched feed committed");
        thread.join().unwrap();
        assert_eq!(committed(&brokers, &group), None);
        admission.stop();
    }

    /// Pins that a periodic commit made after a latch commits nothing.
    #[test]
    fn a_periodic_commit_after_a_latch_commits_nothing() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("latched-periodic");
        let (config, core, producer, mut d) = parts_for(&brokers, &group);
        let admission = admission(&config, &core, &producer, STEP);
        drive(&mut d, "the unit is buffered", |d| {
            !d.core.lock().ready.is_empty()
        });
        acknowledge_first(&d);
        d.core.latch("a test latch");
        d.commit_finished("periodic");
        assert_eq!(
            committed(&brokers, &group),
            None,
            "the latched feed committed"
        );
        admission.stop();
    }

    /// Pins that a revocation served after a latch commits nothing.
    #[test]
    fn a_latched_feed_commits_nothing_on_revocation() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("latched-revoke");
        let (config, core, producer, mut d) = parts_for(&brokers, &group);
        let admission = admission(&config, &core, &producer, STEP);
        drive(&mut d, "the unit is buffered", |d| {
            !d.core.lock().ready.is_empty()
        });
        acknowledge_first(&d);
        d.core.latch("a test latch");
        let assigned = d.consumer.assignment().unwrap();
        d.consumer
            .context()
            .intents
            .lock()
            .unwrap()
            .push_back(Intent::Revoke(assigned));
        d.serve_intents();
        assert_eq!(owns(&d), 0);
        assert_eq!(committed(&brokers, &group), None);
        admission.stop();
    }

    /// Pins that an offset read again while its earlier copy is still
    /// buffered replaces that copy: the buffer holds the new delivery alone,
    /// and acknowledging it finishes the offset.
    #[test]
    fn a_redelivered_offset_replaces_its_buffered_copy() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("redelivered");
        let (config, core, producer, mut d) = parts_for(&brokers, &group);
        let admission = admission(&config, &core, &producer, STEP);
        drive(&mut d, "the unit is buffered", |d| {
            !d.core.lock().ready.is_empty()
        });
        let first = d.core.lock().ready[0].receipt;
        d.consumer
            .seek("units", 0, Offset::Offset(0), Duration::from_secs(10))
            .unwrap();
        drive(&mut d, "the unit is read again", |d| {
            d.core.lock().ready.iter().any(|r| r.receipt != first)
        });
        {
            let mut st = d.core.lock();
            assert_eq!(st.ready.len(), 1, "the earlier copy is still buffered");
            let again = st.ready.pop_front().unwrap();
            assert!(st.tracker.acknowledge(again.receipt));
            assert_eq!(
                st.tracker.take_commits(),
                vec![(super::tp(0), 1)],
                "the offset did not finish with its new copy"
            );
        }
        admission.stop();
    }

    /// Pins that a commit the broker refuses is made again: the refused
    /// points go to the next commit, which lands them.
    #[test]
    fn a_commit_the_broker_refuses_is_made_again() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("refused-commit");
        let (config, core, producer, mut d) = parts_for(&brokers, &group);
        let admission = admission(&config, &core, &producer, STEP);
        drive(&mut d, "the unit is buffered", |d| {
            !d.core.lock().ready.is_empty()
        });
        acknowledge_first(&d);
        // A code librdkafka does not retry on its own.
        cluster.request_errors(
            RDKafkaApiKey::OffsetCommit,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE],
        );
        d.commit_finished("periodic");
        assert_eq!(
            committed(&brokers, &group),
            None,
            "the refused commit landed"
        );
        d.commit_finished("periodic");
        assert_eq!(committed(&brokers, &group), Some(1));
        admission.stop();
    }

    /// Pins that a message read for a partition the tracker gave up is
    /// dropped, and reaches neither the gate nor the buffer.
    #[test]
    fn a_message_for_a_partition_given_up_is_dropped() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("given-up");
        let (_, _, _, mut d) = parts_for(&brokers, &group);
        drive(&mut d, "the driver owns its partitions", |d| owns(d) == 2);
        d.core.lock().tracker.unassign_all();
        wait_until(DEADLINE, "the message is read", || {
            d.poll();
            d.consumer
                .position()
                .unwrap()
                .elements()
                .iter()
                .any(|e| e.topic() == "units" && e.offset() == Offset::Offset(1))
        });
        let st = d.core.lock();
        assert!(st.ready.is_empty());
        assert!(st.gating.is_empty());
    }

    /// Pins that a revocation drops the deliveries waiting at the gate, with
    /// no gate running to take them.
    #[test]
    fn a_revocation_drops_the_units_waiting_at_the_gate() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("revoke-gating");
        let (_, _, _, mut d) = parts_for(&brokers, &group);
        drive(&mut d, "the unit waits at the gate", |d| {
            !d.core.lock().gating.is_empty()
        });
        let assigned = d.consumer.assignment().unwrap();
        d.consumer
            .context()
            .intents
            .lock()
            .unwrap()
            .push_back(Intent::Revoke(assigned));
        d.serve_intents();
        assert!(d.core.lock().gating.is_empty());
        assert_eq!(committed(&brokers, &group), Some(0), "the unit is open");
    }

    /// Pins that units waiting at the gate count against the buffer: with
    /// no gate to take them, a full gate queue pauses the partitions.
    #[test]
    fn a_full_gate_queue_pauses_the_partitions() {
        let cluster = cluster_with(2 * READY_CAPACITY);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("gating-full");
        let (_, _, _, mut d) = parts_for(&brokers, &group);
        wait_until(
            DEADLINE,
            "the gate's queue fills and the thread pauses",
            || {
                d.poll();
                d.serve_intents();
                d.backpressure();
                !d.paused.is_empty()
            },
        );
        assert!(d.core.lock().gating.len() >= READY_CAPACITY);
    }

    /// Pins that units whose attempt write is in flight count against the
    /// buffer: with every produce answered `NOT_ENOUGH_REPLICAS`, which
    /// librdkafka retries itself, the gate takes every unit and their
    /// attempt writes stay unconfirmed, and the partitions stay paused. One
    /// poll adds at most a batch, so the count stops short of the buffer
    /// plus a batch.
    #[test]
    fn a_stalled_attempt_write_pauses_the_partitions() {
        let cluster = cluster_with(READY_CAPACITY * 3);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("stalled-attempt");
        cluster.request_errors(
            RDKafkaApiKey::Produce,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS; 100],
        );
        let (config, core, producer, mut d) = parts_for(&brokers, &group);
        let admission = admission(&config, &core, &producer, STEP);
        wait_until(
            DEADLINE,
            "the gate takes every unit read and the thread pauses",
            || {
                d.poll();
                d.serve_intents();
                d.backpressure();
                let st = d.core.lock();
                st.gating.is_empty() && st.admitting >= READY_CAPACITY && !d.paused.is_empty()
            },
        );
        d.backpressure();
        assert!(!d.paused.is_empty(), "the partitions resumed");
        let admitting = d.core.lock().admitting;
        assert!(admitting < READY_CAPACITY + POLL_BATCH, "{admitting}");
        admission.stop();
    }

    /// Pins that a unit reaches an idle gate at once: with a step of an
    /// hour, the gate waits for a delivery only on the consumer's wake, and
    /// the feed's drop wakes it to stop.
    #[test]
    fn a_unit_reaches_an_idle_gate_without_waiting_for_its_step() {
        let cluster = cluster_with(0);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("idle-gate");
        let (config, core, producer, d) = parts_for(&brokers, &group);
        let follower = Follower::new(&config, Arc::clone(&core))
            .unwrap()
            .spawn()
            .unwrap();
        let gate = Gate::new(Arc::clone(&core), producer.clone(), config.delivery_timeout)
            .with_step(Duration::from_secs(3600))
            .spawn()
            .unwrap();
        let feed = KafkaFeed {
            core,
            producer,
            thread: Some(std::thread::spawn(move || consumer::run(d))),
            gate: Some(gate),
            follower: Some(follower),
            depth: None,
        };
        wait_until(DEADLINE, "the consumer owns its partitions", || {
            owned(&feed) == 1
        });
        let units: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .unwrap();
        units
            .send(BaseRecord::to("units").key("late").payload(b"body"))
            .map_err(|(e, _)| e)
            .unwrap();
        units.flush(Duration::from_secs(30)).unwrap();
        wait_until(DEADLINE, "the unit is buffered", || buffered(&feed) == 1);
        let dropped = returns_within(Duration::from_secs(10), move || drop(feed));
        if !dropped {
            std::mem::forget(cluster);
        }
        assert!(dropped, "dropping the feed did not return");
    }

    /// Pins that a gate pass whose follower does not catch up within the
    /// delivery timeout ends, with its unit back at the gate and its share
    /// of `admitting` given back: with no follower, the gate waits on a
    /// record it never sees, in steps of 10 ms, for 200 ms.
    #[test]
    fn a_pass_the_follower_does_not_catch_up_on_ends_at_the_delivery_timeout() {
        let cluster = cluster_with(0);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("gate-timeout");
        let records: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .unwrap();
        for partition in 0..2 {
            records
                .send(
                    BaseRecord::to(ATTEMPTS)
                        .partition(partition)
                        .key("other")
                        .payload(&super::super::attempt::encode(1)),
                )
                .map_err(|(e, _)| e)
                .unwrap();
        }
        records.flush(Duration::from_secs(30)).unwrap();
        let config = KafkaFeedConfig::from_section(&section(&brokers, &group)).unwrap();
        let core = Arc::new(Core::new(&config, TWO));
        let producer: Producer =
            rdkafka::config::FromClientConfigAndContext::from_config_and_context(
                &config.producer_config(false),
                FeedProducerContext::new(Arc::clone(&core)),
            )
            .unwrap();
        {
            let mut st = core.lock();
            st.tracker.assign([super::tp(0)]);
            let receipt = st.tracker.deliver(&super::tp(0), 0).unwrap();
            st.gating.push_back(Gating {
                receipt,
                unit: Unit::new("u", Vec::new()),
                identity: super::identity(&core, 0),
            });
        }
        // A new producer's first watermark read can fail before it has met a
        // broker, which would end the pass before the timeout.
        producer
            .client()
            .fetch_metadata(Some(ATTEMPTS), Duration::from_secs(10))
            .unwrap();
        let gate = Gate::new(
            Arc::clone(&core),
            Producer::clone(&producer),
            Duration::from_millis(200),
        )
        .with_step(STEP);
        let ended = returns_within(Duration::from_secs(10), move || gate.pass());
        if !ended {
            std::mem::forget(cluster);
        }
        assert!(ended, "the pass did not end at the delivery timeout");
        let st = core.lock();
        assert_eq!(st.gating.len(), 1);
        assert_eq!(st.admitting, 0);
        assert!(st.fatal.is_none());
    }

    /// Pins that a gate pass whose watermark read the broker refuses for
    /// authorization stops the feed, naming the read.
    #[test]
    fn a_refused_watermark_read_in_a_pass_stops_the_feed() {
        let cluster = cluster_with(0);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("refused-watermark");
        cluster.request_errors(
            RDKafkaApiKey::ListOffsets,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED; 50],
        );
        let config = KafkaFeedConfig::from_section(&section(&brokers, &group)).unwrap();
        let core = Arc::new(Core::new(&config, TWO));
        let producer: Producer =
            rdkafka::config::FromClientConfigAndContext::from_config_and_context(
                &config.producer_config(false),
                FeedProducerContext::new(Arc::clone(&core)),
            )
            .unwrap();
        {
            let mut st = core.lock();
            st.tracker.assign([super::tp(0)]);
            let receipt = st.tracker.deliver(&super::tp(0), 0).unwrap();
            st.gating.push_back(Gating {
                receipt,
                unit: Unit::new("u", Vec::new()),
                identity: super::identity(&core, 0),
            });
        }
        // A new producer's first watermark read can fail before it has met a
        // broker, which is retryable and would end the pass without a latch.
        producer
            .client()
            .fetch_metadata(Some(ATTEMPTS), Duration::from_secs(10))
            .unwrap();
        let gate =
            Gate::new(Arc::clone(&core), producer, Duration::from_secs(3600)).with_step(STEP * 100);
        gate.pass();
        let reason = core.lock().fatal.clone().expect("the feed stopped").reason;
        assert!(
            reason.contains("reading the watermarks of `units-attempts`"),
            "{reason}"
        );
    }

    /// Pins that a pass whose watermark read the broker refuses admits
    /// nothing, including the units of a partition it read before the
    /// refusal, whose records the ledger holds: both units stay at the gate
    /// and the feed stops.
    #[test]
    fn a_refused_watermark_read_admits_nothing_from_its_pass() {
        let cluster = cluster_with(0);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("refused-pass");
        let records: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .unwrap();
        records
            .send(
                BaseRecord::to(ATTEMPTS)
                    .partition(0)
                    .key("other")
                    .payload(&super::super::attempt::encode(1)),
            )
            .map_err(|(e, _)| e)
            .unwrap();
        records.flush(Duration::from_secs(30)).unwrap();
        let config = KafkaFeedConfig::from_section(&section(&brokers, &group)).unwrap();
        let core = Arc::new(Core::new(&config, TWO));
        attempt::lock(&core.ledger).apply(0, 0, b"other", attempt::Entry::Count(1));
        let producer: Producer =
            rdkafka::config::FromClientConfigAndContext::from_config_and_context(
                &config.producer_config(false),
                FeedProducerContext::new(Arc::clone(&core)),
            )
            .unwrap();
        let on = |partition| {
            (0..)
                .find(|&o| super::attempt_partition(&core, &super::identity(&core, o)) == partition)
                .unwrap()
        };
        {
            let mut st = core.lock();
            st.tracker.assign([super::tp(0)]);
            for offset in [on(0), on(1)] {
                let receipt = st.tracker.deliver(&super::tp(0), offset).unwrap();
                st.gating.push_back(Gating {
                    receipt,
                    unit: Unit::new(format!("u{offset}"), Vec::new()),
                    identity: super::identity(&core, offset),
                });
            }
        }
        producer
            .client()
            .fetch_metadata(Some(ATTEMPTS), Duration::from_secs(10))
            .unwrap();
        // Partition 0 holds a record the ledger has read, so its read takes
        // its two requests and it is caught up; partition 1's is refused.
        let mut errors = vec![RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR; 2];
        errors.extend([RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED; 50]);
        cluster.request_errors(RDKafkaApiKey::ListOffsets, &errors);
        let gate =
            Gate::new(Arc::clone(&core), producer, Duration::from_secs(3600)).with_step(STEP * 100);
        gate.pass();
        let st = core.lock();
        let reason = &st.fatal.as_ref().expect("the feed stopped").reason;
        assert!(reason.contains("partition 1"), "{reason}");
        assert_eq!(st.gating.len(), 2, "a unit was admitted");
        assert_eq!(st.admitting, 0);
    }

    /// Pins that an attempt partition with no leader holds back only its own
    /// units: the unit whose attempt partition answers is admitted, and the
    /// unit on the leaderless partition waits at the gate. The attempt topic
    /// is empty, so the pass needs no follower; the mock broker crashes when
    /// a follower fetches a partition with no leader.
    #[test]
    fn a_leaderless_attempt_partition_holds_back_only_its_own_units() {
        let cluster = cluster_with(0);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("leaderless");
        let config = KafkaFeedConfig::from_section(&section(&brokers, &group)).unwrap();
        let core = Arc::new(Core::new(&config, TWO));
        let producer: Producer =
            rdkafka::config::FromClientConfigAndContext::from_config_and_context(
                &config.producer_config(false),
                FeedProducerContext::new(Arc::clone(&core)),
            )
            .unwrap();
        let on = |partition| {
            (0..)
                .find(|&o| super::attempt_partition(&core, &super::identity(&core, o)) == partition)
                .unwrap()
        };
        let (on0, on1) = (on(0), on(1));
        producer
            .client()
            .fetch_metadata(Some(ATTEMPTS), Duration::from_secs(10))
            .unwrap();
        cluster.partition_leader(ATTEMPTS, 1, None).unwrap();
        wait_until(DEADLINE, "partition 1 reports no leader", || {
            producer
                .client()
                .fetch_watermarks(ATTEMPTS, 1, Duration::from_millis(500))
                .is_err()
        });
        {
            let mut st = core.lock();
            st.tracker.assign([super::tp(0)]);
            for offset in [on0, on1] {
                let receipt = st.tracker.deliver(&super::tp(0), offset).unwrap();
                st.gating.push_back(Gating {
                    receipt,
                    unit: Unit::new(format!("u{offset}"), Vec::new()),
                    identity: super::identity(&core, offset),
                });
            }
        }
        let gate = Gate::new(Arc::clone(&core), producer, Duration::from_secs(3600))
            .with_step(Duration::from_millis(500));
        // Each wait runs a pass, as the gate thread would.
        wait_until(DEADLINE, "the unit on partition 0 is buffered", || {
            gate.pass();
            core.lock().ready.len() == 1
        });
        let st = core.lock();
        assert_eq!(st.ready[0].unit.id, format!("u{on0}"));
        let waiting: Vec<_> = st.gating.iter().map(|g| g.unit.id.clone()).collect();
        assert_eq!(waiting, [format!("u{on1}")]);
        assert_eq!(st.admitting, 0);
        assert!(st.fatal.is_none());
    }

    /// Pins that a pass which reads every watermark it needs admits its
    /// units and returns without waiting a step: with a step of an hour, a
    /// pass over one unit on a partition with a leader returns, and the unit
    /// is buffered for the reader.
    #[test]
    fn a_pass_that_reads_every_watermark_returns_without_waiting() {
        let cluster = cluster_with(0);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("gate-no-rest");
        let config = KafkaFeedConfig::from_section(&section(&brokers, &group)).unwrap();
        let core = Arc::new(Core::new(&config, TWO));
        let producer: Producer =
            rdkafka::config::FromClientConfigAndContext::from_config_and_context(
                &config.producer_config(false),
                FeedProducerContext::new(Arc::clone(&core)),
            )
            .unwrap();
        let offset = (0..)
            .find(|&o| super::attempt_partition(&core, &super::identity(&core, o)) == 0)
            .unwrap();
        wait_until(DEADLINE, "partition 0 reports its watermarks", || {
            producer
                .client()
                .fetch_watermarks(ATTEMPTS, 0, Duration::from_millis(500))
                .is_ok()
        });
        {
            let mut st = core.lock();
            st.tracker.assign([super::tp(0)]);
            let receipt = st.tracker.deliver(&super::tp(0), offset).unwrap();
            st.gating.push_back(Gating {
                receipt,
                unit: Unit::new("u", Vec::new()),
                identity: super::identity(&core, offset),
            });
        }
        let gate = Gate::new(Arc::clone(&core), producer, Duration::from_secs(3600))
            .with_step(Duration::from_secs(3600));
        assert!(
            returns_within(Duration::from_secs(30), move || gate.pass()),
            "a pass that read every watermark waited a step"
        );
        wait_until(DEADLINE, "the unit is buffered", || {
            core.lock().ready.len() == 1
        });
    }

    /// Pins that a watermark read whose low-watermark request was refused
    /// does not count its partition as caught up: librdkafka reports such a
    /// read as two equal watermarks, as it does an empty partition. With
    /// `NOT_LEADER_FOR_PARTITION` on the first ListOffsets request and a
    /// record no follower has read, the unit stays at the gate.
    #[test]
    fn a_refused_low_watermark_request_keeps_the_unit_at_the_gate() {
        let cluster = cluster_with(0);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("lwm");
        let records: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .unwrap();
        for partition in 0..2 {
            records
                .send(
                    BaseRecord::to(ATTEMPTS)
                        .partition(partition)
                        .key("other")
                        .payload(&super::super::attempt::encode(1)),
                )
                .map_err(|(e, _)| e)
                .unwrap();
        }
        records.flush(Duration::from_secs(30)).unwrap();
        let config = KafkaFeedConfig::from_section(&section(&brokers, &group)).unwrap();
        let core = Arc::new(Core::new(&config, TWO));
        let producer: Producer =
            rdkafka::config::FromClientConfigAndContext::from_config_and_context(
                &config.producer_config(false),
                FeedProducerContext::new(Arc::clone(&core)),
            )
            .unwrap();
        {
            let mut st = core.lock();
            st.tracker.assign([super::tp(0)]);
            let receipt = st.tracker.deliver(&super::tp(0), 0).unwrap();
            st.gating.push_back(Gating {
                receipt,
                unit: Unit::new("u", Vec::new()),
                identity: super::identity(&core, 0),
            });
        }
        producer
            .client()
            .fetch_metadata(Some(ATTEMPTS), Duration::from_secs(10))
            .unwrap();
        let gate = Gate::new(
            Arc::clone(&core),
            Producer::clone(&producer),
            Duration::from_millis(500),
        )
        .with_step(STEP);
        let capture = spate_test::LogCapture::new();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(capture.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        // librdkafka serves the refusal as the read's error when its reply
        // is served alone; only a pass that waits for the follower took the
        // equal watermarks the test is about.
        tracing::subscriber::with_default(subscriber, || {
            wait_until(DEADLINE, "a pass waits for the follower", || {
                cluster.request_errors(
                    RDKafkaApiKey::ListOffsets,
                    &[
                        RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION,
                        RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
                    ],
                );
                gate.pass();
                assert_eq!(core.lock().gating.len(), 1, "admitted with an empty ledger");
                capture
                    .lines()
                    .iter()
                    .any(|l| l.contains("did not catch up"))
            });
        });
    }

    /// Pins that the requests reading equal watermarks again report their
    /// own refusal: on an empty attempt partition whose watermark read
    /// succeeds, a refused low-watermark request stops the feed, naming the
    /// read, and the unit stays at the gate.
    #[test]
    fn a_refused_request_reading_equal_watermarks_again_stops_the_feed() {
        let cluster = cluster_with(0);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("refused-reread");
        let config = KafkaFeedConfig::from_section(&section(&brokers, &group)).unwrap();
        let core = Arc::new(Core::new(&config, TWO));
        let producer: Producer =
            rdkafka::config::FromClientConfigAndContext::from_config_and_context(
                &config.producer_config(false),
                FeedProducerContext::new(Arc::clone(&core)),
            )
            .unwrap();
        {
            let mut st = core.lock();
            st.tracker.assign([super::tp(0)]);
            let receipt = st.tracker.deliver(&super::tp(0), 0).unwrap();
            st.gating.push_back(Gating {
                receipt,
                unit: Unit::new("u", Vec::new()),
                identity: super::identity(&core, 0),
            });
        }
        producer
            .client()
            .fetch_metadata(Some(ATTEMPTS), Duration::from_secs(10))
            .unwrap();
        let mut errors = vec![RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR; 2];
        errors.extend([RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED; 50]);
        cluster.request_errors(RDKafkaApiKey::ListOffsets, &errors);
        let gate =
            Gate::new(Arc::clone(&core), producer, Duration::from_secs(3600)).with_step(STEP * 100);
        gate.pass();
        let st = core.lock();
        let reason = &st.fatal.as_ref().expect("the feed stopped").reason;
        assert!(
            reason.contains("reading the watermarks of `units-attempts`"),
            "{reason}"
        );
        assert_eq!(st.gating.len(), 1, "the unit was admitted");
    }

    /// Pins that a pass which reaches its bound while reading watermarks
    /// keeps what it read: with `delivery_timeout` at 500 ms and attempt
    /// partition 1 led by a broker that answers after 5 s, the unit on
    /// partition 0, read first, is admitted, the units on partitions 1 and
    /// 2 wait at the gate, and the warning names partition 2 as not read
    /// and partition 1 as the slowest read. Partition 2 holds a record, so
    /// its watermarks differ and only the check before its first request
    /// keeps it unread.
    #[test]
    fn a_pass_that_reaches_its_bound_while_reading_admits_what_it_read() {
        let cluster = MockCluster::new(2).expect("mock cluster");
        for topic in ["units", "units-retry", "units-dead"] {
            cluster.create_topic(topic, 1, 1).unwrap();
            cluster.partition_leader(topic, 0, Some(1)).unwrap();
        }
        cluster.create_topic(ATTEMPTS, 3, 1).unwrap();
        for (partition, leader) in [(0, 1), (1, 2), (2, 1)] {
            cluster
                .partition_leader(ATTEMPTS, partition, Some(leader))
                .unwrap();
        }
        let brokers = cluster.bootstrap_servers();
        let records: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .unwrap();
        records
            .send(
                BaseRecord::to(ATTEMPTS)
                    .partition(2)
                    .key("other")
                    .payload(&super::super::attempt::encode(1)),
            )
            .map_err(|(e, _)| e)
            .unwrap();
        records.flush(Duration::from_secs(30)).unwrap();
        let group = spate_test::unique_name("gate-bound-reads");
        let section: FeedSection = serde_yaml::from_str(&format!(
            "kafka:\n  brokers: \"{brokers}\"\n  group_id: {group}\n  topic: units\n  \
             retry_topic: units-retry\n  dead_letter_topic: units-dead\n  \
             attempt_topic: {ATTEMPTS}\n  statistics_interval: 0s\n  \
             delivery_timeout: 500ms\n"
        ))
        .unwrap();
        let config = KafkaFeedConfig::from_section(&section).unwrap();
        let core = Arc::new(Core::new(&config, std::num::NonZeroU32::new(3).unwrap()));
        let producer: Producer =
            rdkafka::config::FromClientConfigAndContext::from_config_and_context(
                &config.producer_config(false),
                FeedProducerContext::new(Arc::clone(&core)),
            )
            .unwrap();
        let on = |partition| {
            (0..)
                .find(|&o| super::attempt_partition(&core, &super::identity(&core, o)) == partition)
                .unwrap()
        };
        let offsets = [on(0), on(1), on(2)];
        {
            let mut st = core.lock();
            st.tracker.assign([super::tp(0)]);
            let mut sorted = offsets;
            sorted.sort_unstable();
            for offset in sorted {
                let receipt = st.tracker.deliver(&super::tp(0), offset).unwrap();
                st.gating.push_back(Gating {
                    receipt,
                    unit: Unit::new(format!("u{offset}"), Vec::new()),
                    identity: super::identity(&core, offset),
                });
            }
        }
        wait_until(
            DEADLINE,
            "partitions 0 and 2 report their watermarks",
            || {
                [0, 2].into_iter().all(|partition| {
                    producer
                        .client()
                        .fetch_watermarks(ATTEMPTS, partition, Duration::from_millis(500))
                        .is_ok()
                })
            },
        );
        cluster
            .broker_round_trip_time(2, Duration::from_secs(5))
            .unwrap();
        let capture = spate_test::LogCapture::new();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(capture.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        let gate = Gate::new(Arc::clone(&core), producer, config.delivery_timeout)
            .with_step(Duration::from_secs(1));
        tracing::subscriber::with_default(subscriber, || gate.pass());
        let waiting: Vec<String> = core
            .lock()
            .gating
            .iter()
            .map(|g| g.unit.id.clone())
            .collect();
        let mut expected = vec![format!("u{}", offsets[1]), format!("u{}", offsets[2])];
        expected.sort_unstable();
        let mut got = waiting.clone();
        got.sort_unstable();
        assert_eq!(got, expected, "the units at the gate");
        wait_until(DEADLINE, "the unit on partition 0 is buffered", || {
            core.lock().ready.len() == 1
        });
        assert_eq!(core.lock().ready[0].unit.id, format!("u{}", offsets[0]));
        let warning = capture.lines().join("\n");
        assert!(
            warning.contains("partitions [2]") && warning.contains("slowest read, of partition 1"),
            "{warning}"
        );
        assert!(!warning.contains("did not catch up"), "{warning}");
    }

    /// Pins that equal watermarks are read again only before the pass's
    /// bound: with `delivery_timeout` at 200 ms and attempt partition 0,
    /// empty, led by a broker that answers after 300 ms, the watermark read
    /// returns past the bound, the unit waits at the gate, and the warning
    /// names partition 0 as not read.
    #[test]
    fn equal_watermarks_read_past_the_bound_are_not_read_again() {
        let cluster = MockCluster::new(2).expect("mock cluster");
        for topic in ["units", "units-retry", "units-dead"] {
            cluster.create_topic(topic, 1, 1).unwrap();
            cluster.partition_leader(topic, 0, Some(1)).unwrap();
        }
        cluster.create_topic(ATTEMPTS, 1, 1).unwrap();
        cluster.partition_leader(ATTEMPTS, 0, Some(2)).unwrap();
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("gate-bound-reread");
        let section: FeedSection = serde_yaml::from_str(&format!(
            "kafka:\n  brokers: \"{brokers}\"\n  group_id: {group}\n  topic: units\n  \
             retry_topic: units-retry\n  dead_letter_topic: units-dead\n  \
             attempt_topic: {ATTEMPTS}\n  statistics_interval: 0s\n  \
             delivery_timeout: 200ms\n"
        ))
        .unwrap();
        let config = KafkaFeedConfig::from_section(&section).unwrap();
        let core = Arc::new(Core::new(&config, std::num::NonZeroU32::MIN));
        let producer: Producer =
            rdkafka::config::FromClientConfigAndContext::from_config_and_context(
                &config.producer_config(false),
                FeedProducerContext::new(Arc::clone(&core)),
            )
            .unwrap();
        {
            let mut st = core.lock();
            st.tracker.assign([super::tp(0)]);
            let receipt = st.tracker.deliver(&super::tp(0), 0).unwrap();
            st.gating.push_back(Gating {
                receipt,
                unit: Unit::new("u", Vec::new()),
                identity: super::identity(&core, 0),
            });
        }
        wait_until(DEADLINE, "partition 0 reports its watermarks", || {
            producer
                .client()
                .fetch_watermarks(ATTEMPTS, 0, Duration::from_millis(500))
                .is_ok()
        });
        cluster
            .broker_round_trip_time(2, Duration::from_millis(300))
            .unwrap();
        let capture = spate_test::LogCapture::new();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(capture.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        let gate = Gate::new(Arc::clone(&core), producer, config.delivery_timeout)
            .with_step(Duration::from_secs(1));
        tracing::subscriber::with_default(subscriber, || gate.pass());
        let st = core.lock();
        assert_eq!(st.gating.len(), 1, "the unit was admitted");
        assert!(st.fatal.is_none());
        let warning = capture.lines().join("\n");
        assert!(warning.contains("partitions [0]"), "{warning}");
    }

    /// Pins that a pass which put deliveries back for a failed watermark read
    /// waits before the next pass, and that a stop ends the wait: with a
    /// step of an hour, the pass is still running once its unit is back at
    /// the gate, and returns once the feed stops.
    #[test]
    fn a_pass_after_a_failed_watermark_read_waits_a_step() {
        let cluster = cluster_with(0);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("gate-rest");
        let config = KafkaFeedConfig::from_section(&section(&brokers, &group)).unwrap();
        let core = Arc::new(Core::new(&config, TWO));
        let producer: Producer =
            rdkafka::config::FromClientConfigAndContext::from_config_and_context(
                &config.producer_config(false),
                FeedProducerContext::new(Arc::clone(&core)),
            )
            .unwrap();
        let offset = (0..)
            .find(|&o| super::attempt_partition(&core, &super::identity(&core, o)) == 1)
            .unwrap();
        cluster.partition_leader(ATTEMPTS, 1, None).unwrap();
        wait_until(DEADLINE, "partition 1 reports no leader", || {
            producer
                .client()
                .fetch_watermarks(ATTEMPTS, 1, Duration::from_millis(500))
                .is_err()
        });
        {
            let mut st = core.lock();
            st.tracker.assign([super::tp(0)]);
            let receipt = st.tracker.deliver(&super::tp(0), offset).unwrap();
            st.gating.push_back(Gating {
                receipt,
                unit: Unit::new("u", Vec::new()),
                identity: super::identity(&core, offset),
            });
        }
        let gate = Gate::new(Arc::clone(&core), producer, Duration::from_secs(3600))
            .with_step(Duration::from_secs(3600));
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            gate.pass();
            let _ = tx.send(());
        });
        wait_until(DEADLINE, "the unit is back at the gate", || {
            let st = core.lock();
            st.gating.len() == 1 && st.admitting == 0
        });
        // Elapsed time is the property: the pass waits after putting back.
        assert!(
            rx.recv_timeout(Duration::from_millis(300)).is_err(),
            "the pass returned without waiting"
        );
        super::super::stop(&core);
        assert!(
            rx.recv_timeout(Duration::from_secs(10)).is_ok(),
            "the stop did not end the wait"
        );
    }

    /// Pins that the follower wakes a gate waiting for it to catch up: the
    /// gate, at a step of an hour, takes a unit while no follower runs and
    /// waits in its catch-up; the follower then starts, reads the record
    /// already on each attempt partition, and the unit reaches the reader.
    #[test]
    fn a_follower_that_catches_up_wakes_the_gate() {
        let cluster = cluster_with(0);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("follower-wakes");
        let records: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .unwrap();
        for partition in 0..2 {
            records
                .send(
                    BaseRecord::to(ATTEMPTS)
                        .partition(partition)
                        .key("other")
                        .payload(&super::super::attempt::encode(1)),
                )
                .map_err(|(e, _)| e)
                .unwrap();
        }
        records.flush(Duration::from_secs(30)).unwrap();
        let config = KafkaFeedConfig::from_section(&section(&brokers, &group)).unwrap();
        let core = Arc::new(Core::new(&config, TWO));
        let producer: Producer =
            rdkafka::config::FromClientConfigAndContext::from_config_and_context(
                &config.producer_config(false),
                FeedProducerContext::new(Arc::clone(&core)),
            )
            .unwrap();
        let gate = Gate::new(
            Arc::clone(&core),
            Producer::clone(&producer),
            config.delivery_timeout,
        )
        .with_step(Duration::from_secs(3600))
        .spawn()
        .unwrap();
        {
            let mut st = core.lock();
            st.tracker.assign([super::tp(0)]);
            let receipt = st.tracker.deliver(&super::tp(0), 0).unwrap();
            st.gating.push_back(Gating {
                receipt,
                unit: Unit::new("u", Vec::new()),
                identity: super::identity(&core, 0),
            });
            core.gate_wake.notify_all();
        }
        wait_until(DEADLINE, "the gate takes the unit", || {
            core.lock().admitting == 1
        });
        let follower = Follower::new(&config, Arc::clone(&core))
            .unwrap()
            .spawn()
            .unwrap();
        let feed = KafkaFeed {
            core: Arc::clone(&core),
            producer,
            thread: None,
            gate: Some(gate),
            follower: Some(follower),
            depth: None,
        };
        wait_until(DEADLINE, "the unit is buffered", || buffered(&feed) == 1);
        assert_eq!(core.lock().ready[0].deliveries, 1);
        let dropped = returns_within(Duration::from_secs(10), move || drop(feed));
        if !dropped {
            std::mem::forget(cluster);
        }
        assert!(dropped, "dropping the feed did not return");
    }

    /// Pins that dropping a feed returns while its gate waits for the
    /// follower to catch up: with no follower and a record on each attempt
    /// partition, the gate waits in its catch-up with a step of an hour.
    /// The drop begins once the gate is parked on the ledger's condvar, so
    /// a missing notify of it fails here. The stop flag stored outside the
    /// state lock shows here only as a race; `stop` is reviewed by reading.
    #[test]
    fn a_dropped_feed_wakes_a_gate_waiting_for_the_follower() {
        let cluster = cluster_with(0);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("gate-follower");
        let records: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .unwrap();
        for partition in 0..2 {
            records
                .send(
                    BaseRecord::to(ATTEMPTS)
                        .partition(partition)
                        .key("other")
                        .payload(&super::super::attempt::encode(1)),
                )
                .map_err(|(e, _)| e)
                .unwrap();
        }
        records.flush(Duration::from_secs(30)).unwrap();
        let config = KafkaFeedConfig::from_section(&section(&brokers, &group)).unwrap();
        let core = Arc::new(Core::new(&config, TWO));
        let producer: Producer =
            rdkafka::config::FromClientConfigAndContext::from_config_and_context(
                &config.producer_config(false),
                FeedProducerContext::new(Arc::clone(&core)),
            )
            .unwrap();
        let gate = Gate::new(
            Arc::clone(&core),
            Producer::clone(&producer),
            config.delivery_timeout,
        )
        .with_step(Duration::from_secs(3600))
        .spawn()
        .unwrap();
        {
            let mut st = core.lock();
            st.tracker.assign([super::tp(0)]);
            let receipt = st.tracker.deliver(&super::tp(0), 0).unwrap();
            st.gating.push_back(Gating {
                receipt,
                unit: Unit::new("u", Vec::new()),
                identity: super::identity(&core, 0),
            });
            core.gate_wake.notify_all();
        }
        // Counted under the ledger's lock that the wait releases, so the
        // gate is parked on the ledger's condvar once this reads 1.
        wait_until(DEADLINE, "the gate waits on the ledger", || {
            attempt::lock(&core.ledger).gate_waits == 1
        });
        let feed = KafkaFeed {
            core,
            producer,
            thread: None,
            gate: Some(gate),
            follower: None,
            depth: None,
        };
        let dropped = returns_within(Duration::from_secs(10), move || drop(feed));
        if !dropped {
            std::mem::forget(cluster);
        }
        assert!(dropped, "dropping the feed did not return");
    }

    /// Pins that acknowledging returns when librdkafka refuses the
    /// tombstone at enqueue for a full queue: the refusal settles inline,
    /// which takes the state lock, so the send runs with it released.
    #[test]
    fn acknowledging_with_a_full_producer_queue_returns() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("full-queue");
        let config = KafkaFeedConfig::from_section(&section_with(
            &brokers,
            &group,
            "    queue.buffering.max.messages: \"1\"\n",
        ))
        .unwrap();
        let (core, producer, mut d) = super::super::parts(&config, TWO).unwrap();
        let admission = admission(&config, &core, &producer, STEP);
        drive(&mut d, "the unit is buffered", |d| {
            !d.core.lock().ready.is_empty()
        });
        let feed = Arc::new(KafkaFeed {
            core,
            producer,
            thread: None,
            gate: None,
            follower: None,
            depth: None,
        });
        let delivery = feed.receive(1).unwrap().pop().unwrap();
        cluster.broker_down(1).unwrap();
        let _queued = feed.publish(Unit::new("fills-the-queue", Vec::new()));
        let acknowledging = Arc::clone(&feed);
        let returned = returns_within(Duration::from_secs(10), move || {
            let _ = acknowledging.acknowledge(delivery.receipt);
        });
        if !returned {
            // The state lock is held for good; stopping would wait on it.
            std::mem::forget(admission);
            std::mem::forget(d);
            std::mem::forget(cluster);
            panic!("acknowledging did not return");
        }
        assert_eq!(
            feed.core.lock().resends.len(),
            1,
            "the tombstone is retried"
        );
        cluster.broker_up(1).unwrap();
        admission.stop();
    }

    /// Pins that an assignment taken while the feed already holds a full
    /// buffer, counting the units at the gate, those with their attempt
    /// write in flight and those buffered, is paused at once, and that one
    /// unit fewer is not.
    #[test]
    fn a_partition_assigned_while_the_feed_holds_a_full_buffer_is_paused() {
        let cluster = cluster_with(0);
        let brokers = cluster.bootstrap_servers();
        for held in [READY_CAPACITY - 1, READY_CAPACITY] {
            let group = spate_test::unique_name("assigned-full");
            let (_, core, _, mut d) = parts_for(&brokers, &group);
            drive(&mut d, "the driver owns its partitions", |d| owns(d) == 2);
            {
                let mut st = core.lock();
                for offset in 0..i64::try_from(held - 2).unwrap() {
                    let receipt = st.tracker.deliver(&super::tp(0), offset).unwrap();
                    st.gating.push_back(Gating {
                        receipt,
                        unit: Unit::new("u", Vec::new()),
                        identity: super::identity(&core, offset),
                    });
                }
                st.admitting = 1;
                let receipt = st.tracker.deliver(&super::tp(0), 1000).unwrap();
                let ready = super::ready(&core, receipt, 1000);
                st.ready.push_back(ready);
            }
            let assigned = d.consumer.assignment().unwrap();
            d.consumer
                .context()
                .intents
                .lock()
                .unwrap()
                .push_back(Intent::Assign(assigned));
            d.serve_intents();
            assert_eq!(
                !d.paused.is_empty(),
                held >= READY_CAPACITY,
                "{held} units held"
            );
        }
    }

    /// A feed over `core` and `producer`, with `thread` as its consumer
    /// thread and no other thread.
    fn feed_over(
        core: &Arc<Core>,
        producer: &Producer,
        thread: Option<JoinHandle<()>>,
    ) -> KafkaFeed {
        KafkaFeed {
            core: Arc::clone(core),
            producer: producer.clone(),
            thread,
            gate: None,
            follower: None,
            depth: None,
        }
    }

    /// The partitions `core`'s tracker owns.
    fn owned_tps(core: &Core) -> BTreeSet<Tp> {
        core.lock().tracker.owned().cloned().collect()
    }

    /// Pins that once the feed stops receiving, the consumer pauses every
    /// partition it owns and keeps them paused with its buffer empty, and
    /// that the stop logs one line counting what it held for the reader,
    /// however often it is called.
    #[test]
    fn a_stopping_feed_keeps_its_partitions_paused_once_its_buffer_drains() {
        let cluster = cluster_with(0);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("stop-paused");
        let (_, core, producer, mut d) = parts_for(&brokers, &group);
        let feed = feed_over(&core, &producer, None);
        drive(&mut d, "the driver owns its partitions", |d| owns(d) == 2);
        let lines = spate_test::capture_logs(tracing::Level::INFO, || {
            feed.stop_receiving();
            feed.stop_receiving();
        });
        let stops: Vec<&String> = lines
            .iter()
            .filter(|l| l.contains("the Kafka feed stopped receiving"))
            .collect();
        assert_eq!(stops.len(), 1, "{lines:#?}");
        assert!(stops[0].contains("buffered=0"), "{lines:#?}");
        d.poll();
        d.serve_intents();
        d.backpressure();
        let owned = owned_tps(&core);
        assert_eq!(owned.len(), 2);
        assert_eq!(d.paused, owned);
    }

    /// Pins that a partition assigned while the feed stops receiving is
    /// paused at once, after a revocation and after a rebalance error alike,
    /// though both resume what the thread had paused.
    #[test]
    fn a_partition_assigned_while_the_feed_stops_is_paused() {
        let cluster = cluster_with(0);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("stop-assigned");
        let (_, core, producer, mut d) = parts_for(&brokers, &group);
        let feed = feed_over(&core, &producer, None);
        drive(&mut d, "the driver owns its partitions", |d| owns(d) == 2);
        feed.stop_receiving();
        let assigned = d.consumer.assignment().unwrap();
        let intents = |d: &Driver, intent: Intent| {
            d.consumer
                .context()
                .intents
                .lock()
                .unwrap()
                .push_back(intent);
        };
        intents(&d, Intent::Revoke(assigned.clone()));
        d.serve_intents();
        assert!(d.paused.is_empty(), "the revocation resumed nothing");
        intents(&d, Intent::Assign(assigned.clone()));
        d.serve_intents();
        assert_eq!(d.paused, owned_tps(&core), "after a revocation");
        // The error arm of the rebalance callback, as librdkafka calls it.
        d.consumer.context().rebalance(
            &d.consumer,
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
            &mut TopicPartitionList::new(),
        );
        d.serve_intents();
        assert!(d.paused.is_empty(), "the rebalance error resumed nothing");
        intents(&d, Intent::Assign(assigned));
        d.serve_intents();
        let owned = owned_tps(&core);
        assert_eq!(owned.len(), 2);
        assert_eq!(d.paused, owned, "after a rebalance error");
    }

    /// Wait until `n` units are buffered, receive them all in the order of
    /// their ids, which is their offsets' order, and stop the follower and
    /// the gate. The gate buffers units in the order their attempt writes
    /// are confirmed, which spreads them over two partitions.
    fn received(
        d: &mut Driver,
        feed: &KafkaFeed,
        admission: Admission,
        n: usize,
    ) -> Vec<spate_core::feed::Delivery> {
        drive(d, "the units are buffered", |d| {
            d.core.lock().ready.len() == n
        });
        admission.stop();
        let mut deliveries = feed.receive(n).unwrap();
        assert_eq!(deliveries.len(), n);
        deliveries.sort_by(|a, b| a.unit.id.cmp(&b.unit.id));
        deliveries
    }

    /// Pins the close's order: it waits for the tombstone in flight,
    /// commits the offset that tombstone finishes, and leaves the group
    /// before it settles. The broker's round trip is slowed so the
    /// tombstone is confirmed hundreds of milliseconds after it is sent; a
    /// close that commits at once commits the unit's own offset. The mock
    /// refuses a commit from a member that has left, so the commit landing
    /// shows it came first.
    #[test]
    fn a_close_commits_what_finished_then_leaves() {
        let cluster = cluster_with(2);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("close-commit");
        let (config, core, producer, mut d) = parts_for(&brokers, &group);
        let feed = feed_over(&core, &producer, None);
        let admission = admission(&config, &core, &producer, STEP);
        let deliveries = received(&mut d, &feed, admission, 2);
        cluster
            .broker_round_trip_time(1, Duration::from_millis(300))
            .unwrap();
        feed.acknowledge(deliveries[0].receipt).unwrap();
        let mut closing = feed.close(Duration::from_secs(10)).expect("a close");
        consumer::run(d);
        assert_eq!(closing.try_outcome(), Some(Ok(())));
        assert_eq!(committed(&brokers, &group), Some(1));
        assert!(returns_within(Duration::from_secs(10), move || drop(feed)));
    }

    /// Pins that a handed-back unit holds its partition's commit point: the
    /// close commits below it though a later unit finished.
    #[test]
    fn a_handed_back_unit_holds_its_partitions_commit_point() {
        let cluster = cluster_with(3);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("close-handed-back");
        let (config, core, producer, mut d) = parts_for(&brokers, &group);
        let feed = feed_over(&core, &producer, None);
        let admission = admission(&config, &core, &producer, STEP);
        let deliveries = received(&mut d, &feed, admission, 3);
        feed.acknowledge(deliveries[0].receipt).unwrap();
        feed.hand_back(deliveries[1].receipt).unwrap();
        feed.acknowledge(deliveries[2].receipt).unwrap();
        let mut closing = feed.close(Duration::from_secs(10)).expect("a close");
        consumer::run(d);
        assert_eq!(closing.try_outcome(), Some(Ok(())));
        assert_eq!(committed(&brokers, &group), Some(1));
    }

    /// Pins that closing a latched feed settles at once with the latched
    /// error, and that the consumer thread then commits nothing.
    #[test]
    fn closing_a_latched_feed_settles_with_its_error_and_commits_nothing() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("close-latched");
        let (config, core, producer, mut d) = parts_for(&brokers, &group);
        let feed = feed_over(&core, &producer, None);
        let admission = admission(&config, &core, &producer, STEP);
        drive(&mut d, "the unit is buffered", |d| {
            !d.core.lock().ready.is_empty()
        });
        admission.stop();
        acknowledge_first(&d);
        core.latch("a test latch");
        let mut closing = feed.close(Duration::from_secs(10)).expect("a close");
        let err = closing.try_outcome().expect("settled").unwrap_err();
        assert_eq!(err.kind, FeedErrorKind::Fatal);
        assert_eq!(err.reason, "a test latch");
        consumer::run(d);
        assert_eq!(committed(&brokers, &group), None);
    }

    /// Pins that a close reaching a consumer thread that holds nothing
    /// settles `Ok` once the thread has ended, the stop waking its loop.
    #[test]
    fn an_idle_close_settles_ok_and_leaves() {
        let cluster = cluster_with(0);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("close-idle");
        let (_, core, producer, mut d) = parts_for(&brokers, &group);
        drive(&mut d, "the driver owns its partitions", |d| owns(d) == 2);
        let thread = std::thread::spawn(move || consumer::run(d));
        let feed = feed_over(&core, &producer, Some(thread));
        let mut closing = feed.close(Duration::from_secs(10)).expect("a close");
        wait_until(DEADLINE, "the close settles", || {
            closing.try_outcome().is_some()
        });
        assert_eq!(closing.try_outcome(), Some(Ok(())));
        let thread = feed.thread.as_ref().expect("the consumer thread");
        wait_until(DEADLINE, "the consumer thread ends", || {
            thread.is_finished()
        });
    }

    /// Pins that a close given no time rejects as Retryable when a
    /// tombstone is still in flight, and commits nothing past its unit.
    #[test]
    fn a_close_with_no_time_left_commits_only_what_was_confirmed() {
        let cluster = cluster_with(2);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("close-no-time");
        let (config, core, producer, mut d) = parts_for(&brokers, &group);
        let feed = feed_over(&core, &producer, None);
        let admission = admission(&config, &core, &producer, STEP);
        let deliveries = received(&mut d, &feed, admission, 2);
        cluster
            .broker_round_trip_time(1, Duration::from_millis(300))
            .unwrap();
        feed.acknowledge(deliveries[0].receipt).unwrap();
        let mut closing = feed.close(Duration::ZERO).expect("a close");
        consumer::run(d);
        let err = closing.try_outcome().expect("settled").unwrap_err();
        assert_eq!(err.kind, FeedErrorKind::Retryable);
        assert!(
            err.reason.contains("writes still in flight"),
            "{}",
            err.reason
        );
        assert_ne!(committed(&brokers, &group), Some(1));
    }

    /// Pins that a close whose last commit the broker refuses rejects as
    /// Retryable naming the commit, and that the warning names the close.
    #[test]
    fn a_close_whose_commit_fails_rejects() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("close-commit-fails");
        let (config, core, producer, mut d) = parts_for(&brokers, &group);
        let feed = feed_over(&core, &producer, None);
        let admission = admission(&config, &core, &producer, STEP);
        let deliveries = received(&mut d, &feed, admission, 1);
        feed.acknowledge(deliveries[0].receipt).unwrap();
        // A code librdkafka does not retry on its own.
        cluster.request_errors(
            RDKafkaApiKey::OffsetCommit,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE],
        );
        let mut closing = feed.close(Duration::from_secs(10)).expect("a close");
        let lines = spate_test::capture_logs(tracing::Level::WARN, || consumer::run(d));
        let err = closing.try_outcome().expect("settled").unwrap_err();
        assert_eq!(err.kind, FeedErrorKind::Retryable);
        assert!(err.reason.contains("last commit failed"), "{}", err.reason);
        assert!(
            lines
                .iter()
                .any(|l| l.contains("committing the feed's offsets failed")
                    && l.contains("commit=\"on close\"")),
            "{lines:#?}"
        );
        assert_eq!(committed(&brokers, &group), None);
    }

    /// Pins that a gate pass under way when the feed stops receiving admits
    /// none of what it took: the unit goes back to the gate with no attempt
    /// record. The broker's round trip is slowed so the pass is still
    /// reading watermarks when the stop lands.
    #[test]
    fn a_pass_under_way_at_the_stop_admits_nothing() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("stop-mid-pass");
        let (config, core, producer, mut d) = parts_for(&brokers, &group);
        let feed = feed_over(&core, &producer, None);
        drive(&mut d, "the unit waits at the gate", |d| {
            d.core.lock().gating.len() == 1
        });
        cluster
            .broker_round_trip_time(1, Duration::from_millis(500))
            .unwrap();
        let gate = Gate::new(Arc::clone(&core), producer.clone(), config.delivery_timeout)
            .with_step(Duration::from_secs(5));
        let pass = std::thread::spawn(move || gate.pass());
        wait_until(
            DEADLINE,
            "the pass takes the unit and begins a read",
            || core.gate_reads.load(Ordering::SeqCst) > 0,
        );
        feed.stop_receiving();
        pass.join().unwrap();
        cluster.broker_round_trip_time(1, Duration::ZERO).unwrap();
        let st = core.lock();
        assert_eq!(st.gating.len(), 1, "the unit left the gate");
        assert_eq!(st.admitting, 0);
        assert!(st.ready.is_empty(), "the unit was admitted");
    }

    /// Pins that a close sends the tombstone a confirmed dead-letter write
    /// queued, so the last commit passes the dead-lettered unit and the
    /// next owner does not move a second copy.
    #[test]
    fn a_close_commits_past_a_unit_dead_lettered_before_it() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("close-dead-letter");
        let section: FeedSection = serde_yaml::from_str(&format!(
            "attempt_cap: 1\nkafka:\n  brokers: \"{brokers}\"\n  group_id: {group}\n  \
             topic: units\n  retry_topic: units-retry\n  dead_letter_topic: units-dead\n  \
             attempt_topic: {ATTEMPTS}\n  statistics_interval: 0s\n  rdkafka:\n    \
             session.timeout.ms: \"6000\"\n    heartbeat.interval.ms: \"2000\"\n"
        ))
        .unwrap();
        let config = KafkaFeedConfig::from_section(&section).unwrap();
        let (core, producer, mut d) = super::super::parts(&config, TWO).unwrap();
        let feed = feed_over(&core, &producer, None);
        let admission = admission(&config, &core, &producer, STEP);
        drive(&mut d, "the unit is buffered", |d| {
            d.core.lock().ready.len() == 1
        });
        let delivery = feed.receive(1).unwrap().pop().unwrap();
        // At the cap: the unit goes to the dead-letter topic, and its
        // tombstone is queued once that write is confirmed. Stopping the
        // gate and the follower first would set the stop flag, which
        // queues no tombstone.
        feed.fail(delivery.receipt, "a test failure").unwrap();
        wait_until(DEADLINE, "the dead-letter write is confirmed", || {
            !core.lock().resends.is_empty()
        });
        admission.stop();
        let mut closing = feed.close(Duration::from_secs(10)).expect("a close");
        consumer::run(d);
        assert_eq!(closing.try_outcome(), Some(Ok(())));
        assert_eq!(committed(&brokers, &group), Some(1));
    }

    /// Pins that a latch landing after the consumer thread has taken the
    /// close rejects the close with the latched error.
    #[test]
    fn a_latch_during_the_close_rejects_it_with_the_latched_error() {
        let cluster = cluster_with(2);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("close-latch");
        let (config, core, producer, mut d) = parts_for(&brokers, &group);
        let feed = feed_over(&core, &producer, None);
        let admission = admission(&config, &core, &producer, STEP);
        let deliveries = received(&mut d, &feed, admission, 2);
        // The tombstone, the commit and the leave each wait out the round
        // trip, so the latch lands while the thread is still closing.
        cluster
            .broker_round_trip_time(1, Duration::from_millis(500))
            .unwrap();
        feed.acknowledge(deliveries[0].receipt).unwrap();
        let mut closing = feed.close(Duration::from_secs(10)).expect("a close");
        let thread = std::thread::spawn(move || consumer::run(d));
        wait_until(DEADLINE, "the consumer thread takes the close", || {
            core.lock().close.is_none()
        });
        core.latch("a test latch");
        thread.join().expect("the consumer thread ends");
        let err = closing.try_outcome().expect("settled").unwrap_err();
        assert_eq!(err.kind, FeedErrorKind::Fatal, "{}", err.reason);
        assert_eq!(err.reason, "a test latch");
    }

    /// Pins that a close settles only once the consumer is gone: the
    /// confirmation's waker runs after the consumer's context is released.
    #[test]
    fn a_close_settles_after_the_consumer_has_left() {
        use std::future::Future as _;
        use std::sync::{Mutex, Weak};
        use std::task::{Context, Wake, Waker};

        struct Probe {
            context: Weak<crate::context::SourceContext>,
            at_settle: Mutex<Option<usize>>,
        }
        impl Wake for Probe {
            fn wake(self: Arc<Self>) {
                *self.at_settle.lock().unwrap() = Some(self.context.strong_count());
            }
        }

        let cluster = cluster_with(0);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("close-after-leave");
        let (_, core, producer, mut d) = parts_for(&brokers, &group);
        drive(&mut d, "the driver owns its partitions", |d| owns(d) == 2);
        let probe = Arc::new(Probe {
            context: Arc::downgrade(d.consumer.context()),
            at_settle: Mutex::new(None),
        });
        let feed = feed_over(&core, &producer, None);
        let mut closing = feed.close(Duration::from_secs(10)).expect("a close");
        let waker = Waker::from(Arc::clone(&probe));
        assert!(
            std::pin::Pin::new(&mut closing)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        consumer::run(d);
        assert_eq!(closing.try_outcome(), Some(Ok(())));
        assert_eq!(
            *probe.at_settle.lock().unwrap(),
            Some(0),
            "the close settled while the consumer was still held"
        );
    }

    use super::super::KafkaFeedPublisher;

    /// A publisher on `brokers` with `rdkafka` appended to its passthrough,
    /// checking only that the topics exist.
    fn connect_publisher(
        brokers: &str,
        rdkafka: &str,
        io: &tokio::runtime::Runtime,
    ) -> KafkaFeedPublisher {
        KafkaFeedPublisher::connect_with(
            &section_with(brokers, "publisher", rdkafka),
            io.handle(),
            Scope::Existence,
            CONNECT_BOUND,
        )
        .unwrap()
    }

    fn io() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    /// The outcome of `confirmation`, waiting for it under the deadline.
    fn settled(
        mut confirmation: spate_core::feed::Confirmation,
    ) -> Result<(), spate_core::feed::FeedError> {
        let mut got = None;
        wait_until(DEADLINE, "the publish settles", || {
            got = confirmation.try_outcome();
            got.is_some()
        });
        got.unwrap()
    }

    fn work_high_watermark(brokers: &str) -> i64 {
        let probe: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .unwrap();
        probe
            .fetch_watermarks("units", 0, Duration::from_secs(10))
            .unwrap()
            .1
    }

    /// Pins that a publish too large for the producer is refused, and that
    /// the publisher goes on publishing.
    #[test]
    fn a_publish_too_large_for_the_producer_is_refused() {
        let cluster = cluster_with(0);
        let io = io();
        let publisher = connect_publisher(
            &cluster.bootstrap_servers(),
            "    message.max.bytes: \"1000\"\n",
            &io,
        );
        let err = settled(publisher.publish(Unit::new("big", vec![b'x'; 2000]))).unwrap_err();
        assert_eq!(err.kind, FeedErrorKind::Refused, "{}", err.reason);
        settled(publisher.publish(Unit::new("small", b"body".to_vec()))).unwrap();
    }

    /// Pins that a publish the cluster refuses as unauthorized stops the
    /// publisher, and a later publish fails Fatal without reaching the work
    /// topic.
    #[test]
    fn a_fatal_publish_latches_the_publisher() {
        let cluster = cluster_with(0);
        let brokers = cluster.bootstrap_servers();
        let io = io();
        let publisher = connect_publisher(&brokers, "", &io);
        cluster.request_errors(
            RDKafkaApiKey::Produce,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED],
        );
        let err = settled(publisher.publish(Unit::new("a", b"body".to_vec()))).unwrap_err();
        assert_eq!(err.kind, FeedErrorKind::Fatal, "{}", err.reason);
        let err = settled(publisher.publish(Unit::new("b", b"body".to_vec()))).unwrap_err();
        assert_eq!(err.kind, FeedErrorKind::Fatal, "{}", err.reason);
        assert_eq!(work_high_watermark(&brokers), 0);
    }

    /// Pins that dropping a publisher with a publish in flight settles that
    /// publish as retryable, within the deadline.
    #[test]
    fn dropping_a_publisher_with_a_publish_in_flight_settles_it() {
        let cluster = cluster_with(0);
        let io = io();
        let publisher = connect_publisher(&cluster.bootstrap_servers(), "", &io);
        cluster.request_errors(
            RDKafkaApiKey::Produce,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS; 100],
        );
        let mut confirmation = publisher.publish(Unit::new("a", b"body".to_vec()));
        assert!(
            confirmation.try_outcome().is_none(),
            "settled before the drop"
        );
        assert!(
            returns_within(DEADLINE, move || drop(publisher)),
            "the drop did not return"
        );
        let err = confirmation.try_outcome().expect("settled").unwrap_err();
        assert_eq!(err.kind, FeedErrorKind::Retryable, "{}", err.reason);
    }

    /// Pins that a publisher whose idempotent producer id the cluster
    /// refuses stops with the cause, and a later publish names it.
    #[test]
    fn a_publisher_without_write_is_fatal_with_its_cause() {
        let cluster = cluster_with(0);
        cluster.request_errors(
            RDKafkaApiKey::InitProducerId,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED],
        );
        let io = io();
        let publisher = connect_publisher(&cluster.bootstrap_servers(), "", &io);
        let first = settled(publisher.publish(Unit::new("a", b"body".to_vec())));
        assert!(first.is_err(), "{first:?}");
        wait_until(DEADLINE, "the publisher latches", || {
            publisher.core().latched()
        });
        let err = settled(publisher.publish(Unit::new("b", b"body".to_vec()))).unwrap_err();
        assert_eq!(err.kind, FeedErrorKind::Fatal, "{}", err.reason);
        assert!(
            err.reason.contains("Cluster authorization failed"),
            "{}",
            err.reason
        );
    }

    /// The error a publish gets once `publisher` latched on its producer's
    /// fatal error.
    fn latched_reason(publisher: &KafkaFeedPublisher) -> spate_core::feed::FeedError {
        wait_until(DEADLINE, "the publisher latches", || {
            publisher.core().latched()
        });
        let err = settled(publisher.publish(Unit::new("later", b"body".to_vec()))).unwrap_err();
        assert_eq!(err.kind, FeedErrorKind::Fatal, "{}", err.reason);
        err
    }

    /// Pins that a retried batch the cluster answers with
    /// `UNKNOWN_PRODUCER_ID`, which librdkafka makes fatal for an idempotent
    /// producer, stops the publisher with the cause.
    #[test]
    fn a_fatal_unknown_producer_id_latches_the_publisher_with_its_cause() {
        let cluster = cluster_with(0);
        let io = io();
        let publisher = connect_publisher(&cluster.bootstrap_servers(), "", &io);
        settled(publisher.publish(Unit::new("warm", b"body".to_vec()))).unwrap();
        cluster.request_errors(
            RDKafkaApiKey::Produce,
            &[
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS,
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_PRODUCER_ID,
            ],
        );
        let first = settled(publisher.publish(Unit::new("a", b"body".to_vec())));
        assert!(first.is_err(), "{first:?}");
        let err = latched_reason(&publisher);
        assert!(
            err.reason.contains("the feed's producer stopped"),
            "{}",
            err.reason
        );
    }

    /// Pins that a producer id request answered `INVALID_PRODUCER_EPOCH`,
    /// which librdkafka raises as a fatal error under `Fenced`, stops the
    /// publisher with the cause.
    #[test]
    fn a_fenced_producer_id_latches_the_publisher_with_its_cause() {
        let cluster = cluster_with(0);
        cluster.request_errors(
            RDKafkaApiKey::InitProducerId,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH],
        );
        let io = io();
        let publisher = connect_publisher(&cluster.bootstrap_servers(), "", &io);
        let first = settled(publisher.publish(Unit::new("a", b"body".to_vec())));
        assert!(first.is_err(), "{first:?}");
        let err = latched_reason(&publisher);
        assert!(
            err.reason.contains("the feed's producer stopped"),
            "{}",
            err.reason
        );
    }

    /// Pins the publisher's start line: its own message, the work topic and
    /// the two values its checks read of it, and no group.
    #[test]
    fn a_publisher_logs_its_own_start_line() {
        let cluster = cluster_with(0);
        let io = io();
        let mut publisher = None;
        let lines = spate_test::capture_logs(tracing::Level::INFO, || {
            publisher = Some(connect_publisher(&cluster.bootstrap_servers(), "", &io));
        });
        assert!(
            lines
                .iter()
                .any(|l| l.contains("the Kafka feed's publisher started")
                    && l.contains("topic=units")
                    && l.contains("cleanup_policy=unreported")
                    && l.contains("timestamp_type=unreported")),
            "{lines:#?}"
        );
        assert!(
            !lines
                .iter()
                .any(|l| l.contains("the Kafka feed started") || l.contains("group_id")),
            "{lines:#?}"
        );
    }
}

mod docker {
    //! The feed against a real broker, for what the MockCluster cannot
    //! show: a rebalance held open, which the mock refuses every commit
    //! during, and a compacted attempt topic, which the mock never cleans.

    use super::super::attempt::{self, Follower};
    use super::super::config::KafkaFeedConfig;
    use super::super::gate::Gate;
    use super::super::producer::FeedProducerContext;
    use super::super::{Core, Gating};
    use super::cluster::{
        acknowledge_first, admission, committed, drive, owns, parts_for, section,
    };
    use crate::feed::KafkaFeed;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::config::FromClientConfigAndContext;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::message::Message;
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    use rdkafka::{ClientConfig, Offset, TopicPartitionList};
    use spate_core::feed::Unit;
    use spate_test::wait_until;
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::time::Duration;
    use testcontainers::ImageExt;
    use testcontainers::runners::SyncRunner;
    use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

    /// Units on the work topic, fewer than the buffer holds.
    const UNITS: usize = 5;

    /// Pins that a revocation commits before it unassigns. Worker A buffers
    /// and acknowledges every unit and runs no periodic commit; worker B
    /// joins, and once A has served the revocation the group has committed
    /// past every unit A acknowledged.
    #[test]
    #[ignore = "requires Docker"]
    fn a_revocation_commits_before_it_unassigns() {
        let (name, tag) = spate_test_support::container_image(&["--pull", "kafka"]);
        let container = Kafka::default()
            .with_name(name)
            .with_tag(tag)
            .start()
            .expect("start kafka container");
        let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
        let brokers = format!("127.0.0.1:{port}");
        let io = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .unwrap();
        let mut topics = vec![
            NewTopic::new("units", 1, TopicReplication::Fixed(1))
                .set("message.timestamp.type", "LogAppendTime"),
            NewTopic::new("units-retry", 1, TopicReplication::Fixed(1)),
            NewTopic::new("units-dead", 1, TopicReplication::Fixed(1)),
        ];
        topics.push(
            NewTopic::new("units-attempts", 2, TopicReplication::Fixed(1))
                .set("cleanup.policy", "compact"),
        );
        for created in io
            .block_on(admin.create_topics(&topics, &AdminOptions::new()))
            .unwrap()
        {
            created.unwrap();
        }
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .unwrap();
        for i in 0..UNITS {
            let id = format!("u{i}");
            producer
                .send(BaseRecord::to("units").key(id.as_bytes()).payload(b"body"))
                .map_err(|(e, _)| e)
                .unwrap();
        }
        producer.flush(Duration::from_secs(30)).unwrap();
        let group = spate_test::unique_name("revoke-order");

        let (config, core, feed_producer, mut a) = parts_for(&brokers, &group);
        let admission = admission(&config, &core, &feed_producer, Duration::from_millis(10));
        drive(&mut a, "A buffers every unit", |d| {
            d.core.lock().ready.len() == UNITS
        });
        for _ in 0..UNITS {
            acknowledge_first(&a);
        }
        let _b = KafkaFeed::connect(&section(&brokers, &group), io.handle()).unwrap();
        drive(&mut a, "A serves the revocation", |d| owns(d) == 0);
        assert_eq!(
            committed(&brokers, &group),
            Some(i64::try_from(UNITS).unwrap()),
            "the revocation did not commit what A finished"
        );
        admission.stop();
    }

    /// Create the one-partition topic `name`, compacted, with segments that
    /// roll and clean within a test's time.
    fn create_compacted(brokers: &str, io: &tokio::runtime::Runtime, name: &str) {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .unwrap();
        let topic = NewTopic::new(name, 1, TopicReplication::Fixed(1))
            .set("cleanup.policy", "compact")
            .set("segment.ms", "100")
            .set("min.cleanable.dirty.ratio", "0.01")
            .set("delete.retention.ms", "0");
        for created in io
            .block_on(admin.create_topics(&[topic], &AdminOptions::new()))
            .unwrap()
        {
            created.unwrap();
        }
    }

    /// Write each `(key, value)` to partition 0 of `topic`, a `None` value
    /// as a tombstone, and wait for the writes' reports.
    fn write(brokers: &str, topic: &str, records: &[(&str, Option<&[u8]>)]) {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .unwrap();
        for (key, value) in records {
            let mut record = BaseRecord::<str, [u8]>::to(topic).partition(0).key(*key);
            if let Some(value) = value {
                record = record.payload(*value);
            }
            producer.send(record).map_err(|(e, _)| e).unwrap();
        }
        producer.flush(Duration::from_secs(30)).unwrap();
    }

    /// Run one gate pass over a unit on each attempt partition of `core`,
    /// with a bound of 10 s, and assert that it admitted every unit.
    fn gate_admits(config: &KafkaFeedConfig, core: &Arc<Core>) {
        let producer = super::super::write::Producer::from_config_and_context(
            &config.producer_config(false),
            FeedProducerContext::new(Arc::clone(core)),
        )
        .unwrap();
        {
            let mut st = core.lock();
            st.tracker.assign([super::tp(0)]);
            let mut wanted: Vec<i32> = (0..).take(core.attempt_partitions.get() as usize).collect();
            for offset in 0.. {
                let identity = super::identity(core, offset);
                let partition = super::attempt_partition(core, &identity);
                if let Some(at) = wanted.iter().position(|&p| p == partition) {
                    wanted.remove(at);
                    let receipt = st.tracker.deliver(&super::tp(0), offset).unwrap();
                    st.gating.push_back(Gating {
                        receipt,
                        unit: Unit::new(format!("u{offset}"), Vec::new()),
                        identity,
                    });
                }
                if wanted.is_empty() {
                    break;
                }
            }
        }
        Gate::new(Arc::clone(core), producer, Duration::from_secs(10)).pass();
        let st = core.lock();
        assert!(
            st.fatal.is_none(),
            "{:?}",
            st.fatal.as_ref().map(|f| &f.reason)
        );
        assert!(st.gating.is_empty(), "the gate held back a unit");
    }

    /// Start a follower of `topic` in a feed's group, wait up to 10 s for
    /// its ledger to reach the partition's high watermark, and check that a
    /// gate pass over the partition admits its unit.
    fn follower_catches_up(brokers: &str, topic: &str) {
        let section: spate_core::config::FeedSection = serde_yaml::from_str(&format!(
            "kafka:\n  brokers: \"{brokers}\"\n  group_id: {}\n  topic: units\n  \
             retry_topic: units-retry\n  dead_letter_topic: units-dead\n  \
             attempt_topic: {topic}\n  statistics_interval: 0s\n",
            spate_test::unique_name("tail")
        ))
        .unwrap();
        let config = KafkaFeedConfig::from_section(&section).unwrap();
        let core = Arc::new(Core::new(&config, NonZeroU32::MIN));
        let thread = Follower::new(&config, Arc::clone(&core))
            .unwrap()
            .spawn()
            .unwrap();
        let reader: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .unwrap();
        wait_until(
            Duration::from_secs(10),
            "the follower reaches the high watermark",
            || {
                let (lwm, hwm) = reader
                    .fetch_watermarks(topic, 0, Duration::from_secs(5))
                    .unwrap();
                attempt::lock(&core.ledger).caught_up(0, lwm, hwm)
            },
        );
        gate_admits(&config, &core);
        crate::feed::stop(&core);
        thread.join().unwrap();
    }

    /// Pins that a gate pass admits the units of an attempt partition that
    /// holds no records, with no follower running: one partition never
    /// written, whose watermarks are both 0, and one whose records were all
    /// deleted, whose watermarks are both 3.
    #[test]
    #[ignore = "requires Docker"]
    fn a_gate_pass_admits_on_an_empty_or_emptied_attempt_partition() {
        let (_container, brokers) = start_broker();
        let io = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .unwrap();
        let topic = NewTopic::new("units-attempts", 2, TopicReplication::Fixed(1));
        for created in io
            .block_on(admin.create_topics(&[topic], &AdminOptions::new()))
            .unwrap()
        {
            created.unwrap();
        }
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .unwrap();
        for _ in 0..3 {
            producer
                .send(
                    BaseRecord::to("units-attempts")
                        .partition(1)
                        .key("x")
                        .payload(&attempt::encode(1)),
                )
                .map_err(|(e, _)| e)
                .unwrap();
        }
        producer.flush(Duration::from_secs(30)).unwrap();
        let mut upto = TopicPartitionList::new();
        upto.add_partition_offset("units-attempts", 1, Offset::Offset(3))
            .unwrap();
        for deleted in io
            .block_on(admin.delete_records(&upto, &AdminOptions::new()))
            .unwrap()
            .elements()
        {
            deleted.error().unwrap();
        }
        wait_until(
            Duration::from_secs(30),
            "the partitions read as empty",
            || {
                [(0, (0, 0)), (1, (3, 3))].into_iter().all(|(p, marks)| {
                    producer
                        .client()
                        .fetch_watermarks("units-attempts", p, Duration::from_secs(5))
                        .is_ok_and(|read| read == marks)
                })
            },
        );
        let config = KafkaFeedConfig::from_section(&section(&brokers, "emptied")).unwrap();
        let core = Arc::new(Core::new(&config, NonZeroU32::new(2).unwrap()));
        gate_admits(&config, &core);
    }

    /// The offset of the first record a fresh read of `topic` finds.
    fn first_offset(brokers: &str, topic: &str) -> Option<i64> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", spate_test::unique_name("first"))
            .create()
            .unwrap();
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, 0, Offset::Beginning)
            .unwrap();
        consumer.assign(&tpl).unwrap();
        (0..50).find_map(|_| {
            consumer
                .poll(Duration::from_millis(100))
                .map(|m| m.unwrap().offset())
        })
    }

    fn start_broker() -> (testcontainers::Container<Kafka>, String) {
        let (name, tag) = spate_test_support::container_image(&["--pull", "kafka"]);
        let container = Kafka::default()
            .with_name(name)
            .with_tag(tag)
            .start()
            .expect("start kafka container");
        let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
        (container, format!("127.0.0.1:{port}"))
    }

    /// Pins that a follower catches up on a compacted attempt partition
    /// whose earlier records, tombstones included, the cleaner removed: the
    /// last record, in the active segment, stays readable.
    #[test]
    #[ignore = "requires Docker"]
    fn a_follower_catches_up_on_a_compacted_live_tail() {
        let (_container, brokers) = start_broker();
        let io = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        create_compacted(&brokers, &io, "live-tail");
        let one: &[u8] = br#"{"v":1,"attempts":1}"#;
        write(
            &brokers,
            "live-tail",
            &[("a", Some(one)), ("a", None), ("b", Some(one)), ("b", None)],
        );
        // Elapsed time is the property: a write after `segment.ms` rolls the
        // segment, which leaves the records above for the cleaner.
        std::thread::sleep(Duration::from_millis(200));
        write(&brokers, "live-tail", &[("c", Some(one))]);
        wait_until(
            Duration::from_secs(120),
            "the cleaner removes the first record",
            || first_offset(&brokers, "live-tail").is_some_and(|o| o > 0),
        );
        follower_catches_up(&brokers, "live-tail");
    }

    /// Pins that a follower catches up on a compacted attempt partition
    /// whose last records are tombstones, after the topic sat idle for
    /// longer than `segment.ms`.
    #[test]
    #[ignore = "requires Docker"]
    fn a_follower_catches_up_when_the_tail_is_a_tombstone() {
        let (_container, brokers) = start_broker();
        let io = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        create_compacted(&brokers, &io, "tombstone-tail");
        let one: &[u8] = br#"{"v":1,"attempts":1}"#;
        write(
            &brokers,
            "tombstone-tail",
            &[("a", Some(one)), ("b", Some(one)), ("a", None), ("b", None)],
        );
        // Elapsed time is the property: the topic sits idle past
        // `segment.ms` with no append.
        std::thread::sleep(Duration::from_secs(6));
        follower_catches_up(&brokers, "tombstone-tail");
    }
}

/// Pins that a broker certificate the client fails to verify after connect
/// stops a publish-only handle with the rejection's text.
#[test]
fn a_certificate_the_client_rejects_latches_the_publisher() {
    let publisher = idle_publisher();
    report_error(
        &publisher,
        C::SSL,
        "ssl://127.0.0.1:9093/bootstrap: SSL handshake failed: error:0A000086:SSL routines::\
         certificate verify failed: broker certificate could not be verified, verify that \
         ssl.ca.location is correctly configured or root CA certificates are installed \
         (after 9ms in state SSL_HANDSHAKE)",
    );
    let err = publisher
        .publish(Unit::new("u", b"body".to_vec()))
        .try_outcome()
        .expect("settled")
        .unwrap_err();
    assert_eq!(err.kind, FeedErrorKind::Fatal);
    assert!(
        err.reason
            .starts_with("the client's credential or TLS handshake was rejected: "),
        "{}",
        err.reason
    );
}
