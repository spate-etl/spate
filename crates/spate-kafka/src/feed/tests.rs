//! Unit tests of the backend's pure parts: the cap route, reading a
//! message, classifying a failed write, settling a write against the
//! tracker, and the lock discipline around commits.

use super::config::KafkaFeedConfig;
use super::consumer::{
    Read, commit_moved, commit_revoked, committed_lookup_timeout, poll_class, read, take_due,
};
use super::events::Cause;
use super::inflight::Tp;
use super::producer::FeedProducerContext;
use super::write::Producer;
use super::write::{
    self, DELIVERIES_HEADER, Destination, Failure, Move, Outgoing, Write, WriteClass, classify,
    deliveries_of, route,
};
use super::{Core, KafkaFeed, Ready};
use rdkafka::config::FromClientConfigAndContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode as C};
use spate_core::config::FeedSection;
use spate_core::error::ErrorClass;
use spate_core::feed::{Feed, FeedError, FeedErrorKind, Unit, confirmation, wake_channel};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

fn core(cap: u32) -> Core {
    let section: FeedSection = serde_yaml::from_str(&format!(
        "attempt_cap: {cap}\nkafka:\n  brokers: localhost:9092\n  group_id: workers\n  \
         topic: units\n  retry_topic: units-retry\n  dead_letter_topic: units-dead\n"
    ))
    .unwrap();
    Core::new(&KafkaFeedConfig::from_section(&section).unwrap())
}

fn tp(partition: i32) -> Tp {
    Tp::new(&Arc::from("units"), partition)
}

fn cap(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

/// The count a message with `header` as its `spate.deliveries` value is
/// delivered with.
fn delivered_with(header: u32) -> u32 {
    deliveries_of(Some(header.to_string().as_bytes()))
}

/// Pins the cap boundary on the header count: a message whose header is
/// `cap - 2` fails to the retry topic, one at `cap - 1` to the dead-letter
/// topic, and one past the cap, as after the cap was lowered, to the
/// dead-letter topic.
#[test]
fn a_unit_fails_to_the_dead_letter_topic_from_the_cap() {
    let cap = cap(5);
    assert_eq!(route(delivered_with(3), cap), Destination::Retry);
    assert_eq!(
        route(delivered_with(4), cap),
        Destination::DeadLetter(Cause::AttemptCap)
    );
    assert_eq!(
        route(delivered_with(9), cap),
        Destination::DeadLetter(Cause::AttemptCap)
    );
    // A cap of one dead-letters the first failure.
    assert_eq!(
        route(deliveries_of(None), NonZeroU32::MIN),
        Destination::DeadLetter(Cause::AttemptCap)
    );
}

/// Pins the count read from the header: absent is no failure, so the first
/// delivery counts 1, and a header that is not a number counts as absent.
#[test]
fn a_delivery_counts_the_reported_failures_plus_one() {
    assert_eq!(deliveries_of(None), 1);
    assert_eq!(deliveries_of(Some(b"4")), 5);
    assert_eq!(deliveries_of(Some(b"x")), 1);
    assert_eq!(deliveries_of(Some(&[0xff])), 1);
    assert_eq!(
        deliveries_of(Some(u32::MAX.to_string().as_bytes())),
        u32::MAX
    );
}

/// Pins what a message names: a UTF-8 key is the unit's id and the value
/// its body, with the count from the last `spate.deliveries` header. A
/// missing or non-UTF-8 key names no unit, and the message is kept as read
/// for the dead-letter topic.
#[test]
fn a_message_names_a_unit_by_its_key() {
    let headers = vec![
        ("other".to_owned(), Some(b"9".to_vec())),
        (DELIVERIES_HEADER.to_owned(), Some(b"7".to_vec())),
        (DELIVERIES_HEADER.to_owned(), Some(b"2".to_vec())),
    ];
    match read("units", Some(b"day-1"), Some(b"body"), headers) {
        Read::Unit { unit, deliveries } => {
            assert_eq!(unit, Unit::new("day-1", b"body".to_vec()));
            assert_eq!(deliveries, 3);
        }
        other => panic!("{other:?}"),
    }
    match read("units", Some(b"empty"), None, Vec::new()) {
        Read::Unit { unit, deliveries } => {
            assert!(unit.body.is_empty());
            assert_eq!(deliveries, 1);
        }
        other => panic!("{other:?}"),
    }
    for key in [None, Some(&[0xff, 0xfe][..])] {
        let headers = vec![("h".to_owned(), None)];
        match read("units-retry", key, Some(b"raw"), headers.clone()) {
            Read::NoUnit(out) => {
                assert_eq!(&*out.topic, "units-retry");
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

/// Pins that a message librdkafka reports as bad stops the feed, and that a
/// transport error does not.
#[test]
fn a_bad_message_stops_the_feed() {
    let bad = KafkaError::MessageConsumption(C::BadMessage);
    assert_eq!(poll_class(&bad, true, None), ErrorClass::Fatal);
    assert_eq!(poll_class(&bad, false, None), ErrorClass::Fatal);
    let down = KafkaError::MessageConsumption(C::AllBrokersDown);
    assert_eq!(poll_class(&down, true, None), ErrorClass::Retryable);
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
fn moving(core: &Core, receipt: spate_core::feed::Receipt) -> Box<Write> {
    let position = core.lock().tracker.release(receipt).unwrap();
    let unit = Unit::new("u", b"b".to_vec());
    Box::new(Write::Move(Move::new(
        Outgoing::unit(Arc::clone(&core.topics.retry), &unit, 1),
        position,
        Destination::Retry,
        Some(unit.id),
        1,
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

/// Pins that a resend is taken once due, and that a move whose partition
/// was given up since is dropped, its offset left for the next owner.
#[test]
fn a_resend_for_a_partition_given_up_is_dropped() {
    let core = core(10);
    let (kept, dropped) = {
        let mut st = core.lock();
        st.tracker.assign([tp(0), tp(1)]);
        (
            st.tracker.deliver(&tp(0), 1).unwrap(),
            st.tracker.deliver(&tp(1), 1).unwrap(),
        )
    };
    for receipt in [kept, dropped] {
        let write = moving(&core, receipt);
        core.settle(*write, Err(failure(C::MessageTimedOut)));
    }
    let mut st = core.lock();
    st.tracker.revoke(&[tp(1)]);
    let soon = std::time::Instant::now();
    assert!(take_due(&mut st, soon).is_empty(), "not due yet");
    let due = take_due(&mut st, soon + std::time::Duration::from_secs(1));
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
            st.ready.push_back(Ready {
                receipt,
                unit: Unit::new("u", Vec::new()),
                deliveries: 1,
            });
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
    assert_eq!(st.ready[0].receipt, spate_core::feed::Receipt(2));
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

/// Pins that the write a no-unit message and a failed unit become carry
/// the unit's key and value, and the count header only once a failure was
/// reported.
#[test]
fn a_unit_is_written_with_its_count() {
    let unit = Unit::new("day-1", b"body".to_vec());
    let first = Outgoing::unit(Arc::from("units"), &unit, 0);
    assert_eq!(first.key.as_deref(), Some(&b"day-1"[..]));
    assert_eq!(first.payload.as_deref(), Some(&b"body"[..]));
    assert!(first.headers.is_empty());
    let retried = Outgoing::unit(Arc::from("units-retry"), &unit, 3);
    assert_eq!(
        retried.headers,
        vec![(DELIVERIES_HEADER.to_owned(), Some(b"3".to_vec()))]
    );
    assert_eq!(
        deliveries_of(retried.headers[0].1.as_deref()),
        4,
        "read back, a third failure counts the fourth delivery"
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

/// A feed with no consumer thread, whose producer reaches no broker.
fn idle_feed() -> KafkaFeed {
    let section: FeedSection = serde_yaml::from_str(
        "kafka:\n  brokers: 127.0.0.1:1\n  group_id: workers\n  topic: units\n  \
         retry_topic: units-retry\n  dead_letter_topic: units-dead\n  statistics_interval: 0s\n",
    )
    .unwrap();
    let config = KafkaFeedConfig::from_section(&section).unwrap();
    let core = Arc::new(Core::new(&config));
    let producer = Producer::from_config_and_context(
        &config.producer_config(false),
        FeedProducerContext::new(Arc::clone(&core)),
    )
    .unwrap();
    KafkaFeed {
        core,
        producer,
        thread: None,
    }
}

/// Buffer one unit of partition 0 for `feed`'s reader.
fn buffer_one(feed: &KafkaFeed) {
    let mut st = feed.core.lock();
    st.tracker.assign([tp(0)]);
    let receipt = st.tracker.deliver(&tp(0), 0).unwrap();
    st.ready.push_back(Ready {
        receipt,
        unit: Unit::new("u", Vec::new()),
        deliveries: 1,
    });
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

mod cluster {
    //! The feed against rdkafka's in-process MockCluster, where a test reads
    //! the feed's buffer and ownership directly.

    use super::super::config::KafkaFeedConfig;
    use super::super::consumer::{self, Driver};
    use super::super::{KafkaFeed, READY_CAPACITY};
    use crate::context::Intent;
    use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    use rdkafka::types::RDKafkaRespErr;
    use rdkafka::{ClientConfig, Offset, TopicPartitionList};
    use spate_core::config::FeedSection;
    use spate_core::feed::{Feed, FeedErrorKind};
    use spate_test::wait_until;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    const DEADLINE: Duration = Duration::from_secs(60);

    /// Units produced to each of the work topic's two partitions, more than
    /// the buffer holds.
    const PER_PARTITION: usize = 2 * READY_CAPACITY;

    pub(super) fn section(brokers: &str, group: &str) -> FeedSection {
        serde_yaml::from_str(&format!(
            "kafka:\n  brokers: \"{brokers}\"\n  group_id: {group}\n  topic: units\n  \
             retry_topic: units-retry\n  dead_letter_topic: units-dead\n  \
             statistics_interval: 0s\n  rdkafka:\n    session.timeout.ms: \"6000\"\n    \
             heartbeat.interval.ms: \"2000\"\n"
        ))
        .unwrap()
    }

    fn connect(brokers: &str, group: &str, io: &tokio::runtime::Runtime) -> KafkaFeed {
        // The MockCluster answers no request for a topic's configuration.
        KafkaFeed::connect_with(
            &section(brokers, group),
            io.handle(),
            crate::feed::startup::Scope::Existence,
            crate::feed::startup::CONNECT_BOUND,
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
    /// dead-letter topics, and `n` messages keyed `u0`, `u1`, … on the work
    /// topic.
    fn cluster_with(n: usize) -> MockCluster<'static, rdkafka::producer::DefaultProducerContext> {
        let cluster = MockCluster::new(1).expect("mock cluster");
        for topic in ["units", "units-retry", "units-dead"] {
            cluster.create_topic(topic, 1, 1).unwrap();
        }
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

    /// The feed's thread state for `group`, with the consumer subscribed,
    /// driven by the test instead of a thread.
    pub(super) fn driver(brokers: &str, group: &str) -> Driver {
        let config = KafkaFeedConfig::from_section(&section(brokers, group)).unwrap();
        super::super::parts(&config).unwrap().2
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

    /// Run `f` on a thread of its own and report whether it returned
    /// within `limit`.
    fn returns_within(limit: Duration, f: impl FnOnce() + Send + 'static) -> bool {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            f();
            let _ = tx.send(());
        });
        rx.recv_timeout(limit).is_ok()
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
        let config = KafkaFeedConfig::from_section(&section(&brokers, &group)).unwrap();
        let (core, producer, mut d) = super::super::parts(&config).unwrap();
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

    /// Pins that stopping the feed commits the offsets finished since the
    /// last commit.
    #[test]
    fn stopping_commits_what_finished() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("stop-commit");
        let mut d = driver(&brokers, &group);
        drive(&mut d, "the unit is buffered", |d| {
            !d.core.lock().ready.is_empty()
        });
        acknowledge_first(&d);
        // Stopped before it starts, the thread runs no periodic commit.
        d.core.stop.store(true, Ordering::Release);
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
        let mut d = driver(&brokers, &group);
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
    }

    /// Pins that a rebalance error resumes the partitions the thread paused:
    /// once the partitions are assigned again, they are fetched.
    #[test]
    fn a_rebalance_error_resumes_what_was_paused() {
        let cluster = cluster_with(2 * READY_CAPACITY);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("lost-paused");
        let mut d = driver(&brokers, &group);
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
        assert!(d.core.lock().ready.is_empty(), "the buffer was dropped");
        drive(&mut d, "the reassigned partitions are fetched", |d| {
            !d.core.lock().ready.is_empty()
        });
    }

    /// Pins that a thread started on a latched feed ends without committing
    /// an offset finished before the latch.
    #[test]
    fn a_latched_feed_commits_nothing_more() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("latched");
        let mut d = driver(&brokers, &group);
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
    }

    /// Pins that a periodic commit made after a latch commits nothing.
    #[test]
    fn a_periodic_commit_after_a_latch_commits_nothing() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("latched-periodic");
        let mut d = driver(&brokers, &group);
        drive(&mut d, "the unit is buffered", |d| {
            !d.core.lock().ready.is_empty()
        });
        acknowledge_first(&d);
        d.core.latch("a test latch");
        d.commit_finished();
        assert_eq!(
            committed(&brokers, &group),
            None,
            "the latched feed committed"
        );
    }

    /// Pins that a revocation served after a latch commits nothing.
    #[test]
    fn a_latched_feed_commits_nothing_on_revocation() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("latched-revoke");
        let mut d = driver(&brokers, &group);
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
    }

    /// Pins that an offset read again while its earlier copy is still
    /// buffered replaces that copy: the buffer holds the new delivery alone,
    /// and acknowledging it finishes the offset.
    #[test]
    fn a_redelivered_offset_replaces_its_buffered_copy() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("redelivered");
        let mut d = driver(&brokers, &group);
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

    /// Pins that a commit the broker refuses is made again: the refused
    /// points go to the next commit, which lands them.
    #[test]
    fn a_commit_the_broker_refuses_is_made_again() {
        use rdkafka::types::RDKafkaApiKey;
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("refused-commit");
        let mut d = driver(&brokers, &group);
        drive(&mut d, "the unit is buffered", |d| {
            !d.core.lock().ready.is_empty()
        });
        acknowledge_first(&d);
        // A code librdkafka does not retry on its own.
        cluster.request_errors(
            RDKafkaApiKey::OffsetCommit,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE],
        );
        d.commit_finished();
        assert_eq!(
            committed(&brokers, &group),
            None,
            "the refused commit landed"
        );
        d.commit_finished();
        assert_eq!(committed(&brokers, &group), Some(1));
    }

    /// Pins that a message read for a partition the tracker gave up is
    /// dropped, and buffers nothing.
    #[test]
    fn a_message_for_a_partition_given_up_is_dropped() {
        let cluster = cluster_with(1);
        let brokers = cluster.bootstrap_servers();
        let group = spate_test::unique_name("given-up");
        let mut d = driver(&brokers, &group);
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
        assert!(d.core.lock().ready.is_empty());
    }
}

mod docker {
    //! The feed's thread state driven step by step against a real broker,
    //! for a rebalance the MockCluster cannot hold open: the mock refuses
    //! every commit while a member's join is in progress.

    use super::cluster::{acknowledge_first, committed, drive, driver, owns, section};
    use crate::feed::KafkaFeed;
    use rdkafka::ClientConfig;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
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
        let topics: Vec<NewTopic<'_>> = ["units", "units-retry", "units-dead"]
            .iter()
            .map(|t| NewTopic::new(t, 1, TopicReplication::Fixed(1)))
            .collect();
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
        producer.flush(std::time::Duration::from_secs(30)).unwrap();
        let group = spate_test::unique_name("revoke-order");

        let mut a = driver(&brokers, &group);
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
    }
}
