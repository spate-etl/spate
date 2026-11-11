//! Dropping the Kafka feed while a revocation waits on its group member's
//! queue and the member holds no partition. A binary of its own: the test
//! installs a process-global log subscriber, which holds the feed's consumer
//! thread at a chosen line.

mod support;

use rdkafka::types::RDKafkaApiKey;
use spate_core::feed::Feed as _;
use spate_core::task::spawn_thread;
use spate_kafka::feed::KafkaFeed;
use spate_test::{HANG, wait_until};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::Duration;
use support::feed::{ATTEMPT, ATTEMPT_PARTITIONS, DEAD, RETRY, WORK, section};
use support::{Joiner, MockKafka};

/// The line the feed's consumer thread writes on each assignment it accepts,
/// after the poll that brought it.
const ACCEPTED: &str = "the Kafka feed accepted an assignment";

/// librdkafka's line as the consumer's close starts.
const CLOSING: &str = "Closing consumer";

/// librdkafka's line as its main thread takes the `unassign` that answers a
/// revocation.
const ANSWERED: &str = "received op ASSIGN in state up (join-state wait-unassign-call)";

/// librdkafka's line as the group answers the member's leave. A member that
/// rejoins after the `unassign` holds its leave behind the join, and the leave
/// times out instead.
const LEFT: &str = "LeaveGroup response received";

/// A log writer that keeps the lines the feed's consumer thread writes, and
/// holds that thread at its first [`ACCEPTED`] line until the test releases
/// it.
#[derive(Clone, Default)]
struct Hold {
    lines: Arc<Mutex<Vec<String>>>,
    /// Whether the thread is held, and whether the test released it.
    state: Arc<(Mutex<(bool, bool)>, Condvar)>,
}

impl Hold {
    fn await_held(&self) {
        wait_until(HANG, "the consumer thread held at its assignment", || {
            self.state.0.lock().unwrap().0
        });
    }

    fn release(&self) {
        self.state.0.lock().unwrap().1 = true;
        self.state.1.notify_all();
    }

    /// The index of the first consumer-thread line containing `text`.
    fn first(&self, text: &str) -> Option<usize> {
        self.lines
            .lock()
            .unwrap()
            .iter()
            .position(|l| l.contains(text))
    }
}

impl std::io::Write for Hold {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if std::thread::current().name() != Some("spate-kafka-feed") {
            return Ok(buf.len());
        }
        let line = String::from_utf8_lossy(buf).into_owned();
        let accepted = line.contains(ACCEPTED);
        self.lines.lock().unwrap().push(line);
        let (lock, released) = &*self.state;
        let mut state = lock.lock().unwrap();
        if accepted && !state.0 {
            state.0 = true;
            while !state.1 {
                state = released.wait(state).unwrap();
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Hold {
    type Writer = Hold;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Pins that the feed's drop answers a revocation queued after the consumer
/// thread's last poll before the consumer's close starts, that the member
/// then leaves the group without rejoining, and that the drop returns. Left
/// for the close, the revocation of a member that holds no partition can meet
/// a terminated group, and its `unassign` then never returns. The test leads
/// the group to give the feed's member nothing, and holds the consumer thread
/// after its last poll while a new member starts a rebalance. Regression for
/// #1698.
#[test]
fn the_drop_answers_a_revocation_queued_after_the_last_poll() {
    let hold = Hold::default();
    tracing_subscriber::fmt()
        .with_writer(hold.clone())
        .with_max_level(tracing::Level::DEBUG)
        .without_time()
        .init();

    let cluster = MockKafka::new();
    for topic in [WORK, RETRY, DEAD] {
        cluster.create_topic(topic, 1, 1).unwrap();
    }
    cluster
        .create_topic(ATTEMPT, ATTEMPT_PARTITIONS, 1)
        .unwrap();
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("drop-queued-revoke");
    let leader = cluster.lead_group_with(&group, WORK, |_, _| Vec::new());
    // The mock answers a rebalance 44 s after a member joins. librdkafka's
    // group lines carry the close and the revocation's answer.
    let section = section(&format!(
        "feed:\n  kafka:\n    brokers: \"{brokers}\"\n    group_id: {group}\n    \
         topic: {WORK}\n    retry_topic: {RETRY}\n    dead_letter_topic: {DEAD}\n    \
         attempt_topic: {ATTEMPT}\n    statistics_interval: 0s\n    rdkafka:\n      \
         session.timeout.ms: \"45000\"\n      heartbeat.interval.ms: \"1000\"\n      \
         debug: consumer,cgrp\n"
    ));
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let feed = KafkaFeed::connect_without_config_checks(&section, io.handle()).unwrap();
    hold.await_held();
    {
        let lines = hold.lines.lock().unwrap();
        assert!(
            lines
                .iter()
                .any(|l| l.contains(ACCEPTED) && l.contains("partitions=0")),
            "{lines:#?}"
        );
    }
    let closed = feed.close(Duration::from_secs(20));

    // A new member starts a rebalance. The leader heartbeats once more at
    // most before it rejoins, and the new member sends none while it joins,
    // so two of three heartbeats after its join are the feed member's. That
    // member sends the second only once it has handled the answer to the
    // first, which queued the revocation.
    let before = cluster.received_keys().len();
    let joiner = Joiner::join_group(&brokers, WORK, &group);
    wait_until(HANG, "the feed's member heard of the rebalance", || {
        let keys = cluster.received_keys();
        let since = &keys[before..];
        since
            .iter()
            .position(|k| *k == RDKafkaApiKey::JoinGroup as i16)
            .is_some_and(|join| {
                since[join + 1..]
                    .iter()
                    .filter(|k| **k == RDKafkaApiKey::Heartbeat as i16)
                    .count()
                    >= 3
            })
    });
    hold.release();

    let (dropped, on_drop) = mpsc::channel();
    let dropping = spawn_thread("drop-feed", move || {
        drop(feed);
        let _ = dropped.send(());
    })
    .unwrap();
    if on_drop.recv_timeout(HANG).is_err() {
        // The hung drop holds the feed's clients, which the cluster's drop
        // would wait on.
        std::mem::forget((dropping, cluster, leader, joiner, io, closed));
        panic!("dropping the feed did not return");
    }
    dropping.join().unwrap();
    let (answered, closing) = (hold.first(ANSWERED), hold.first(CLOSING));
    assert!(
        answered.is_some_and(|a| closing.is_some_and(|c| a < c)),
        "the revocation was not answered before the close: {:#?}",
        hold.lines.lock().unwrap()
    );
    assert!(
        hold.first(LEFT).is_some(),
        "the group did not answer the member's leave: {:#?}",
        hold.lines.lock().unwrap()
    );
    drop(joiner);
    drop(leader);
}
