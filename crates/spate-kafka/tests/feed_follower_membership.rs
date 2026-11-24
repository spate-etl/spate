//! The Kafka feed's attempt follower beside the feed's own member of the
//! group, on a real broker. A binary of its own: the test installs a
//! process-global log subscriber, which the feed's threads log through.
//!
//! The test needs Docker and is `#[ignore]`d. Run it with:
//!
//! ```sh
//! cargo nextest run --profile docker -p spate-kafka --all-features --locked \
//!   --run-ignored ignored-only --test feed_follower_membership
//! ```

mod support;

use spate_core::feed::{Feed, Unit, wake_channel};
use spate_kafka::feed::KafkaFeed;
use spate_test::{HANG, LogCapture, wait_for_value, wait_until};
use spate_test_support::SingleBroker;
use std::time::{Duration, Instant};
use support::broker;
use support::feed::{WORK, committed_once, create_topics, feed_yaml, publish, section};

/// The member's session timeout, as the harness's section sets it.
const SESSION: Duration = Duration::from_secs(6);

/// The line the feed's consumer logs on each assignment it accepts.
const ASSIGNED: &str = "the Kafka feed accepted an assignment";

fn assignments(capture: &LogCapture) -> usize {
    capture
        .lines()
        .iter()
        .filter(|line| line.contains(ASSIGNED))
        .count()
}

/// Pins that the attempt follower, an assign-only consumer in the feed's
/// group, leaves the feed's member alone: over twice the session timeout
/// of units read and acknowledged one at a time, the member accepts one
/// assignment, every unit is read with 1, and the group has one member.
#[test]
#[ignore = "requires Docker"]
fn the_attempt_follower_leaves_the_feed_members_assignment_alone() {
    let capture = LogCapture::install_global(tracing::Level::INFO);
    let kafka = SingleBroker::start(broker()).expect("start kafka container");
    let brokers = kafka.bootstrap().to_owned();
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    create_topics(&brokers);

    let group = spate_test::unique_name("membership");
    let feed = KafkaFeed::connect(&section(&feed_yaml(&brokers, &group, "", "")), io.handle())
        .expect("connect");
    let connected = Instant::now();
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    wait_until(HANG, "the member accepted an assignment", || {
        assignments(&capture) >= 1
    });

    let mut seen = Vec::new();
    // Elapsed time is the property: the follower stays beside the member
    // for longer than three heartbeat intervals and the broker's initial
    // rebalance delay.
    while connected.elapsed() < SESSION * 2 {
        let n = seen.len();
        publish(&feed, Unit::new(format!("u{n}"), b"body".to_vec()));
        let mut delivery = None;
        wait_until(HANG, "the unit is delivered", || {
            delivery = feed.receive(1).ok().and_then(|mut got| got.pop());
            if delivery.is_none() {
                park.park(Duration::from_millis(100));
            }
            delivery.is_some()
        });
        let delivery = delivery.unwrap();
        seen.push(delivery.deliveries);
        feed.acknowledge(delivery.receipt).unwrap();
        let next = i64::try_from(n + 1).unwrap();
        wait_for_value(
            HANG,
            "the unit's offset is committed",
            || committed_once(&brokers, &group, WORK),
            |read| *read == Ok(Some(next)),
        )
        .expect("the wait ends on a read");
    }

    assert_eq!(assignments(&capture), 1, "{:#?}", capture.lines());
    assert!(seen.iter().all(|&count| count == 1), "{seen:?}");
    let groups = support::group_listing(&brokers, &group, "the group listing", |_| true);
    let members: Vec<_> = groups.iter().map(|g| g.members).collect();
    assert_eq!(members, [1], "the group's members");
}
