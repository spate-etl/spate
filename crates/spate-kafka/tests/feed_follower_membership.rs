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

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use spate_core::feed::{Feed, Unit, wake_channel};
use spate_kafka::feed::KafkaFeed;
use spate_test::{LogCapture, wait_until};
use std::time::{Duration, Instant};
use support::broker;
use support::feed::{ATTEMPT, DEAD, DEADLINE, RETRY, WORK, committed, feed_yaml, publish, section};
use testcontainers::runners::SyncRunner;
use testcontainers_modules::kafka::apache::KAFKA_PORT;

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
    let capture = LogCapture::new();
    tracing_subscriber::fmt()
        .with_writer(capture.clone())
        .with_max_level(tracing::Level::INFO)
        .without_time()
        .init();
    let container = broker().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("admin client");
    let mut topics = vec![
        NewTopic::new(WORK, 1, TopicReplication::Fixed(1))
            .set("message.timestamp.type", "LogAppendTime"),
        NewTopic::new(RETRY, 1, TopicReplication::Fixed(1)),
        NewTopic::new(DEAD, 1, TopicReplication::Fixed(1)),
    ];
    topics.push(
        NewTopic::new(ATTEMPT, 2, TopicReplication::Fixed(1)).set("cleanup.policy", "compact"),
    );
    for created in io
        .block_on(admin.create_topics(&topics, &AdminOptions::new()))
        .expect("create topics")
    {
        created.expect("topic created");
    }

    let group = spate_test::unique_name("membership");
    let feed = KafkaFeed::connect(&section(&feed_yaml(&brokers, &group, "", "")), io.handle())
        .expect("connect");
    let connected = Instant::now();
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    wait_until(DEADLINE, "the member accepted an assignment", || {
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
        wait_until(DEADLINE, "the unit is delivered", || {
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
        wait_until(DEADLINE, "the unit's offset is committed", || {
            committed(&brokers, &group, WORK) == Some(next)
        });
    }

    assert_eq!(assignments(&capture), 1, "{:#?}", capture.lines());
    assert!(seen.iter().all(|&count| count == 1), "{seen:?}");
    let probe: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("probe");
    let groups = probe
        .fetch_group_list(Some(&group), Duration::from_secs(10))
        .expect("group list");
    let members: Vec<_> = groups.groups().iter().map(|g| g.members().len()).collect();
    assert_eq!(members, [1], "the group's members");
}
