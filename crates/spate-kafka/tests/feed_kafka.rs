//! The Kafka feed against a real broker, for what the MockCluster does not
//! model: a member that dies is evicted at the session timeout, where the
//! mock evicts an unpolled member on a timer of its own, a topic's
//! configuration, which the mock does not describe, and a confirmed write's
//! place below the high watermark.
//!
//! Every test needs Docker and is `#[ignore]`d. Run them with:
//!
//! ```sh
//! cargo nextest run --profile docker -p spate-kafka --all-features --locked \
//!   --run-ignored ignored-only --test feed_kafka
//! ```

mod support;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use spate_core::pipeline::ExitState;
use spate_kafka::feed::{KafkaFeed, KafkaFeedError, KafkaFeedPublisher};
use spate_test::{WriteOutcome, wait_until};
use spate_test_support::ChildTest;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use support::broker;
use support::feed::{
    ATTEMPT, ATTEMPT_PARTITIONS, DEAD, DEADLINE, WORK, committed, create_topics, data_dir, exists,
    feed_yaml, high_watermark, launch_with, produce_raw, read_attempts, recs, sorted, stage,
    written,
};
use testcontainers::runners::SyncRunner;
use testcontainers_modules::kafka::apache::KAFKA_PORT;

/// The members' session timeout. A member that dies holds its partitions
/// for this long after its last heartbeat.
const SESSION: Duration = Duration::from_secs(6);

/// The members' heartbeat interval, as the harness's section sets it. A
/// member's last heartbeat comes at most this long before it dies.
const HEARTBEAT: Duration = Duration::from_secs(2);

/// The variables the parent hands the killed worker.
const CHILD_BROKERS: &str = "SPATE_FEED_CHILD_BROKERS";
const CHILD_GROUP: &str = "SPATE_FEED_CHILD_GROUP";
const CHILD_DATA: &str = "SPATE_FEED_CHILD_DATA";
const CHILD_MARKER: &str = "SPATE_FEED_CHILD_MARKER";
/// The attempt cap the killed worker runs at, when the parent sets one.
const CHILD_CAP: &str = "SPATE_FEED_CHILD_CAP";

/// The `feed:` section both workers run, at the session timeout above.
fn section(brokers: &str, group: &str) -> String {
    section_with(brokers, group, "")
}

/// [`section`] with `extra` added to the section, beside `kafka:`.
fn section_with(brokers: &str, group: &str, extra: &str) -> String {
    feed_yaml(brokers, group, extra, "").replace(
        "session.timeout.ms: \"6000\"",
        &format!("session.timeout.ms: \"{}\"", SESSION.as_millis()),
    )
}

/// The child half of `a_unit_whose_worker_is_killed_mid_read_is_delivered_again`:
/// worker A reads the unit, holds its first write, writes the marker file and
/// waits to be killed. Without the parent's variables it does nothing.
#[test]
#[ignore = "requires Docker"]
fn worker_a_holds_a_unit_until_it_is_killed() {
    let Ok(brokers) = std::env::var(CHILD_BROKERS) else {
        return;
    };
    let group = std::env::var(CHILD_GROUP).unwrap();
    let data = PathBuf::from(std::env::var_os(CHILD_DATA).unwrap());
    let marker = PathBuf::from(std::env::var_os(CHILD_MARKER).unwrap());
    let cap = std::env::var(CHILD_CAP)
        .map_or_else(|_| String::new(), |cap| format!("  attempt_cap: {cap}\n"));
    let a = launch_with(
        KafkaFeed::connect,
        &data,
        &section_with(&brokers, &group, &cap),
        |sink| {
            sink.enqueue_global(WriteOutcome::ok().held());
        },
    );
    wait_until(DEADLINE, "A holds the unit's write", || {
        a.script.held_writes() == 1
    });
    std::fs::write(&marker, b"held").unwrap();
    loop {
        std::thread::park();
    }
}

/// Pins that a unit whose worker dies mid-read returns after the session
/// timeout, and that the read it died in counts: worker A, a child process,
/// is killed with SIGKILL while it holds the unit's write, and worker B,
/// which joins after the kill, reads the unit from its start with 2, writes
/// every row and commits its offset. The attempt record holds 1, then 2,
/// then a tombstone.
#[test]
#[ignore = "requires Docker"]
fn a_unit_whose_worker_is_killed_mid_read_is_delivered_again() {
    let container = broker().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");
    create_topics(&brokers);
    let group = spate_test::unique_name("killed");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let marker = dir.path().join("held");
    let rows = recs("k", 3);
    let unit = stage(&data, "k.ndjson", &rows);
    // As a publish writes it: the id as the key, the body as the value.
    produce_raw(&brokers, WORK, &[(Some(unit.id.as_bytes()), &unit.body)]);

    let a = ChildTest::spawn("worker_a_holds_a_unit_until_it_is_killed", |child| {
        child
            .arg("--ignored")
            .env(CHILD_BROKERS, &brokers)
            .env(CHILD_GROUP, &group)
            .env(CHILD_DATA, &data)
            .env(CHILD_MARKER, &marker)
    });
    wait_until(DEADLINE, "A holds the unit's write", || marker.exists());
    let status = a.kill();
    let killed = Instant::now();
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(9), "{status:?}");
    }

    // B's join waits for A's session to lapse, and B is then assigned the
    // partition at the offset A never committed past.
    let b = launch_with(
        KafkaFeed::connect,
        &data,
        &section(&brokers, &group),
        |_| {},
    );
    wait_until(SESSION * 5, "B reads the unit", || {
        !b.recording.seen.lock().unwrap().is_empty()
    });
    let waited = killed.elapsed();
    assert_eq!(b.recording.seen.lock().unwrap()[0], 2);
    assert!(
        waited >= SESSION - HEARTBEAT,
        "B read the unit {waited:?} after A was killed, inside A's session"
    );
    wait_until(
        DEADLINE,
        "B writes every row and commits the unit's offset",
        || {
            written(&b.script) == sorted(rows.clone())
                && committed(&brokers, &group, WORK) == Some(1)
        },
    );
    // The unit's identity, `units/0/0`, hashes to partition 1 of 2.
    let records: Vec<(i32, Option<u32>)> = read_attempts(&brokers, ATTEMPT_PARTITIONS, 3)
        .into_iter()
        .map(|(partition, _, count)| (partition, count))
        .collect();
    assert_eq!(records, [(1, Some(1)), (1, Some(2)), (1, None)]);
    assert_eq!(b.stop(), ExitState::Completed);
}

/// Pins that a unit that kills its worker on every read reaches the
/// dead-letter topic: at `attempt_cap: 2`, two children are each killed
/// with SIGKILL while they hold the unit, and worker B, which joins after,
/// moves the unit to the dead-letter topic without reading it and commits
/// its offset.
#[test]
#[ignore = "requires Docker"]
fn a_unit_that_kills_its_worker_at_each_read_reaches_the_dead_letter_topic() {
    let container = broker().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");
    create_topics(&brokers);
    let group = spate_test::unique_name("killed-twice");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let unit = stage(&data, "k.ndjson", &recs("k", 3));
    produce_raw(&brokers, WORK, &[(Some(unit.id.as_bytes()), &unit.body)]);

    for read in 1..=2 {
        let marker = dir.path().join(format!("held-{read}"));
        let child = ChildTest::spawn("worker_a_holds_a_unit_until_it_is_killed", |child| {
            child
                .arg("--ignored")
                .env(CHILD_BROKERS, &brokers)
                .env(CHILD_GROUP, &group)
                .env(CHILD_DATA, &data)
                .env(CHILD_MARKER, &marker)
                .env(CHILD_CAP, "2")
        });
        wait_until(SESSION * 5, &format!("child {read} holds the unit"), || {
            marker.exists()
        });
        child.kill();
    }

    let b = launch_with(
        KafkaFeed::connect,
        &data,
        &section_with(&brokers, &group, "  attempt_cap: 2\n"),
        |_| {},
    );
    wait_until(
        SESSION * 5,
        "B dead-letters the unit and commits its offset",
        || high_watermark(&brokers, DEAD) == 1 && committed(&brokers, &group, WORK) == Some(1),
    );
    assert!(
        b.recording.seen.lock().unwrap().is_empty(),
        "B read the unit"
    );
    assert_eq!(b.stop(), ExitState::Completed);
}

/// Create each topic of `topics`, one partition each, with its
/// configuration entries.
fn create_configured(brokers: &str, topics: &[(String, Vec<(&str, &str)>)]) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let new: Vec<NewTopic<'_>> = topics
        .iter()
        .map(|(name, entries)| {
            entries.iter().fold(
                NewTopic::new(name, 1, TopicReplication::Fixed(1)),
                |topic, (k, v)| topic.set(k, v),
            )
        })
        .collect();
    let io = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for created in io
        .block_on(admin.create_topics(&new, &AdminOptions::new()))
        .expect("create topics")
    {
        created.expect("topic created");
    }
}

/// The retry, dead-letter and attempt topics' names for the work topic
/// `work`.
fn retry_of(work: &str) -> String {
    format!("{work}-retry")
}

fn dead_of(work: &str) -> String {
    format!("{work}-dead")
}

fn attempts_of(work: &str) -> String {
    format!("{work}-attempts")
}

/// The attempt topic of `work`, compacted.
fn compacted_attempts(work: &str) -> (String, Vec<(&'static str, &'static str)>) {
    (attempts_of(work), vec![("cleanup.policy", "compact")])
}

/// Connect a feed on the work topic `work` and its retry, dead-letter and
/// attempt topics, on a runtime like the pipeline's.
fn connect_on(brokers: &str, work: &str) -> Result<KafkaFeed, KafkaFeedError> {
    let section: spate_core::config::FeedSection = serde_yaml::from_str(&format!(
        "kafka:\n  brokers: \"{brokers}\"\n  group_id: {group}\n  topic: {work}\n  \
         retry_topic: {retry}\n  dead_letter_topic: {dead}\n  attempt_topic: {attempts}\n  \
         statistics_interval: 0s\n",
        group = spate_test::unique_name("checked"),
        retry = retry_of(work),
        dead = dead_of(work),
        attempts = attempts_of(work),
    ))
    .unwrap();
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    KafkaFeed::connect(&section, io.handle())
}

fn refusal(connected: Result<KafkaFeed, KafkaFeedError>) -> String {
    match connected {
        Err(KafkaFeedError::Refused(message)) => message,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// Pins that connecting refuses a missing work, retry or dead-letter topic
/// by its role, on a broker that creates a topic on first use, and leaves
/// the topic missing.
#[test]
#[ignore = "requires Docker"]
fn startup_refuses_a_missing_retry_or_dead_letter_topic() {
    let container = broker().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");
    for (work, role, missing) in [
        ("no-retry", "retry", retry_of("no-retry")),
        ("no-dead", "dead-letter", dead_of("no-dead")),
        ("no-work", "work", "no-work".to_owned()),
    ] {
        let mut present: Vec<(String, Vec<(&str, &str)>)> =
            [work.to_owned(), retry_of(work), dead_of(work)]
                .into_iter()
                .filter(|t| *t != missing)
                .map(|t| (t, vec![]))
                .collect();
        present.push(compacted_attempts(work));
        create_configured(&brokers, &present);
        let message = refusal(connect_on(&brokers, work));
        assert!(
            message.contains(&format!("the {role} topic `{missing}` does not exist")),
            "{message}"
        );
        assert!(
            !exists(&brokers, &missing),
            "connecting created `{missing}`"
        );
    }
}

/// Pins that connecting refuses a work, retry or dead-letter topic whose
/// `cleanup.policy` includes `compact`, by its role, and that topics at
/// `delete` pass, with each topic's policy in the line logged at connect.
#[test]
#[ignore = "requires Docker"]
fn startup_refuses_a_compacted_work_retry_or_dead_letter_topic() {
    let container = broker().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");
    for (work, role, policy) in [
        ("compacted-work", "work", "compact"),
        ("compacted-retry", "retry", "compact,delete"),
        ("compacted-dead", "dead-letter", "compact"),
    ] {
        let compacted = match role {
            "work" => work.to_owned(),
            "retry" => retry_of(work),
            _ => dead_of(work),
        };
        let mut topics: Vec<(String, Vec<(&str, &str)>)> =
            [work.to_owned(), retry_of(work), dead_of(work)]
                .into_iter()
                .map(|t| {
                    let mut entries = vec![];
                    if t == compacted {
                        entries.push(("cleanup.policy", policy));
                    }
                    (t, entries)
                })
                .collect();
        topics.push(compacted_attempts(work));
        create_configured(&brokers, &topics);
        let message = refusal(connect_on(&brokers, work));
        assert!(
            message.contains(&format!("the {role} topic `{compacted}` is compacted")),
            "{message}"
        );
    }

    let work = "deleting";
    create_configured(
        &brokers,
        &[
            (
                work.to_owned(),
                vec![("message.timestamp.type", "LogAppendTime")],
            ),
            (retry_of(work), vec![]),
            (dead_of(work), vec![("cleanup.policy", "delete")]),
            compacted_attempts(work),
        ],
    );
    let mut connected = None;
    let lines = spate_test::capture_logs(tracing::Level::INFO, || {
        connected = Some(connect_on(&brokers, work));
    });
    connected.unwrap().unwrap();
    let line = lines
        .iter()
        .find(|l| l.contains("the Kafka feed started"))
        .unwrap_or_else(|| panic!("no connect line: {lines:#?}"));
    for field in [
        " cleanup_policy=delete",
        " retry_cleanup_policy=delete",
        " dead_letter_cleanup_policy=delete",
        " attempt_cleanup_policy=compact",
        " timestamp_type=LogAppendTime",
        " attempt_partitions=1",
    ] {
        assert!(line.contains(field), "{field}: {line}");
    }
}

/// Pins that connecting refuses a work topic created with the broker's
/// default `message.timestamp.type`, naming the topic, and that a work topic
/// stamped on append connects, with its timestamp type in the line logged at
/// connect.
#[test]
#[ignore = "requires Docker"]
fn startup_refuses_a_notification_topic_without_log_append_time() {
    let container = broker().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");
    let topics_for = |work: &str, entries: Vec<(&'static str, &'static str)>| {
        vec![
            (work.to_owned(), entries),
            (retry_of(work), vec![]),
            (dead_of(work), vec![]),
            compacted_attempts(work),
        ]
    };

    let work = "stamped-by-producer";
    create_configured(&brokers, &topics_for(work, vec![]));
    let message = refusal(connect_on(&brokers, work));
    assert!(
        message.contains(&format!(
            "the work topic `{work}` keeps each message's own timestamp"
        )),
        "{message}"
    );
    assert!(
        message.contains("message.timestamp.type=LogAppendTime"),
        "{message}"
    );

    let work = "stamped-on-append";
    create_configured(
        &brokers,
        &topics_for(work, vec![("message.timestamp.type", "LogAppendTime")]),
    );
    let mut connected = None;
    let lines = spate_test::capture_logs(tracing::Level::INFO, || {
        connected = Some(connect_on(&brokers, work));
    });
    connected.unwrap().unwrap();
    let line = lines
        .iter()
        .find(|l| l.contains("the Kafka feed started"))
        .unwrap_or_else(|| panic!("no connect line: {lines:#?}"));
    assert!(line.contains(" timestamp_type=LogAppendTime"), "{line}");
}

/// Pins that connecting refuses a missing attempt topic by its role, and
/// an attempt topic whose `cleanup.policy` is anything but `compact`,
/// naming the value: `delete`, the broker's default, and `compact,delete`.
#[test]
#[ignore = "requires Docker"]
fn startup_refuses_a_missing_or_uncompacted_attempt_topic() {
    let container = broker().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");
    for (work, policy, needle) in [
        ("no-attempts", None, "does not exist".to_owned()),
        (
            "default-attempts",
            Some(None),
            "`cleanup.policy` is `delete`".to_owned(),
        ),
        (
            "mixed-attempts",
            Some(Some("compact,delete")),
            "`cleanup.policy` is `compact,delete`".to_owned(),
        ),
    ] {
        let mut topics: Vec<(String, Vec<(&str, &str)>)> = vec![
            (work.to_owned(), vec![]),
            (retry_of(work), vec![]),
            (dead_of(work), vec![]),
        ];
        match policy {
            None => {}
            Some(None) => topics.push((attempts_of(work), vec![])),
            Some(Some(policy)) => {
                topics.push((attempts_of(work), vec![("cleanup.policy", policy)]));
            }
        }
        create_configured(&brokers, &topics);
        let message = refusal(connect_on(&brokers, work));
        assert!(
            message.contains(&format!("the attempt topic `{}`", attempts_of(work))),
            "{message}"
        );
        assert!(message.contains(&needle), "{message}");
        assert!(
            !exists(&brokers, &attempts_of(work)) || policy.is_some(),
            "connecting created the attempt topic"
        );
    }
}

/// Pins the read-your-write the gate relies on: once an `acks=all` write's
/// report confirms it, a watermark read puts the high watermark past its
/// offset, on each of 200 writes.
#[test]
#[ignore = "requires Docker"]
fn a_confirmed_write_is_below_the_next_high_watermark() {
    use rdkafka::producer::{FutureProducer, FutureRecord};
    let container = broker().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");
    create_topics(&brokers);
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .create()
        .expect("producer");
    let io = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for i in 0..200 {
        let key = format!("units/0/{i}");
        let delivered = io
            .block_on(
                producer.send(
                    FutureRecord::to(ATTEMPT)
                        .partition(i % ATTEMPT_PARTITIONS)
                        .key(&key)
                        .payload(r#"{"v":1,"attempts":1}"#),
                    Duration::from_secs(30),
                ),
            )
            .map_err(|(e, _)| e)
            .expect("confirmed");
        let (_, hwm) = rdkafka::producer::Producer::client(&producer)
            .fetch_watermarks(ATTEMPT, delivered.partition, Duration::from_secs(10))
            .expect("watermarks");
        assert!(
            hwm > delivered.offset,
            "write {i}: offset {} confirmed, high watermark {hwm}",
            delivered.offset
        );
    }
}

/// Create the work topic `work` with `entries` and its retry, dead-letter
/// and compacted attempt topics, one partition each.
fn create_topics_of(brokers: &str, work: &str, entries: &[(&str, &str)]) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let retry = format!("{work}-retry");
    let dead = format!("{work}-dead");
    let attempts = format!("{work}-attempts");
    let topics = [
        entries.iter().fold(
            NewTopic::new(work, 1, TopicReplication::Fixed(1)),
            |topic, (k, v)| topic.set(k, v),
        ),
        NewTopic::new(&retry, 1, TopicReplication::Fixed(1)),
        NewTopic::new(&dead, 1, TopicReplication::Fixed(1)),
        NewTopic::new(&attempts, 1, TopicReplication::Fixed(1)).set("cleanup.policy", "compact"),
    ];
    let io = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for created in io
        .block_on(admin.create_topics(&topics, &AdminOptions::new()))
        .expect("create topics")
    {
        created.expect("topic created");
    }
}

/// Pins that `KafkaFeedPublisher::connect` runs the configuration checks
/// under the connect bound: it refuses a compacted work topic by its role,
/// and connects and publishes on topics a worker accepts.
#[test]
#[ignore = "requires Docker"]
fn the_publisher_runs_the_configuration_checks_at_connect() {
    let container = broker().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let connect = |work: &str| {
        let section: spate_core::config::FeedSection = serde_yaml::from_str(&format!(
            "kafka:\n  brokers: \"{brokers}\"\n  group_id: {group}\n  topic: {work}\n  \
             retry_topic: {work}-retry\n  dead_letter_topic: {work}-dead\n  \
             attempt_topic: {work}-attempts\n  statistics_interval: 0s\n",
            group = spate_test::unique_name("publisher"),
        ))
        .unwrap();
        KafkaFeedPublisher::connect(&section, io.handle())
    };

    create_topics_of(&brokers, "pub-compacted", &[("cleanup.policy", "compact")]);
    match connect("pub-compacted") {
        Err(KafkaFeedError::Refused(message)) => assert!(
            message.contains("the work topic `pub-compacted` is compacted"),
            "{message}"
        ),
        other => panic!("expected a refusal, got {other:?}"),
    }

    create_topics_of(
        &brokers,
        "pub-good",
        &[("message.timestamp.type", "LogAppendTime")],
    );
    let publisher = connect("pub-good").expect("connect");
    support::feed::publish_to(
        &publisher,
        spate_core::feed::Unit::new("u", b"body".to_vec()),
    );
}
