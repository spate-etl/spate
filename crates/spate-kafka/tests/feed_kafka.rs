//! The Kafka feed against a real broker, for what the MockCluster does not
//! model: a member that dies is evicted at the session timeout, where the
//! mock evicts an unpolled member on a timer of its own.
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
use spate_test::{WriteOutcome, wait_until};
use spate_test_support::ChildTest;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use support::broker;
use support::feed::{
    DEAD, DEADLINE, RETRY, WORK, committed, data_dir, feed_yaml, launch, produce_raw, recs, sorted,
    stage, written,
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

/// The `feed:` section both workers run, at the session timeout above.
fn section(brokers: &str, group: &str) -> String {
    feed_yaml(brokers, group, "", "").replace(
        "session.timeout.ms: \"6000\"",
        &format!("session.timeout.ms: \"{}\"", SESSION.as_millis()),
    )
}

/// Create the work, retry and dead-letter topics, one partition each. The
/// feed never creates a topic.
fn create_topics(brokers: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let topics: Vec<NewTopic<'_>> = [WORK, RETRY, DEAD]
        .iter()
        .map(|t| NewTopic::new(t, 1, TopicReplication::Fixed(1)))
        .collect();
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
    let a = launch(&data, &section(&brokers, &group), |sink| {
        sink.enqueue_global(WriteOutcome::ok().held());
    });
    wait_until(DEADLINE, "A holds the unit's write", || {
        a.script.held_writes() == 1
    });
    std::fs::write(&marker, b"held").unwrap();
    loop {
        std::thread::park();
    }
}

/// Pins that a unit whose worker dies mid-read returns after the session
/// timeout: worker A, a child process, is killed with SIGKILL while it holds
/// the unit's write, and worker B, which joins after the kill, reads the
/// unit from its start, writes every row and commits its offset. The kill
/// reports nothing, so B reads the unit as its first counted delivery.
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
    let b = launch(&data, &section(&brokers, &group), |_| {});
    wait_until(SESSION * 5, "B reads the unit", || {
        !b.recording.seen.lock().unwrap().is_empty()
    });
    let waited = killed.elapsed();
    assert_eq!(b.recording.seen.lock().unwrap()[0], 1);
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
    assert_eq!(b.stop(), ExitState::Completed);
}
