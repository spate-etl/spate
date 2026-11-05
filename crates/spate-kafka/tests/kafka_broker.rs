//! Integration tests against a real Kafka broker (Docker via
//! testcontainers). Run explicitly:
//!
//! ```sh
//! cargo test -p spate-kafka --test kafka_broker -- --ignored
//! ```

mod support;

use support::Raw as _;

use rdkafka::consumer::{BaseConsumer, Consumer};
use spate_core::checkpoint::Checkpointer;
use spate_core::record::PartitionId;
use spate_core::source::{Source, SourceCtx, SourceEvent, SourceLane};
use spate_kafka::{KafkaSource, KafkaSourceConfig};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use support::{
    Transaction, Watched, await_assignment, broker, drain_lane, produce, produce_transaction,
    transactional_broker,
};
use testcontainers::Container;
use testcontainers::runners::SyncRunner;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

const TOPIC: &str = "orders";

fn config(brokers: &str, group: &str) -> KafkaSourceConfig {
    let mut cfg = KafkaSourceConfig::new(brokers, TOPIC, group);
    cfg.commit_interval = Duration::from_millis(200);
    cfg.startup_timeout = Duration::from_secs(60);
    cfg.statistics_interval = Duration::from_secs(1);
    // Tests produce before the consumer joins; without this, librdkafka's
    // `latest` default correctly delivers nothing (see mock_cluster.rs).
    cfg.rdkafka = BTreeMap::from([("auto.offset.reset".to_string(), "earliest".to_string())]);
    cfg
}

#[test]
#[ignore = "requires Docker"]
fn real_broker_full_lifecycle() {
    let container = broker().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");

    // Auto-created topic has a single partition, which is enough for the
    // lifecycle; the multi-partition rebalance path is covered by the
    // MockCluster suite.
    produce(&brokers, TOPIC, 100, 1, "real");

    let mut cp = Checkpointer::new();
    let mut source = KafkaSource::new(config(&brokers, "real-life"));
    source.open(SourceCtx::new(cp.handle())).expect("open");

    // Await assignment.
    let mut lanes = await_assignment(&mut source, Duration::from_secs(60));
    assert_eq!(lanes.len(), 1);
    cp.begin_epoch(&[lanes[0].partition()], 1);

    let rows = drain_lane(&mut source, &mut lanes[0], 100, Duration::from_secs(60));
    assert!(
        rows.iter()
            .all(|(payload, _, _)| payload.starts_with(b"real-"))
    );
    drop(lanes);

    // Ack → watermark → store → sync commit.
    cp.drain();
    let watermarks = cp.take_watermarks();
    assert_eq!(watermarks.len(), 1);
    assert_eq!(watermarks[0].1, 100);
    source.commit(&watermarks).expect("store");
    source.flush_commits().expect("sync commit");

    // The group's committed offset is visible to a probe member.
    assert_eq!(
        support::committed(&brokers, "real-life", TOPIC, 1),
        [rdkafka::Offset::Offset(100)]
    );
}

/// End-to-end regression for the revocation drain-commit (finding: revoked
/// partitions were filtered out of the final commit). A member drains its
/// partition, then a second member joins and forces a revoke; the drained
/// watermark must be committed for the partition being revoked so the work is
/// not replayed after the partition moves. This exercises the full path a
/// real broker allows (a pre-rebalance commit at the valid generation, and
/// librdkafka's commit of stored offsets when `unassign` removes them). The
/// in-process MockCluster rejects offset commits while the group is joining,
/// so this case can only be verified against a real broker.
#[test]
#[ignore = "requires Docker"]
#[expect(clippy::disallowed_methods, reason = "the test joins the thread")]
fn real_broker_revocation_commit_persists_revoked_offsets() {
    let container = broker().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");

    produce(&brokers, TOPIC, 50, 1, "rev");

    let mut cp = Checkpointer::new();
    let mut cfg = config(&brokers, "rev-commit");
    // No interval commit, so only the flush can commit before `unassign` does.
    cfg.commit_interval = Duration::from_hours(1);
    let mut source = KafkaSource::new(cfg);
    source.open(SourceCtx::new(cp.handle())).expect("open");

    // Await assignment and drain all 50 records, producing a watermark at
    // offset 50, but do not commit yet (the inter-tick window).
    let mut lanes = await_assignment(&mut source, Duration::from_secs(60));
    assert_eq!(lanes.len(), 1);
    cp.begin_epoch(&[lanes[0].partition()], 1);
    drain_lane(&mut source, &mut lanes[0], 50, Duration::from_secs(60));
    drop(lanes);
    cp.drain();
    let watermarks = cp.take_watermarks();
    assert_eq!(watermarks, vec![(watermarks[0].0, 50)]);

    // A second member joins, forcing an eager revoke.
    let brokers2 = brokers.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = std::sync::Arc::clone(&stop);
    let joiner = std::thread::spawn(move || {
        let c: BaseConsumer = support::raw_config()
            .set("bootstrap.servers", &brokers2)
            .set("group.id", "rev-commit")
            .set("auto.offset.reset", "earliest")
            .build()
            .expect("second consumer");
        c.subscribe(&[TOPIC]).expect("subscribe");
        while !stop2.load(Ordering::Relaxed) {
            let _ = c.poll(Duration::from_millis(100));
        }
    });

    let probe: BaseConsumer = support::raw_config()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "rev-commit")
        .build()
        .expect("probe");

    // Run the revocation choreography: store the drained watermark for the
    // partition being revoked, then keep polling so `unassign` completes.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut stored = false;
    let mut ticks = 0;
    while Instant::now() < deadline {
        if let SourceEvent::LanesRevoked {
            lanes: ids,
            barrier,
        } = source
            .poll_events(Duration::from_millis(200))
            .expect("poll_events")
        {
            support::complete_revocation(&mut source, &ids, &barrier, &watermarks).expect("flush");
            // The next poll calls `unassign`, which commits what is stored, so
            // the flush's own commit is only visible here.
            assert_eq!(
                support::committed_by(&probe, TOPIC, 1),
                [rdkafka::Offset::Offset(50)]
            );
            stored = true;
        }
        if stored {
            ticks += 1;
            if ticks >= 15 {
                break;
            }
        }
    }
    assert!(stored, "revoke observed and drained offset stored");

    // The drained watermark is committed for the revoked partition.
    spate_test::wait_until(
        Duration::from_secs(30),
        "revoked partition committed offset 50",
        || support::committed_by(&probe, TOPIC, 1) == [rdkafka::Offset::Offset(50)],
    );

    stop.store(true, Ordering::Relaxed);
    joiner.join().expect("joiner thread");
}

/// Acceptance for the production symptom: a consumer sitting on a real
/// backlog must report that backlog, and the sum across its partitions must
/// match what the broker holds.
///
/// The sum is the assertion that matters. Operators compare our number
/// against the broker's group lag, which is a sum across partitions, so a
/// per-member maximum is not the same quantity and silently under-reports by
/// roughly the partition count.
#[test]
#[ignore = "requires Docker"]
fn real_broker_backlog_reports_summable_lag() {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};

    const PARTITIONS: i32 = 4;
    const PER_PARTITION: usize = 5_000;
    const CONSUME_PER_LANE: usize = 100;

    let container = broker().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");

    // Explicit multi-partition topic: auto-creation yields a single partition,
    // which would not exercise the aggregate at all.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let admin: AdminClient<_> = support::raw_config()
        .set("bootstrap.servers", &brokers)
        .build()
        .expect("admin client");
    rt.block_on(admin.create_topics(
        &[NewTopic::new(TOPIC, PARTITIONS, TopicReplication::Fixed(1))],
        &AdminOptions::new(),
    ))
    .expect("create topics")
    .into_iter()
    .for_each(|r| {
        r.expect("topic created");
    });

    produce(&brokers, TOPIC, PER_PARTITION, PARTITIONS, "backlog");
    let produced = PER_PARTITION * PARTITIONS as usize;

    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let mut committed_total: i64 = 0;
    metrics::with_local_recorder(&recorder, || {
        let metrics = std::sync::Arc::new(spate_core::metrics::SourceMetrics::new(
            &spate_core::metrics::ComponentLabels::new("backlog", "source", "kafka"),
        ));
        let mut cp = Checkpointer::new();
        let mut source = KafkaSource::new(config(&brokers, "backlog"));
        source
            .open(SourceCtx::new(cp.handle()).with_stage_metrics(Some(metrics)))
            .expect("open");

        let deadline = Instant::now() + Duration::from_secs(120);
        let mut lanes = await_assignment(&mut source, Duration::from_secs(120));
        assert_eq!(lanes.len(), PARTITIONS as usize, "one lane per partition");
        let partitions: Vec<_> = lanes.iter().map(SourceLane::partition).collect();
        cp.begin_epoch(&partitions, 1);

        // Consume a small prefix so a commit lands: `consumer_lag` is measured
        // against the committed offset and stays unknown (`-1`) until then.
        for lane in &mut lanes {
            let left = deadline.saturating_duration_since(Instant::now());
            drain_lane(&mut source, lane, CONSUME_PER_LANE, left);
        }
        cp.drain();
        let watermarks = cp.take_watermarks();
        committed_total = watermarks.iter().map(|(_, o)| *o).sum();
        source.commit(&watermarks).expect("store offsets");
        source.flush_commits().expect("sync commit");

        // Stop consuming and drive the control plane until every partition
        // has reported. Waiting for the first series would race: a snapshot
        // can carry a number for one partition while another is still `-1`,
        // and the assertions below need all four.
        loop {
            source
                .poll_events(Duration::from_millis(200))
                .expect("poll_events");
            let rendered = handle.render();
            let reported = rendered
                .lines()
                .filter(|l| l.starts_with("spate_source_lag_records{"))
                .count();
            if reported == PARTITIONS as usize {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "only {reported}/{PARTITIONS} partitions published consumer lag \
                 within deadline:\n{rendered}"
            );
        }
    });

    let rendered = handle.render();
    let series: Vec<f64> = rendered
        .lines()
        .filter(|l| l.starts_with("spate_source_lag_records{"))
        .map(|l| l.rsplit(' ').next().expect("value").parse().expect("lag"))
        .collect();
    assert_eq!(
        series.len(),
        PARTITIONS as usize,
        "one lag series per owned partition:\n{rendered}"
    );
    assert!(
        series.iter().all(|v| *v > 0.0),
        "a backlogged partition must never report zero lag:\n{rendered}"
    );
    let total: f64 = series.iter().sum();
    let expected = produced as f64 - committed_total as f64;
    assert_eq!(
        total, expected,
        "summed lag must equal the backlog the broker holds \
         (produced {produced} - committed {committed_total}):\n{rendered}"
    );
}

/// The client and group sequence around a member that never answers its offer,
/// on a real broker: the group removes the member at its rebalance timeout and
/// offers its partition to the other member.
#[test]
#[ignore = "requires Docker"]
fn real_broker_a_member_that_never_answers_its_offer_is_replaced() {
    let container = broker().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");
    produce(&brokers, TOPIC, 1, 1, "never");
    for (answer, group) in [
        (support::Answer::Empty, "never-answers-empty"),
        (support::Answer::Offer, "never-answers-offer"),
    ] {
        support::a_member_that_never_answers_its_offer(&brokers, TOPIC, group, answer);
    }
}

/// Revocation on a real broker with two consumers in the source: the
/// watermarks drained for each consumer's partition are stored through the
/// consumer that owns it, while it still owns it, and both are committed.
/// The in-process MockCluster rejects offset commits while the group is
/// joining, so only a real broker shows the commit at the revoking
/// generation.
#[test]
#[ignore = "requires Docker"]
fn real_broker_revocation_commits_every_consumers_offsets() {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};

    let container = broker().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let admin: AdminClient<_> = support::raw_config()
        .set("bootstrap.servers", &brokers)
        .build()
        .expect("admin client");
    rt.block_on(admin.create_topics(
        &[NewTopic::new(TOPIC, 2, TopicReplication::Fixed(1))],
        &AdminOptions::new(),
    ))
    .expect("create topics")
    .into_iter()
    .for_each(|r| {
        r.expect("topic created");
    });
    produce(&brokers, TOPIC, 25, 2, "both");

    let mut cfg = config(&brokers, "rev-each");
    cfg.consumers = 2;
    let mut cp = Checkpointer::new();
    let mut source = KafkaSource::new(cfg);
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let statistics = recorder.handle();
    let meter = spate_core::metrics::Meter::with_namespace("kafka", "rev-each", "source", "kafka");
    metrics::with_local_recorder(&recorder, || {
        source
            .open(SourceCtx::new(cp.handle()).with_meter(Some(meter)))
            .expect("open");
    });

    // Settle with both partitions held and each consumer in a stable group,
    // completing any revocation a late consumer's join causes.
    let mut lanes = Vec::new();
    spate_test::wait_until(Duration::from_secs(90), "both partitions held", || {
        match source
            .poll_events(Duration::from_millis(200))
            .expect("poll_events")
        {
            SourceEvent::LanesAssigned(new) => lanes = new,
            SourceEvent::LanesRevoked {
                lanes: ids,
                barrier,
            } => {
                lanes.clear();
                support::complete_revocation(&mut source, &ids, &barrier, &[]).expect("flush");
            }
            _ => {}
        }
        let healthy =
            spate_test::metric_series(&statistics.render(), "spate_kafka_group_healthy", &[]);
        lanes.len() == 2 && healthy.len() == 2 && healthy.iter().all(|s| s.value == 1.0)
    });
    let partitions: Vec<_> = lanes.iter().map(SourceLane::partition).collect();
    cp.begin_epoch(&partitions, 1);
    for lane in &mut lanes {
        drain_lane(&mut source, lane, 25, Duration::from_secs(60));
    }
    drop(lanes);
    cp.drain();
    let watermarks = cp.take_watermarks();
    assert_eq!(watermarks.len(), 2, "{watermarks:?}");

    let _joiner = support::Joiner::join_group(&brokers, TOPIC, "rev-each");
    support::complete_next_revocation(&mut source, || watermarks.clone());

    let probe = support::probe(&brokers, "rev-each");
    assert_eq!(
        support::committed_by(&probe, TOPIC, 2),
        [rdkafka::Offset::Offset(25); 2]
    );
}

/// A broker that takes transactions, and its bootstrap address. The container
/// stops when the handle drops.
fn transaction_broker() -> (Container<Kafka>, String) {
    let container = transactional_broker()
        .start()
        .expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    (container, format!("127.0.0.1:{port}"))
}

/// [`config`] on `topic`.
fn topic_config(brokers: &str, topic: &str, group: &str) -> KafkaSourceConfig {
    let mut cfg = config(brokers, group);
    cfg.topic = topic.to_owned();
    cfg
}

/// The end of partition 0's log. A `read_committed` client reads the last stable
/// offset here instead, which an open transaction holds below the log's end.
fn log_end(brokers: &str, topic: &str) -> i64 {
    let probe: BaseConsumer = support::raw_config()
        .set("bootstrap.servers", brokers)
        .set("isolation.level", "read_uncommitted")
        .build()
        .expect("probe");
    probe
        .fetch_watermarks(topic, 0, Duration::from_secs(10))
        .expect("watermarks")
        .1
}

/// Read `records` from `topic` and commit through `committed`, then read it back
/// from a second source on the group: lag is 0 and the offset holds both times.
fn lag_reaches_zero_across_a_restart(
    brokers: &str,
    cfg: KafkaSourceConfig,
    records: usize,
    committed: i64,
) {
    let topic = cfg.topic.clone();
    let group = cfg.group_id.clone();
    let mut watched = Watched::open(cfg.clone());
    assert_eq!(watched.read(0, records).len(), records);
    watched.poll_until_committed(0, 0, committed);
    watched.wait_lag(0, 0.0);
    drop(watched);
    let offset = rdkafka::Offset::Offset(committed);
    assert_eq!(support::committed(brokers, &group, &topic, 1), [offset]);

    let mut second = Watched::open(cfg);
    second.serve();
    assert_eq!(second.poll(0), None, "nothing to deliver or retire");
    second.wait_lag(0, 0.0);
    assert_eq!(support::committed(brokers, &group, &topic, 1), [offset]);
}

/// A committed transaction's marker reaches the committed offset on a real
/// broker, and a plain topic on the same broker keeps reading as it did.
/// Regression for #1709.
#[test]
#[ignore = "requires Docker"]
fn real_broker_lag_reaches_zero_after_a_committed_transaction() {
    let (_container, brokers) = transaction_broker();
    produce(&brokers, "plain", 100, 1, "plain");
    lag_reaches_zero_across_a_restart(
        &brokers,
        topic_config(&brokers, "plain", "plain-group"),
        100,
        100,
    );

    let _ = produce_transaction(&brokers, "txn", 100, "only", Transaction::Commit);
    lag_reaches_zero_across_a_restart(
        &brokers,
        topic_config(&brokers, "txn", "txn-group"),
        100,
        101,
    );
}

/// An aborted transaction's records and marker are passed under
/// `read_committed`, with nothing delivered for them. Regression for #1709.
#[test]
#[ignore = "requires Docker"]
fn real_broker_lag_reaches_zero_after_an_aborted_transaction() {
    let (_container, brokers) = transaction_broker();
    let _ = produce_transaction(&brokers, TOPIC, 100, "kept", Transaction::Commit);
    let _ = produce_transaction(&brokers, TOPIC, 100, "dropped", Transaction::Abort);
    lag_reaches_zero_across_a_restart(&brokers, config(&brokers, "aborted"), 100, 202);
}

/// Under `read_uncommitted` aborted records are delivered, and only the markers
/// are passed. Regression for #1709.
#[test]
#[ignore = "requires Docker"]
fn real_broker_lag_reaches_zero_reading_uncommitted() {
    let (_container, brokers) = transaction_broker();
    let _ = produce_transaction(&brokers, TOPIC, 100, "kept", Transaction::Commit);
    let _ = produce_transaction(&brokers, TOPIC, 100, "dropped", Transaction::Abort);
    let mut cfg = config(&brokers, "uncommitted");
    cfg.rdkafka
        .insert("isolation.level".into(), "read_uncommitted".into());
    lag_reaches_zero_across_a_restart(&brokers, cfg, 200, 202);
}

/// An open transaction holds the readable end at the last stable offset: lag
/// reads 0 and nothing past it is read until it ends, when everything arrives
/// with no gap and the offset reaches the log's end. Regression for #1709.
#[test]
#[ignore = "requires Docker"]
fn real_broker_an_open_transaction_holds_lag_at_the_last_stable_offset() {
    let (_container, brokers) = transaction_broker();
    produce(&brokers, TOPIC, 100, 1, "plain");
    let open = produce_transaction(&brokers, TOPIC, 50, "open", Transaction::Open);
    let _ = produce_transaction(&brokers, TOPIC, 10, "behind", Transaction::Commit);

    let mut watched = Watched::open(config(&brokers, "open"));
    assert_eq!(watched.read(0, 100), (0..100).collect::<Vec<i64>>());
    assert_eq!(watched.commit(), [(PartitionId(0), 100)]);
    watched.wait_lag(0, 0.0);
    assert!(
        log_end(&brokers, TOPIC) > 100,
        "the log runs past the stable offset"
    );
    assert_eq!(watched.poll(0), None, "read past the open transaction");
    assert_eq!(
        support::committed(&brokers, "open", TOPIC, 1),
        [rdkafka::Offset::Offset(100)]
    );

    rdkafka::producer::Producer::commit_transaction(&open, Duration::from_secs(60))
        .expect("commit");
    let later = watched.read(0, 60);
    assert_eq!(later.len(), 60);
    assert!(later.is_sorted() && later[0] == 100, "{later:?}");
    let end = log_end(&brokers, TOPIC);
    watched.poll_until_committed(0, 0, end);
    watched.wait_lag(0, 0.0);
}

/// Plain, committed and aborted writes in turn settle at the log's end, and the
/// watermark never falls. Regression for #1709.
#[test]
#[ignore = "requires Docker"]
fn real_broker_alternating_writes_settle_at_zero() {
    let (_container, brokers) = transaction_broker();
    produce(&brokers, TOPIC, 1, 1, "first");
    let mut watched = Watched::open(config(&brokers, "alternating"));
    watched.read(0, 1);
    let mut seen = Vec::new();
    for round in 0..3 {
        produce(&brokers, TOPIC, 5, 1, &format!("plain-{round}"));
        watched.read(0, 5);
        let tag = format!("kept-{round}");
        let _ = produce_transaction(&brokers, TOPIC, 5, &tag, Transaction::Commit);
        watched.read(0, 5);
        let tag = format!("dropped-{round}");
        let _ = produce_transaction(&brokers, TOPIC, 5, &tag, Transaction::Abort);
        let end = log_end(&brokers, TOPIC);
        seen.extend(watched.poll_until_committed(0, 0, end));
    }
    assert!(seen.is_sorted(), "the watermark fell: {seen:?}");
    watched.wait_lag(0, 0.0);
    assert_eq!(
        support::committed(&brokers, "alternating", TOPIC, 1),
        [rdkafka::Offset::Offset(log_end(&brokers, TOPIC))]
    );
}

/// A commit past a tail marker is a position the broker restarts from, under
/// `latest` too: no reset, and the next record is the first delivered.
/// Regression for #1709.
#[test]
#[ignore = "requires Docker"]
fn real_broker_a_commit_past_a_marker_restarts_at_the_next_record() {
    let (_container, brokers) = transaction_broker();
    let _ = produce_transaction(&brokers, TOPIC, 10, "kept", Transaction::Commit);
    let mut watched = Watched::open(config(&brokers, "restarts"));
    watched.read(0, 10);
    watched.poll_until_committed(0, 0, 11);
    drop(watched);

    produce(&brokers, TOPIC, 5, 1, "next");
    let mut cfg = config(&brokers, "restarts");
    cfg.rdkafka
        .insert("auto.offset.reset".into(), "latest".into());
    let mut restarted = Watched::open(cfg);
    assert_eq!(restarted.read(0, 5), [11, 12, 13, 14, 15]);
}

/// Under `latest` a group that starts past everything has served no op, so its
/// position is not set and it commits nothing.
#[test]
#[ignore = "requires Docker"]
fn real_broker_latest_on_a_marker_only_partition_commits_nothing() {
    let (_container, brokers) = transaction_broker();
    let _ = produce_transaction(&brokers, TOPIC, 5, "kept", Transaction::Commit);
    let mut cfg = config(&brokers, "latest");
    cfg.rdkafka
        .insert("auto.offset.reset".into(), "latest".into());
    let mut watched = Watched::open(cfg);
    for _ in 0..5 {
        watched.serve();
        assert_eq!(watched.poll(0), None);
    }
    assert!(watched.commit().is_empty());
    assert_eq!(
        support::committed(&brokers, "latest", TOPIC, 1),
        [rdkafka::Offset::Invalid]
    );
}

/// Under `earliest` a partition holding only an aborted transaction delivers
/// nothing, and the offset still passes its marker. Regression for #1709.
#[test]
#[ignore = "requires Docker"]
fn real_broker_earliest_on_an_aborted_only_partition_commits_past_its_marker() {
    let (_container, brokers) = transaction_broker();
    let _ = produce_transaction(&brokers, TOPIC, 100, "dropped", Transaction::Abort);
    let mut watched = Watched::open(config(&brokers, "aborted-only"));
    assert_eq!(watched.poll_until_committed(0, 0, 101), [101]);
    watched.wait_lag(0, 0.0);
}

/// A group that passed an aborted transaction under `read_committed` delivers
/// none of it under `read_uncommitted`: its first record is the one after the
/// marker.
#[test]
#[ignore = "requires Docker"]
fn real_broker_a_switch_to_read_uncommitted_delivers_nothing_it_passed() {
    let (_container, brokers) = transaction_broker();
    let _ = produce_transaction(&brokers, TOPIC, 5, "dropped", Transaction::Abort);
    let mut watched = Watched::open(config(&brokers, "switch"));
    assert_eq!(watched.poll_until_committed(0, 0, 6), [6]);
    drop(watched);

    produce(&brokers, TOPIC, 1, 1, "next");
    let mut cfg = config(&brokers, "switch");
    cfg.rdkafka
        .insert("isolation.level".into(), "read_uncommitted".into());
    let mut switched = Watched::open(cfg);
    assert_eq!(switched.read(0, 1), [6]);
}

/// Serve `watched` until its revocation arrives, and complete it with whatever
/// the checkpointer holds.
fn complete_the_revocation(watched: &mut Watched) {
    let Watched {
        source, lanes, cp, ..
    } = watched;
    support::complete_next_revocation(source, || {
        lanes.clear();
        cp.drain();
        cp.take_watermarks()
    });
}

/// A revocation ahead of a marker the lane has not served loses nothing: the
/// offset stops at the last record, and the new owner passes the marker. A marker
/// served before the revocation reaches the drain's commit. Regression for #1709.
#[test]
#[ignore = "requires Docker"]
fn real_broker_revocation_before_a_marker_loses_nothing() {
    let (_container, brokers) = transaction_broker();
    let _ = produce_transaction(&brokers, "unserved", 5, "kept", Transaction::Commit);
    let cfg = topic_config(&brokers, "unserved", "unserved-group");
    let mut watched = Watched::open(cfg.clone());
    // Stops at the fifth record, ahead of the marker: the lane fills a batch only
    // up to what it is asked for, and a poll that returns a record reads nothing.
    let mut read = 0;
    spate_test::wait_until(Duration::from_secs(60), "the records", || {
        watched.serve();
        if let Some(mut batch) = watched.lanes[0]
            .poll(5 - read, Duration::from_millis(200))
            .expect("lane poll")
        {
            while spate_core::source::PayloadBatch::next_payload(&mut batch).is_some() {
                read += 1;
            }
        }
        read == 5
    });
    {
        let _joiner = support::Joiner::join_group(&brokers, "unserved", "unserved-group");
        complete_the_revocation(&mut watched);
    }
    assert_eq!(
        support::committed(&brokers, "unserved-group", "unserved", 1),
        [rdkafka::Offset::Offset(5)]
    );
    drop(watched);
    let mut owner = Watched::open(cfg);
    assert_eq!(owner.poll_until_committed(0, 0, 6), [6]);
    owner.wait_lag(0, 0.0);

    let _ = produce_transaction(&brokers, "served", 5, "kept", Transaction::Commit);
    let mut watched = Watched::open(topic_config(&brokers, "served", "served-group"));
    watched.read(0, 5);
    assert_eq!(watched.commit(), [(PartitionId(0), 5)]);
    spate_test::wait_until(Duration::from_secs(60), "the marker's batch", || {
        watched.serve();
        watched.poll(0) == Some(vec![])
    });
    {
        let _joiner = support::Joiner::join_group(&brokers, "served", "served-group");
        complete_the_revocation(&mut watched);
    }
    assert_eq!(
        support::committed(&brokers, "served-group", "served", 1),
        [rdkafka::Offset::Offset(6)]
    );
}
