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
use spate_test::HANG;
use spate_test_support::{KAFKA_PORT, KafkaBroker};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use support::{
    Transaction, Watched, await_assignment, broker, drain_lane, log_end, produce,
    produce_transaction, settled_end, transactional_broker,
};
use testcontainers::Container;
use testcontainers::runners::SyncRunner;

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
    let mut lanes = await_assignment(&mut source, HANG);
    assert_eq!(lanes.len(), 1);
    cp.begin_epoch(&[lanes[0].partition()], 1);

    let rows = drain_lane(&mut source, &mut lanes[0], 100, HANG);
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
        support::committed_by(&brokers, "real-life", TOPIC, 1),
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
    let mut lanes = await_assignment(&mut source, HANG);
    assert_eq!(lanes.len(), 1);
    cp.begin_epoch(&[lanes[0].partition()], 1);
    drain_lane(&mut source, &mut lanes[0], 50, HANG);
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

    let probe = support::probe(&brokers, "rev-commit");

    // Run the revocation choreography: store the drained watermark for the
    // partition being revoked, then poll so `unassign` runs.
    let mut stored = false;
    spate_test::wait_until(HANG, "the revocation", || {
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
                support::committed(&probe, TOPIC, 1),
                [rdkafka::Offset::Offset(50)]
            );
            stored = true;
        }
        stored
    });
    // One more poll runs that `unassign`.
    let _ = source.poll_events(Duration::ZERO);

    // The drained watermark is committed for the revoked partition.
    spate_test::wait_until(HANG, "revoked partition committed offset 50", || {
        support::committed(&probe, TOPIC, 1) == [rdkafka::Offset::Offset(50)]
    });

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
    const PARTITIONS: i32 = 4;
    const PER_PARTITION: usize = 5_000;
    const CONSUME_PER_LANE: usize = 100;

    let container = broker().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");

    // Explicit multi-partition topic: auto-creation yields a single partition,
    // which would not exercise the aggregate at all.
    support::create_topic(&brokers, TOPIC, PARTITIONS);

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
    let container = broker().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");
    support::create_topic(&brokers, TOPIC, 2);
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
        drain_lane(&mut source, lane, 25, HANG);
    }
    drop(lanes);
    cp.drain();
    let watermarks = cp.take_watermarks();
    assert_eq!(watermarks.len(), 2, "{watermarks:?}");

    let _joiner = support::Joiner::join_group(&brokers, TOPIC, "rev-each");
    support::complete_next_revocation(&mut source, || watermarks.clone());

    let probe = support::probe(&brokers, "rev-each");
    assert_eq!(
        support::committed(&probe, TOPIC, 2),
        [rdkafka::Offset::Offset(25); 2]
    );
}

/// A broker that takes transactions, and its bootstrap address. The container
/// stops when the handle drops.
fn transaction_broker() -> (Container<KafkaBroker>, String) {
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
    assert_eq!(support::committed_by(brokers, &group, &topic, 1), [offset]);

    let mut second = Watched::open(cfg);
    second.serve();
    assert_eq!(second.poll(0), None, "nothing to deliver or retire");
    second.wait_lag(0, 0.0);
    assert_eq!(support::committed_by(brokers, &group, &topic, 1), [offset]);
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
        settled_end(&brokers, "txn", 0),
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
    let end = settled_end(&brokers, TOPIC, 0);
    lag_reaches_zero_across_a_restart(&brokers, config(&brokers, "aborted"), 100, end);
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
    lag_reaches_zero_across_a_restart(&brokers, cfg, 200, settled_end(&brokers, TOPIC, 0));
}

/// An open transaction holds the readable end at the last stable offset: lag
/// reads 0 and nothing past it is read until it ends, when everything arrives
/// in order and the offset reaches the log's end. Regression for #1709.
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
        log_end(&brokers, TOPIC, 0) > 100,
        "the log runs past the stable offset"
    );
    assert_eq!(watched.poll(0), None, "read past the open transaction");
    assert_eq!(
        support::committed_by(&brokers, "open", TOPIC, 1),
        [rdkafka::Offset::Offset(100)]
    );

    rdkafka::producer::Producer::commit_transaction(&open, Duration::from_secs(60))
        .expect("commit");
    let later = watched.read(0, 60);
    assert_eq!(later.len(), 60);
    assert!(later.is_sorted() && later[0] >= 100, "{later:?}");
    let end = settled_end(&brokers, TOPIC, 0);
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
        let end = settled_end(&brokers, TOPIC, 0);
        seen.extend(watched.poll_until_committed(0, 0, end));
    }
    assert!(seen.is_sorted(), "the watermark fell: {seen:?}");
    watched.wait_lag(0, 0.0);
    assert_eq!(
        support::committed_by(&brokers, "alternating", TOPIC, 1),
        [rdkafka::Offset::Offset(log_end(&brokers, TOPIC, 0))]
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
    let end = settled_end(&brokers, TOPIC, 0);
    watched.poll_until_committed(0, 0, end);
    drop(watched);

    produce(&brokers, TOPIC, 5, 1, "next");
    let mut cfg = config(&brokers, "restarts");
    cfg.rdkafka
        .insert("auto.offset.reset".into(), "latest".into());
    let mut restarted = Watched::open(cfg);
    assert_eq!(restarted.read(0, 5), (end..end + 5).collect::<Vec<i64>>());
}

/// Under `latest` a group that starts past everything has served no op, so its
/// position is not set and its lane retires nothing. The committed offset is the
/// start the source committed on assignment.
#[test]
#[ignore = "requires Docker"]
fn real_broker_latest_on_a_marker_only_partition_retires_nothing() {
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
        support::committed_by(&brokers, "latest", TOPIC, 1),
        [rdkafka::Offset::Offset(settled_end(&brokers, TOPIC, 0))]
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
    let end = settled_end(&brokers, TOPIC, 0);
    assert_eq!(watched.poll_until_committed(0, 0, end), [end]);
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
    let end = settled_end(&brokers, TOPIC, 0);
    assert_eq!(watched.poll_until_committed(0, 0, end), [end]);
    drop(watched);

    produce(&brokers, TOPIC, 1, 1, "next");
    let mut cfg = config(&brokers, "switch");
    cfg.rdkafka
        .insert("isolation.level".into(), "read_uncommitted".into());
    let mut switched = Watched::open(cfg);
    assert_eq!(switched.read(0, 1), [end]);
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
    // The marker takes the last offset, so the last record ends one short.
    let end = settled_end(&brokers, "unserved", 0);
    let cfg = topic_config(&brokers, "unserved", "unserved-group");
    let mut watched = Watched::open(cfg.clone());
    // Stops at the fifth record, ahead of the marker: the lane fills a batch only
    // up to what it is asked for, and a poll that returns a record reads nothing.
    let mut read = 0;
    spate_test::wait_until(HANG, "the records", || {
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
        support::committed_by(&brokers, "unserved-group", "unserved", 1),
        [rdkafka::Offset::Offset(end - 1)]
    );
    drop(watched);
    let mut owner = Watched::open(cfg);
    assert_eq!(owner.poll_until_committed(0, 0, end), [end]);
    owner.wait_lag(0, 0.0);

    let _ = produce_transaction(&brokers, "served", 5, "kept", Transaction::Commit);
    let end = settled_end(&brokers, "served", 0);
    let mut watched = Watched::open(topic_config(&brokers, "served", "served-group"));
    watched.read(0, 5);
    assert_eq!(watched.commit(), [(PartitionId(0), end - 1)]);
    spate_test::wait_until(HANG, "the marker's batch", || {
        watched.serve();
        watched.poll(0) == Some(vec![])
    });
    {
        let _joiner = support::Joiner::join_group(&brokers, "served", "served-group");
        complete_the_revocation(&mut watched);
    }
    assert_eq!(
        support::committed_by(&brokers, "served-group", "served", 1),
        [rdkafka::Offset::Offset(end)]
    );
}

/// Runs over a partition with no committed offset, on a real broker.
mod latest_start {
    use super::*;
    use rdkafka::admin::{AdminClient, AdminOptions, NewPartitions};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::producer::{BaseRecord, Producer as _};
    use spate_test::HANG;
    use support::latest;

    fn on_a_broker(partitions: i32, body: impl FnOnce(&str, &str)) {
        let container = broker().start().expect("start kafka container");
        let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
        let brokers = format!("127.0.0.1:{port}");
        support::create_topic(&brokers, TOPIC, partitions);
        body(&brokers, TOPIC);
    }

    /// A quiet partition reads what arrived while the pipeline was down.
    /// Regression for #1704.
    #[test]
    #[ignore = "requires Docker"]
    fn a_quiet_partition_reads_what_arrived_across_a_clean_restart() {
        on_a_broker(2, |b, t| {
            latest::a_quiet_partition_across_a_restart(b, t, "latest")
        });
    }

    /// The control: `earliest` reads the same records with no start commit.
    #[test]
    #[ignore = "requires Docker"]
    fn a_quiet_partition_reads_what_arrived_under_earliest() {
        on_a_broker(2, |b, t| {
            latest::a_quiet_partition_across_a_restart(b, t, "earliest")
        });
    }

    /// A new group whose sink is down keeps its start, and a restart reads it.
    /// Regression for #1704.
    #[test]
    #[ignore = "requires Docker"]
    fn a_new_group_with_its_sink_down_reads_from_its_start_after_a_restart() {
        on_a_broker(2, |b, t| {
            latest::a_sink_down_across_a_restart(b, t, "latest")
        });
    }

    /// A second process joining a group with no commit loses no record, which
    /// also decides that a commit during the group's join is accepted.
    /// Regression for #1704.
    #[test]
    #[ignore = "requires Docker"]
    fn a_second_process_joining_a_group_with_no_commit_loses_no_record() {
        on_a_broker(2, |b, t| latest::a_second_process_joining(b, t, "latest"));
    }

    /// The next member reads from where a removed member started.
    /// Regression for #1704.
    #[test]
    #[ignore = "requires Docker"]
    fn a_removed_single_consumer_loses_no_record() {
        on_a_broker(1, |b, t| latest::a_removed_single_consumer(b, t, "latest"));
    }

    /// A record the chain fails on stops the pipeline at every restart, with
    /// nothing committed past it. Regression for #1704.
    #[test]
    #[ignore = "requires Docker"]
    fn a_failing_record_on_an_uncommitted_partition_replays_after_a_restart() {
        on_a_broker(2, latest::a_failing_record_across_a_restart);
    }
    /// Under `latest` a partition added to the topic starts at its end, and
    /// records written before the group first assigned it are skipped. Pins the
    /// exception INV-1 records.
    #[test]
    #[ignore = "requires Docker"]
    fn a_partition_added_to_a_running_group_starts_at_its_end() {
        on_a_broker(1, |brokers, topic| {
            let group = spate_test::unique_name("expansion");
            let mut cfg = latest::config(brokers, topic, &group, "latest");
            cfg.rdkafka.insert(
                "topic.metadata.refresh.interval.ms".to_string(),
                "1000".to_string(),
            );
            let run = latest::Run::start(cfg);
            let mut sentinel = 0;
            spate_test::wait_until(Duration::from_secs(120), "the run to read", || {
                support::produce_to(brokers, topic, 0, sentinel..sentinel + 1);
                sentinel += 1;
                !run.written(0).is_empty()
            });
            let admin: AdminClient<DefaultClientContext> = support::raw_config()
                .set("bootstrap.servers", brokers)
                .build()
                .expect("admin client");
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime")
                .block_on(
                    admin.create_partitions(&[NewPartitions::new(topic, 2)], &AdminOptions::new()),
                )
                .expect("create partitions")
                .into_iter()
                .for_each(|r| {
                    r.expect("partition created");
                });
            support::produce_to(brokers, topic, 1, 0..20);
            let mut next = 20;
            spate_test::wait_until(Duration::from_secs(120), "the new partition's read", || {
                support::produce_to(brokers, topic, 1, next..next + 1);
                next += 1;
                !run.written(1).is_empty()
            });
            let first = *run.written(1).first().expect("a record");
            run.stop();
            assert!(
                first > 0,
                "records written before the first assignment are skipped"
            );
        });
    }

    /// A commit retention has passed resumes at the log start, so the records
    /// the log still holds reach the sink. Regression for #1713.
    #[test]
    #[ignore = "requires Docker"]
    fn a_commit_below_the_log_start_loses_no_retained_record() {
        on_a_broker(2, |b, t| {
            latest::a_commit_below_the_log_start_across_a_restart(b, t, "latest")
        });
    }

    /// A member the group has moved past cannot replace a peer's offset, which
    /// the start commit relies on: the broker checks the generation.
    #[test]
    #[ignore = "requires Docker"]
    fn a_start_commit_never_replaces_a_peers_offset() {
        on_a_broker(1, |brokers, topic| {
            let group = spate_test::unique_name("peer");
            support::a_stale_member_cannot_replace_a_peers_offset(brokers, topic, &group);
        });
    }

    /// A start commit made before any read reads back as the group's offset, and
    /// a restart reads the records written after it. Regression for #1704.
    #[test]
    #[ignore = "requires Docker"]
    fn a_start_recorded_before_any_read_reads_back_as_committed() {
        on_a_broker(1, |brokers, topic| {
            let group = spate_test::unique_name("read-back");
            support::produce_to(brokers, topic, 0, 0..3);
            let accept = || {
                let cp = Checkpointer::new();
                let mut source = KafkaSource::new(latest::config(brokers, topic, &group, "latest"));
                source.open(SourceCtx::new(cp.handle())).expect("open");
                let lanes = await_assignment(&mut source, Duration::from_secs(90));
                (source, cp, lanes)
            };
            {
                let _first = accept();
                assert_eq!(
                    support::committed_by(brokers, &group, topic, 1),
                    [rdkafka::Offset::Offset(3)]
                );
            }
            support::produce_to(brokers, topic, 0, 3..6);
            let (mut source, _cp, mut lanes) = accept();
            let rows = drain_lane(&mut source, &mut lanes[0], 3, HANG);
            let offsets: Vec<i64> = rows.iter().map(|(_, _, o)| *o).collect();
            assert_eq!(offsets, [3, 4, 5]);
        });
    }

    /// The start is what the isolation level reads as the end: the last stable
    /// offset under `read_committed`, so an open transaction's records arrive
    /// once it commits, and the high watermark under `read_uncommitted`.
    #[test]
    #[ignore = "requires Docker"]
    fn an_open_transaction_is_not_skipped_by_a_new_group() {
        let container = transactional_broker()
            .start()
            .expect("start kafka container");
        let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
        let brokers = format!("127.0.0.1:{port}");
        support::create_topic(&brokers, TOPIC, 1);
        support::produce_to(&brokers, TOPIC, 0, 0..2);
        let stable = support::log_end(&brokers, TOPIC, 0);

        let producer = support::transactional_producer(&brokers, TOPIC, "open-transaction");
        producer.begin_transaction().expect("begin");
        for i in 0..3 {
            let payload = format!("0:{}", 2 + i);
            producer
                .send(
                    BaseRecord::<(), _>::to(TOPIC)
                        .partition(0)
                        .payload(payload.as_bytes()),
                )
                .expect("enqueue");
        }
        producer.flush(Duration::from_secs(60)).expect("flush");
        let high = support::log_end(&brokers, TOPIC, 0);

        let start = |level: &str, group: &str| {
            let mut cfg = latest::config(&brokers, TOPIC, group, "latest");
            cfg.rdkafka
                .insert("isolation.level".to_string(), level.to_string());
            let cp = Checkpointer::new();
            let mut source = KafkaSource::new(cfg);
            source.open(SourceCtx::new(cp.handle())).expect("open");
            let lanes = await_assignment(&mut source, Duration::from_secs(90));
            let at = support::committed_by(&brokers, group, TOPIC, 1)[0];
            (source, cp, lanes, at)
        };
        let (_uncommitted, _cp, _lanes, at) = start("read_uncommitted", "uncommitted");
        assert_eq!(at, rdkafka::Offset::Offset(high), "the high watermark");
        let (mut source, _cp, mut lanes, at) = start("read_committed", "committed");
        assert_eq!(
            at,
            rdkafka::Offset::Offset(stable),
            "the last stable offset"
        );

        producer
            .commit_transaction(Duration::from_secs(60))
            .expect("commit");
        let rows = drain_lane(&mut source, &mut lanes[0], 3, HANG);
        let offsets: Vec<i64> = rows.iter().map(|(_, _, o)| *o).collect();
        assert_eq!(offsets, (high - 3..high).collect::<Vec<i64>>());
    }
}
