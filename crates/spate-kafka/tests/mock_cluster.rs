//! Integration tests against rdkafka's in-process MockCluster (no Docker).

mod support;

use support::Raw as _;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
use spate_core::checkpoint::Checkpointer;
use spate_core::clock::TestClock;
use spate_core::error::ErrorClass;
use spate_core::metrics::names;
use spate_core::record::PartitionId;
use spate_core::source::{PayloadBatch, Source, SourceCtx, SourceEvent, SourceLane};
use spate_kafka::{KafkaSource, KafkaSourceConfig};
use spate_test::wait_until;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};
use support::MockKafka;
use support::{
    Marker, RawBatch, Watched, await_assignment, control_batch, corrupt_batch, drain_lane, produce,
    produce_batch, serve_events, v1_message, v2_batch,
};

const TOPIC: &str = "orders";

fn config(brokers: &str, group: &str) -> KafkaSourceConfig {
    let mut cfg = KafkaSourceConfig::new(brokers, TOPIC, group);
    cfg.commit_interval = Duration::from_millis(200);
    cfg.startup_timeout = Duration::from_secs(30);
    // Statistics off: deterministic tests.
    cfg.statistics_interval = Duration::ZERO;
    cfg.rdkafka = BTreeMap::from([
        // These tests produce BEFORE the consumer joins. librdkafka's
        // default (`latest`) would legitimately deliver nothing. Any past
        // "green" run of that shape was a pause-race leaking a message to
        // the main queue, whose rewind seek overrode the reset policy.
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        // The mock broker paces a rebalance of an *established* group at
        // `session.timeout.ms - 1000` (rdkafka_mock_cgrp.c: the JOINING
        // timeout), so librdkafka's 45s default made every second-member
        // test wait ~44s for its reassignment. Only the group's *first*
        // formation uses `group.initial.rebalance.delay.ms`. 6s keeps us at
        // real Kafka's `group.min.session.timeout.ms` floor while capping
        // that wait at 5s.
        ("session.timeout.ms".to_string(), "6000".to_string()),
        ("heartbeat.interval.ms".to_string(), "2000".to_string()),
    ]);
    cfg
}

/// How long a mock-cluster group takes to hand out an assignment.
const ASSIGNMENT: Duration = Duration::from_secs(30);

/// Group state changes after startup reach the tracing subscriber.
/// Regression for #651.
#[test]
fn consumer_debug_logs_continue_through_group_join() {
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let mut cfg = config(&cluster.bootstrap_servers(), "log-level-source");
    cfg.rdkafka.insert("debug".into(), "cgrp".into());

    let lines = spate_test::capture_logs(tracing::Level::DEBUG, || {
        let cp = Checkpointer::new();
        let mut source = KafkaSource::new(cfg);
        source.open(SourceCtx::new(cp.handle())).expect("open");
        let _lanes = await_assignment(&mut source, ASSIGNMENT);
    });

    let states: Vec<_> = lines
        .iter()
        .filter(|line| line.contains("librdkafka") && line.contains("fac=\"CGRPSTATE\""))
        .collect();
    assert!(
        states.iter().any(|line| line.contains("-> up")),
        "no post-startup group state debug line: {states:#?}"
    );
}

#[test]
fn full_lifecycle_polls_acks_and_commits() {
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 3, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 10, 3, "a");

    let mut cp = Checkpointer::new();
    let mut source = KafkaSource::new(config(&brokers, "life"));
    source.open(SourceCtx::new(cp.handle())).expect("open");

    let mut lanes = await_assignment(&mut source, ASSIGNMENT);
    assert_eq!(lanes.len(), 3, "one lane per partition");
    let partitions: Vec<PartitionId> = lanes.iter().map(SourceLane::partition).collect();
    cp.begin_epoch(&partitions, 1);

    for lane in &mut lanes {
        let rows = drain_lane(&mut source, lane, 10, Duration::from_secs(30));
        // Offsets are contiguous from zero within a partition.
        let offsets: Vec<i64> = rows.iter().map(|(_, _, o)| *o).collect();
        assert_eq!(offsets, (0..10).collect::<Vec<_>>());
        // Payload and key content round-trip.
        let p = lane.partition().0;
        assert!(rows[3].0.starts_with(format!("a-p{p}-").as_bytes()));
        assert!(rows[3].1.starts_with(format!("k{p}-").as_bytes()));
    }
    // Batches were dropped in drain_lane: acknowledgments resolve.
    drop(lanes);
    cp.drain();
    let mut watermarks = cp.take_watermarks();
    watermarks.sort();
    assert_eq!(
        watermarks,
        partitions.iter().map(|p| (*p, 10)).collect::<Vec<_>>(),
        "each partition committable at offset 10"
    );
    source.commit(&watermarks).expect("store offsets");
    source.flush_commits().expect("sync commit");

    // A probe consumer in the same group sees the committed positions.
    assert_eq!(
        support::committed_by(&brokers, "life", TOPIC, 3),
        [rdkafka::Offset::Offset(10); 3]
    );
}

#[test]
fn pause_stops_delivery_and_resume_recovers_gapless() {
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();

    let mut cfg = config(&brokers, "pause");
    // Statistics feed the warning for a held partition that stops fetching.
    cfg.statistics_interval = Duration::from_millis(250);
    let mut cp = Checkpointer::new();
    let mut source = KafkaSource::new(cfg);
    let meter = spate_core::metrics::Meter::with_namespace("kafka", "pause", "source", "kafka");
    source
        .open(SourceCtx::new(cp.handle()).with_meter(Some(meter)))
        .expect("open");
    let mut lanes = await_assignment(&mut source, ASSIGNMENT);
    assert_eq!(lanes.len(), 1);
    cp.begin_epoch(&[lanes[0].partition()], 1);

    source.pause(&[lanes[0].id()]).expect("pause");
    produce(&brokers, TOPIC, 5, 1, "b");

    // A failure from here on carries the source's warnings: a main-queue
    // rewind, librdkafka's errors, and a held partition that is not fetching,
    // with its fetch state.
    spate_test::show_logs(tracing::Level::WARN, || {
        let mut errors = Vec::new();
        // While paused nothing is delivered (pause also purges prefetch).
        let idle_until = Instant::now() + Duration::from_secs(2);
        while Instant::now() < idle_until {
            serve_events(&mut source, &mut errors);
            assert!(
                lanes[0]
                    .poll(64, Duration::from_millis(100))
                    .expect("poll")
                    .is_none(),
                "paused lane must not deliver; retryable errors: {errors:?}"
            );
        }

        source.resume(&[lanes[0].id()]).expect("resume");
        let rows = drain_lane(&mut source, &mut lanes[0], 5, Duration::from_secs(30));
        let offsets: Vec<i64> = rows.iter().map(|(_, _, o)| *o).collect();
        assert_eq!(offsets, (0..5).collect::<Vec<_>>(), "gap-free redelivery");
    });
}

/// The op version in a librdkafka `topic` debug line: the `N` in `(vN)`.
fn op_version(line: &str) -> Option<u32> {
    let start = line.rfind("(v")? + 2;
    let len = line[start..].find(')')?;
    line[start..start + len].parse().ok()
}

/// A partition's fetcher starts before the source hands out its lane, so a
/// pause issued after the assignment is ordered after the fetch start.
/// Regression for #638.
#[test]
fn the_fetcher_starts_before_the_lane_is_handed_out() {
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 5, 1, "c");
    // Replies land after the request that asked for them has returned.
    cluster
        .broker_round_trip_time(1, Duration::from_millis(300))
        .expect("round trip time");

    let mut cfg = config(&brokers, "fetch-start");
    cfg.rdkafka.insert("debug".into(), "topic".into());
    let cp = Checkpointer::new();
    let mut source = KafkaSource::new(cfg);

    let lines = spate_test::capture_logs(tracing::Level::DEBUG, || {
        source.open(SourceCtx::new(cp.handle())).expect("open");
        let mut lanes = await_assignment(&mut source, ASSIGNMENT);
        source.pause(&[lanes[0].id()]).expect("pause");
        source.resume(&[lanes[0].id()]).expect("resume");
        let rows = drain_lane(&mut source, &mut lanes[0], 5, Duration::from_secs(30));
        let offsets: Vec<i64> = rows.iter().map(|(_, _, o)| *o).collect();
        assert_eq!(offsets, (0..5).collect::<Vec<_>>());
    });

    let partition = format!("{TOPIC} [0]");
    let version_of = |prefix: &str| {
        lines
            .iter()
            .filter(|l| l.contains("librdkafka") && l.contains(&partition))
            .find(|l| l.contains(prefix))
            .and_then(|l| op_version(l))
    };
    let start = version_of("Start consuming").expect("no fetch start logged");
    let pause = version_of("Pause ").expect("no pause logged");
    assert!(
        start < pause,
        "fetch start v{start} was issued after the pause v{pause}"
    );
    let outdated: Vec<_> = lines.iter().filter(|l| l.contains("outdated op")).collect();
    assert!(
        outdated.is_empty(),
        "ops dropped as outdated: {outdated:#?}"
    );
}

/// Commit `offset` on partition 0 for `group`, as a previous member would.
fn commit_as(brokers: &str, group: &str, offset: i64) {
    let probe = support::probe(brokers, group);
    let mut tpl = rdkafka::TopicPartitionList::new();
    tpl.add_partition_offset(TOPIC, 0, rdkafka::Offset::Offset(offset))
        .expect("offset");
    probe
        .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
        .expect("commit");
}

/// A group with no committed offset starts where `topic.auto.offset.reset`
/// says. Regression for #1255.
#[test]
fn a_topic_prefixed_reset_policy_reads_a_new_group_from_the_start() {
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 3, 1, "t");

    let mut cfg = config(&brokers, "topic-prefixed-reset");
    cfg.rdkafka.remove("auto.offset.reset");
    cfg.rdkafka
        .insert("topic.auto.offset.reset".into(), "earliest".into());
    let cp = Checkpointer::new();
    let mut source = KafkaSource::new(cfg);
    source.open(SourceCtx::new(cp.handle())).expect("open");
    let mut lanes = await_assignment(&mut source, ASSIGNMENT);
    let rows = drain_lane(&mut source, &mut lanes[0], 3, Duration::from_secs(30));
    let offsets: Vec<i64> = rows.iter().map(|(_, _, o)| *o).collect();
    assert_eq!(offsets, vec![0, 1, 2]);
}

/// A failed committed-offset lookup starts the partition from its committed
/// offset, never from the reset policy.
#[test]
fn a_failed_offset_lookup_starts_from_the_committed_offset() {
    use rdkafka::types::RDKafkaRespErr::{
        RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS as LOAD_IN_PROGRESS,
        RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED as AUTHORIZATION_FAILED,
    };
    /// Load-in-progress replies queued for librdkafka's own query. At the
    /// one-second retry backoff cap they outlast every deadline in the test.
    const HELD_REPLIES: usize = 1000;

    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 6, 1, "d");
    commit_as(&brokers, "lookup-error", 3);
    // The first error fails the source's lookup. librdkafka retries its own
    // query on load-in-progress without limit, so that query is answered only
    // once the errors are cleared below. A reply that lands between the
    // source's pause and resume can drop the resume as outdated and leave the
    // partition paused (librdkafka#5591).
    let mut errors = vec![AUTHORIZATION_FAILED];
    errors.extend([LOAD_IN_PROGRESS; HELD_REPLIES]);
    cluster.request_errors(rdkafka::types::RDKafkaApiKey::OffsetFetch, &errors);

    let cfg = config(&brokers, "lookup-error");
    let cp = Checkpointer::new();
    let mut source = KafkaSource::new(cfg);

    let lines = spate_test::capture_logs(tracing::Level::WARN, || {
        source.open(SourceCtx::new(cp.handle())).expect("open");
        let mut lanes = await_assignment(&mut source, ASSIGNMENT);
        cluster.clear_request_errors(rdkafka::types::RDKafkaApiKey::OffsetFetch);
        let rows = drain_lane(&mut source, &mut lanes[0], 3, Duration::from_secs(30));
        let offsets: Vec<i64> = rows.iter().map(|(_, _, o)| *o).collect();
        assert_eq!(offsets, vec![3, 4, 5]);
    });
    assert!(
        lines
            .iter()
            .any(|l| l.contains("committed offset lookup failed")),
        "the injected error did not reach the lookup: {lines:#?}"
    );
}

/// A member that starts from a committed offset publishes its lag before it
/// commits anything itself.
#[test]
fn a_restarted_member_publishes_lag_before_committing() {
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 10, 1, "e");
    commit_as(&brokers, "restart", 3);

    let mut cfg = config(&brokers, "restart");
    cfg.statistics_interval = Duration::from_millis(100);
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let metrics = std::sync::Arc::new(spate_core::metrics::SourceMetrics::new(
            &spate_core::metrics::ComponentLabels::new("restart", "source", "kafka"),
        ));
        let cp = Checkpointer::new();
        let mut source = KafkaSource::new(cfg);
        source
            .open(SourceCtx::new(cp.handle()).with_stage_metrics(Some(metrics)))
            .expect("open");
        let _lanes = await_assignment(&mut source, ASSIGNMENT);
        spate_test::wait_until(Duration::from_secs(30), "lag published", || {
            source
                .poll_events(Duration::from_millis(100))
                .expect("poll_events");
            lag_series_count(&handle.render()) == 1
        });
    });

    let rendered = handle.render();
    assert_eq!(
        support::lag_of(&rendered, "restart", 0),
        Some(7.0),
        "lag must equal produced - committed:\n{rendered}"
    );
}

#[test]
#[expect(clippy::disallowed_methods, reason = "the test joins the thread")]
fn second_member_triggers_revoke_then_fresh_assignment() {
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 4, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();

    let cp = Checkpointer::new();
    let mut source = KafkaSource::new(config(&brokers, "grp"));
    source.open(SourceCtx::new(cp.handle())).expect("open");
    let lanes = await_assignment(&mut source, ASSIGNMENT);
    assert_eq!(lanes.len(), 4, "sole member owns everything");
    let first_ids: Vec<_> = lanes.iter().map(SourceLane::id).collect();

    // A second member joins; keep polling it from a helper thread so its
    // side of the rebalance completes.
    let brokers2 = brokers.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = std::sync::Arc::clone(&stop);
    let joiner = std::thread::spawn(move || {
        let c: BaseConsumer = support::raw_config()
            .set("bootstrap.servers", &brokers2)
            .set("group.id", "grp")
            .set("auto.offset.reset", "earliest")
            .build()
            .expect("second consumer");
        c.subscribe(&[TOPIC]).expect("subscribe");
        while !stop2.load(Ordering::Relaxed) {
            let _ = c.poll(Duration::from_millis(100));
        }
    });

    // Expect the eager revoke of all four lanes, with the barrier sized to
    // the lane count; simulate the drivers draining.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut revoked = false;
    let mut reassigned = 0usize;
    while Instant::now() < deadline {
        match source
            .poll_events(Duration::from_millis(200))
            .expect("poll_events")
        {
            SourceEvent::LanesRevoked {
                lanes: ids,
                barrier,
            } => {
                assert!(!revoked, "single eager revoke expected");
                revoked = true;
                let mut sorted = ids.clone();
                sorted.sort();
                let mut expected = first_ids.clone();
                expected.sort();
                assert_eq!(sorted, expected, "all lanes revoked eagerly");
                assert_eq!(barrier.remaining(), ids.len());
                let _ = support::retryable(
                    "flush",
                    support::complete_revocation(&mut source, &ids, &barrier, &[]),
                );
            }
            SourceEvent::LanesAssigned(new_lanes) => {
                if revoked {
                    reassigned = new_lanes.len();
                    break;
                }
                // Pre-join duplicate assignments should not happen.
                panic!("unexpected assignment before revoke");
            }
            _ => {}
        }
    }
    stop.store(true, Ordering::Relaxed);
    joiner.join().expect("joiner thread");

    assert!(revoked, "revocation observed");
    assert!(
        (1..4).contains(&reassigned),
        "re-assignment shares partitions with the second member (got {reassigned})"
    );
    drop(lanes); // old lanes drop cleanly after revocation completed
}

#[test]
fn startup_without_brokers_times_out_fatally() {
    let mut cfg = config("127.0.0.1:1", "nope");
    // Out of real time's reach, so only the injected clock can end it.
    cfg.startup_timeout = Duration::from_secs(300);
    let cp = Checkpointer::new();
    let clock = TestClock::frozen();
    let mut source = KafkaSource::new(cfg).with_clock(Arc::clone(&clock) as _);
    source.open(SourceCtx::new(cp.handle())).expect("open");
    clock.advance(Duration::from_secs(301));

    wait_until(Duration::from_secs(20), "the startup timeout", || {
        match source.poll_events(Duration::from_millis(100)) {
            Ok(_) => false,
            // Transient transport errors are expected with no broker; the
            // runtime logs and keeps polling on Retryable, and so does this
            // loop. Only the fatal startup timeout ends it.
            Err(e) if e.class() == ErrorClass::Retryable => false,
            Err(e) => {
                assert!(
                    e.to_string().contains("no partition assignment"),
                    "unexpected error: {e}"
                );
                true
            }
        }
    });
}

/// Drive one source one step, mirroring the runtime driver: apply a new
/// assignment (opening an epoch), or complete a revocation (barrier + final
/// commit). Returns how many lanes the source now owns, or `None` on an idle
/// tick.
fn drive_step(
    source: &mut KafkaSource,
    lanes: &mut Vec<<KafkaSource as Source>::Lane>,
    cp: &mut Checkpointer,
    epoch: &mut u32,
    timeout: Duration,
) -> Option<usize> {
    match source.poll_events(timeout).expect("poll_events") {
        SourceEvent::LanesAssigned(new) => {
            let parts: Vec<PartitionId> = new.iter().map(SourceLane::partition).collect();
            *epoch += 1;
            cp.begin_epoch(&parts, *epoch);
            let n = new.len();
            *lanes = new;
            Some(n)
        }
        SourceEvent::LanesRevoked {
            lanes: ids,
            barrier,
        } => {
            lanes.clear();
            let _ = support::retryable(
                "flush",
                support::complete_revocation(source, &ids, &barrier, &[]),
            );
            Some(0)
        }
        _ => None,
    }
}

/// Regression: a member that is offered an empty assignment (more group
/// members than partitions) must still acknowledge the rebalance with
/// `assign()`. Under the deferred-completion protocol, skipping that leaves
/// librdkafka's rebalance in progress forever, so the member can never
/// complete a later rebalance; it sits idle and cannot pick up partitions
/// even after the owner leaves. Two same-group members share a single
/// partition: whichever ends up empty must still be able to take the
/// partition over once the owner departs.
#[test]
fn empty_assignment_completes_rebalance_protocol() {
    let cluster = MockKafka::new();
    cluster.hold_first_rebalance();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 5, 1, "e");

    let mut cp_a = Checkpointer::new();
    let mut cp_b = Checkpointer::new();
    let mut a = KafkaSource::new(config(&brokers, "empty"));
    let mut b = KafkaSource::new(config(&brokers, "empty"));
    a.open(SourceCtx::new(cp_a.handle())).expect("open a");
    b.open(SourceCtx::new(cp_b.handle())).expect("open b");

    let mut a_lanes: Vec<<KafkaSource as Source>::Lane> = Vec::new();
    let mut b_lanes: Vec<<KafkaSource as Source>::Lane> = Vec::new();
    let (mut a_ep, mut b_ep) = (0u32, 0u32);
    let (mut a_count, mut b_count) = (0usize, 0usize);

    // Round-robin both members from this thread until the group settles with
    // one owner and one empty member (stable for several quiet ticks).
    //
    // Both conditions are required. Quiet alone is not settled: if the two
    // members do not land in the same initial rebalance window, the second
    // join re-forms an established group, and the mock broker paces that at
    // `session.timeout.ms - 1000`, long enough to look quiet while the
    // reassignment is still in flight.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut quiet = 0;
    while !(quiet >= 25 && a_count + b_count == 1) {
        // Assert rather than fall out of the loop: on the deadline path the
        // counts can transiently read 1 mid-flight, which would satisfy the
        // assertion below and let the test continue against a group that
        // never settled. Requiring the settled predicate makes reaching the
        // deadline a failure with a diagnosis attached.
        assert!(
            Instant::now() < deadline,
            "the group never settled: quiet={quiet}, a={a_count}, b={b_count} \
             (want one owner, one empty member, stable for 25 ticks)"
        );
        let sa = drive_step(
            &mut a,
            &mut a_lanes,
            &mut cp_a,
            &mut a_ep,
            Duration::from_millis(100),
        );
        let sb = drive_step(
            &mut b,
            &mut b_lanes,
            &mut cp_b,
            &mut b_ep,
            Duration::from_millis(100),
        );
        if let Some(n) = sa {
            a_count = n;
        }
        if let Some(n) = sb {
            b_count = n;
        }
        if sa.is_none() && sb.is_none() {
            quiet += 1;
        } else {
            quiet = 0;
        }
    }
    assert_eq!(
        a_count + b_count,
        1,
        "exactly one member owns the single partition (the other is empty)"
    );

    // Drop the owner (it leaves the group) and keep driving the previously
    // empty member. If its empty-assignment rebalance was completed, it now
    // acquires the partition; if it was wedged, it never does.
    let (mut src, mut lanes, mut cp, mut ep) = if a_count == 1 {
        drop(a_lanes);
        drop(a);
        (b, b_lanes, cp_b, b_ep)
    } else {
        drop(b_lanes);
        drop(b);
        (a, a_lanes, cp_a, a_ep)
    };

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut acquired = 0usize;
    while Instant::now() < deadline {
        if let Some(n) = drive_step(
            &mut src,
            &mut lanes,
            &mut cp,
            &mut ep,
            Duration::from_millis(200),
        ) {
            acquired = n;
            if n == 1 {
                break;
            }
        }
    }
    assert_eq!(
        acquired, 1,
        "previously-empty member acquired the partition after the owner left \
         (its rebalance protocol was not wedged)"
    );

    // Sanity: the recovered member can consume the partition.
    let rows = drain_lane(&mut src, &mut lanes[0], 5, Duration::from_secs(30));
    assert_eq!(rows.len(), 5, "recovered member drains the partition");
}

/// Regression: a member the group assigns nothing keeps running, past
/// `assignment_timeout` and for as long as the group holds that shape.
/// Two same-group members share a single partition, so one settles empty.
///
/// The deadline counts from losing partitions, and any accepted
/// assignment clears it, an empty one included. A deadline that counted
/// "this member owns no partitions" would fail this member every
/// `assignment_timeout` and restart it into the same state, for as long as
/// the deployment ran more members than partitions. The arm-then-clear
/// ordering is pinned by the unit tests; the member here may join straight
/// into its empty assignment and never have owned anything at all.
///
/// Both members read one frozen clock, so the gaps in the mock broker's
/// rebalance pacing, where a member legitimately holds nothing, do not count
/// toward `DEADLINE` while the group settles.
#[test]
fn an_empty_member_survives_the_assignment_deadline() {
    const DEADLINE: Duration = Duration::from_secs(8);

    let cluster = MockKafka::new();
    cluster.hold_first_rebalance();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();

    let clock = TestClock::frozen();
    let member = |group: &str| {
        let mut cfg = config(&brokers, group);
        cfg.assignment_timeout = DEADLINE;
        KafkaSource::new(cfg).with_clock(Arc::clone(&clock) as _)
    };
    let mut cp_a = Checkpointer::new();
    let mut cp_b = Checkpointer::new();
    let mut a = member("empty-deadline");
    let mut b = member("empty-deadline");
    a.open(SourceCtx::new(cp_a.handle())).expect("open a");
    b.open(SourceCtx::new(cp_b.handle())).expect("open b");

    let mut a_lanes: Vec<<KafkaSource as Source>::Lane> = Vec::new();
    let mut b_lanes: Vec<<KafkaSource as Source>::Lane> = Vec::new();
    let (mut a_ep, mut b_ep) = (0u32, 0u32);
    let (mut a_count, mut b_count) = (0usize, 0usize);

    // Settle the group with one owner and one empty member, on the
    // conditions `empty_assignment_completes_rebalance_protocol` documents.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut quiet = 0;
    while !(quiet >= 25 && a_count + b_count == 1) {
        assert!(
            Instant::now() < deadline,
            "the group never settled: quiet={quiet}, a={a_count}, b={b_count} \
             (want one owner, one empty member, stable for 25 ticks)"
        );
        let sa = drive_step(
            &mut a,
            &mut a_lanes,
            &mut cp_a,
            &mut a_ep,
            Duration::from_millis(100),
        );
        let sb = drive_step(
            &mut b,
            &mut b_lanes,
            &mut cp_b,
            &mut b_ep,
            Duration::from_millis(100),
        );
        if let Some(n) = sa {
            a_count = n;
        }
        if let Some(n) = sb {
            b_count = n;
        }
        if sa.is_none() && sb.is_none() {
            quiet += 1;
        } else {
            quiet = 0;
        }
    }

    // Drive both past the deadline, keeping the owner in the group so the
    // empty member stays empty, and hold the empty one to `Ok`.
    let (mut empty, mut owner, mut owner_lanes, mut owner_cp, mut owner_ep) = if a_count == 0 {
        (a, b, b_lanes, cp_b, b_ep)
    } else {
        (b, a, a_lanes, cp_a, a_ep)
    };
    clock.advance(DEADLINE + Duration::from_secs(2));
    for _ in 0..25 {
        drive_step(
            &mut owner,
            &mut owner_lanes,
            &mut owner_cp,
            &mut owner_ep,
            Duration::from_millis(50),
        );
        match empty.poll_events(Duration::from_millis(50)) {
            Ok(SourceEvent::LanesAssigned(lanes)) => panic!(
                "the empty member took {} partitions, so the group moved and \
                 the deadline was never exercised",
                lanes.len()
            ),
            Ok(_) => {}
            Err(e) => panic!("an empty member must keep running: {e}"),
        }
    }
}

#[test]
fn statistics_populate_kafka_source_metrics() {
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 2, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 5, 2, "stats");

    // MockCluster runs real librdkafka, so a non-zero interval fires the
    // stats callback; field values are environment-dependent, so this test
    // asserts series presence (the plumbing), not values.
    let mut cfg = config(&brokers, "stats");
    cfg.statistics_interval = Duration::from_millis(100);

    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let cp = Checkpointer::new();
        let mut source = KafkaSource::new(cfg);
        // The runtime would mint this Meter from `component_type()` via
        // `Meter::for_component`; tests use the public constructor (prefix
        // `spate_kafka_` instead of the role-scoped `spate_kafka_source_`).
        let meter = spate_core::metrics::Meter::with_namespace("kafka", "stats", "source", "kafka");
        source
            .open(
                SourceCtx::new(cp.handle())
                    .with_meter(Some(meter))
                    .with_partition_detail(true),
            )
            .expect("open");
        // A failure carries the source's warnings: a partition that is not
        // fetching, with its fetch state, and any main-queue rewind.
        spate_test::show_logs(tracing::Level::WARN, || {
            let _lanes = await_assignment(&mut source, ASSIGNMENT);

            // Poll past at least one statistics interval; stop as soon as the
            // families appear. The not-fetching series is waited on by value
            // rather than by presence, because the first snapshot after an
            // assignment legitimately catches a partition at `offset-query`.
            let needles = [
                "spate_kafka_group_assignment_size".to_owned(),
                "spate_kafka_rx_responses_total".to_owned(),
                "spate_kafka_broker_up".to_owned(),
                not_fetching(0),
                not_fetching(1),
            ];
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                source
                    .poll_events(Duration::from_millis(100))
                    .expect("poll_events");
                let rendered = handle.render();
                let missing: Vec<&str> = needles
                    .iter()
                    .filter(|n| !rendered.contains(n.as_str()))
                    .map(String::as_str)
                    .collect();
                if missing.is_empty() {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "statistics series did not appear within deadline, \
                 missing {missing:?}:\n{rendered}"
                );
            }
        });
    });

    let rendered = handle.render();
    for needle in [
        // Fixed families (registered at open).
        r#"spate_kafka_rx_responses_total{pipeline="stats",component="source",component_type="kafka"}"#,
        "spate_kafka_reply_queue_depth",
        "spate_kafka_fetch_queue_messages",
        "spate_kafka_group_assignment_size",
        "spate_kafka_group_healthy",
        // Lazily-registered per-broker series (the MockCluster broker).
        "spate_kafka_broker_up{",
    ] {
        assert!(rendered.contains(needle), "missing `{needle}`:\n{rendered}");
    }
}

/// The exposition line for a partition that is fetching. Real librdkafka
/// fills `fetch_state`, so waiting on this pins the field the source reads
/// as well as the value a healthy assignment produces.
fn not_fetching(partition: i32) -> String {
    format!(
        r#"spate_kafka_partition_not_fetching{{pipeline="stats",component="source",component_type="kafka",partition="{partition}"}} 0"#
    )
}

#[test]
fn statistics_disabled_registers_no_families() {
    // `statistics_interval: 0s` must disable the whole `spate_kafka_source_*`
    // family, not just stop updating it. Registering the fixed handles at
    // `open` regardless would leave them frozen at their unset default (e.g.
    // `group_healthy 0`, a documented alert signal) even though librdkafka
    // never emits a snapshot.
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 2, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();

    let cfg = config(&brokers, "no-stats"); // config() leaves statistics off
    assert!(cfg.statistics_interval.is_zero());

    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let cp = Checkpointer::new();
        let mut source = KafkaSource::new(cfg);
        // A Meter is present (the runtime always mints one from a declared
        // `component_type`); only the disabled interval should suppress the
        // families.
        let meter = spate_core::metrics::Meter::with_namespace("kafka", "stats", "source", "kafka");
        source
            .open(
                SourceCtx::new(cp.handle())
                    .with_meter(Some(meter))
                    .with_partition_detail(true),
            )
            .expect("open");
        // Drive the control plane a few times; with stats off no snapshot
        // ever arrives and nothing should register.
        let _lanes = await_assignment(&mut source, ASSIGNMENT);
        for _ in 0..5 {
            source
                .poll_events(Duration::from_millis(100))
                .expect("poll_events");
        }
    });

    let rendered = handle.render();
    assert!(
        !rendered.contains("spate_kafka_"),
        "statistics disabled must register no `spate_kafka_*` series:\n{rendered}"
    );
}

/// How many `spate_source_lag_records` series the exposition carries. The family
/// is per-partition only, so this is the number of partitions that have
/// reported a measured lag.
fn lag_series_count(rendered: &str) -> usize {
    rendered
        .lines()
        .filter(|l| l.starts_with("spate_source_lag_records{"))
        .count()
}

/// The regression this pins: a consumer that commits a small prefix and then stops
/// consuming must report the remaining backlog as consumer lag.
///
/// This is the test that did not exist while `spate_source_lag_records` rendered
/// a permanent `0` on every Kafka pipeline. The two properties it pins are the
/// ones that failed:
///
/// 1. the framework's source-stage handles reach the connector at `open` (via
///    `SourceCtx::stage_metrics`); the existing statistics test opens without
///    them, so it could never have caught this;
/// 2. the published value is the real backlog, per partition, rather than a
///    registered-but-never-written gauge's zero.
#[test]
fn a_backlogged_consumer_publishes_its_lag() {
    const PARTITIONS: i32 = 2;
    const PRODUCED: usize = 500;
    const CONSUMED: usize = 50;

    let cluster = MockKafka::new();
    cluster
        .create_topic(TOPIC, PARTITIONS, 1)
        .expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, PRODUCED, PARTITIONS, "lag");

    let mut cfg = config(&brokers, "lag");
    cfg.statistics_interval = Duration::from_millis(100);

    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    // Committed offset per partition, captured from the run: `drain_lane`
    // polls in batches so it overshoots `CONSUMED`, and lag is measured
    // against what was committed.
    let mut committed: Vec<(PartitionId, i64)> = Vec::new();
    metrics::with_local_recorder(&recorder, || {
        // Resolved inside the local recorder, exactly as the runtime resolves
        // them before handing them to `open`.
        let metrics = std::sync::Arc::new(spate_core::metrics::SourceMetrics::new(
            &spate_core::metrics::ComponentLabels::new("lag", "source", "kafka"),
        ));

        let mut cp = Checkpointer::new();
        let mut source = KafkaSource::new(cfg);
        source
            .open(SourceCtx::new(cp.handle()).with_stage_metrics(Some(metrics)))
            .expect("open");

        let mut lanes = await_assignment(&mut source, ASSIGNMENT);
        assert_eq!(lanes.len(), PARTITIONS as usize, "one lane per partition");
        let partitions: Vec<PartitionId> = lanes.iter().map(SourceLane::partition).collect();
        cp.begin_epoch(&partitions, 1);

        // Consume a prefix and commit it: `consumer_lag` is
        // `(hi_offset or ls_offset) - committed_offset`, so it stays `-1`,
        // unknown and correctly unpublished, until a commit lands.
        for lane in &mut lanes {
            let rows = drain_lane(&mut source, lane, CONSUMED, Duration::from_secs(30));
            assert!(rows.len() >= CONSUMED, "drained {} rows", rows.len());
        }
        cp.drain();
        let watermarks = cp.take_watermarks();
        assert_eq!(
            watermarks.len(),
            PARTITIONS as usize,
            "every partition committable"
        );
        source.commit(&watermarks).expect("store offsets");
        source.flush_commits().expect("sync commit");
        committed = watermarks;

        // Stop consuming (lanes stay assigned but unpolled) and drive the
        // control plane until every partition has reported. Waiting for the
        // *first* series is not enough: `consumer_lag` is per partition, so a
        // snapshot can carry a number for p0 while p1 is still `-1`, and the
        // assertions below require one series each.
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            source
                .poll_events(Duration::from_millis(100))
                .expect("poll_events");
            if lag_series_count(&handle.render()) == PARTITIONS as usize {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "not every partition published consumer lag within deadline:\n{}",
                handle.render()
            );
        }
    });

    let rendered = handle.render();
    for (partition, offset) in &committed {
        let expected = PRODUCED as f64 - *offset as f64;
        assert!(expected > 0.0, "the test must leave a backlog");
        assert_eq!(
            support::lag_of(&rendered, "lag", i32::try_from(partition.0).unwrap()),
            Some(expected),
            "partition {partition:?} lag must equal produced - committed:\n{rendered}"
        );
    }

    // No aggregate series shares the family name: readers aggregate with
    // `sum`/`max`, which double-counts if an unlabeled series exists.
    assert!(
        !rendered
            .lines()
            .filter(|l| l.starts_with("spate_source_lag_records{"))
            .any(|l| !l.contains("partition=")),
        "every lag series must carry a partition label:\n{rendered}"
    );
}

/// A directly assigned consumer (no group coordination), positioned at the
/// beginning of every partition.
fn assigned_consumer(brokers: &str, group: &str, partitions: i32) -> std::sync::Arc<BaseConsumer> {
    let consumer: BaseConsumer = support::raw_config()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .build()
        .expect("consumer");
    let mut tpl = rdkafka::TopicPartitionList::new();
    for p in 0..partitions {
        tpl.add_partition_offset(TOPIC, p, rdkafka::Offset::Beginning)
            .expect("tpl entry");
    }
    consumer.assign(&tpl).expect("assign");
    std::sync::Arc::new(consumer)
}

/// Count the messages the consumer's main queue hands back over `window`.
fn drain_main_queue(consumer: &BaseConsumer, window: Duration) -> usize {
    let deadline = Instant::now() + window;
    let mut delivered = 0usize;
    while Instant::now() < deadline {
        if let Some(result) = consumer.poll(Duration::from_millis(100)) {
            result.expect("main queue message");
            delivered += 1;
        }
    }
    delivered
}

/// An assigned partition with no split queue delivers on the consumer's main
/// queue. Without it, a zero from a main-queue drain is not evidence about
/// routing.
#[test]
fn the_main_queue_delivers_a_partition_that_was_never_split() {
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 20, 1, "p");

    let consumer = assigned_consumer(&brokers, "plain", 1);
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut delivered = false;
    while !delivered && Instant::now() < deadline {
        if let Some(result) = consumer.poll(Duration::from_millis(100)) {
            result.expect("main queue message");
            delivered = true;
        }
    }
    assert!(delivered, "an unsplit partition delivered nothing");
}

/// How long a main-queue drain runs before it reports an absence. A first
/// main-queue delivery lands about 0.5s after the assign on MockCluster, an
/// order of magnitude inside this.
const WINDOW: Duration = Duration::from_secs(6);

/// A partition whose queue has been split stays off the main queue. The
/// `RD_KAFKA_Q_F_FWD_APP` mark survives dropping the `PartitionQueue`, an
/// unassign and a re-assign, so the main queue delivers nothing for it.
#[test]
fn a_split_partition_never_returns_to_the_main_queue() {
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 20, 1, "w");

    let consumer = assigned_consumer(&brokers, "fwdapp", 1);
    let queue = consumer
        .split_partition_queue(TOPIC, 0)
        .expect("split partition queue");

    // A partition that never fetched also reports zero on the main queue, so
    // the split queue has to deliver before the drain below means anything.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut split_delivered = false;
    while !split_delivered && Instant::now() < deadline {
        let _ = consumer.poll(Duration::from_millis(100));
        if let Some(result) = queue.poll(Duration::from_millis(100)) {
            result.expect("split queue message");
            split_delivered = true;
        }
    }
    assert!(split_delivered, "the split queue delivered nothing");

    drop(queue);
    consumer.unassign().expect("unassign");
    let mut tpl = rdkafka::TopicPartitionList::new();
    tpl.add_partition_offset(TOPIC, 0, rdkafka::Offset::Beginning)
        .expect("tpl entry");
    consumer.assign(&tpl).expect("re-assign");

    let main_queue = drain_main_queue(&consumer, WINDOW);

    // The re-assign positions the partition at the beginning, so a fetch that
    // restarted hands back offset 0; residue from the first fetch starts at 1
    // or higher.
    let requeued = consumer
        .split_partition_queue(TOPIC, 0)
        .expect("re-split partition queue");
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut resplit_offset = None;
    while resplit_offset.is_none() && Instant::now() < deadline {
        let _ = consumer.poll(Duration::from_millis(100));
        if let Some(result) = requeued.poll(Duration::from_millis(100)) {
            let msg = result.expect("re-split queue message");
            resplit_offset = Some(rdkafka::Message::offset(&msg));
        }
    }
    assert_eq!(
        (main_queue, resplit_offset),
        (0, Some(0)),
        "main queue delivered {main_queue}, re-split queue delivered {resplit_offset:?}"
    );
}

/// The source reads zstd-compressed batches in full.
/// Regression for #1253.
#[test]
fn zstd_batches_are_read() {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    let producer: BaseProducer = support::raw_config()
        .set("bootstrap.servers", &brokers)
        .set("compression.codec", "zstd")
        .set("linger.ms", "100")
        .build()
        .expect("zstd producer");
    for i in 0..20 {
        let payload = format!("z-{i}");
        producer
            .send(
                BaseRecord::<[u8], [u8]>::to(TOPIC)
                    .partition(0)
                    .payload(payload.as_bytes()),
            )
            .map_err(|(e, _)| e)
            .expect("enqueue");
    }
    producer.flush(Duration::from_secs(30)).expect("flush");

    let mut cp = Checkpointer::new();
    let mut source = KafkaSource::new(config(&brokers, "zstd"));
    source.open(SourceCtx::new(cp.handle())).expect("open");
    let mut lanes = await_assignment(&mut source, ASSIGNMENT);
    cp.begin_epoch(&[lanes[0].partition()], 1);

    let rows = drain_lane(&mut source, &mut lanes[0], 20, Duration::from_secs(30));
    let offsets: Vec<i64> = rows.iter().map(|(_, _, o)| *o).collect();
    assert_eq!(offsets, (0..20).collect::<Vec<_>>());
    assert_eq!(rows[7].0, b"z-7");
}

/// A message librdkafka reports as corrupt after a lane poll's first message
/// ends the batch, which is delivered and acknowledged, and the next poll
/// fails as Fatal. Nothing at or past the corrupt offset is delivered.
/// Regression for #1257.
#[test]
fn a_corrupt_batch_fails_the_lane() {
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 1, 1, "before");
    assert_eq!(
        produce_batch(&brokers, TOPIC, 0, &corrupt_batch(b"lost", b"body")),
        0,
        "the corrupt batch is stored at offset 1"
    );
    produce(&brokers, TOPIC, 1, 1, "after");

    let mut cfg = config(&brokers, "corrupt-batch");
    cfg.rdkafka.insert("check.crcs".into(), "true".into());
    cfg.statistics_interval = Duration::from_millis(50);
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let mut cp = Checkpointer::new();
        let mut source = KafkaSource::new(cfg);
        let meter =
            spate_core::metrics::Meter::with_namespace("kafka", "corrupt-batch", "source", "kafka");
        source
            .open(
                SourceCtx::new(cp.handle())
                    .with_meter(Some(meter))
                    .with_partition_detail(true),
            )
            .expect("open");
        let mut lanes = await_assignment(&mut source, ASSIGNMENT);
        cp.begin_epoch(&[PartitionId(0)], 1);
        let lane = &mut lanes[0];

        // librdkafka counts a queued error as an entry, and its queue holds
        // one entry before any data, so three entries are the record at
        // offset 0 and the corrupt batch's error, and the lane's first poll
        // meets the error after its first message.
        let mut errors = Vec::new();
        wait_until(ASSIGNMENT, "the record and the error queued", || {
            serve_events(&mut source, &mut errors);
            support::fetch_queue_entries(&handle.render(), "corrupt-batch") >= 3.0
        });

        let mut batch = lane
            .poll(64, Duration::from_secs(5))
            .expect("the poll meeting the error delivers what precedes it")
            .expect("a batch");
        let mut offsets = Vec::new();
        while let Some(raw) = batch.next_payload() {
            offsets.push(raw.offset);
        }
        drop(batch);
        assert_eq!(offsets, [0]);

        let error = lane
            .poll(64, Duration::from_secs(5))
            .expect_err("the next poll fails")
            .to_string();
        assert!(
            error.contains("(Fatal)") && error.contains("BadMessage"),
            "{error}"
        );

        drop(lanes);
        cp.drain();
        assert_eq!(cp.take_watermarks(), [(PartitionId(0), 1)]);
    });
}

/// A v1 message librdkafka skips as corrupt, followed in the same fetch by
/// a readable one, fails the lane before the readable one is delivered.
/// Fetch is capped at v11, the last version librdkafka reads v0/v1 data
/// from. The mock accepts only a payload that opens with a magic-2 batch, so
/// the v1 messages follow one. Regression for #1257.
#[test]
fn a_skipped_v1_message_fails_the_lane() {
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    cluster
        .apiversion(rdkafka::types::RDKafkaApiKey::Fetch, Some(0), Some(11))
        .expect("cap Fetch");
    let brokers = cluster.bootstrap_servers();
    let mut payload = v2_batch(b"before", b"body", 3);
    payload.extend(v1_message(1, b"lost", b"body", true));
    payload.extend(v1_message(2, b"after", b"body", false));
    assert_eq!(produce_batch(&brokers, TOPIC, 0, &payload), 0);

    let mut cfg = config(&brokers, "skipped-v1");
    cfg.rdkafka.insert("check.crcs".into(), "true".into());
    let mut cp = Checkpointer::new();
    let mut source = KafkaSource::new(cfg);
    source.open(SourceCtx::new(cp.handle())).expect("open");
    let mut lanes = await_assignment(&mut source, ASSIGNMENT);
    cp.begin_epoch(&[PartitionId(0)], 1);
    let lane = &mut lanes[0];

    let mut offsets = Vec::new();
    let mut outcome = None;
    let mut errors = Vec::new();
    wait_until(
        ASSIGNMENT,
        "a lane poll fails or offset 2 is delivered",
        || {
            serve_events(&mut source, &mut errors);
            match lane.poll(64, Duration::from_millis(200)) {
                Ok(Some(mut batch)) => {
                    while let Some(raw) = batch.next_payload() {
                        offsets.push(raw.offset);
                    }
                }
                Ok(None) => {}
                Err(e) => outcome = Some(e.to_string()),
            }
            outcome.is_some() || offsets.contains(&2)
        },
    );
    let error = outcome.expect("a poll fails");
    assert!(
        error.contains("(Fatal)") && error.contains("BadMessage"),
        "{error}"
    );
    assert_eq!(offsets, [0], "delivered offsets");
    drop(lanes);
    cp.drain();
    assert_eq!(cp.take_watermarks(), [(PartitionId(0), 1)]);
}

/// A fatal fetch error queued behind records ends the lane's batch, and the
/// next poll returns it. librdkafka queues a `TopicAuthorizationFailed`
/// fetch error only when the partition's last error differs, so a lane that
/// dropped it would not see it again until a fetch succeeds or the
/// partition's fetch restarts after a pause, seek or new assignment.
#[test]
fn a_mid_batch_authorization_failure_fails_the_lane() {
    use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 2, 1, "rec");

    let mut cfg = config(&brokers, "mid-batch-auth");
    cfg.statistics_interval = Duration::from_millis(50);
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let mut cp = Checkpointer::new();
        let mut source = KafkaSource::new(cfg);
        let meter = spate_core::metrics::Meter::with_namespace(
            "kafka",
            "mid-batch-auth",
            "source",
            "kafka",
        );
        source
            .open(
                SourceCtx::new(cp.handle())
                    .with_meter(Some(meter))
                    .with_partition_detail(true),
            )
            .expect("open");
        let mut lanes = await_assignment(&mut source, ASSIGNMENT);
        cp.begin_epoch(&[PartitionId(0)], 1);
        let lane = &mut lanes[0];

        let entries = || support::fetch_queue_entries(&handle.render(), "mid-batch-auth");
        let mut errors = Vec::new();
        // One entry before any data, then the two records.
        wait_until(ASSIGNMENT, "both records queued", || {
            serve_events(&mut source, &mut errors);
            entries() >= 3.0
        });
        let before = entries();
        cluster.request_errors(
            RDKafkaApiKey::Fetch,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED; 1000],
        );
        wait_until(ASSIGNMENT, "the error queued behind them", || {
            serve_events(&mut source, &mut errors);
            entries() >= before + 1.0
        });

        let mut batch = lane
            .poll(64, Duration::from_secs(5))
            .expect("the poll meeting the error delivers what precedes it")
            .expect("a batch");
        let mut offsets = Vec::new();
        while let Some(raw) = batch.next_payload() {
            offsets.push(raw.offset);
        }
        drop(batch);
        assert_eq!(offsets, [0, 1]);

        let error = lane
            .poll(64, Duration::from_secs(5))
            .expect_err("the next poll fails")
            .to_string();
        assert!(
            error.contains("(Fatal)") && error.contains("TopicAuthorizationFailed"),
            "{error}"
        );
    });
}

type Lane = <KafkaSource as Source>::Lane;

/// Plays the runtime's part for one source: begins an epoch on each
/// assignment, admits each added lane to it, and drains, commits and flushes
/// on each revocation. Asserts the runtime's contract on every event: an
/// assignment arrives with no lane live, and no lane id is reused.
struct Driver {
    source: KafkaSource,
    cp: Checkpointer,
    lanes: Vec<Lane>,
    epoch: u32,
    seen: Vec<spate_core::source::LaneId>,
    revocations: usize,
    /// The reasons of the flushes that failed during a revocation.
    flush_failures: Vec<String>,
    /// The statistics of a driver from [`Driver::open_watched`].
    statistics: Option<metrics_exporter_prometheus::PrometheusHandle>,
}

impl Driver {
    fn open(cfg: KafkaSourceConfig) -> Driver {
        Driver::open_with(KafkaSource::new(cfg), |ctx| ctx)
    }

    fn open_with(mut source: KafkaSource, ctx: impl FnOnce(SourceCtx) -> SourceCtx) -> Driver {
        let cp = Checkpointer::new();
        source.open(ctx(SourceCtx::new(cp.handle()))).expect("open");
        Driver {
            source,
            cp,
            lanes: Vec::new(),
            epoch: 0,
            seen: Vec::new(),
            revocations: 0,
            flush_failures: Vec::new(),
            statistics: None,
        }
    }

    /// Open `source` with its statistics on, publishing to a recorder of its
    /// own so [`Driver::settle_group`] can read them. `group` labels the
    /// series.
    fn open_watched(source: KafkaSource, group: &str) -> Driver {
        let (recorder, handle, meter) = support::recorded_meter(group.to_owned());
        let mut driver = metrics::with_local_recorder(&recorder, || {
            Driver::open_with(source, |ctx| ctx.with_meter(Some(meter)))
        });
        driver.statistics = Some(handle);
        driver
    }

    /// Serve one `poll_events` call.
    fn step(&mut self, timeout: Duration) {
        match support::retryable("poll_events", self.source.poll_events(timeout)) {
            Ok(SourceEvent::LanesAssigned(new)) => {
                assert!(
                    self.lanes.is_empty(),
                    "an assignment arrived with {} lanes live",
                    self.lanes.len()
                );
                self.admit(&new);
                self.epoch += 1;
                let parts: Vec<PartitionId> = new.iter().map(SourceLane::partition).collect();
                self.cp.begin_epoch(&parts, self.epoch);
                self.lanes = new;
            }
            Ok(SourceEvent::LanesAdded(new)) => {
                self.admit(&new);
                let parts: Vec<PartitionId> = new.iter().map(SourceLane::partition).collect();
                self.cp.extend_epoch(&parts).expect("admit added lanes");
                self.lanes.extend(new);
            }
            Ok(SourceEvent::LanesRevoked { lanes, barrier }) => {
                self.lanes.retain(|l| !lanes.contains(&l.id()));
                self.cp.drain();
                let watermarks = self.cp.take_watermarks();
                self.flush_failures.extend(
                    support::retryable(
                        "flush",
                        support::complete_revocation(
                            &mut self.source,
                            &lanes,
                            &barrier,
                            &watermarks,
                        ),
                    )
                    .err(),
                );
                self.revocations += 1;
            }
            Ok(_) | Err(_) => {}
        }
    }

    fn admit(&mut self, new: &[Lane]) {
        for lane in new {
            assert!(
                !self.seen.contains(&lane.id()),
                "lane id {:?} reused",
                lane.id()
            );
            self.seen.push(lane.id());
        }
    }

    /// The partitions of the live lanes, sorted.
    fn partitions(&self) -> Vec<u32> {
        let mut parts: Vec<u32> = self.lanes.iter().map(|l| l.partition().0).collect();
        parts.sort_unstable();
        parts
    }

    /// Step until the live lanes cover partitions `0..n`.
    fn settle(&mut self, n: u32, timeout: Duration) {
        let want: Vec<u32> = (0..n).collect();
        wait_until(timeout, "the live lanes to cover every partition", || {
            if self.partitions() == want {
                return true;
            }
            self.step(Duration::from_millis(100));
            false
        });
    }

    // ANCHOR: settle_group
    /// Step until the live lanes cover partitions `0..partitions` and each of
    /// `consumers` consumers reports itself in a stable group, so the group
    /// has finished forming. Takes a driver from [`Driver::open_watched`].
    fn settle_group(&mut self, consumers: usize, partitions: u32, timeout: Duration) {
        let want: Vec<u32> = (0..partitions).collect();
        wait_until(timeout, "every consumer in a stable group", || {
            self.step(Duration::from_millis(100));
            let rendered = self
                .statistics
                .as_ref()
                .expect("a driver from open_watched")
                .render();
            // Read directly: a lone consumer's series has no `client` label.
            let healthy = spate_test::metric_series(&rendered, "spate_kafka_group_healthy", &[]);
            self.partitions() == want
                && healthy.len() == consumers
                && healthy.iter().all(|s| s.value == 1.0)
        });
    }
    // ANCHOR_END: settle_group

    /// Read every live lane, stepping between polls, until each partition has
    /// delivered `n` offsets, and return the offsets read per partition. Each
    /// batch is dropped once read, which acks it.
    fn read(
        &mut self,
        n: i64,
        timeout: Duration,
    ) -> BTreeMap<u32, std::collections::BTreeSet<i64>> {
        let mut got: BTreeMap<u32, std::collections::BTreeSet<i64>> = BTreeMap::new();
        let done = |got: &BTreeMap<u32, std::collections::BTreeSet<i64>>, parts: &[u32]| {
            !parts.is_empty()
                && parts
                    .iter()
                    .all(|p| got.get(p).is_some_and(|o| o.len() as i64 >= n))
        };
        wait_until(timeout, "each live lane's offsets", || {
            if done(&got, &self.partitions()) {
                return true;
            }
            self.step(Duration::from_millis(20));
            self.poll_lanes(Duration::from_millis(50), |partition, offset| {
                got.entry(partition).or_default().insert(offset);
            });
            false
        });
        got
    }

    /// Poll each live lane once for up to `wait`, handing every offset it delivers
    /// to `read`. Each batch is dropped once read, which acks it.
    fn poll_lanes(&mut self, wait: Duration, mut read: impl FnMut(u32, i64)) {
        for lane in &mut self.lanes {
            let partition = lane.partition().0;
            if let Some(mut batch) = lane.poll(64, wait).expect("lane poll") {
                while let Some(raw) = batch.next_payload() {
                    read(partition, raw.offset);
                }
            }
        }
    }

    /// Read every live lane while `produce` runs on each tick, until each
    /// partition has delivered a record, and return the first offset read per
    /// partition. Each batch is dropped once read, which acks it.
    fn read_from_end(
        &mut self,
        timeout: Duration,
        mut produce: impl FnMut(),
    ) -> BTreeMap<u32, i64> {
        let mut first: BTreeMap<u32, i64> = BTreeMap::new();
        let mut tick = 0;
        wait_until(timeout, "a record from each live lane", || {
            if tick % 20 == 0 {
                produce();
            }
            tick += 1;
            self.step(Duration::from_millis(10));
            self.poll_lanes(Duration::from_millis(10), |partition, offset| {
                first.entry(partition).or_insert(offset);
            });
            let parts = self.partitions();
            !parts.is_empty() && parts.iter().all(|p| first.contains_key(p))
        });
        first
    }

    /// Refuse one heartbeat so a member loses its assignment, step to the
    /// revocation that follows, and return what that revocation's flush reported.
    /// Takes a stable group, so the next revocation is this one. `flush_failures`
    /// can hold more: a group can remove a member while it forms, and that lost
    /// revocation records a failure of its own.
    fn lose_a_member(&mut self, cluster: &MockKafka) -> Vec<String> {
        let seen = self.flush_failures.len();
        let revocations = self.revocations;
        cluster.request_errors(
            RDKafkaApiKey::Heartbeat,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION],
        );
        wait_until(Duration::from_secs(30), "the revocation", || {
            self.step(Duration::from_millis(100));
            self.revocations > revocations
        });
        self.flush_failures[seen..].to_vec()
    }

    /// Commit and flush what the read batches made committable.
    fn commit(&mut self) {
        self.cp.drain();
        let watermarks = self.cp.take_watermarks();
        self.source.commit(&watermarks).expect("commit");
        self.source.flush_commits().expect("flush");
    }
}

fn consumers_config(brokers: &str, group: &str, consumers: usize) -> KafkaSourceConfig {
    let mut cfg = config(brokers, group);
    cfg.consumers = consumers;
    cfg
}

/// [`consumers_config`] publishing statistics fast enough for
/// [`Driver::settle_group`] to wait on.
fn watched_config(brokers: &str, group: &str, consumers: usize) -> KafkaSourceConfig {
    let mut cfg = consumers_config(brokers, group, consumers);
    cfg.statistics_interval = Duration::from_millis(100);
    cfg
}

/// Committed offsets of `group` on partitions `0..partitions`.
fn committed(brokers: &str, group: &str, partitions: i32) -> Vec<rdkafka::Offset> {
    support::committed_by(brokers, group, TOPIC, partitions)
}

/// Two consumers' assignments reach the runtime as one lane set: every
/// assignment arrives with no lane live and fresh lane ids, and the source
/// ends up holding every partition between them.
#[test]
fn consumers_share_one_epoch() {
    let cluster = MockKafka::new();
    cluster.hold_first_rebalance();
    cluster.create_topic(TOPIC, 4, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 5, 4, "s");

    let mut driver = Driver::open(consumers_config(&brokers, "share", 2));
    driver.settle(4, Duration::from_secs(60));
    driver.read(5, Duration::from_secs(30));
}

/// Each partition's offsets are stored and committed through the consumer
/// that owns it. With the interval commit out of the way, only
/// `flush_commits` commits, and every partition reads back committed.
#[test]
fn two_consumers_poll_ack_and_commit_every_partition() {
    let cluster = MockKafka::new();
    cluster.hold_first_rebalance();
    cluster.create_topic(TOPIC, 4, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 10, 4, "c");

    let mut cfg = watched_config(&brokers, "commit-each", 2);
    cfg.commit_interval = Duration::from_hours(1);
    let mut driver = Driver::open_watched(KafkaSource::new(cfg), "commit-each");
    driver.settle_group(2, 4, Duration::from_secs(60));
    driver.read(10, Duration::from_secs(30));
    driver.commit();

    assert_eq!(
        support::committed_by(&brokers, "commit-each", TOPIC, 4),
        vec![rdkafka::Offset::Offset(10); 4]
    );
}

/// A pause reaches the consumer that owns each lane's partition: while both
/// lanes are paused neither delivers, and on resume both deliver gap-free.
#[test]
fn pause_reaches_the_owning_consumer() {
    let cluster = MockKafka::new();
    cluster.hold_first_rebalance();
    cluster.create_topic(TOPIC, 2, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();

    let mut driver = Driver::open(consumers_config(&brokers, "pause-owner", 2));
    driver.settle(2, Duration::from_secs(60));
    let ids: Vec<_> = driver.lanes.iter().map(SourceLane::id).collect();
    driver.source.pause(&ids).expect("pause");
    produce(&brokers, TOPIC, 5, 2, "p");

    // While paused nothing is delivered (pause also purges prefetch).
    let idle_until = Instant::now() + Duration::from_secs(2);
    while Instant::now() < idle_until {
        driver.step(Duration::from_millis(50));
        for lane in &mut driver.lanes {
            assert!(
                lane.poll(64, Duration::from_millis(50))
                    .expect("poll")
                    .is_none(),
                "paused lane on partition {} delivered",
                lane.partition().0
            );
        }
    }

    driver.source.resume(&ids).expect("resume");
    driver.read(5, Duration::from_secs(30));
}

/// A member joining from outside revokes every consumer of the source, and
/// once it leaves the source holds every partition again under fresh lane ids.
#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "the test joins the joiner thread"
)]
fn an_outside_member_rebalances_every_in_process_consumer() {
    let cluster = MockKafka::new();
    cluster.hold_first_rebalance();
    cluster.create_topic(TOPIC, 4, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();

    let mut driver = Driver::open_watched(
        KafkaSource::new(watched_config(&brokers, "outside", 2)),
        "outside",
    );
    driver.settle_group(2, 4, Duration::from_secs(60));
    let before = driver.revocations;

    let stop = Arc::new(AtomicBool::new(false));
    let joiner = {
        let brokers = brokers.clone();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let c: BaseConsumer = support::raw_config()
                .set("bootstrap.servers", &brokers)
                .set("group.id", "outside")
                .set("session.timeout.ms", "6000")
                .build()
                .expect("joiner");
            c.subscribe(&[TOPIC]).expect("subscribe");
            while !stop.load(Ordering::Relaxed) {
                let _ = c.poll(Duration::from_millis(100));
            }
        })
    };
    wait_until(
        Duration::from_secs(60),
        "the joiner takes partitions",
        || {
            driver.step(Duration::from_millis(100));
            driver.revocations > before && !driver.lanes.is_empty() && driver.lanes.len() < 4
        },
    );

    stop.store(true, Ordering::Relaxed);
    joiner.join().expect("joiner thread");
    driver.settle_group(2, 4, Duration::from_secs(60));
}

/// A consumer the group assigns nothing stays in the group past both
/// deadlines, while the other consumer's lane keeps delivering and
/// committing.
#[test]
fn more_consumers_than_partitions_keeps_running() {
    let cluster = MockKafka::new();
    cluster.hold_first_rebalance();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();

    let clock = TestClock::frozen();
    let cfg = watched_config(&brokers, "idle-consumer", 2);
    let deadlines = cfg.startup_timeout + cfg.assignment_timeout;
    let source = KafkaSource::new(cfg).with_clock(Arc::clone(&clock) as _);
    let mut driver = Driver::open_watched(source, "idle-consumer");
    driver.settle_group(2, 1, Duration::from_secs(60));

    clock.advance(deadlines + Duration::from_secs(1));
    produce(&brokers, TOPIC, 5, 1, "i");
    driver.read(5, Duration::from_secs(30));
    driver.commit();
    assert_eq!(
        support::committed_by(&brokers, "idle-consumer", TOPIC, 1),
        [rdkafka::Offset::Offset(5)]
    );
}

/// A member told its generation is gone fails the flush that follows the
/// drained revocation, and the source then holds and reads every partition
/// again.
#[test]
fn a_lost_assignment_fails_the_flush_and_the_source_recovers() {
    for consumers in [1, 2] {
        let cluster = MockKafka::new();
        cluster.hold_first_rebalance();
        cluster.create_topic(TOPIC, 4, 1).expect("create topic");
        let brokers = cluster.bootstrap_servers();
        produce(&brokers, TOPIC, 5, 4, "l");

        let group = format!("lost-{consumers}");
        let cfg = watched_config(&brokers, &group, consumers);
        let mut driver = Driver::open_watched(KafkaSource::new(cfg), &group);
        driver.settle_group(consumers, 4, Duration::from_secs(60));

        let failures = driver.lose_a_member(&cluster);
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].contains("AssignmentLost"), "{failures:?}");

        driver.settle(4, Duration::from_secs(60));
        driver.read(5, Duration::from_secs(30));
    }
}

/// Runs `body` on a mock cluster whose group forms at the first join, with one
/// `JoinGroup` refused, so one consumer of a source holds an offer before the
/// other has joined.
fn on_a_cluster_electing_at_once(partitions: i32, body: impl FnOnce(&str)) {
    on_a_mock_electing_at_once(partitions, |cluster| body(&cluster.bootstrap_servers()));
}

/// Past [`on_a_mock_electing_at_once`]'s refusal, let one JoinGroup through and
/// refuse the next few, so one consumer of a source is accepted alone and the
/// other's join starts a new epoch. Without it a slow runner can put both in the
/// first generation, and no epoch follows.
fn hold_back_the_second_join(cluster: &MockKafka) {
    let refused = RDKafkaRespErr::RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS;
    cluster.request_errors(
        RDKafkaApiKey::JoinGroup,
        &[
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
            refused,
            refused,
            refused,
        ],
    );
}

/// [`on_a_cluster_electing_at_once`], handing the body the cluster to script.
fn on_a_mock_electing_at_once(partitions: i32, body: impl FnOnce(&MockKafka)) {
    let cluster = MockKafka::new();
    cluster
        .create_topic(TOPIC, partitions, 1)
        .expect("create topic");
    cluster.request_errors(
        RDKafkaApiKey::JoinGroup,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS],
    );
    // A slower host than the one this test was written on.
    cluster
        .broker_round_trip_time(1, Duration::from_millis(250))
        .expect("round trip time");
    body(&cluster);
}

/// A consumer removed from the group while it holds an offer is not a double
/// assignment: the source keeps running and every record is delivered and
/// committed. The frozen clock keeps the hold from ending before the group
/// moves on. Regression for #1685.
#[test]
fn a_held_offer_the_group_has_left_is_not_fatal() {
    on_a_cluster_electing_at_once(1, |brokers| {
        let clock = TestClock::frozen();
        let cfg = watched_config(brokers, "stale-held", 2);
        let source = KafkaSource::new(cfg).with_clock(Arc::clone(&clock) as _);
        let mut driver = Driver::open_watched(source, "stale-held");
        driver.settle_group(2, 1, Duration::from_secs(60));
        produce(brokers, TOPIC, 5, 1, "s");
        driver.read(5, Duration::from_secs(30));
        driver.commit();
        assert_eq!(
            support::committed_by(brokers, "stale-held", TOPIC, 1),
            [rdkafka::Offset::Offset(5)]
        );
    });
}

/// A consumer removed while it holds an offer rejoins, and the group ends up
/// shared between both consumers with every partition read. Regression for
/// #1685.
#[test]
fn a_late_consumer_ends_up_sharing_every_partition() {
    on_a_cluster_electing_at_once(4, |brokers| {
        produce(brokers, TOPIC, 5, 4, "l");
        let clock = TestClock::frozen();
        let cfg = watched_config(brokers, "late-share", 2);
        let source = KafkaSource::new(cfg).with_clock(Arc::clone(&clock) as _);
        let mut driver = Driver::open_watched(source, "late-share");
        driver.settle_group(2, 4, Duration::from_secs(60));
        // Both consumers at two partitions and every lane live: a sum of four
        // also holds while the discarded consumer has not rejoined.
        wait_until(Duration::from_secs(60), "two partitions each", || {
            driver.step(Duration::from_millis(100));
            let rendered = driver.statistics.as_ref().expect("watched").render();
            let sizes = client_values(&rendered, "spate_kafka_group_assignment_size");
            driver.partitions() == [0, 1, 2, 3]
                && sizes.len() == 2
                && sizes.iter().all(|(_, size)| *size == 2.0)
        });
        let read = driver.read(5, Duration::from_secs(30));
        assert_eq!(read.len(), 4, "{read:?}");
    });
}

/// Dropping a source after it discarded an offer returns.
#[test]
fn dropping_after_a_discard_returns() {
    on_a_cluster_electing_at_once(1, |brokers| {
        let clock = TestClock::frozen();
        let cfg = watched_config(brokers, "drop-discard", 2);
        let source = KafkaSource::new(cfg).with_clock(Arc::clone(&clock) as _);
        let mut driver = Driver::open_watched(source, "drop-discard");
        driver.settle_group(2, 1, Duration::from_secs(60));
        returns_within("dropping after a discard", move || drop(driver));
    });
}

/// Dropping a source in the pass after it discarded an offer returns, with the
/// discarded consumer's lost revocation still to come. A run whose offers share
/// one generation accepts them and goes on to a plain drop.
#[test]
fn dropping_in_the_pass_after_a_discard_returns() {
    on_a_cluster_electing_at_once(4, |brokers| {
        let clock = TestClock::frozen();
        let cfg = consumers_config(brokers, "drop-pass", 2);
        let mut source = KafkaSource::new(cfg).with_clock(Arc::clone(&clock) as _);
        let cp = Checkpointer::new();
        source.open(SourceCtx::new(cp.handle())).expect("open");
        wait_until(Duration::from_secs(60), "a discard or an accept", || {
            let mut event = None;
            let warned = spate_test::capture_logs(tracing::Level::WARN, || {
                event = Some(source.poll_events(Duration::from_millis(50)));
            });
            warned.iter().any(|l| l.contains("discarded an assignment"))
                || matches!(event, Some(Ok(SourceEvent::LanesAssigned(_))))
        });
        let _ = source.flush_commits();
        returns_within("dropping in the pass after a discard", move || drop(source));
    });
}

/// A consumer joining after the hold ended starts a new epoch: the driver's
/// assertion that an assignment arrives with no lane live holds throughout,
/// and both consumers end up in a stable group.
#[test]
fn a_consumer_joining_after_the_hold_ended_starts_a_new_epoch() {
    on_a_mock_electing_at_once(4, |cluster| {
        hold_back_the_second_join(cluster);
        let brokers = cluster.bootstrap_servers();
        let clock = TestClock::frozen();
        let mut cfg = watched_config(&brokers, "hold-ended", 2);
        // This test advances the frozen clock while the group forms.
        cfg.startup_timeout = Duration::from_hours(1);
        let source = KafkaSource::new(cfg).with_clock(Arc::clone(&clock) as _);
        let mut driver = Driver::open_watched(source, "hold-ended");
        // Past the hold as soon as the first offer arrives, whenever that is.
        wait_until(
            Duration::from_secs(30),
            "the first consumer's lanes",
            || {
                clock.advance(Duration::from_millis(500));
                driver.step(Duration::from_millis(20));
                !driver.lanes.is_empty()
            },
        );
        driver.settle_group(2, 4, Duration::from_secs(60));
        // A slow runner can add a rebalance, so this counts at least one.
        assert!(
            driver.revocations >= 1,
            "the second consumer's join drains the first epoch"
        );
    });
}

/// The client and group sequence around a member that never answers its offer,
/// on the mock broker. Fails on a librdkafka that changes the sequence.
#[test]
fn a_member_that_never_answers_its_offer_is_replaced() {
    for (answer, group) in [
        (support::Answer::Empty, "never-answers-empty"),
        (support::Answer::Offer, "never-answers-offer"),
    ] {
        let cluster = MockKafka::new();
        cluster.create_topic(TOPIC, 1, 1).expect("create topic");
        support::a_member_that_never_answers_its_offer(
            &cluster.bootstrap_servers(),
            TOPIC,
            group,
            answer,
        );
    }
}

/// A lost member's failed flush leaves its peer's commit in place, and no
/// offset past what was read is committed. The partitions read again from at
/// or before the committed offset, so a record the lost member read and could
/// not commit is not skipped. A lost revocation ahead of the read changes none
/// of this, and its failure is its own.
#[test]
fn a_lost_member_keeps_its_peers_commit_and_loses_no_record() {
    for (group, earlier_loss) in [("lost-peer", false), ("lost-peer-again", true)] {
        let cluster = MockKafka::new();
        cluster.hold_first_rebalance();
        cluster.create_topic(TOPIC, 4, 1).expect("create topic");
        let brokers = cluster.bootstrap_servers();
        produce(&brokers, TOPIC, 10, 4, "k");

        let mut cfg = watched_config(&brokers, group, 2);
        cfg.commit_interval = Duration::from_hours(1);
        let mut driver = Driver::open_watched(KafkaSource::new(cfg), group);
        driver.settle_group(2, 4, Duration::from_secs(60));
        if earlier_loss {
            driver.lose_a_member(&cluster);
            driver.settle_group(2, 4, Duration::from_secs(60));
        }
        driver.read(10, Duration::from_secs(30));

        let failures = driver.lose_a_member(&cluster);
        assert_eq!(failures.len(), 1, "{failures:?}");
        let at_ten = rdkafka::Offset::Offset(10);
        let read_back = support::committed_by(&brokers, group, TOPIC, 4);
        assert!(
            read_back.iter().filter(|o| **o == at_ten).count() >= 2,
            "the peer's two partitions: {read_back:?}"
        );
        assert!(
            read_back
                .iter()
                .all(|o| matches!(o, rdkafka::Offset::Invalid) || *o == at_ten),
            "nothing past what was read: {read_back:?}"
        );

        produce(&brokers, TOPIC, 5, 4, "m");
        driver.settle_group(2, 4, Duration::from_secs(60));
        let second = driver.read(5, Duration::from_secs(30));
        for (partition, offsets) in &second {
            let (first, last) = (
                *offsets.first().expect("read"),
                *offsets.last().expect("read"),
            );
            assert!(first <= 10, "partition {partition} skipped: {offsets:?}");
            assert_eq!(
                usize::try_from(last - first + 1).expect("ordered"),
                offsets.len(),
                "partition {partition} has a gap: {offsets:?}"
            );
        }
    }
}

/// Each consumer publishes the lag of the partitions it holds as soon as its
/// statistics arrive, measured from the committed offsets read at assignment.
#[test]
fn two_consumers_publish_lag_before_committing() {
    let cluster = MockKafka::new();
    cluster.hold_first_rebalance();
    cluster.create_topic(TOPIC, 2, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 10, 2, "l");
    let probe = support::probe(&brokers, "lag-each");
    let mut tpl = rdkafka::TopicPartitionList::new();
    for p in 0..2 {
        tpl.add_partition_offset(TOPIC, p, rdkafka::Offset::Offset(3))
            .expect("offset");
    }
    probe
        .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
        .expect("commit");
    drop(probe);

    let mut cfg = consumers_config(&brokers, "lag-each", 2);
    cfg.statistics_interval = Duration::from_millis(100);
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let metrics = Arc::new(spate_core::metrics::SourceMetrics::new(
            &spate_core::metrics::ComponentLabels::new("lag-each", "source", "kafka"),
        ));
        let mut driver = Driver::open_with(KafkaSource::new(cfg), |ctx| {
            ctx.with_stage_metrics(Some(metrics))
        });
        driver.settle(2, Duration::from_secs(60));
        wait_until(Duration::from_secs(30), "lag published", || {
            driver.step(Duration::from_millis(100));
            lag_series_count(&handle.render()) == 2
        });
    });

    let rendered = handle.render();
    for p in 0..2 {
        assert_eq!(
            support::lag_of(&rendered, "lag-each", p),
            Some(7.0),
            "partition {p}:\n{rendered}"
        );
    }
}

/// The values of every series of `family` labelled with a `client`.
fn client_values(rendered: &str, family: &str) -> Vec<(String, f64)> {
    spate_test::metric_series(rendered, family, &[])
        .into_iter()
        .filter_map(|s| Some((s.label("client")?.to_owned(), s.value)))
        .collect()
}

/// Above one consumer, each consumer's statistics land on series of their
/// own, labelled `client="consumer-<k>"`, which sum to the source's totals.
/// With statistics off no family is registered.
#[test]
fn two_consumers_publish_their_own_statistics() {
    let cluster = MockKafka::new();
    cluster.hold_first_rebalance();
    cluster.create_topic(TOPIC, 2, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 5, 2, "st");

    let run = |interval: Duration, group: &'static str| -> String {
        let mut cfg = consumers_config(&brokers, group, 2);
        cfg.statistics_interval = interval;
        let (recorder, handle, meter) = support::recorded_meter(group);
        metrics::with_local_recorder(&recorder, || {
            let mut driver = Driver::open_with(KafkaSource::new(cfg), |ctx| {
                ctx.with_meter(Some(meter)).with_partition_detail(true)
            });
            driver.settle(2, Duration::from_secs(60));
            driver.read(5, Duration::from_secs(30));
            if interval.is_zero() {
                return;
            }
            wait_until(
                Duration::from_secs(30),
                "both consumers' statistics",
                || {
                    driver.step(Duration::from_millis(100));
                    let rendered = handle.render();
                    let sizes = client_values(&rendered, "spate_kafka_group_assignment_size");
                    let rx: f64 = client_values(&rendered, "spate_kafka_rx_messages_total")
                        .iter()
                        .map(|(_, v)| v)
                        .sum();
                    sizes.len() == 2
                        && sizes.iter().map(|(_, v)| v).sum::<f64>() == 2.0
                        && rx >= 10.0
                },
            );
        });
        handle.render()
    };

    let rendered = run(Duration::from_millis(100), "stats-each");
    let mut clients: Vec<String> = client_values(&rendered, "spate_kafka_group_assignment_size")
        .into_iter()
        .map(|(c, _)| c)
        .collect();
    clients.sort();
    assert_eq!(clients, ["consumer-0", "consumer-1"], "{rendered}");
    assert!(
        !rendered
            .lines()
            .any(|l| l.starts_with("spate_kafka_group_assignment_size{") && !l.contains("client=")),
        "no unlabelled series above one consumer:\n{rendered}"
    );

    let rendered = run(Duration::ZERO, "stats-off");
    assert!(
        !rendered.lines().any(|l| l.starts_with("spate_kafka_")),
        "statistics off registers no family:\n{rendered}"
    );
}

/// The `client` label is the consumer's index: a `client.id` set in the
/// passthrough reaches neither the series nor the source's own logs.
#[test]
fn consumer_series_carry_no_passthrough_value() {
    let cluster = MockKafka::new();
    cluster.hold_first_rebalance();
    cluster.create_topic(TOPIC, 2, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();

    let mut cfg = watched_config(&brokers, "no-secret", 2);
    cfg.rdkafka.insert("client.id".into(), "hunter2".into());
    let (recorder, handle, meter) = support::recorded_meter("no-secret");
    let lines = spate_test::capture_logs(tracing::Level::DEBUG, || {
        metrics::with_local_recorder(&recorder, || {
            let mut driver =
                Driver::open_with(KafkaSource::new(cfg), |ctx| ctx.with_meter(Some(meter)));
            driver.settle(2, Duration::from_secs(60));
            wait_until(Duration::from_secs(30), "statistics", || {
                driver.step(Duration::from_millis(100));
                client_values(&handle.render(), "spate_kafka_group_assignment_size").len() == 2
            });
        });
    });
    let rendered = handle.render();
    assert!(!rendered.contains("hunter2"), "{rendered}");
    let leaked: Vec<_> = lines
        .iter()
        .filter(|l| !l.contains("librdkafka") && l.contains("hunter2"))
        .collect();
    assert!(leaked.is_empty(), "{leaked:#?}");
}

/// Run `f` on a helper thread and fail if it does not return in time: a
/// drop that waits on an unacknowledged rebalance never returns.
#[expect(
    clippy::disallowed_methods,
    reason = "a thread that never returns is the failure; the test fails instead of joining it"
)]
fn returns_within(what: &str, f: impl FnOnce() + Send + 'static) {
    let (done, finished) = mpsc::channel();
    std::thread::spawn(move || {
        f();
        let _ = done.send(());
    });
    finished
        .recv_timeout(Duration::from_secs(30))
        .unwrap_or_else(|_| panic!("{what} did not return"));
}

/// Poll `source` until it surfaces a revocation, which it leaves
/// unacknowledged.
fn await_revocation(source: &mut KafkaSource, what: &str) {
    wait_until(Duration::from_secs(60), what, || {
        matches!(
            source.poll_events(Duration::from_millis(100)),
            Ok(SourceEvent::LanesRevoked { .. })
        )
    });
}

/// Dropping the source between a surfaced revocation and its completion
/// returns. Consumer close completes a revocation that was not lost.
#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "the test joins the joiner thread"
)]
fn dropping_mid_revoke_returns() {
    let cluster = MockKafka::new();
    cluster.hold_first_rebalance();
    cluster.create_topic(TOPIC, 4, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();

    let mut driver = Driver::open(consumers_config(&brokers, "drop-revoke", 2));
    driver.settle(4, Duration::from_secs(60));
    let stop = Arc::new(AtomicBool::new(false));
    let joiner = {
        let brokers = brokers.clone();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let c: BaseConsumer = support::raw_config()
                .set("bootstrap.servers", &brokers)
                .set("group.id", "drop-revoke")
                .build()
                .expect("joiner");
            c.subscribe(&[TOPIC]).expect("subscribe");
            while !stop.load(Ordering::Relaxed) {
                let _ = c.poll(Duration::from_millis(100));
            }
        })
    };
    let mut source = driver.source;
    let lanes = driver.lanes;
    await_revocation(&mut source, "a revocation");
    returns_within("dropping the source mid-revoke", move || {
        drop(lanes);
        drop(source);
    });
    stop.store(true, Ordering::Relaxed);
    joiner.join().expect("joiner thread");
}

/// Dropping the source between a surfaced revocation of a lost assignment
/// and its completion returns. Consumer close does not complete a lost
/// revocation on its own. It finishes once the source calls `unassign`.
#[test]
fn dropping_mid_lost_revoke_returns() {
    use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
    let cluster = MockKafka::new();
    cluster.hold_first_rebalance();
    cluster.create_topic(TOPIC, 2, 1).expect("create topic");
    let mut cfg = consumers_config(&cluster.bootstrap_servers(), "drop-lost-revoke", 2);
    cfg.rdkafka.insert("debug".into(), "cgrp".into());
    let mut driver = Driver::open(cfg);
    driver.settle(2, Duration::from_secs(60));
    let Driver {
        mut source, lanes, ..
    } = driver;
    let lines = spate_test::capture_logs(tracing::Level::DEBUG, || {
        cluster.request_errors(
            RDKafkaApiKey::Heartbeat,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID],
        );
        await_revocation(&mut source, "a lost revocation");
    });
    assert!(
        lines
            .iter()
            .any(|l| l.contains("librdkafka") && l.contains("fac=\"LOST\"")),
        "no assignment was marked lost: {lines:#?}"
    );
    returns_within("dropping the source mid lost revoke", move || {
        drop(lanes);
        drop(source);
    });
}

/// The source may be dropped before the lanes it handed out: each lane keeps
/// its consumer alive, and dropping the lanes afterwards completes.
#[test]
fn dropping_the_source_before_its_lanes_completes() {
    let cluster = MockKafka::new();
    cluster.hold_first_rebalance();
    cluster.create_topic(TOPIC, 2, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();

    let mut driver = Driver::open(consumers_config(&brokers, "drop-order", 2));
    driver.settle(2, Duration::from_secs(60));
    let Driver { source, lanes, .. } = driver;
    returns_within("dropping the source, then its lanes", move || {
        drop(source);
        drop(lanes);
    });
}

/// Pins that dropping a source that was never polled returns with a backlog on
/// its topic, in whatever state its consumers have reached.
#[test]
fn dropping_an_unpolled_source_with_a_backlog_returns() {
    let cluster = MockKafka::new();
    cluster.hold_first_rebalance();
    cluster.create_topic(TOPIC, 2, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 100, 2, "b");

    let Driver { source, .. } = Driver::open(consumers_config(&brokers, "drop-unpolled", 2));
    returns_within("dropping an unpolled source", move || drop(source));
}

/// A commit that carries a watermark for a partition no consumer of the
/// source owns still stores the offsets of the partitions it does own.
#[test]
fn a_partial_refusal_stores_the_owned_offsets() {
    let cluster = MockKafka::new();
    cluster.hold_first_rebalance();
    cluster.create_topic(TOPIC, 4, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 10, 4, "r");

    let mut cfg = watched_config(&brokers, "partial-refusal", 2);
    cfg.commit_interval = Duration::from_hours(1);
    let mut driver = Driver::open_watched(KafkaSource::new(cfg), "partial-refusal");
    driver.settle_group(2, 4, Duration::from_secs(60));
    driver.read(10, Duration::from_secs(30));
    driver.cp.drain();
    let mut watermarks = driver.cp.take_watermarks();
    watermarks.push((PartitionId(9), 3));
    driver.source.commit(&watermarks).expect("commit");
    driver.source.flush_commits().expect("flush");

    assert_eq!(
        support::committed_by(&brokers, "partial-refusal", TOPIC, 4),
        vec![rdkafka::Offset::Offset(10); 4]
    );
}

/// Accepting an assignment for several consumers spends one
/// committed-offset lookup bound in total, so one `poll_events` call does
/// not block the controller thread for a lookup per consumer.
#[test]
fn one_assignment_spends_one_lookup_bound() {
    /// `COMMITTED_LOOKUP_TIMEOUT`, plus slack for the assign itself.
    const BOUND: Duration = Duration::from_secs(13);
    let cluster = MockKafka::new();
    cluster.hold_first_rebalance();
    cluster.create_topic(TOPIC, 3, 1).expect("create topic");
    // A coordinator still loading its offsets: librdkafka retries every
    // OffsetFetch until the caller's timeout.
    cluster.request_errors(
        rdkafka::types::RDKafkaApiKey::OffsetFetch,
        &vec![
            rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS;
            100_000
        ],
    );
    let mut cfg = consumers_config(&cluster.bootstrap_servers(), "slow-lookup", 3);
    cfg.startup_timeout = Duration::from_secs(120);
    let mut source = KafkaSource::new(cfg);
    let cp = Checkpointer::new();
    source.open(SourceCtx::new(cp.handle())).expect("open");

    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        assert!(Instant::now() < deadline, "no assignment");
        let started = Instant::now();
        let outcome = source.poll_events(Duration::from_millis(100));
        let took = started.elapsed();
        assert!(
            took <= BOUND,
            "one poll_events call blocked the controller thread for {took:?}"
        );
        // A consumer that joins after the hold ended starts a later epoch, so
        // this waits for the accept that carries every lane.
        if let Ok(SourceEvent::LanesAssigned(lanes)) = outcome
            && lanes.len() == 3
        {
            return;
        }
    }
}

/// A control batch written by hand reaches a split queue as a transaction
/// marker: it takes an offset, nothing is delivered for it, and polling the
/// queue moves the position past it.
#[test]
fn the_mock_serves_a_control_batch_as_a_marker() {
    const RECORDS: usize = 3;
    for marker in [Marker::Commit, Marker::Abort] {
        let cluster = MockKafka::new();
        cluster.create_topic(TOPIC, 1, 1).expect("create topic");
        let brokers = cluster.bootstrap_servers();
        produce(&brokers, TOPIC, RECORDS, 1, "before");
        assert_eq!(
            produce_batch(&brokers, TOPIC, 0, &control_batch(marker)),
            0,
            "{marker:?}"
        );

        let consumer = assigned_consumer(&brokers, "marker", 1);
        let queue = consumer
            .split_partition_queue(TOPIC, 0)
            .expect("split queue");

        let mut delivered = 0;
        wait_until(
            Duration::from_secs(30),
            "the position passes the marker",
            || {
                if queue.poll(Duration::from_millis(100)).is_some() {
                    delivered += 1;
                }
                consumer.position().expect("position").elements()[0].offset()
                    == rdkafka::Offset::Offset(RECORDS as i64 + 1)
            },
        );
        assert_eq!(delivered, RECORDS, "{marker:?}");
    }
}

/// A control batch sets both attribute bits, carries a CRC, and states its
/// marker's type in its key: 0 for abort and 1 for commit. The consumer acts on
/// none of them, so the marker test above cannot tell them apart. The key follows
/// the 61-byte batch header and the record's five one-byte varints.
#[test]
fn a_control_batch_states_its_marker() {
    for (marker, control_type) in [(Marker::Abort, 0i16), (Marker::Commit, 1)] {
        let batch = control_batch(marker);
        assert_ne!(batch[17..21], [0u8; 4], "{marker:?}");
        assert_eq!(batch[21..23], 0x30i16.to_be_bytes(), "{marker:?}");
        assert_eq!(batch[66..68], 0i16.to_be_bytes(), "{marker:?}");
        assert_eq!(batch[68..70], control_type.to_be_bytes(), "{marker:?}");
    }
}

/// One topic of `partitions` partitions on a mock cluster of its own.
fn marker_cluster(partitions: i32) -> MockKafka {
    let cluster = MockKafka::new();
    cluster
        .create_topic(TOPIC, partitions, 1)
        .expect("create topic");
    cluster
}

/// Write `marker` to `partition`.
fn write_marker(brokers: &str, partition: i32, marker: Marker) {
    support::write_marker(brokers, TOPIC, partition, marker);
}

/// A marker at the tail reaches the committed offset once the lane polls past
/// it, so both lag figures read 0. Regression for #1709.
#[test]
fn a_marker_at_the_tail_leaves_no_lag() {
    for marker in [Marker::Commit, Marker::Abort] {
        let cluster = marker_cluster(1);
        let brokers = cluster.bootstrap_servers();
        produce(&brokers, TOPIC, 5, 1, "before");
        write_marker(&brokers, 0, marker);

        let group = spate_test::unique_name("tail");
        let mut watched = Watched::open(config(&brokers, &group));
        assert_eq!(watched.read(0, 5), [0, 1, 2, 3, 4]);
        watched.poll_until_committed(0, 0, 6);
        watched.wait_lag(0, 0.0);
        let pipeline = watched.pipeline.clone();
        let mut stored = None;
        wait_until(ASSIGNMENT, "the stored lag reads 0", || {
            watched.serve();
            stored = spate_test::metric_value(
                &watched.rendered(),
                "spate_kafka_partition_lag_stored_records",
                &[("pipeline", &pipeline), ("partition", "0")],
            );
            stored == Some(0.0)
        });
        assert_eq!(
            support::committed_by(&brokers, &group, TOPIC, 1),
            [rdkafka::Offset::Offset(6)],
            "{marker:?}"
        );
    }
}

/// A marker that arrives after the last record's commit, on a lane already
/// idle, moves the committed offset on its own. Regression for #1709.
#[test]
fn a_marker_read_after_the_last_commit_leaves_no_lag() {
    let cluster = marker_cluster(1);
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 3, 1, "before");
    let mut watched = Watched::open(config(&brokers, &spate_test::unique_name("late")));
    watched.read(0, 3);
    watched.poll_until_committed(0, 0, 3);
    watched.wait_lag(0, 0.0);

    write_marker(&brokers, 0, Marker::Commit);
    watched.wait_lag(0, 1.0);
    watched.poll_until_committed(0, 0, 4);
    watched.wait_lag(0, 0.0);
}

/// A member restarted past the last record passes a marker with no record to
/// deliver: the position alone moves its offset. Regression for #1709.
#[test]
fn a_restarted_member_past_a_marker_leaves_no_lag() {
    let cluster = marker_cluster(1);
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 3, 1, "before");
    write_marker(&brokers, 0, Marker::Commit);
    let group = spate_test::unique_name("restart");
    commit_as(&brokers, &group, 3);

    let mut watched = Watched::open(config(&brokers, &group));
    watched.wait_lag(0, 1.0);
    assert_eq!(watched.poll_until_committed(0, 0, 4), [4]);
    watched.wait_lag(0, 0.0);
}

/// A marker that lands right behind one the lane already retired takes one offset
/// past it, and moves the committed offset by that one.
#[test]
fn a_marker_right_behind_a_retired_one_leaves_no_lag() {
    let cluster = marker_cluster(1);
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 1, 1, "before");
    write_marker(&brokers, 0, Marker::Commit);
    let mut watched = Watched::open(config(&brokers, &spate_test::unique_name("behind")));
    watched.read(0, 1);
    watched.poll_until_committed(0, 0, 2);

    write_marker(&brokers, 0, Marker::Commit);
    watched.poll_until_committed(0, 0, 3);
    watched.wait_lag(0, 0.0);
}

/// Consecutive markers are passed together or in turn, and the offset ends past
/// them all. Regression for #1709.
#[test]
fn consecutive_markers_leave_no_lag() {
    let cluster = marker_cluster(1);
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 3, 1, "before");
    write_marker(&brokers, 0, Marker::Commit);
    write_marker(&brokers, 0, Marker::Abort);
    let mut watched = Watched::open(config(&brokers, &spate_test::unique_name("two")));
    watched.read(0, 3);
    watched.poll_until_committed(0, 0, 5);
    watched.wait_lag(0, 0.0);
}

/// A commit past a marker loses nothing and repeats nothing: records behind a
/// marker are delivered around its offset, watermarks only rise, and a second
/// source on the group delivers nothing new. Regression for #1709.
#[test]
fn records_after_a_marker_are_delivered_and_committed() {
    let cluster = marker_cluster(1);
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 3, 1, "first");
    write_marker(&brokers, 0, Marker::Commit);
    produce(&brokers, TOPIC, 2, 1, "second");
    write_marker(&brokers, 0, Marker::Commit);

    let group = spate_test::unique_name("around");
    let mut watched = Watched::open(config(&brokers, &group));
    assert_eq!(watched.read(0, 5), [0, 1, 2, 4, 5]);
    let seen = watched.poll_until_committed(0, 0, 7);
    assert!(seen.is_sorted(), "watermarks fell: {seen:?}");
    drop(watched);

    let mut second = Watched::open(config(&brokers, &group));
    second.wait_lag(0, 0.0);
    for _ in 0..3 {
        second.serve();
        assert_eq!(second.poll(0).unwrap_or_default(), Vec::<i64>::new());
    }
    assert!(second.commit().is_empty(), "nothing new to commit");
}

/// A marker is retired once: further empty returns issue nothing. Regression
/// for #1709.
#[test]
fn a_marker_advances_once() {
    let cluster = marker_cluster(1);
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 2, 1, "before");
    write_marker(&brokers, 0, Marker::Commit);
    let mut watched = Watched::open(config(&brokers, &spate_test::unique_name("once")));
    watched.read(0, 2);
    watched.poll_until_committed(0, 0, 3);
    for _ in 0..2 {
        assert_eq!(watched.poll(0), None);
        assert!(watched.commit().is_empty());
    }
    assert_eq!(watched.cp.pending(PartitionId(0)), 0);
}

/// A zero-timeout empty return leaves the position unread until its turn comes:
/// the first such poll serves the marker and retires nothing, and a later one
/// retires it. Regression for #1709.
#[test]
fn a_zero_timeout_poll_reads_the_position_in_turn() {
    let cluster = marker_cluster(1);
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 1, 1, "before");
    write_marker(&brokers, 0, Marker::Commit);
    let mut watched = Watched::open(config(&brokers, &spate_test::unique_name("turn")));
    let pipeline = watched.pipeline.clone();
    // One version barrier, the record and the marker.
    wait_until(ASSIGNMENT, "the fetcher runs ahead", || {
        watched.serve();
        support::fetch_queue_entries(&watched.rendered(), &pipeline) >= 3.0
    });
    assert_eq!(watched.read(0, 1), [0]);
    assert_eq!(watched.commit(), [(PartitionId(0), 1)]);

    let poll = |lane: &mut <KafkaSource as Source>::Lane| {
        lane.poll(64, Duration::ZERO).expect("lane poll").is_some()
    };
    assert!(!poll(&mut watched.lanes[0]), "read out of turn");
    wait_until(ASSIGNMENT, "the read's turn comes", || {
        poll(&mut watched.lanes[0])
    });
    assert_eq!(watched.commit(), [(PartitionId(0), 2)]);
}

/// An idle lane on a plain topic has nothing to retire, so it issues no ack.
#[test]
fn an_idle_plain_lane_issues_no_ack() {
    let cluster = marker_cluster(1);
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 2, 1, "before");
    let mut watched = Watched::open(config(&brokers, &spate_test::unique_name("plain")));
    watched.read(0, 2);
    assert_eq!(watched.commit(), [(PartitionId(0), 2)]);
    for _ in 0..3 {
        assert_eq!(watched.poll(0), None);
        assert!(watched.commit().is_empty());
    }
    assert_eq!(watched.cp.pending(PartitionId(0)), 0);
}

/// Poll lane `lane` of `watched` for one record, and return a clone of the
/// record's ack, which holds the commit there after the batch drops.
fn hold_a_record(watched: &mut Watched, lane: usize) -> spate_core::checkpoint::AckRef {
    let mut held = None;
    wait_until(ASSIGNMENT, "the record", || {
        watched.serve();
        if let Some(batch) = watched.lanes[lane]
            .poll(1, Duration::from_millis(100))
            .expect("lane poll")
        {
            held = Some(batch.ack().clone());
        }
        held.is_some()
    });
    held.expect("held")
}

/// Hold the ack of the record ahead of a marker on `watched`'s lane, once both are
/// in its fetch queue, and poll until the marker is served, with no batch issued
/// for it. Returns the clone.
fn hold_a_record_past_its_marker(watched: &mut Watched) -> spate_core::checkpoint::AckRef {
    let pipeline = watched.pipeline.clone();
    wait_until(ASSIGNMENT, "the record and the marker are queued", || {
        watched.serve();
        support::fetch_queue_entries(&watched.rendered(), &pipeline) >= 2.0
    });
    let held = hold_a_record(watched, 0);
    wait_until(ASSIGNMENT, "the marker is served", || {
        watched.serve();
        assert_eq!(watched.poll(0), None, "a batch behind a live ack");
        support::fetch_queue_entries(&watched.rendered(), &pipeline) == 0.0
    });
    held
}

/// An ack outstanding past its batch holds the marker's offset back, and
/// resolving it releases both. Regression for #1709.
#[test]
fn an_acknowledgement_held_past_its_batch_holds_the_marker_back() {
    let cluster = marker_cluster(1);
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 1, 1, "before");
    write_marker(&brokers, 0, Marker::Commit);
    let group = spate_test::unique_name("held");
    let mut watched = Watched::open(config(&brokers, &group));
    let ack = hold_a_record_past_its_marker(&mut watched);

    assert!(watched.commit().is_empty(), "a watermark passed a live ack");
    assert_eq!(
        support::committed_by(&brokers, &group, TOPIC, 1),
        [rdkafka::Offset::Invalid]
    );
    drop(ack);
    assert_eq!(watched.commit(), [(PartitionId(0), 1)]);
    assert_eq!(watched.poll_until_committed(0, 0, 2), [2]);
}

/// A fetch answered out of range takes the lane back below its watermark. The
/// head it reads again holds the markers behind it back, whatever that watermark
/// says. Regression for #1709.
#[test]
fn a_rewind_below_the_watermark_holds_markers_back() {
    let cluster = marker_cluster(1);
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 1, 1, "head");
    write_marker(&brokers, 0, Marker::Commit);
    write_marker(&brokers, 0, Marker::Abort);
    let group = spate_test::unique_name("rewind");
    let mut watched = Watched::open(config(&brokers, &group));
    assert_eq!(watched.read(0, 1), [0]);
    watched.poll_until_committed(0, 0, 3);

    cluster.request_errors(
        RDKafkaApiKey::Fetch,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE],
    );
    write_marker(&brokers, 0, Marker::Commit);
    let ack = hold_a_record_past_its_marker(&mut watched);

    assert!(watched.commit().is_empty(), "a watermark passed a live ack");
    drop(ack);
    assert_eq!(watched.commit(), [(PartitionId(0), 1)]);
    assert_eq!(watched.poll_until_committed(0, 0, 4), [4]);
}

/// A watermark past what the lane issued, as a retried commit of an earlier epoch
/// stores, holds the marker back until the next store. Regression for #1709.
#[test]
fn a_watermark_past_what_the_lane_issued_waits_for_the_next_store() {
    let cluster = marker_cluster(1);
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 1, 1, "before");
    write_marker(&brokers, 0, Marker::Commit);
    let group = spate_test::unique_name("past");
    let mut watched = Watched::open(config(&brokers, &group));
    let ack = hold_a_record_past_its_marker(&mut watched);

    watched
        .source
        .commit(&[(PartitionId(0), 100)])
        .expect("store");
    assert_eq!(watched.poll(0), None, "a batch behind a live ack");

    drop(ack);
    assert_eq!(watched.commit(), [(PartitionId(0), 1)]);
    assert_eq!(watched.poll_until_committed(0, 0, 2), [2]);
}

/// A failed batch ahead of a marker holds the committed offset where it is, and
/// the partition keeps reading its lag. Regression for #1709.
#[test]
fn a_failed_batch_before_a_marker_holds_the_commit() {
    let cluster = marker_cluster(1);
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 1, 1, "first");
    let group = spate_test::unique_name("failed");
    let mut watched = Watched::open(config(&brokers, &group));
    watched.read(0, 1);
    assert_eq!(watched.commit(), [(PartitionId(0), 1)]);

    produce(&brokers, TOPIC, 1, 1, "second");
    write_marker(&brokers, 0, Marker::Commit);
    hold_a_record_past_its_marker(&mut watched).fail();
    assert!(watched.commit().is_empty(), "a watermark passed a failure");
    watched.wait_lag(0, 2.0);
    assert_eq!(
        support::committed_by(&brokers, &group, TOPIC, 1),
        [rdkafka::Offset::Offset(1)]
    );
}

/// Each partition passes its own marker, whatever another partition holds.
/// Regression for #1709.
#[test]
fn each_partition_advances_past_its_own_marker() {
    let cluster = marker_cluster(2);
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 1, 2, "before");
    write_marker(&brokers, 0, Marker::Commit);
    write_marker(&brokers, 1, Marker::Commit);
    let group = spate_test::unique_name("each");
    let mut watched = Watched::open(config(&brokers, &group));
    let index_of = |watched: &Watched, p: u32| {
        watched
            .lanes
            .iter()
            .position(|l| l.partition() == PartitionId(p))
            .expect("lane")
    };
    let (zero, one) = (index_of(&watched, 0), index_of(&watched, 1));

    let _held = hold_a_record(&mut watched, one);
    watched.read(zero, 1);
    watched.poll_until_committed(zero, 0, 2);
    watched.wait_lag(0, 0.0);
    assert_eq!(
        support::committed_by(&brokers, &group, TOPIC, 2),
        [rdkafka::Offset::Offset(2), rdkafka::Offset::Invalid]
    );
}

/// Under two consumers each lane passes its own marker and the watermark reaches
/// the member that owns the partition. Regression for #1709.
#[test]
fn two_consumers_each_advance_past_their_own_marker() {
    let cluster = marker_cluster(2);
    cluster.hold_first_rebalance();
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 1, 2, "before");
    write_marker(&brokers, 0, Marker::Commit);
    write_marker(&brokers, 1, Marker::Commit);
    let group = spate_test::unique_name("two-consumers");
    let mut driver = Driver::open_watched(
        KafkaSource::new(watched_config(&brokers, &group, 2)),
        &group,
    );
    driver.settle_group(2, 2, ASSIGNMENT);
    driver.read(1, ASSIGNMENT);

    wait_until(ASSIGNMENT, "both partitions pass their marker", || {
        driver.step(Duration::from_millis(50));
        for lane in &mut driver.lanes {
            let _ = lane.poll(64, Duration::from_millis(50)).expect("lane poll");
        }
        driver.commit();
        support::committed_by(&brokers, &group, TOPIC, 2) == [rdkafka::Offset::Offset(2); 2]
    });
}

/// A gap with no record and no marker leaves no position change, so it stays in
/// the figure: the reading the connector page documents.
#[test]
fn lag_stays_above_zero_over_a_compacted_gap() {
    let cluster = marker_cluster(1);
    let brokers = cluster.bootstrap_servers();
    let batch = RawBatch {
        last_offset_delta: 1,
        record_count: 2,
        value: b"kept",
        ..RawBatch::default()
    };
    assert_eq!(produce_batch(&brokers, TOPIC, 0, &batch.encode()), 0);
    let mut watched = Watched::open(config(&brokers, &spate_test::unique_name("gap")));
    assert_eq!(watched.read(0, 1), [0]);
    assert_eq!(watched.commit(), [(PartitionId(0), 1)]);
    watched.wait_lag(0, 1.0);
    assert_eq!(watched.poll(0), None);
    assert!(watched.commit().is_empty());
}

/// A member whose position equals its committed offset has nothing stored, and
/// its flush still succeeds.
#[test]
fn a_flush_with_nothing_stored_succeeds() {
    let cluster = marker_cluster(1);
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 2, 1, "before");
    let group = spate_test::unique_name("nothing-stored");
    commit_as(&brokers, &group, 2);
    let mut watched = Watched::open(config(&brokers, &group));
    assert_eq!(watched.poll(0), None);
    assert!(watched.commit().is_empty());
    watched.source.flush_commits().expect("flush");
    assert_eq!(
        support::committed_by(&brokers, &group, TOPIC, 1),
        [rdkafka::Offset::Offset(2)]
    );
}

/// A pipeline over the Kafka source on `brokers`, capturing to a sink, publishing
/// its metrics under a name of its own. Returns the name, the exposition, the
/// sink's script, the shutdown handle and the running pipeline.
fn run_marker_pipeline(
    brokers: &str,
    group: &str,
    threads: usize,
    checkpoint: &str,
    pre: impl FnOnce(&spate_test::SinkScript),
) -> (
    String,
    spate_core::metrics::MetricsHandle,
    spate_test::SinkScript,
    spate_core::pipeline::ShutdownHandle,
    spate_test::PipelineRun,
) {
    let name = spate_test::unique_name("marker-pipeline");
    let yaml = format!(
        "pipeline: {{ name: {name}, threads: {threads} }}\nadmin: {{ listen: none }}\n\
         checkpoint: {checkpoint}\nmetrics: {{ exporter: prometheus, per_partition_detail: true }}\n\
         source: {{ memory: {{}} }}\nsink: {{ capture: {{ chunk: {{ target_bytes: 64 }} }} }}\n"
    );
    let config = spate_core::config::PipelineConfig::from_str(&yaml).expect("config");
    let handle = spate_core::metrics::install(&spate_core::metrics::MetricsSettings {
        exporter: spate_core::metrics::Exporter::Prometheus,
        per_partition_detail: true,
        e2e_basis: spate_core::metrics::E2eBasis::Ingest,
    })
    .expect("install the exporter");
    let mut cfg = self::config(brokers, group);
    cfg.statistics_interval = Duration::from_millis(100);
    let (sink, script) = spate_test::capture_sink(1, 1);
    pre(&script);
    let runtime = spate_test::passthrough_runtime(
        spate_core::pipeline::Pipeline::from_config(config).expect("pipeline"),
        KafkaSource::new(cfg),
        sink,
        spate_test::TestEncoder,
        spate_core::pipeline::RuntimeOptions::default(),
    );
    let shutdown = runtime.shutdown_handle();
    let run = spate_test::PipelineRun::spawn(move || runtime.run());
    (name, handle, script, shutdown, run)
}

/// Through the public pipeline, a topic ending on a marker reaches lag 0, the
/// broker holds the offset past it, and the run's last watermark agrees.
/// Regression for #1709.
#[test]
fn a_pipeline_reaches_zero_lag_past_a_marker() {
    let cluster = marker_cluster(1);
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 5, 1, "before");
    write_marker(&brokers, 0, Marker::Commit);
    let group = spate_test::unique_name("pipeline");
    let (name, handle, script, shutdown, run) =
        run_marker_pipeline(&brokers, &group, 1, "{ interval: 100ms }", |_| {});

    wait_until(Duration::from_secs(60), "the lag reaches 0", || {
        support::lag_of(&handle.render(), &name, 0) == Some(0.0)
    });
    assert_eq!(
        support::committed_by(&brokers, &group, TOPIC, 1),
        [rdkafka::Offset::Offset(6)]
    );
    shutdown.trigger();
    let report = run
        .wait_exit(Duration::from_secs(30))
        .expect("the pipeline exits")
        .expect("run");
    assert_eq!(report.final_watermarks, [(PartitionId(0), 6)]);
    let rows: usize = script
        .writes()
        .iter()
        .map(|w| spate_test::decode_rows(&w.payload).len())
        .sum();
    assert_eq!(rows, 5, "each record reached the sink once");
}

/// A partition idle beside a busy one on the same thread still passes its marker.
/// Regression for #1709.
#[test]
#[expect(clippy::disallowed_methods, reason = "the test joins the thread")]
fn an_idle_partition_beside_a_busy_one_reaches_zero_lag() {
    let cluster = marker_cluster(2);
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 2, 2, "before");
    write_marker(&brokers, 0, Marker::Commit);
    let stop = Arc::new(AtomicBool::new(false));
    let feeding = {
        let stop = Arc::clone(&stop);
        let brokers = brokers.clone();
        std::thread::spawn(move || {
            let producer: rdkafka::producer::BaseProducer = support::raw_config()
                .set("bootstrap.servers", &brokers)
                .build()
                .expect("producer");
            while !stop.load(Ordering::Relaxed) {
                producer
                    .send(
                        rdkafka::producer::BaseRecord::<(), _>::to(TOPIC)
                            .partition(1)
                            .payload(&b"busy"[..]),
                    )
                    .expect("enqueue");
                producer.poll(Duration::from_millis(1));
            }
        })
    };
    let group = spate_test::unique_name("beside");
    let (name, handle, _script, shutdown, run) =
        run_marker_pipeline(&brokers, &group, 1, "{ interval: 100ms }", |_| {});

    wait_until(
        Duration::from_secs(60),
        "the idle partition's lag reaches 0",
        || support::lag_of(&handle.render(), &name, 0) == Some(0.0),
    );
    stop.store(true, Ordering::Relaxed);
    feeding.join().expect("feeding thread");
    shutdown.trigger();
    run.wait_exit(Duration::from_secs(30))
        .expect("the pipeline exits")
        .expect("run");
}

/// Markers behind a head batch the sink has not acknowledged take no room under
/// the pending ceiling: the lane keeps polling and serving them, and one batch
/// covers them all once the head resolves. Regression for #1709.
#[test]
fn markers_behind_a_stalled_head_leave_the_lane_polling() {
    const LATER_MARKERS: usize = 5;
    let cluster = marker_cluster(1);
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, TOPIC, 1, 1, "first");
    let group = spate_test::unique_name("stalled");
    let (name, handle, script, shutdown, run) = run_marker_pipeline(
        &brokers,
        &group,
        1,
        "{ interval: 100ms, max_pending_batches: 2 }",
        |script| {
            script.enqueue_global(spate_test::WriteOutcome::ok());
            let mut held = spate_test::WriteOutcome::ok();
            held.hold = true;
            script.enqueue_global(held);
        },
    );
    wait_until(Duration::from_secs(60), "the first record commits", || {
        support::committed_by(&brokers, &group, TOPIC, 1) == [rdkafka::Offset::Offset(1)]
    });

    produce(&brokers, TOPIC, 1, 1, "head");
    for _ in 0..LATER_MARKERS {
        write_marker(&brokers, 0, Marker::Abort);
    }
    let end = 2 + LATER_MARKERS as i64;
    let figure = |metric: &str| {
        spate_test::metric_value(
            &handle.render(),
            metric,
            &[("pipeline", name.as_str()), ("partition", "0")],
        )
    };
    // The lag shows the fetcher reached the end, and the empty queue beside it shows
    // the lane served what it fetched.
    wait_until(
        Duration::from_secs(60),
        "the lane serves the markers",
        || {
            figure("spate_source_lag_records") == Some((end - 1) as f64)
                && figure("spate_kafka_source_partition_fetch_queue_messages") == Some(0.0)
                && figure(names::CHECKPOINT_PENDING_BATCHES) == Some(1.0)
                && script.held_writes() == 1
        },
    );

    script.release_held();
    wait_until(
        Duration::from_secs(60),
        "the head and the markers commit",
        || support::committed_by(&brokers, &group, TOPIC, 1) == [rdkafka::Offset::Offset(end)],
    );
    shutdown.trigger();
    run.wait_exit(Duration::from_secs(30))
        .expect("the pipeline exits")
        .expect("run");
}

/// Runs over a partition with no committed offset, on the mock broker. A test
/// that fails without the start commit ends its doc `Regression for #1704.`
mod latest_start {
    use super::*;
    use support::latest;

    fn on_a_cluster(partitions: i32, body: impl FnOnce(&str, &str)) {
        let cluster = MockKafka::new();
        cluster
            .create_topic(TOPIC, partitions, 1)
            .expect("create topic");
        body(&cluster.bootstrap_servers(), TOPIC);
    }

    /// A quiet partition reads what arrived while the pipeline was down.
    /// Regression for #1704.
    #[test]
    fn a_quiet_partition_reads_what_arrived_across_a_clean_restart() {
        on_a_cluster(2, |b, t| {
            latest::a_quiet_partition_across_a_restart(b, t, "latest")
        });
    }

    /// The control: `earliest` reads the same records with no start commit.
    #[test]
    fn a_quiet_partition_reads_what_arrived_under_earliest() {
        on_a_cluster(2, |b, t| {
            latest::a_quiet_partition_across_a_restart(b, t, "earliest")
        });
    }

    /// A new group whose sink is down keeps its start, and a restart reads it.
    /// Regression for #1704.
    #[test]
    fn a_new_group_with_its_sink_down_reads_from_its_start_after_a_restart() {
        on_a_cluster(2, |b, t| {
            latest::a_sink_down_across_a_restart(b, t, "latest")
        });
    }

    /// A second process joining a group with no commit loses no record.
    /// Regression for #1704.
    #[test]
    fn a_second_process_joining_a_group_with_no_commit_loses_no_record() {
        on_a_cluster(2, |b, t| latest::a_second_process_joining(b, t, "latest"));
    }

    /// The next member reads from where a removed member started.
    /// Regression for #1704.
    #[test]
    fn a_removed_single_consumer_loses_no_record() {
        on_a_cluster(1, |b, t| latest::a_removed_single_consumer(b, t, "latest"));
    }

    /// The control: `earliest` reads from the start with no start commit.
    #[test]
    fn a_removed_single_consumer_loses_no_record_under_earliest() {
        on_a_cluster(1, |b, t| {
            latest::a_removed_single_consumer(b, t, "earliest")
        });
    }
    /// Run `body` with this thread's warnings going to `capture`, which a test
    /// reads during the run.
    fn with_warnings(capture: &spate_test::LogCapture, body: impl FnOnce()) {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .with_writer(capture.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, body);
    }

    /// Advance `clock` past the hold until `driver`'s first consumer has lanes.
    fn past_the_hold(driver: &mut Driver, clock: &TestClock) {
        wait_until(ASSIGNMENT, "the first consumer's lanes", || {
            clock.advance(Duration::from_millis(500));
            driver.step(Duration::from_millis(20));
            !driver.lanes.is_empty()
        });
    }

    /// `config` under `latest`, whose interval commit never fires in a test, and
    /// whose startup deadline is out of reach of a test advancing a frozen clock.
    fn latest_config(brokers: &str, group: &str, consumers: usize) -> KafkaSourceConfig {
        let mut cfg = watched_config(brokers, group, consumers);
        cfg.commit_interval = Duration::from_hours(1);
        cfg.startup_timeout = Duration::from_hours(1);
        cfg.rdkafka
            .insert("auto.offset.reset".into(), "latest".into());
        cfg
    }

    /// [`latest_config`] for one consumer with statistics off.
    fn quiet_latest(brokers: &str, group: &str) -> KafkaSourceConfig {
        let mut cfg = latest_config(brokers, group, 1);
        cfg.statistics_interval = Duration::ZERO;
        cfg
    }

    /// A cluster with one topic of `partitions`, each holding three records.
    fn cluster_with_records(partitions: i32) -> (MockKafka, String) {
        let cluster = MockKafka::new();
        cluster
            .create_topic(TOPIC, partitions, 1)
            .expect("create topic");
        let brokers = cluster.bootstrap_servers();
        produce(&brokers, TOPIC, 3, partitions, "before");
        (cluster, brokers)
    }

    fn produce_round(brokers: &str, partitions: i32) -> impl FnMut() {
        let brokers = brokers.to_owned();
        let mut round = 0;
        move || {
            round += 1;
            produce(&brokers, TOPIC, 2, partitions, &format!("r{round}"));
        }
    }

    /// Lanes for a source on `cfg`, opened and assigned.
    fn accepted(cfg: KafkaSourceConfig) -> (KafkaSource, Checkpointer, Vec<Lane>) {
        let cp = Checkpointer::new();
        let mut source = KafkaSource::new(cfg);
        source.open(SourceCtx::new(cp.handle())).expect("open");
        let lanes = await_assignment(&mut source, ASSIGNMENT);
        (source, cp, lanes)
    }

    /// Read across one epoch and the next: what each partition delivered has no
    /// gap, so the second epoch skipped nothing the first did not deliver.
    fn assert_no_gap_across_epochs(driver: &mut Driver, brokers: &str) {
        let mut produce = produce_round(brokers, 4);
        produce();
        let mut read = driver.read(2, Duration::from_secs(30));
        driver.settle_group(2, 4, Duration::from_secs(60));
        produce();
        for (p, offsets) in driver.read(2, Duration::from_secs(30)) {
            read.entry(p).or_default().extend(offsets);
        }
        for (p, offsets) in &read {
            let (first, last) = (
                *offsets.first().expect("read"),
                *offsets.last().expect("read"),
            );
            assert_eq!(
                usize::try_from(last - first + 1).expect("ordered"),
                offsets.len(),
                "partition {p} has a gap: {offsets:?}"
            );
        }
    }

    /// A partition with no commit has one once its lane is handed out.
    /// Regression for #1704.
    #[test]
    fn a_new_partition_is_committed_before_its_lane_is_handed_out() {
        let (_cluster, brokers) = cluster_with_records(1);
        let lines = spate_test::capture_logs(tracing::Level::INFO, || {
            let _accepted = accepted(quiet_latest(&brokers, "start-commit"));
            assert_eq!(
                committed(&brokers, "start-commit", 1),
                [rdkafka::Offset::Offset(3)]
            );
        });
        assert!(
            lines
                .iter()
                .any(|l| l.contains("accepted assignment") && l.contains("started_at_end=1")),
            "{lines:#?}"
        );
    }

    /// The first record a new partition delivers is at its start commit.
    /// Regression for #1704.
    #[test]
    fn a_record_produced_after_the_start_is_read() {
        let (_cluster, brokers) = cluster_with_records(1);
        let mut driver = Driver::open(quiet_latest(&brokers, "start-read"));
        driver.settle(1, ASSIGNMENT);
        let start = committed(&brokers, "start-read", 1)[0];
        let first = driver.read_from_end(Duration::from_secs(30), produce_round(&brokers, 1));
        assert_eq!(start, rdkafka::Offset::Offset(first[&0]));
    }

    /// A start commit the broker refuses for a removed member discards the
    /// offer with no lanes, and the next accept lists the end again. The refused
    /// commit queues no rejoin while an offer is held, and a broker that refuses
    /// the commit fails the member's heartbeats too, so the test scripts the
    /// heartbeat once the discard has happened. Regression for #1704.
    #[test]
    fn a_refused_start_commit_discards_the_offer() {
        for (code, group) in [
            (
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
                "refused-generation",
            ),
            (
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID,
                "refused-member",
            ),
            (
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
                "refused-rebalance",
            ),
        ] {
            let (cluster, brokers) = cluster_with_records(1);
            cluster.request_errors(RDKafkaApiKey::OffsetCommit, &[code]);
            let capture = spate_test::LogCapture::new();
            let discards = || {
                capture
                    .lines()
                    .iter()
                    .filter(|l| l.contains("discarded an assignment"))
                    .count()
            };
            with_warnings(&capture, || {
                let cp = Checkpointer::new();
                let mut source = KafkaSource::new(quiet_latest(&brokers, group));
                source.open(SourceCtx::new(cp.handle())).expect("open");
                let mut scripted = false;
                let mut lanes = None;
                wait_until(Duration::from_secs(60), "lanes after the discard", || {
                    if !scripted && discards() == 1 {
                        assert_eq!(committed(&brokers, group, 1), [rdkafka::Offset::Invalid]);
                        cluster.request_errors(RDKafkaApiKey::Heartbeat, &[code]);
                        scripted = true;
                    }
                    match source.poll_events(Duration::from_millis(50)) {
                        Ok(SourceEvent::LanesAssigned(new)) => {
                            assert!(scripted, "lanes before the discard");
                            lanes = Some(new);
                            true
                        }
                        _ => false,
                    }
                });
                assert_eq!(committed(&brokers, group, 1), [rdkafka::Offset::Offset(3)]);
                drop(lanes);
            });
            assert_eq!(discards(), 1, "{code:?}");
        }
    }

    /// A start commit that fails with a transient code holds the offer until
    /// the pace comes due, and then lanes arrive with the commit in place.
    /// Regression for #1704.
    #[test]
    fn a_failed_start_commit_is_retried_and_the_offer_survives() {
        let (cluster, brokers) = cluster_with_records(1);
        cluster.request_errors(
            RDKafkaApiKey::OffsetCommit,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN],
        );
        let clock = TestClock::frozen();
        let mut source = KafkaSource::new(quiet_latest(&brokers, "start-retry"))
            .with_clock(Arc::clone(&clock) as _);
        let cp = Checkpointer::new();
        let capture = spate_test::LogCapture::new();
        let attempts = || {
            capture
                .lines()
                .iter()
                .filter(|l| l.contains("could not settle a start position"))
                .count()
        };
        with_warnings(&capture, || {
            source.open(SourceCtx::new(cp.handle())).expect("open");
            wait_until(ASSIGNMENT, "the first attempt", || {
                assert!(
                    !matches!(
                        source.poll_events(Duration::from_millis(50)),
                        Ok(SourceEvent::LanesAssigned(_))
                    ),
                    "lanes before the commit"
                );
                attempts() == 1
            });
            // Held at the pace: the frozen clock keeps the offer from retrying.
            for _ in 0..5 {
                assert!(matches!(
                    source.poll_events(Duration::from_millis(50)),
                    Ok(SourceEvent::Idle)
                ));
            }
            assert_eq!(attempts(), 1, "one line per attempt");
            clock.advance(Duration::from_secs(1));
            let lanes = await_assignment(&mut source, ASSIGNMENT);
            assert_eq!(lanes.len(), 1);
            assert_eq!(
                committed(&brokers, "start-retry", 1),
                [rdkafka::Offset::Offset(3)]
            );
        });
    }

    /// A reassign whose lookup fails keeps its offers and keeps polling until
    /// the pace comes due, and then every consumer's lanes arrive together.
    /// Regression for #1704.
    #[test]
    fn a_failed_reassign_lookup_is_retried_at_its_pace() {
        on_a_mock_electing_at_once(4, |cluster| {
            hold_back_the_second_join(cluster);
            let brokers = cluster.bootstrap_servers();
            let clock = TestClock::frozen();
            let source = KafkaSource::new(latest_config(&brokers, "retried-epoch", 2))
                .with_clock(Arc::clone(&clock) as _);
            let capture = spate_test::LogCapture::new();
            let attempts = || {
                capture
                    .lines()
                    .iter()
                    .filter(|l| l.contains("could not settle a start position"))
                    .count()
            };
            with_warnings(&capture, || {
                let mut driver = Driver::open_watched(source, "retried-epoch");
                past_the_hold(&mut driver, &clock);
                // The second consumer's offer drains these lanes, and the lookup
                // of the accept after that drain fails once.
                cluster.request_errors(
                    RDKafkaApiKey::OffsetFetch,
                    &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN],
                );
                wait_until(Duration::from_secs(60), "the failed lookup", || {
                    driver.step(Duration::from_millis(20));
                    attempts() == 1
                });
                // Held at the pace with the clock frozen, and still polling.
                for _ in 0..5 {
                    driver.step(Duration::from_millis(20));
                    assert!(driver.lanes.is_empty(), "lanes before the pace");
                }
                assert_eq!(attempts(), 1, "one line per attempt");
                clock.advance(Duration::from_secs(1));
                driver.settle_group(2, 4, Duration::from_secs(60));
            });
        });
    }

    /// A member joining during a retried start takes the partition from the
    /// start the group holds, and the retrying member goes on to its own offer.
    /// Regression for #1704.
    #[test]
    fn a_revocation_during_a_retried_start_loses_no_record() {
        let (cluster, brokers) = cluster_with_records(1);
        // The first attempt fails. The source below is the only committer until
        // the joining member arrives, and that member's commit needs no retry.
        cluster.request_errors(
            RDKafkaApiKey::OffsetCommit,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN],
        );
        let clock = TestClock::frozen();
        let capture = spate_test::LogCapture::new();
        with_warnings(&capture, || {
            let first = KafkaSource::new(quiet_latest(&brokers, "retry-revoked"))
                .with_clock(Arc::clone(&clock) as _);
            let mut first = Driver::open_with(first, |ctx| ctx);
            wait_until(ASSIGNMENT, "the failed start", || {
                first.step(Duration::from_millis(50));
                capture
                    .lines()
                    .iter()
                    .any(|l| l.contains("could not settle a start position"))
            });
            let mut second = Driver::open(quiet_latest(&brokers, "retry-revoked"));
            wait_until(Duration::from_secs(60), "a lane on either member", || {
                clock.advance(Duration::from_secs(1));
                first.step(Duration::from_millis(20));
                second.step(Duration::from_millis(20));
                !first.lanes.is_empty() || !second.lanes.is_empty()
            });
            let start = committed(&brokers, "retry-revoked", 1)[0];
            let holder = if first.lanes.is_empty() {
                &mut second
            } else {
                &mut first
            };
            let read = holder.read_from_end(Duration::from_secs(30), produce_round(&brokers, 1));
            assert_eq!(start, rdkafka::Offset::Offset(read[&0]));
        });
    }

    /// A member holding a start gives it up when another member's join starts a
    /// rebalance, which refuses its commit and waits for it to rejoin. The mock's
    /// rebalance runs for most of a session timeout, past this test's wait, so the
    /// member has to act on the refusal.
    /// Regression for #1704.
    #[test]
    fn a_held_start_gives_way_to_a_rebalance() {
        use rdkafka::bindings as sys;

        let cluster = MockKafka::new();
        cluster.create_topic(TOPIC, 1, 1).expect("create topic");
        let brokers = cluster.bootstrap_servers();
        produce(&brokers, TOPIC, 3, 1, "before");
        let mock = cluster.native();
        // SAFETY: the pointer is valid while `cluster` lives, across this test.
        unsafe { sys::rd_kafka_mock_start_request_tracking(mock) };
        let joins = || {
            let mut count = 0;
            // SAFETY: as above. The array holds `count` requests until it is destroyed.
            unsafe {
                let requests = sys::rd_kafka_mock_get_requests(mock, &raw mut count);
                let seen = (0..count)
                    .filter(|i| {
                        sys::rd_kafka_mock_request_api_key(*requests.add(*i))
                            == RDKafkaApiKey::JoinGroup as i16
                    })
                    .count();
                sys::rd_kafka_mock_request_destroy_array(requests, count);
                seen
            }
        };
        let config = || {
            let mut cfg = quiet_latest(&brokers, "held-rebalance");
            cfg.rdkafka
                .insert("session.timeout.ms".into(), "120000".into());
            cfg
        };
        cluster.request_errors(
            RDKafkaApiKey::OffsetCommit,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN],
        );
        let clock = TestClock::frozen();
        let capture = spate_test::LogCapture::new();
        with_warnings(&capture, || {
            let first = KafkaSource::new(config()).with_clock(Arc::clone(&clock) as _);
            let mut first = Driver::open_with(first, |ctx| ctx);
            wait_until(ASSIGNMENT, "the failed start", || {
                first.step(Duration::from_millis(50));
                capture
                    .lines()
                    .iter()
                    .any(|l| l.contains("could not settle a start position"))
            });
            // The frozen clock keeps the retry back until the join has started the
            // rebalance.
            let before = joins();
            let mut second = Driver::open(config());
            wait_until(ASSIGNMENT, "the second member's join", || {
                second.step(Duration::from_millis(20));
                joins() > before
            });
            wait_until(Duration::from_secs(30), "the offer given up", || {
                clock.advance(Duration::from_secs(1));
                first.step(Duration::from_millis(20));
                second.step(Duration::from_millis(20));
                capture
                    .lines()
                    .iter()
                    .any(|l| l.contains("discarded an assignment"))
            });
            assert!(first.lanes.is_empty(), "lanes from a refused start");
            assert_eq!(
                committed(&brokers, "held-rebalance", 1),
                [rdkafka::Offset::Invalid]
            );
        });
    }

    /// A start commit that keeps failing ends the run at the assignment
    /// deadline, once the source has held an assignment before.
    /// Regression for #1704.
    #[test]
    fn a_start_commit_that_keeps_failing_ends_at_the_assignment_deadline() {
        let (cluster, brokers) = cluster_with_records(1);
        let clock = TestClock::frozen();
        let mut cfg = quiet_latest(&brokers, "start-deadline");
        cfg.assignment_timeout = Duration::from_secs(5);
        let source = KafkaSource::new(cfg).with_clock(Arc::clone(&clock) as _);
        let mut driver = Driver::open_with(source, |ctx| ctx);
        driver.settle(1, ASSIGNMENT);
        // On the rejoin the partition has a commit, so its lookup is what fails.
        cluster.request_errors(
            RDKafkaApiKey::OffsetFetch,
            &vec![RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN; 100_000],
        );
        cluster.request_errors(
            RDKafkaApiKey::Heartbeat,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION],
        );
        // Hold the clock until a failed lookup has named the cause, so the
        // deadline reads that cause and not the revocation before it.
        let capture = spate_test::LogCapture::new();
        with_warnings(&capture, || {
            wait_until(Duration::from_secs(60), "the failed lookup", || {
                if let Ok(SourceEvent::LanesRevoked { lanes, barrier }) =
                    driver.source.poll_events(Duration::from_millis(50))
                {
                    let _ = support::complete_revocation(&mut driver.source, &lanes, &barrier, &[]);
                    driver.lanes.clear();
                }
                capture
                    .lines()
                    .iter()
                    .any(|l| l.contains("could not settle a start position"))
            });
        });
        clock.advance(Duration::from_secs(6));
        match driver.source.poll_events(Duration::from_millis(50)) {
            Err(e) if e.class() == ErrorClass::Fatal => {
                assert!(e.to_string().contains("start position"), "{e}")
            }
            other => panic!("expected the deadline error, got {other:?}"),
        }
    }

    /// A read with no acknowledgment replays from the start commit after a
    /// crash. Regression for #1704.
    #[test]
    fn a_read_with_no_acknowledgment_replays_from_the_start_commit() {
        let (_cluster, brokers) = cluster_with_records(1);
        let cfg = quiet_latest(&brokers, "crash");
        let first_at = {
            let mut driver = Driver::open(cfg.clone());
            driver.settle(1, ASSIGNMENT);
            driver.read_from_end(Duration::from_secs(30), produce_round(&brokers, 1))[&0]
        };
        let mut driver = Driver::open(cfg);
        driver.settle(1, ASSIGNMENT);
        let second_at =
            driver.read_from_end(Duration::from_secs(30), produce_round(&brokers, 1))[&0];
        assert_eq!(second_at, first_at, "the new process reads from the start");
    }

    /// A consumer the group removes reads from its start when it returns, with
    /// one consumer and with two, and a partition its peer committed resumes
    /// from that commit. Regression for #1704.
    #[test]
    fn a_removed_member_under_latest_loses_no_record() {
        for consumers in [1, 2] {
            let (cluster, brokers) = cluster_with_records(2);
            cluster.hold_first_rebalance();
            let group: &'static str = if consumers == 1 {
                "removed-one"
            } else {
                "removed-two"
            };
            let mut driver = Driver::open_watched(
                KafkaSource::new(latest_config(&brokers, group, consumers)),
                group,
            );
            driver.settle_group(consumers, 2, Duration::from_secs(60));
            let before = driver.read_from_end(Duration::from_secs(30), produce_round(&brokers, 2));
            assert!(
                !driver.lose_a_member(&cluster).is_empty(),
                "the group refuses a removed member's commit"
            );
            driver.settle(2, Duration::from_secs(60));
            let read_back = committed(&brokers, group, 2);
            let after = driver.read_from_end(Duration::from_secs(30), produce_round(&brokers, 2));
            for (p, first) in &after {
                let at = read_back[usize::try_from(*p).expect("partition")];
                assert!(
                    *first <= before[p] || at == rdkafka::Offset::Offset(*first),
                    "partition {p} read from {first}, past {} with commit {at:?}",
                    before[p],
                );
            }
        }
    }

    /// The first consumer accepted alone reads, and the second consumer's join
    /// starts a new epoch over partitions with no acknowledged commit. No offset
    /// is skipped, and the re-accept writes nothing over the start commit.
    /// Regression for #1704.
    #[test]
    fn a_reassign_with_no_commit_loses_no_record() {
        on_a_cluster_electing_at_once(4, |brokers| {
            let clock = TestClock::frozen();
            let source = KafkaSource::new(latest_config(brokers, "reassign-start", 2))
                .with_clock(Arc::clone(&clock) as _);
            let mut driver = Driver::open_watched(source, "reassign-start");
            past_the_hold(&mut driver, &clock);
            assert_no_gap_across_epochs(&mut driver, brokers);
        });
    }

    /// A consumer accepted alone and then removed loses no record, with its
    /// peer taking the next epoch: the two-consumer route. Regression for #1704.
    #[test]
    fn a_member_accepted_alone_then_removed_loses_no_record() {
        on_a_cluster_electing_at_once(4, |brokers| {
            let clock = TestClock::frozen();
            let source = KafkaSource::new(latest_config(brokers, "alone-removed", 2))
                .with_clock(Arc::clone(&clock) as _);
            let mut driver = Driver::open_watched(source, "alone-removed");
            past_the_hold(&mut driver, &clock);
            assert_no_gap_across_epochs(&mut driver, brokers);
        });
    }

    /// A discard under `latest` writes no commit past the group's start and
    /// loses nothing.
    #[test]
    fn a_discarded_offer_under_latest_loses_no_record() {
        on_a_cluster_electing_at_once(4, |brokers| {
            let clock = TestClock::frozen();
            let source = KafkaSource::new(latest_config(brokers, "discard-latest", 2))
                .with_clock(Arc::clone(&clock) as _);
            let mut driver = Driver::open_watched(source, "discard-latest");
            driver.settle_group(2, 4, Duration::from_secs(60));
            let first = driver.read_from_end(Duration::from_secs(30), produce_round(brokers, 4));
            assert_eq!(first.len(), 4, "{first:?}");
        });
    }

    /// A commit refused for one consumer discards that consumer's offer alone:
    /// the consumer settled before it in the same accept gets its lanes.
    #[test]
    fn a_stale_offer_leaves_a_settled_one_accepted() {
        let (cluster, brokers) = cluster_with_records(4);
        cluster.hold_first_rebalance();
        cluster.request_errors(
            RDKafkaApiKey::OffsetCommit,
            &[
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
            ],
        );
        let mut cfg = latest_config(&brokers, "stale-beside-settled", 2);
        cfg.statistics_interval = Duration::ZERO;
        let lines = spate_test::capture_logs(tracing::Level::WARN, || {
            let mut driver = Driver::open(cfg);
            wait_until(
                Duration::from_secs(30),
                "the settled consumer's lanes",
                || {
                    driver.step(Duration::from_millis(50));
                    !driver.lanes.is_empty()
                },
            );
            assert_eq!(driver.lanes.len(), 2, "{:?}", driver.partitions());
        });
        assert!(
            lines.iter().any(|l| l.contains("discarded an assignment")),
            "{lines:#?}"
        );
    }

    /// A failed lookup holds the offer under `latest`, and the commit follows.
    /// Regression for #1704.
    #[test]
    fn a_failed_lookup_on_an_uncommitted_partition_loses_no_record() {
        let (cluster, brokers) = cluster_with_records(1);
        cluster.request_errors(
            RDKafkaApiKey::OffsetFetch,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN],
        );
        let mut driver = Driver::open(quiet_latest(&brokers, "lookup-latest"));
        driver.settle(1, ASSIGNMENT);
        assert_eq!(
            committed(&brokers, "lookup-latest", 1),
            [rdkafka::Offset::Offset(3)]
        );
        let first = driver.read_from_end(Duration::from_secs(30), produce_round(&brokers, 1))[&0];
        assert_eq!(first, 3);
    }

    /// A failed lookup under `latest` keeps a partition's commit: no lane until
    /// it answers, and then the partition reads from the commit.
    /// Regression for #1704.
    #[test]
    fn a_failed_lookup_under_latest_keeps_the_commit() {
        let (cluster, brokers) = cluster_with_records(1);
        produce(&brokers, TOPIC, 3, 1, "more");
        commit_as(&brokers, "keeps-commit", 3);
        cluster.request_errors(
            RDKafkaApiKey::OffsetFetch,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN; 3],
        );
        let (mut source, _cp, mut lanes) = accepted(quiet_latest(&brokers, "keeps-commit"));
        let rows = drain_lane(&mut source, &mut lanes[0], 3, Duration::from_secs(30));
        let offsets: Vec<i64> = rows.iter().map(|(_, _, o)| *o).collect();
        assert_eq!(offsets, vec![3, 4, 5]);
        assert_eq!(
            committed(&brokers, "keeps-commit", 1),
            [rdkafka::Offset::Offset(3)]
        );
    }

    /// A lookup that fails for one consumer of an accept keeps the offer of the
    /// consumer settled before it and of the consumer after it, and each one's
    /// partitions get a lane.
    #[test]
    fn a_failed_lookup_keeps_every_consumers_offer() {
        for (api, errors, group) in [
            (
                RDKafkaApiKey::OffsetFetch,
                &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN][..],
                "kept-on-lookup",
            ),
            (
                RDKafkaApiKey::OffsetCommit,
                &[
                    RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
                    RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN,
                ][..],
                "kept-on-commit",
            ),
        ] {
            let (cluster, brokers) = cluster_with_records(4);
            cluster.hold_first_rebalance();
            cluster.request_errors(api, errors);
            let mut cfg = latest_config(&brokers, group, 2);
            cfg.statistics_interval = Duration::ZERO;
            let lines = spate_test::capture_logs(tracing::Level::WARN, || {
                let mut driver = Driver::open(cfg);
                driver.settle(4, Duration::from_secs(60));
            });
            assert!(
                lines
                    .iter()
                    .any(|l| l.contains("could not settle a start position")),
                "{group}: {lines:#?}"
            );
            assert_eq!(
                committed(&brokers, group, 4),
                [rdkafka::Offset::Offset(3); 4],
                "{group}"
            );
        }
    }

    /// A failing ListOffsets writes no commit and hands out no lane until it
    /// answers. Regression for #1704.
    #[test]
    fn a_failed_list_writes_no_commit() {
        let (cluster, brokers) = cluster_with_records(1);
        cluster.request_errors(
            RDKafkaApiKey::ListOffsets,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN],
        );
        let lines = spate_test::capture_logs(tracing::Level::WARN, || {
            let _accepted = accepted(quiet_latest(&brokers, "failed-list"));
            assert_eq!(
                committed(&brokers, "failed-list", 1),
                [rdkafka::Offset::Offset(3)]
            );
        });
        assert!(
            lines
                .iter()
                .any(|l| l.contains("could not settle a start position")),
            "{lines:#?}"
        );
    }

    /// A broker that does not yet know a partition just added to the topic holds
    /// the offer, and the run goes on once it does. Regression for #1704.
    #[test]
    fn a_partition_the_broker_does_not_know_yet_is_retried() {
        let (cluster, brokers) = cluster_with_records(1);
        cluster.request_errors(
            RDKafkaApiKey::ListOffsets,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART],
        );
        let _accepted = accepted(quiet_latest(&brokers, "not-known-yet"));
        assert_eq!(
            committed(&brokers, "not-known-yet", 1),
            [rdkafka::Offset::Offset(3)]
        );
    }

    /// Every spelling of the policy, and leaving it unset, takes the start rule.
    /// Regression for #1704.
    #[test]
    fn a_new_partition_takes_the_start_rule_on_every_spelling() {
        for (key, policy) in [
            (Some("auto.offset.reset"), "latest"),
            (Some("auto.offset.reset"), "LATEST"),
            (Some("auto.offset.reset"), "largest"),
            (Some("auto.offset.reset"), "end"),
            (Some("topic.auto.offset.reset"), "latest"),
            (None, ""),
        ] {
            let (_cluster, brokers) = cluster_with_records(1);
            let mut cfg = quiet_latest(&brokers, "spelling");
            cfg.rdkafka.remove("auto.offset.reset");
            if let Some(key) = key {
                cfg.rdkafka.insert(key.into(), policy.into());
            }
            let _accepted = accepted(cfg);
            assert_eq!(
                committed(&brokers, "spelling", 1),
                [rdkafka::Offset::Offset(3)],
                "{key:?} {policy}"
            );
        }
    }

    /// `error` is left to librdkafka and commits nothing.
    #[test]
    fn an_error_policy_partition_is_left_to_librdkafka() {
        let (_cluster, brokers) = cluster_with_records(1);
        let mut cfg = quiet_latest(&brokers, "error-policy");
        cfg.rdkafka
            .insert("auto.offset.reset".into(), "error".into());
        let _accepted = accepted(cfg);
        assert_eq!(
            committed(&brokers, "error-policy", 1),
            [rdkafka::Offset::Invalid]
        );
    }

    /// A new group's first start under `latest` skips what came before it, with
    /// the key set and unset.
    #[test]
    fn a_new_group_under_latest_skips_what_was_produced_before_it_joined() {
        for unset in [false, true] {
            let (_cluster, brokers) = cluster_with_records(1);
            let mut cfg = quiet_latest(&brokers, "first-start");
            if unset {
                cfg.rdkafka.remove("auto.offset.reset");
            }
            let mut driver = Driver::open(cfg);
            driver.settle(1, ASSIGNMENT);
            let first =
                driver.read_from_end(Duration::from_secs(30), produce_round(&brokers, 1))[&0];
            assert!(first >= 3, "read {first}");
        }
    }

    /// A partition with a commit keeps it, and the one beside it with none gains
    /// its end: a group upgrading with one quiet partition.
    /// Regression for #1704.
    #[test]
    fn a_partition_with_no_commit_beside_a_committed_one() {
        let (_cluster, brokers) = cluster_with_records(2);
        commit_as(&brokers, "upgrade", 1);
        let _accepted = accepted(quiet_latest(&brokers, "upgrade"));
        assert_eq!(
            committed(&brokers, "upgrade", 2),
            [rdkafka::Offset::Offset(1), rdkafka::Offset::Offset(3)]
        );
    }

    /// The interval commit stores nothing over a start with no acknowledgment:
    /// partition 0 reads its start once partition 1 has committed.
    /// Regression for #1704.
    #[test]
    fn an_unacknowledged_batch_is_not_committed_by_the_interval() {
        let (_cluster, brokers) = cluster_with_records(2);
        let mut cfg = quiet_latest(&brokers, "interval");
        cfg.commit_interval = Duration::from_millis(200);
        let mut driver = Driver::open(cfg);
        driver.settle(2, ASSIGNMENT);
        let at = driver
            .lanes
            .iter()
            .position(|l| l.partition().0 == 1)
            .expect("partition 1's lane");
        let mut lane = driver.lanes.remove(at);
        // Partition 0 receives these and its lane goes unread.
        produce(&brokers, TOPIC, 2, 2, "held");
        let mut read = 0;
        wait_until(Duration::from_secs(30), "partition 1's read", || {
            let _ = driver.source.poll_events(Duration::from_millis(10));
            if let Some(mut batch) = lane.poll(64, Duration::from_millis(10)).expect("poll") {
                while batch.next_payload().is_some() {
                    read += 1;
                }
            }
            read >= 2
        });
        driver.cp.drain();
        let watermarks = driver.cp.take_watermarks();
        driver.source.commit(&watermarks).expect("store");
        wait_until(Duration::from_secs(30), "partition 1's commit", || {
            let _ = driver.source.poll_events(Duration::from_millis(10));
            committed(&brokers, "interval", 2)[1] == rdkafka::Offset::Offset(5)
        });
        assert_eq!(
            committed(&brokers, "interval", 2)[0],
            rdkafka::Offset::Offset(3),
            "partition 0 keeps its start until a batch of it is acknowledged"
        );
    }

    /// A new partition's lag series appears before any watermark moves.
    /// Regression for #1704.
    #[test]
    fn a_new_partition_publishes_lag_before_its_first_commit() {
        let (_cluster, brokers) = cluster_with_records(1);
        let mut cfg = quiet_latest(&brokers, "lag-start");
        cfg.statistics_interval = Duration::from_millis(100);
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            let metrics = Arc::new(spate_core::metrics::SourceMetrics::new(
                &spate_core::metrics::ComponentLabels::new("lag-start", "source", "kafka"),
            ));
            let cp = Checkpointer::new();
            let mut source = KafkaSource::new(cfg);
            source
                .open(SourceCtx::new(cp.handle()).with_stage_metrics(Some(metrics)))
                .expect("open");
            let _lanes = await_assignment(&mut source, ASSIGNMENT);
            wait_until(Duration::from_secs(30), "lag published", || {
                source
                    .poll_events(Duration::from_millis(100))
                    .expect("poll_events");
                lag_series_count(&handle.render()) == 1
            });
        });
    }

    /// One accept under `latest` spends one lookup bound across the list too.
    #[test]
    fn one_assignment_under_latest_spends_one_lookup_bound() {
        /// `COMMITTED_LOOKUP_TIMEOUT`, plus slack for the assign itself.
        const BOUND: Duration = Duration::from_secs(13);
        let (cluster, brokers) = cluster_with_records(3);
        cluster.hold_first_rebalance();
        cluster.request_errors(
            RDKafkaApiKey::ListOffsets,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION; 3],
        );
        let mut cfg = latest_config(&brokers, "slow-list", 3);
        cfg.statistics_interval = Duration::ZERO;
        let mut source = KafkaSource::new(cfg);
        let cp = Checkpointer::new();
        source.open(SourceCtx::new(cp.handle())).expect("open");
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            assert!(Instant::now() < deadline, "no assignment");
            let started = Instant::now();
            let outcome = source.poll_events(Duration::from_millis(100));
            let took = started.elapsed();
            assert!(took <= BOUND, "one poll blocked for {took:?}");
            if let Ok(SourceEvent::LanesAssigned(lanes)) = outcome
                && lanes.len() == 3
            {
                break;
            }
        }
    }

    /// A start commit's wait stays inside the broker's own bound, one
    /// `socket.timeout.ms` here, with slack.
    #[test]
    fn a_start_commit_waits_outside_the_lookup_bound() {
        const BOUND: Duration = Duration::from_secs(25);
        let (_cluster, brokers) = cluster_with_records(1);
        let mut cfg = quiet_latest(&brokers, "commit-bound");
        cfg.rdkafka
            .insert("socket.timeout.ms".into(), "2000".into());
        let mut source = KafkaSource::new(cfg);
        let cp = Checkpointer::new();
        source.open(SourceCtx::new(cp.handle())).expect("open");
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            assert!(Instant::now() < deadline, "no assignment");
            let started = Instant::now();
            let outcome = source.poll_events(Duration::from_millis(100));
            assert!(
                started.elapsed() <= BOUND,
                "blocked {:?}",
                started.elapsed()
            );
            if matches!(outcome, Ok(SourceEvent::LanesAssigned(_))) {
                break;
            }
        }
    }
}
