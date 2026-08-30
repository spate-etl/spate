//! Integration tests against rdkafka's in-process MockCluster (no Docker).

use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use spate_core::checkpoint::Checkpointer;
use spate_core::error::{ErrorClass, SourceError};
use spate_core::record::PartitionId;
use spate_core::source::{PayloadBatch, Source, SourceCtx, SourceEvent, SourceLane};
use spate_kafka::{KafkaSource, KafkaSourceConfig};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

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

fn produce(brokers: &str, per_partition: usize, partitions: i32, tag: &str) {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("producer");
    for p in 0..partitions {
        for i in 0..per_partition {
            let payload = format!("{tag}-p{p}-{i}");
            let key = format!("k{p}-{i}");
            producer
                .send(
                    BaseRecord::to(TOPIC)
                        .partition(p)
                        .payload(payload.as_bytes())
                        .key(key.as_bytes()),
                )
                .expect("enqueue");
        }
    }
    producer.flush(Duration::from_secs(10)).expect("flush");
}

/// Drive `poll_events` until an assignment arrives (or panic on deadline).
fn await_assignment(source: &mut KafkaSource) -> Vec<<KafkaSource as Source>::Lane> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(Instant::now() < deadline, "no assignment within deadline");
        match source
            .poll_events(Duration::from_millis(200))
            .expect("poll_events")
        {
            SourceEvent::LanesAssigned(lanes) => return lanes,
            _ => continue,
        }
    }
}

/// Poll a lane until `want` payloads arrive; returns (payload, key, offset).
fn drain_lane(
    lane: &mut <KafkaSource as Source>::Lane,
    want: usize,
) -> Vec<(Vec<u8>, Vec<u8>, i64)> {
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while got.len() < want {
        assert!(
            Instant::now() < deadline,
            "lane delivered {}/{want} before deadline",
            got.len()
        );
        let Some(mut batch) = lane
            .poll(64, Duration::from_millis(500))
            .expect("lane poll")
        else {
            continue;
        };
        while let Some(raw) = batch.next_payload() {
            got.push((
                raw.bytes.to_vec(),
                raw.key.unwrap_or(&[]).to_vec(),
                raw.offset,
            ));
        }
    }
    got
}

#[test]
fn full_lifecycle_polls_acks_and_commits() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(TOPIC, 3, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, 10, 3, "a");

    let mut cp = Checkpointer::new();
    let mut source = KafkaSource::new(config(&brokers, "life"));
    source.open(SourceCtx::new(cp.handle())).expect("open");

    let mut lanes = await_assignment(&mut source);
    assert_eq!(lanes.len(), 3, "one lane per partition");
    let partitions: Vec<PartitionId> = lanes.iter().map(SourceLane::partition).collect();
    cp.begin_epoch(&partitions, 1);

    for lane in &mut lanes {
        let rows = drain_lane(lane, 10);
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
    let probe: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "life")
        .create()
        .expect("probe");
    let mut tpl = rdkafka::TopicPartitionList::new();
    for p in 0..3 {
        tpl.add_partition(TOPIC, p);
    }
    let committed = probe
        .committed_offsets(tpl, Duration::from_secs(10))
        .expect("committed");
    for elem in committed.elements() {
        assert_eq!(
            elem.offset(),
            rdkafka::Offset::Offset(10),
            "partition {} committed",
            elem.partition()
        );
    }
}

#[test]
fn pause_stops_delivery_and_resume_recovers_gapless() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();

    let mut cp = Checkpointer::new();
    let mut source = KafkaSource::new(config(&brokers, "pause"));
    source.open(SourceCtx::new(cp.handle())).expect("open");
    let mut lanes = await_assignment(&mut source);
    assert_eq!(lanes.len(), 1);
    cp.begin_epoch(&[lanes[0].partition()], 1);

    source.pause(&[lanes[0].id()]).expect("pause");
    produce(&brokers, 5, 1, "b");

    // While paused nothing is delivered (pause also purges prefetch).
    let idle_until = Instant::now() + Duration::from_secs(2);
    while Instant::now() < idle_until {
        // poll_events keeps the client machinery served.
        let _ = source.poll_events(Duration::from_millis(50));
        assert!(
            lanes[0]
                .poll(64, Duration::from_millis(100))
                .expect("poll")
                .is_none(),
            "paused lane must not deliver"
        );
    }

    source.resume(&[lanes[0].id()]).expect("resume");
    let rows = drain_lane(&mut lanes[0], 5);
    let offsets: Vec<i64> = rows.iter().map(|(_, _, o)| *o).collect();
    assert_eq!(offsets, (0..5).collect::<Vec<_>>(), "gap-free redelivery");
}

#[test]
fn second_member_triggers_revoke_then_fresh_assignment() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(TOPIC, 4, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();

    let cp = Checkpointer::new();
    let mut source = KafkaSource::new(config(&brokers, "grp"));
    source.open(SourceCtx::new(cp.handle())).expect("open");
    let lanes = await_assignment(&mut source);
    assert_eq!(lanes.len(), 4, "sole member owns everything");
    let first_ids: Vec<_> = lanes.iter().map(SourceLane::id).collect();

    // A second member joins; keep polling it from a helper thread so its
    // side of the rebalance completes.
    let brokers2 = brokers.clone();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop2 = std::sync::Arc::clone(&stop);
    let joiner = std::thread::spawn(move || {
        let c: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers2)
            .set("group.id", "grp")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("second consumer");
        c.subscribe(&[TOPIC]).expect("subscribe");
        while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
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
                for _ in &ids {
                    barrier.arrive(); // drivers drained
                }
                // What the runtime does after the barrier: commit + flush.
                source.commit(&[]).expect("commit");
                source.flush_commits().expect("flush");
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
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
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
    cfg.startup_timeout = Duration::from_secs(2);
    let cp = Checkpointer::new();
    let mut source = KafkaSource::new(cfg);
    source.open(SourceCtx::new(cp.handle())).expect("open");

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(Instant::now() < deadline, "startup timeout never fired");
        match source.poll_events(Duration::from_millis(100)) {
            Ok(_) => continue,
            // Transient transport errors are expected with no broker; the
            // runtime logs and keeps polling on Retryable, and so does this
            // loop. Only the fatal startup timeout ends it.
            Err(SourceError::Client {
                class: ErrorClass::Retryable,
                ..
            }) => continue,
            Err(e) => {
                assert!(
                    e.to_string().contains("no partition assignment"),
                    "unexpected error: {e}"
                );
                break;
            }
        }
    }
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
            for _ in &ids {
                barrier.arrive();
            }
            source.commit(&[]).expect("commit");
            source.flush_commits().expect("flush");
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
    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, 5, 1, "e");

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
    let rows = drain_lane(&mut lanes[0], 5);
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
/// `DEADLINE` sits above the mock broker's rebalance pacing
/// (`session.timeout.ms - 1000`, so 5s with the 6000 this file's `config`
/// sets), since the gap between a revoke and the assignment that follows it
/// is time the member legitimately holds nothing. The test costs the
/// deadline plus the settle, so the margin over that 5s is what it can
/// afford to trade for wall-clock.
#[test]
fn an_empty_member_survives_the_assignment_deadline() {
    const DEADLINE: Duration = Duration::from_secs(8);

    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();

    let member = |group: &str| {
        let mut cfg = config(&brokers, group);
        cfg.assignment_timeout = DEADLINE;
        KafkaSource::new(cfg)
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
    let until = Instant::now() + DEADLINE + Duration::from_secs(2);
    while Instant::now() < until {
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
    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(TOPIC, 2, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, 5, 2, "stats");

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
        let _lanes = await_assignment(&mut source);

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
    let cluster = MockCluster::new(1).expect("mock cluster");
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
        let _lanes = await_assignment(&mut source);
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

    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster
        .create_topic(TOPIC, PARTITIONS, 1)
        .expect("create topic");
    let brokers = cluster.bootstrap_servers();
    produce(&brokers, PRODUCED, PARTITIONS, "lag");

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

        let mut lanes = await_assignment(&mut source);
        assert_eq!(lanes.len(), PARTITIONS as usize, "one lane per partition");
        let partitions: Vec<PartitionId> = lanes.iter().map(SourceLane::partition).collect();
        cp.begin_epoch(&partitions, 1);

        // Consume a prefix and commit it: `consumer_lag` is
        // `(hi_offset or ls_offset) - committed_offset`, so it stays `-1`,
        // unknown and correctly unpublished, until a commit lands.
        for lane in &mut lanes {
            let rows = drain_lane(lane, CONSUMED);
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
        let needle = format!(
            r#"spate_source_lag_records{{pipeline="lag",component="source",component_type="kafka",partition="{}"}} "#,
            partition.0
        );
        let line = rendered
            .lines()
            .find(|l| l.starts_with(&needle))
            .unwrap_or_else(|| panic!("no lag series for partition {partition:?}:\n{rendered}"));
        let value: f64 = line
            .rsplit(' ')
            .next()
            .expect("value")
            .parse()
            .expect("numeric lag");
        assert!(
            value > 0.0,
            "a backlogged partition must not report zero lag: {line}"
        );
        assert_eq!(
            value, expected,
            "partition {partition:?} lag must equal produced - committed: {line}"
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
