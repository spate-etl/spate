//! Integration tests against rdkafka's in-process MockCluster (no Docker).

use etl_core::checkpoint::Checkpointer;
use etl_core::error::{ErrorClass, SourceError};
use etl_core::record::PartitionId;
use etl_core::source::{PayloadBatch, Source, SourceCtx, SourceEvent, SourceLane};
use etl_kafka::{KafkaSource, KafkaSourceConfig};
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

const TOPIC: &str = "orders";

fn config(brokers: &str, group: &str) -> KafkaSourceConfig {
    KafkaSourceConfig {
        brokers: brokers.to_string(),
        topic: TOPIC.to_string(),
        group_id: group.to_string(),
        commit_interval: Duration::from_millis(200),
        startup_timeout: Duration::from_secs(30),
        // Statistics off: deterministic tests.
        statistics_interval: Duration::ZERO,
        // These tests produce BEFORE the consumer joins. librdkafka's
        // default (`latest`) would legitimately deliver nothing — any past
        // "green" run of that shape was a pause-race leaking a message to
        // the main queue, whose rewind seek overrode the reset policy.
        rdkafka: BTreeMap::from([("auto.offset.reset".to_string(), "earliest".to_string())]),
    }
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
    // Batches were dropped in drain_lane: acknowledgements resolve.
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
            // runtime logs and keeps polling on Retryable — so does this
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
