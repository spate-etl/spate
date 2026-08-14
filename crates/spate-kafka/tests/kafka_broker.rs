//! Integration tests against a real Kafka broker (Docker via
//! testcontainers). Run explicitly:
//!
//! ```sh
//! cargo test -p spate-kafka --test kafka_broker -- --ignored
//! ```

use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use spate_core::checkpoint::Checkpointer;
use spate_core::source::{PayloadBatch, Source, SourceCtx, SourceEvent, SourceLane};
use spate_kafka::{KafkaSource, KafkaSourceConfig};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use testcontainers::runners::SyncRunner;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

const TOPIC: &str = "orders";

fn config(brokers: &str, group: &str) -> KafkaSourceConfig {
    KafkaSourceConfig {
        brokers: brokers.to_string(),
        topic: TOPIC.to_string(),
        group_id: group.to_string(),
        commit_interval: Duration::from_millis(200),
        startup_timeout: Duration::from_secs(60),
        statistics_interval: Duration::from_secs(1),
        // Tests produce before the consumer joins; without this, librdkafka's
        // `latest` default correctly delivers nothing (see mock_cluster.rs).
        rdkafka: BTreeMap::from([("auto.offset.reset".to_string(), "earliest".to_string())]),
    }
}

#[test]
#[ignore = "requires Docker"]
fn real_broker_full_lifecycle() {
    let container = Kafka::default().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");

    // Auto-created topic has a single partition, which is enough for the
    // lifecycle; the multi-partition rebalance path is covered by the
    // MockCluster suite.
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "30000")
        .create()
        .expect("producer");
    for i in 0..100 {
        let payload = format!("real-{i}");
        producer
            .send(
                BaseRecord::to(TOPIC)
                    .payload(payload.as_bytes())
                    .key(format!("k{i}").as_bytes())
                    .partition(0),
            )
            .expect("enqueue");
    }
    producer.flush(Duration::from_secs(30)).expect("flush");

    let mut cp = Checkpointer::new();
    let mut source = KafkaSource::new(config(&brokers, "real-life"));
    source.open(SourceCtx::new(cp.handle())).expect("open");

    // Await assignment.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut lanes = loop {
        assert!(Instant::now() < deadline, "no assignment");
        if let SourceEvent::LanesAssigned(lanes) = source
            .poll_events(Duration::from_millis(200))
            .expect("poll_events")
        {
            break lanes;
        }
    };
    assert_eq!(lanes.len(), 1);
    cp.begin_epoch(&[lanes[0].partition()], 1);

    // Drain all 100 records.
    let mut got = 0usize;
    let deadline = Instant::now() + Duration::from_secs(60);
    while got < 100 {
        assert!(Instant::now() < deadline, "drained {got}/100");
        if let Some(mut batch) = lanes[0]
            .poll(64, Duration::from_millis(500))
            .expect("lane poll")
        {
            while let Some(raw) = batch.next_payload() {
                assert!(raw.bytes.starts_with(b"real-"));
                got += 1;
            }
        }
    }
    drop(lanes);

    // Ack → watermark → store → sync commit.
    cp.drain();
    let watermarks = cp.take_watermarks();
    assert_eq!(watermarks.len(), 1);
    assert_eq!(watermarks[0].1, 100);
    source.commit(&watermarks).expect("store");
    source.flush_commits().expect("sync commit");

    // The group's committed offset is visible to a probe member.
    let probe: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "real-life")
        .create()
        .expect("probe");
    let mut tpl = rdkafka::TopicPartitionList::new();
    tpl.add_partition(TOPIC, 0);
    let committed = probe
        .committed_offsets(tpl, Duration::from_secs(10))
        .expect("committed");
    assert_eq!(
        committed.elements()[0].offset(),
        rdkafka::Offset::Offset(100)
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
fn real_broker_revocation_commit_persists_revoked_offsets() {
    let container = Kafka::default().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");

    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "30000")
        .create()
        .expect("producer");
    for i in 0..50 {
        producer
            .send(
                BaseRecord::to(TOPIC)
                    .payload(format!("rev-{i}").as_bytes())
                    .key(format!("k{i}").as_bytes())
                    .partition(0),
            )
            .expect("enqueue");
    }
    producer.flush(Duration::from_secs(30)).expect("flush");

    let mut cp = Checkpointer::new();
    let mut source = KafkaSource::new(config(&brokers, "rev-commit"));
    source.open(SourceCtx::new(cp.handle())).expect("open");

    // Await assignment and drain all 50 records, producing a watermark at
    // offset 50, but do not commit yet (the inter-tick window).
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut lanes = loop {
        assert!(Instant::now() < deadline, "no assignment");
        if let SourceEvent::LanesAssigned(lanes) = source
            .poll_events(Duration::from_millis(200))
            .expect("poll_events")
        {
            break lanes;
        }
    };
    assert_eq!(lanes.len(), 1);
    cp.begin_epoch(&[lanes[0].partition()], 1);
    let mut drained = 0usize;
    let deadline = Instant::now() + Duration::from_secs(60);
    while drained < 50 {
        assert!(Instant::now() < deadline, "drained {drained}/50");
        if let Some(mut batch) = lanes[0].poll(64, Duration::from_millis(500)).expect("poll") {
            while batch.next_payload().is_some() {
                drained += 1;
            }
        }
    }
    drop(lanes);
    cp.drain();
    let watermarks = cp.take_watermarks();
    assert_eq!(watermarks, vec![(watermarks[0].0, 50)]);

    // A second member joins, forcing an eager revoke.
    let brokers2 = brokers.clone();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop2 = std::sync::Arc::clone(&stop);
    let joiner = std::thread::spawn(move || {
        let c: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers2)
            .set("group.id", "rev-commit")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("second consumer");
        c.subscribe(&[TOPIC]).expect("subscribe");
        while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = c.poll(Duration::from_millis(100));
        }
    });

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
            for _ in &ids {
                barrier.arrive();
            }
            source.commit(&watermarks).expect("store revoked offsets");
            let _ = source.flush_commits();
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
    let probe: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "rev-commit")
        .create()
        .expect("probe");
    spate_test::wait_until(
        Duration::from_secs(30),
        "revoked partition committed offset 50",
        || {
            let mut tpl = rdkafka::TopicPartitionList::new();
            tpl.add_partition(TOPIC, 0);
            let committed = probe
                .committed_offsets(tpl, Duration::from_secs(10))
                .expect("committed");
            committed.elements()[0].offset() == rdkafka::Offset::Offset(50)
        },
    );

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
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

    let container = Kafka::default().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");

    // Explicit multi-partition topic: auto-creation yields a single partition,
    // which would not exercise the aggregate at all.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
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

    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "30000")
        .create()
        .expect("producer");
    for p in 0..PARTITIONS {
        for i in 0..PER_PARTITION {
            producer
                .send(
                    BaseRecord::to(TOPIC)
                        .payload(format!("backlog-p{p}-{i}").as_bytes())
                        .key(format!("k{p}-{i}").as_bytes())
                        .partition(p),
                )
                .expect("enqueue");
        }
    }
    producer.flush(Duration::from_secs(60)).expect("flush");
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
        let mut lanes = loop {
            assert!(Instant::now() < deadline, "no assignment");
            if let SourceEvent::LanesAssigned(lanes) = source
                .poll_events(Duration::from_millis(200))
                .expect("poll_events")
            {
                break lanes;
            }
        };
        assert_eq!(lanes.len(), PARTITIONS as usize, "one lane per partition");
        let partitions: Vec<_> = lanes.iter().map(SourceLane::partition).collect();
        cp.begin_epoch(&partitions, 1);

        // Consume a small prefix so a commit lands: `consumer_lag` is measured
        // against the committed offset and stays unknown (`-1`) until then.
        for lane in &mut lanes {
            let mut seen = 0usize;
            while seen < CONSUME_PER_LANE {
                assert!(Instant::now() < deadline, "lane starved");
                if let Some(mut batch) = lane
                    .poll(64, Duration::from_millis(500))
                    .expect("lane poll")
                {
                    while batch.next_payload().is_some() {
                        seen += 1;
                    }
                }
            }
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
