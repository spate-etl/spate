//! The Kafka source on a record batch librdkafka cannot read, served by a
//! MockCluster. The batches go out in raw produce requests, because no
//! client writes a message format librdkafka does not know.

mod support;

use spate_core::checkpoint::Checkpointer;
use spate_core::error::ErrorClass;
use spate_core::source::{PayloadBatch, Source, SourceCtx, SourceLane};
use spate_kafka::{KafkaSource, KafkaSourceConfig};
use spate_test::HANG;
use std::collections::BTreeMap;
use std::time::Duration;
use support::MockKafka;
use support::{
    Marker, RawBatch, await_assignment, control_batch, corrupt_batch, offsets_of, produce_batch,
};

const TOPIC: &str = "orders";

/// A record batch holding one record, with the header's `magic`,
/// `LastOffsetDelta` and `RecordCount` as given. The CRC is left zero, which
/// neither the mock broker nor a consumer without `check.crcs` reads.
fn batch(magic: u8, base_offset: i64, last_delta: i32, count: i32, value: &[u8]) -> Vec<u8> {
    RawBatch {
        magic,
        base_offset,
        last_offset_delta: last_delta,
        record_count: count,
        value,
        ..RawBatch::default()
    }
    .encode()
}

fn source_cfg(brokers: &str, group: &str) -> KafkaSourceConfig {
    let mut cfg = KafkaSourceConfig::new(brokers, TOPIC, group);
    cfg.startup_timeout = Duration::from_secs(30);
    cfg.statistics_interval = Duration::from_millis(100);
    cfg.rdkafka = BTreeMap::from([
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("session.timeout.ms".to_string(), "6000".to_string()),
        ("heartbeat.interval.ms".to_string(), "2000".to_string()),
    ]);
    cfg
}

/// Serve `source` until partition 0's fetch queue holds `at_least` ops, so a poll
/// after this finds them without waiting on the fetcher. `pipeline` labels the
/// source's meter.
fn wait_for_fetch_queue(
    source: &mut KafkaSource,
    handle: &metrics_exporter_prometheus::PrometheusHandle,
    pipeline: &str,
    at_least: f64,
) {
    spate_test::wait_until(HANG, "the fetcher runs ahead", || {
        source
            .poll_events(Duration::from_millis(50))
            .expect("poll_events");
        support::fetch_queue_entries(&handle.render(), pipeline) >= at_least
    });
}

/// A batch with an unknown message format that starts its own fetch, read
/// by a lane behind the fetcher, fails the lane before any record past it
/// is delivered, and the watermark stops at the unreadable offset. The
/// reason carries librdkafka's text and says why the batch failed.
/// Regression for #1253.
#[test]
fn an_unreadable_batch_behind_the_fetcher_fails_the_lane() {
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    assert_eq!(
        produce_batch(&brokers, TOPIC, 0, &batch(2, 0, 0, 1, b"good-0")),
        0
    );
    // The mock refuses a first batch whose magic is not 2, so a carrier
    // batch librdkafka skips as already read goes ahead of the magic 3 batch.
    let mut unreadable = batch(2, 0, -1, 1, b"carrier");
    unreadable.extend_from_slice(&batch(3, 1, 0, 1, b"unreadable"));
    assert_eq!(produce_batch(&brokers, TOPIC, 0, &unreadable), 0);
    for i in 2..5 {
        let value = format!("after-{i}");
        assert_eq!(
            produce_batch(&brokers, TOPIC, 0, &batch(2, 0, 0, 1, value.as_bytes())),
            0
        );
    }

    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let mut cp = Checkpointer::new();
        let mut source = KafkaSource::new(source_cfg(&brokers, "unreadable"));
        let meter =
            spate_core::metrics::Meter::with_namespace("kafka", "unreadable", "source", "kafka");
        source
            .open(
                SourceCtx::new(cp.handle())
                    .with_meter(Some(meter))
                    .with_partition_detail(true),
            )
            .expect("open");
        let mut lanes = await_assignment(&mut source, HANG);
        cp.begin_epoch(&[lanes[0].partition()], 1);

        // The fetcher has queued record 0, the error for offset 1 and the
        // records after it, so the error arrives inside the lane's first batch.
        wait_for_fetch_queue(&mut source, &handle, "unreadable", 4.0);

        let mut first = lanes[0]
            .poll(64, Duration::from_secs(5))
            .expect("record 0")
            .expect("a batch");
        let mut offsets = Vec::new();
        while let Some(raw) = first.next_payload() {
            offsets.push(raw.offset);
        }
        assert_eq!(offsets, [0]);
        drop(first);

        match lanes[0].poll(64, Duration::from_secs(5)) {
            Err(e) if e.class() == ErrorClass::Fatal => {
                assert!(e.reason().contains("MagicByte 3"), "{e}");
                assert!(e.reason().contains("cannot read"), "{e}");
            }
            Err(other) => panic!("{other}"),
            Ok(batch) => panic!(
                "delivered past the unreadable batch: {:?}",
                offsets_of(batch)
            ),
        }

        drop(lanes);
        cp.drain();
        assert_eq!(
            cp.take_watermarks(),
            [(spate_core::record::PartitionId(0), 1)]
        );
    });
}

/// An unreadable batch at the end of the partition, read behind record 0,
/// fails the lane's next poll although nothing follows it.
#[test]
fn an_unreadable_batch_at_the_tail_fails_the_next_poll() {
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    assert_eq!(
        produce_batch(&brokers, TOPIC, 0, &batch(2, 0, 0, 1, b"good-0")),
        0
    );
    let mut unreadable = batch(2, 0, -1, 1, b"carrier");
    unreadable.extend_from_slice(&batch(3, 1, 0, 1, b"unreadable"));
    assert_eq!(produce_batch(&brokers, TOPIC, 0, &unreadable), 0);

    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let mut cp = Checkpointer::new();
        let mut source = KafkaSource::new(source_cfg(&brokers, "tail"));
        let meter = spate_core::metrics::Meter::with_namespace("kafka", "tail", "source", "kafka");
        source
            .open(
                SourceCtx::new(cp.handle())
                    .with_meter(Some(meter))
                    .with_partition_detail(true),
            )
            .expect("open");
        let mut lanes = await_assignment(&mut source, HANG);
        cp.begin_epoch(&[lanes[0].partition()], 1);

        // One version barrier, record 0 and the error.
        wait_for_fetch_queue(&mut source, &handle, "tail", 3.0);

        let first = lanes[0].poll(2, Duration::from_secs(10)).expect("record 0");
        assert_eq!(offsets_of(first), Some(vec![0]));
        match lanes[0].poll(2, Duration::from_secs(10)) {
            Err(e) if e.class() == ErrorClass::Fatal => {
                assert!(e.reason().contains("MagicByte 3"), "{e}");
            }
            Err(other) => panic!("{other}"),
            Ok(b) => panic!("no error after the unreadable batch: {:?}", offsets_of(b)),
        }
    });
}

/// A batch in a codec this build does not know, read first in a lane's poll,
/// fails the lane, and the reason names the codec and says why it failed.
#[test]
fn an_unknown_codec_first_in_a_poll_names_the_codec() {
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    let mut unknown = batch(2, 0, 0, 1, b"unknown-codec");
    // The attributes field: codec id 5, which no librdkafka build knows.
    unknown[21..23].copy_from_slice(&5i16.to_be_bytes());
    assert_eq!(produce_batch(&brokers, TOPIC, 0, &unknown), 0);

    let mut cp = Checkpointer::new();
    let mut source = KafkaSource::new(source_cfg(&brokers, "unknown-codec"));
    source.open(SourceCtx::new(cp.handle())).expect("open");
    let mut lanes = await_assignment(&mut source, HANG);
    cp.begin_epoch(&[lanes[0].partition()], 1);

    let mut reason = None;
    spate_test::wait_until(HANG, "the lane fails", || {
        source
            .poll_events(Duration::from_millis(10))
            .expect("poll_events");
        match lanes[0].poll(64, Duration::from_millis(200)) {
            Ok(None) => false,
            Ok(b) => panic!("delivered an unreadable batch: {:?}", offsets_of(b)),
            Err(e) => {
                assert_eq!(e.class(), ErrorClass::Fatal, "{e}");
                reason = Some(e.reason().to_owned());
                true
            }
        }
    });
    let reason = reason.expect("reason");
    assert!(reason.contains("codec 0x5"), "{reason}");
    assert!(reason.contains("cannot read"), "{reason}");
}

/// Pins that a record batch whose CRC does not match is delivered when
/// `check.crcs` is left at librdkafka's default, and the watermark moves past
/// it. The source page states this.
#[test]
fn a_batch_that_fails_its_crc_is_delivered_by_default() {
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    support::produce(&brokers, TOPIC, 1, 1, "good");
    assert_eq!(
        produce_batch(&brokers, TOPIC, 0, &corrupt_batch(b"changed", b"body")),
        0
    );

    let mut cp = Checkpointer::new();
    let mut source = KafkaSource::new(source_cfg(&brokers, "crc-default"));
    source.open(SourceCtx::new(cp.handle())).expect("open");
    let mut lanes = await_assignment(&mut source, HANG);
    cp.begin_epoch(&[lanes[0].partition()], 1);

    let mut delivered = Vec::new();
    spate_test::wait_until(HANG, "both records are delivered", || {
        source
            .poll_events(Duration::from_millis(10))
            .expect("poll_events");
        let batch = lanes[0]
            .poll(64, Duration::from_millis(100))
            .expect("no check, so no error");
        delivered.extend(offsets_of(batch).unwrap_or_default());
        delivered.len() >= 2
    });
    assert_eq!(delivered, [0, 1]);
    drop(lanes);
    cp.drain();
    assert_eq!(
        cp.take_watermarks(),
        [(spate_core::record::PartitionId(0), 2)]
    );
}

/// A marker behind a batch the lane cannot read stays unpassed: the lane delivers
/// the record before it and fails, the poll after that serves the marker, and the
/// watermark stops at that record. The same holds for a batch that fails its CRC.
#[test]
fn a_marker_behind_an_unreadable_batch_leaves_the_commit_alone() {
    let mut unknown_format = batch(2, 0, -1, 1, b"carrier");
    unknown_format.extend_from_slice(&batch(3, 1, 0, 1, b"unreadable"));
    let bad_crc = corrupt_batch(b"lost", b"body");
    for (name, bad, check_crcs) in [("format", unknown_format, false), ("crc", bad_crc, true)] {
        let cluster = MockKafka::new();
        cluster.create_topic(TOPIC, 1, 1).expect("create topic");
        let brokers = cluster.bootstrap_servers();
        support::produce(&brokers, TOPIC, 1, 1, "good");
        assert_eq!(produce_batch(&brokers, TOPIC, 0, &bad), 0);
        assert_eq!(
            produce_batch(&brokers, TOPIC, 0, &control_batch(Marker::Commit)),
            0
        );

        let mut cfg = source_cfg(&brokers, name);
        if check_crcs {
            cfg.rdkafka.insert("check.crcs".into(), "true".into());
        }
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder = metrics::set_default_local_recorder(&recorder);
        let mut cp = Checkpointer::new();
        let mut source = KafkaSource::new(cfg);
        source
            .open(
                SourceCtx::new(cp.handle())
                    .with_meter(Some(spate_core::metrics::Meter::with_namespace(
                        "kafka", name, "source", "kafka",
                    )))
                    .with_partition_detail(true),
            )
            .expect("open");
        let mut lanes = await_assignment(&mut source, HANG);
        cp.begin_epoch(&[lanes[0].partition()], 1);
        // One version barrier, record 0, the error and the marker.
        wait_for_fetch_queue(&mut source, &handle, name, 4.0);

        let mut delivered = Vec::new();
        let mut failed = false;
        spate_test::wait_until(HANG, "the lane fails", || {
            source
                .poll_events(Duration::from_millis(10))
                .expect("poll_events");
            match lanes[0].poll(64, Duration::from_millis(100)) {
                Ok(b) => delivered.extend(offsets_of(b).unwrap_or_default()),
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Fatal, "{name}: {e}");
                    failed = true;
                }
            }
            failed
        });
        assert_eq!(delivered, [0], "{name}");
        // An unknown format is reported once, so this poll serves the marker. A
        // bad CRC is reported again, so this poll fails.
        assert!(
            matches!(
                lanes[0].poll(64, Duration::from_millis(100)),
                Ok(None) | Err(_)
            ),
            "{name}: the marker was passed"
        );
        drop(lanes);
        cp.drain();
        assert_eq!(
            cp.take_watermarks(),
            [(spate_core::record::PartitionId(0), 1)],
            "{name}"
        );
    }
}

/// An error bars the marker behind it, since the marker moves the position past
/// the offset that failed, and the next record lifts the bar. librdkafka queues
/// this error once and fetches past the batch, so with every op queued ahead of
/// the polls, the poll after the error serves the marker. A lane asking for one
/// record meets the error first in its next poll, and one asking for more holds
/// it behind the record's batch. Regression for #1709.
#[test]
fn an_error_bars_the_marker_advance() {
    for max_records in [1, 64] {
        let cluster = MockKafka::new();
        cluster.create_topic(TOPIC, 1, 1).expect("create topic");
        let brokers = cluster.bootstrap_servers();
        let marker = || {
            assert_eq!(
                produce_batch(&brokers, TOPIC, 0, &control_batch(Marker::Commit)),
                0
            );
        };
        assert_eq!(
            produce_batch(&brokers, TOPIC, 0, &batch(2, 0, 0, 1, b"good-0")),
            0
        );
        let mut unreadable = batch(2, 0, -1, 1, b"carrier");
        unreadable.extend_from_slice(&batch(3, 1, 0, 1, b"unreadable"));
        assert_eq!(produce_batch(&brokers, TOPIC, 0, &unreadable), 0);
        marker();

        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            let mut cp = Checkpointer::new();
            let mut source = KafkaSource::new(source_cfg(&brokers, "barred"));
            let meter =
                spate_core::metrics::Meter::with_namespace("kafka", "barred", "source", "kafka");
            source
                .open(
                    SourceCtx::new(cp.handle())
                        .with_meter(Some(meter))
                        .with_partition_detail(true),
                )
                .expect("open");
            let mut lanes = await_assignment(&mut source, HANG);
            cp.begin_epoch(&[lanes[0].partition()], 1);
            let partition = lanes[0].partition();

            // One version barrier, record 0, the error and the marker.
            wait_for_fetch_queue(&mut source, &handle, "barred", 4.0);

            let first = lanes[0]
                .poll(max_records, Duration::from_secs(10))
                .expect("record 0");
            assert_eq!(offsets_of(first), Some(vec![0]));
            assert!(lanes[0].poll(max_records, Duration::from_secs(10)).is_err());
            cp.drain();
            let settled = cp.take_watermarks();
            assert_eq!(settled, [(partition, 1)], "the record before the error");
            source.commit(&settled).expect("store");
            let after = lanes[0]
                .poll(max_records, Duration::from_millis(100))
                .expect("the marker is served");
            assert_eq!(offsets_of(after), None, "the marker was passed");
            cp.drain();
            assert_eq!(cp.take_watermarks(), [], "the marker was passed");

            assert_eq!(
                produce_batch(&brokers, TOPIC, 0, &batch(2, 0, 0, 1, b"later")),
                0
            );
            marker();
            let mut watermarks = Vec::new();
            spate_test::wait_until(HANG, "the marker behind it", || {
                source
                    .poll_events(Duration::from_millis(10))
                    .expect("poll_events");
                let _ = offsets_of(
                    lanes[0]
                        .poll(max_records, Duration::from_millis(100))
                        .expect("poll"),
                );
                cp.drain();
                let moved = cp.take_watermarks();
                source.commit(&moved).expect("store");
                watermarks.extend(moved);
                watermarks.last() == Some(&(partition, 5))
            });
        });
    }
}

/// A partition with no committed offset under `latest` keeps its start across a
/// restart, so a record the lane could not read stops the source again.
/// Regression for #1704.
#[test]
fn a_corrupt_message_before_the_first_commit_stops_again_after_a_restart() {
    let cluster = MockKafka::new();
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    assert_eq!(
        produce_batch(&brokers, TOPIC, 0, &batch(2, 0, 0, 1, b"good-0")),
        0
    );
    let mut cfg = source_cfg(&brokers, "corrupt-start");
    cfg.statistics_interval = Duration::ZERO;
    cfg.rdkafka
        .insert("auto.offset.reset".into(), "latest".into());

    let stops = |cfg: KafkaSourceConfig, produce_after: bool| -> String {
        let cp = Checkpointer::new();
        let mut source = KafkaSource::new(cfg);
        source.open(SourceCtx::new(cp.handle())).expect("open");
        let mut lanes = await_assignment(&mut source, HANG);
        if produce_after {
            let mut unreadable = batch(2, 0, -1, 1, b"carrier");
            unreadable.extend_from_slice(&batch(3, 1, 0, 1, b"unreadable"));
            assert_eq!(produce_batch(&brokers, TOPIC, 0, &unreadable), 0);
        }
        let mut reason = None;
        spate_test::wait_until(HANG, "the lane to fail", || {
            let _ = source.poll_events(Duration::from_millis(10));
            match lanes[0].poll(64, Duration::from_millis(50)) {
                Err(e) if e.class() == ErrorClass::Fatal => {
                    reason = Some(e.reason().to_owned());
                    true
                }
                Err(other) => panic!("{other}"),
                Ok(_) => false,
            }
        });
        reason.expect("a reason")
    };
    assert!(stops(cfg.clone(), true).contains("MagicByte 3"));
    assert!(stops(cfg, false).contains("MagicByte 3"));
}
