//! The Kafka source on a record batch librdkafka cannot read, served by a
//! MockCluster. The batches go out in raw produce requests, because no
//! client writes a message format librdkafka does not know.

mod support;

use rdkafka::mocking::MockCluster;
use spate_core::checkpoint::Checkpointer;
use spate_core::error::{ErrorClass, SourceError};
use spate_core::source::{PayloadBatch, Source, SourceCtx, SourceLane};
use spate_kafka::{KafkaSource, KafkaSourceConfig};
use std::collections::BTreeMap;
use std::io::{Read as _, Write as _};
use std::net::TcpStream;
use std::time::Duration;
use support::await_assignment;

const TOPIC: &str = "orders";

fn zigzag(buf: &mut Vec<u8>, v: i64) {
    let mut z = ((v << 1) ^ (v >> 63)) as u64;
    loop {
        let b = (z & 0x7f) as u8;
        z >>= 7;
        if z == 0 {
            buf.push(b);
            return;
        }
        buf.push(b | 0x80);
    }
}

/// A record batch holding one record, with the header's `magic`,
/// `LastOffsetDelta` and `RecordCount` as given. The CRC is left zero, which
/// neither the mock broker nor a consumer without `check.crcs` reads.
fn batch(magic: u8, base_offset: i64, last_delta: i32, count: i32, value: &[u8]) -> Vec<u8> {
    let mut record = vec![0u8];
    zigzag(&mut record, 0);
    zigzag(&mut record, 0);
    zigzag(&mut record, -1);
    zigzag(&mut record, value.len() as i64);
    record.extend_from_slice(value);
    zigzag(&mut record, 0);
    let mut records = Vec::new();
    zigzag(&mut records, record.len() as i64);
    records.extend_from_slice(&record);

    let mut tail = Vec::new();
    tail.extend_from_slice(&0i16.to_be_bytes()); // attributes
    tail.extend_from_slice(&last_delta.to_be_bytes());
    tail.extend_from_slice(&0i64.to_be_bytes()); // first timestamp
    tail.extend_from_slice(&0i64.to_be_bytes()); // max timestamp
    tail.extend_from_slice(&(-1i64).to_be_bytes()); // producer id
    tail.extend_from_slice(&(-1i16).to_be_bytes()); // producer epoch
    tail.extend_from_slice(&(-1i32).to_be_bytes()); // base sequence
    tail.extend_from_slice(&count.to_be_bytes());
    tail.extend_from_slice(&records);

    let mut b = Vec::new();
    b.extend_from_slice(&base_offset.to_be_bytes());
    b.extend_from_slice(&i32::try_from(4 + 1 + 4 + tail.len()).unwrap().to_be_bytes());
    b.extend_from_slice(&(-1i32).to_be_bytes()); // partition leader epoch
    b.push(magic);
    b.extend_from_slice(&0u32.to_be_bytes()); // crc
    b.extend_from_slice(&tail);
    b
}

/// Send `records` to partition 0 in one Produce v3 request, and return the
/// partition's error code.
fn produce(broker: &str, records: &[u8]) -> i16 {
    let mut b = Vec::new();
    b.extend_from_slice(&0i16.to_be_bytes()); // api key: Produce
    b.extend_from_slice(&3i16.to_be_bytes()); // api version
    b.extend_from_slice(&7i32.to_be_bytes()); // correlation id
    b.extend_from_slice(&5i16.to_be_bytes());
    b.extend_from_slice(b"spate"); // client id
    b.extend_from_slice(&(-1i16).to_be_bytes()); // transactional id
    b.extend_from_slice(&1i16.to_be_bytes()); // acks
    b.extend_from_slice(&10_000i32.to_be_bytes()); // timeout
    b.extend_from_slice(&1i32.to_be_bytes()); // one topic
    b.extend_from_slice(&i16::try_from(TOPIC.len()).unwrap().to_be_bytes());
    b.extend_from_slice(TOPIC.as_bytes());
    b.extend_from_slice(&1i32.to_be_bytes()); // one partition
    b.extend_from_slice(&0i32.to_be_bytes()); // partition 0
    b.extend_from_slice(&i32::try_from(records.len()).unwrap().to_be_bytes());
    b.extend_from_slice(records);

    let mut s = TcpStream::connect(broker).expect("connect");
    s.write_all(&i32::try_from(b.len()).unwrap().to_be_bytes())
        .expect("write");
    s.write_all(&b).expect("write");
    let mut len = [0u8; 4];
    s.read_exact(&mut len).expect("read");
    let mut resp = vec![0u8; usize::try_from(i32::from_be_bytes(len)).unwrap()];
    s.read_exact(&mut resp).expect("read");
    // correlation id, topic count, topic name, partition count, partition id.
    let at = 4 + 4 + 2 + TOPIC.len() + 4 + 4;
    i16::from_be_bytes([resp[at], resp[at + 1]])
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

fn offsets_of(
    batch: Option<<<KafkaSource as Source>::Lane as SourceLane>::Batch<'_>>,
) -> Option<Vec<i64>> {
    batch.map(|mut b| {
        let mut o = Vec::new();
        while let Some(raw) = b.next_payload() {
            o.push(raw.offset);
        }
        o
    })
}

/// A batch with an unknown message format that starts its own fetch, read
/// by a lane behind the fetcher, fails the lane before any record past it
/// is delivered, and the watermark stops at the unreadable offset. The
/// reason carries librdkafka's text and says why the batch failed.
/// Regression for #1253.
#[test]
fn an_unreadable_batch_behind_the_fetcher_fails_the_lane() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    assert_eq!(produce(&brokers, &batch(2, 0, 0, 1, b"good-0")), 0);
    // The mock refuses a first batch whose magic is not 2, so a carrier
    // batch librdkafka skips as already read goes ahead of the magic 3 batch.
    let mut unreadable = batch(2, 0, -1, 1, b"carrier");
    unreadable.extend_from_slice(&batch(3, 1, 0, 1, b"unreadable"));
    assert_eq!(produce(&brokers, &unreadable), 0);
    for i in 2..5 {
        let value = format!("after-{i}");
        assert_eq!(produce(&brokers, &batch(2, 0, 0, 1, value.as_bytes())), 0);
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
        let mut lanes = await_assignment(&mut source, Duration::from_secs(30));
        cp.begin_epoch(&[lanes[0].partition()], 1);

        // The fetcher has queued record 0, the error for offset 1 and the
        // records after it, so the error arrives inside the lane's first batch.
        let queued = r#"spate_kafka_partition_fetch_queue_messages{pipeline="unreadable",component="source",component_type="kafka",partition="0"} "#;
        spate_test::wait_until(Duration::from_secs(30), "the fetcher runs ahead", || {
            source
                .poll_events(Duration::from_millis(50))
                .expect("poll_events");
            handle.render().lines().any(|line| {
                line.strip_prefix(queued)
                    .and_then(|n| n.parse::<f64>().ok())
                    .is_some_and(|n| n >= 4.0)
            })
        });

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
            Err(SourceError::Client {
                class: ErrorClass::Fatal,
                reason,
            }) => {
                assert!(reason.contains("MagicByte 3"), "{reason}");
                assert!(reason.contains("cannot read"), "{reason}");
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
    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    assert_eq!(produce(&brokers, &batch(2, 0, 0, 1, b"good-0")), 0);
    let mut unreadable = batch(2, 0, -1, 1, b"carrier");
    unreadable.extend_from_slice(&batch(3, 1, 0, 1, b"unreadable"));
    assert_eq!(produce(&brokers, &unreadable), 0);

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
        let mut lanes = await_assignment(&mut source, Duration::from_secs(30));
        cp.begin_epoch(&[lanes[0].partition()], 1);

        // One version barrier, record 0 and the error.
        let queued = r#"spate_kafka_partition_fetch_queue_messages{pipeline="tail",component="source",component_type="kafka",partition="0"} "#;
        spate_test::wait_until(Duration::from_secs(30), "the fetcher runs ahead", || {
            source
                .poll_events(Duration::from_millis(50))
                .expect("poll_events");
            handle.render().lines().any(|line| {
                line.strip_prefix(queued)
                    .and_then(|n| n.parse::<f64>().ok())
                    .is_some_and(|n| n >= 3.0)
            })
        });

        let first = lanes[0].poll(2, Duration::from_secs(10)).expect("record 0");
        assert_eq!(offsets_of(first), Some(vec![0]));
        match lanes[0].poll(2, Duration::from_secs(10)) {
            Err(SourceError::Client {
                class: ErrorClass::Fatal,
                reason,
            }) => assert!(reason.contains("MagicByte 3"), "{reason}"),
            Err(other) => panic!("{other}"),
            Ok(b) => panic!("no error after the unreadable batch: {:?}", offsets_of(b)),
        }
    });
}

/// A batch in a codec this build does not know, read first in a lane's poll,
/// fails the lane, and the reason names the codec and says why it failed.
#[test]
fn an_unknown_codec_first_in_a_poll_names_the_codec() {
    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster.create_topic(TOPIC, 1, 1).expect("create topic");
    let brokers = cluster.bootstrap_servers();
    let mut unknown = batch(2, 0, 0, 1, b"unknown-codec");
    // The attributes field: codec id 5, which no librdkafka build knows.
    unknown[21..23].copy_from_slice(&5i16.to_be_bytes());
    assert_eq!(produce(&brokers, &unknown), 0);

    let mut cp = Checkpointer::new();
    let mut source = KafkaSource::new(source_cfg(&brokers, "unknown-codec"));
    source.open(SourceCtx::new(cp.handle())).expect("open");
    let mut lanes = await_assignment(&mut source, Duration::from_secs(30));
    cp.begin_epoch(&[lanes[0].partition()], 1);

    let mut reason = None;
    spate_test::wait_until(Duration::from_secs(30), "the lane fails", || {
        source
            .poll_events(Duration::from_millis(10))
            .expect("poll_events");
        match lanes[0].poll(64, Duration::from_millis(200)) {
            Ok(None) => false,
            Ok(b) => panic!("delivered an unreadable batch: {:?}", offsets_of(b)),
            Err(SourceError::Client { class, reason: r }) => {
                assert_eq!(class, ErrorClass::Fatal, "{r}");
                reason = Some(r);
                true
            }
            Err(other) => panic!("{other}"),
        }
    });
    let reason = reason.expect("reason");
    assert!(reason.contains("codec 0x5"), "{reason}");
    assert!(reason.contains("cannot read"), "{reason}");
}
