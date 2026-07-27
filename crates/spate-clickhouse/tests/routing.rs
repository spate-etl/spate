//! Docker-free routing seam test: the real [`DistributedRouter`], minted by
//! [`ClickHouseSink::router`] from a validated weighted config, driven
//! through the framework's actual terminal stage (`chain(...).sink(...)`).
//!
//! The container suite proves placement parity against a live cluster; this
//! test keeps the *wiring* — builder bound, per-record `route_record`
//! dispatch, weighted interval selection, per-shard queue placement — under
//! the default `cargo test` gate.

use bytes::BytesMut;
use spate_clickhouse::config::{self, ClickHouseSinkConfig};
use spate_clickhouse::{DistributedRouter, ShardKey};
use spate_core::backpressure::InflightBudget;
use spate_core::checkpoint::AckRef;
use spate_core::deser::{Deserializer, EmitRecord, Owned};
use spate_core::error::{DeserError, ErrorClass, SinkError};
use spate_core::ops::{ChunkConfig, PushOutcome, chain};
use spate_core::record::{PartitionId, RawPayload, Record};
use spate_core::sink::{EncodedChunk, RowEncoder, shard_queues};
use spate_core::source::PayloadBatch;
use std::sync::Arc;

/// The terminal record type: one sensor name per row.
#[derive(Debug, serde::Serialize)]
struct SensorRow {
    sensor: String,
}

/// Sharding key: the row's own `sensor` field (a fn item — the extractor is
/// higher-ranked over the payload lifetime).
fn sensor_key(row: &SensorRow) -> ShardKey<'_> {
    ShardKey::Str(&row.sensor)
}

/// One record per payload; the payload bytes are the sensor name.
#[derive(Clone)]
struct LineDeser;

impl Deserializer<Owned<SensorRow>> for LineDeser {
    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, SensorRow>,
    ) -> Result<(), DeserError> {
        let sensor = std::str::from_utf8(raw.bytes)
            .map_err(|e| DeserError::Malformed {
                reason: e.to_string(),
            })?
            .to_string();
        let _ = out.emit(Record {
            payload: SensorRow { sensor },
            meta: raw.meta(),
            ack: ack.clone(),
        });
        Ok(())
    }
}

/// The crate's real RowBinary row serializer behind the framework's
/// `RowEncoder` seam.
#[derive(Clone)]
struct RowBinaryEncoder;

impl RowEncoder<Owned<SensorRow>> for RowBinaryEncoder {
    fn encode<'buf>(
        &mut self,
        rec: &Record<SensorRow>,
        buf: &mut BytesMut,
    ) -> Result<(), SinkError> {
        spate_clickhouse::serialize_row(&rec.payload, buf).map_err(|e| SinkError::Client {
            class: ErrorClass::RecordLevel,
            reason: e.to_string(),
        })
    }
}

/// A minimal in-memory [`PayloadBatch`]: one payload per entry, offsets in
/// order, one ack handle covering the batch.
struct VecBatch<'buf> {
    payloads: &'buf [Vec<u8>],
    idx: usize,
    ack: AckRef,
}

impl<'buf> PayloadBatch<'buf> for VecBatch<'buf> {
    fn next_payload(&mut self) -> Option<RawPayload<'buf>> {
        let bytes = self.payloads.get(self.idx)?;
        let offset = self.idx as i64;
        self.idx += 1;
        Some(RawPayload {
            bytes,
            key: None,
            partition: PartitionId(0),
            offset,
            timestamp_ms: 0,
        })
    }

    fn ack(&self) -> &AckRef {
        &self.ack
    }
}

/// Decode the single-`String`-column RowBinary rows in one queue: each row
/// is a one-byte length prefix (all test sensors are short) plus UTF-8.
fn drain_sensors(rx: &mut tokio::sync::mpsc::Receiver<EncodedChunk>) -> Vec<String> {
    let mut out = Vec::new();
    while let Ok(chunk) = rx.try_recv() {
        let mut rest = &chunk.frame[..];
        while !rest.is_empty() {
            let len = rest[0] as usize;
            out.push(String::from_utf8(rest[1..1 + len].to_vec()).expect("utf8 sensor"));
            rest = &rest[1 + len..];
        }
        // These tests play the sink: consuming a chunk stands in for a
        // durable write (an AckSet fails its batches on plain drop).
        chunk.acks.deliver();
    }
    out
}

#[test]
fn sink_minted_router_places_rows_by_payload_key_through_the_terminal_stage() {
    // A real 2-shard *weighted* config — the router comes from the sink,
    // exactly as production wiring mints it.
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(
        r#"
table: t
columns: [sensor]
shards:
  - replicas: ["http://ch-0:8123"]
    weight: 9
  - replicas: ["http://ch-1:8123"]
    weight: 10
"#,
    )
    .expect("config yaml");
    let sink = config::build(cfg).expect("valid sink config");
    let router = sink.router::<Owned<SensorRow>>(sensor_key);

    let (queues, mut rxs) = shard_queues(2, 64);
    let mut c = chain(LineDeser)
        .sink(
            RowBinaryEncoder,
            router.clone(),
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();

    let sensors: Vec<String> = (0..32).map(|i| format!("sensor-{i:02}")).collect();
    let payloads: Vec<Vec<u8>> = sensors.iter().map(|s| s.as_bytes().to_vec()).collect();
    let (ack, _rx) = AckRef::test_pair();
    let mut batch = VecBatch {
        payloads: &payloads,
        idx: 0,
        ack,
    };
    assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
    assert!(matches!(c.flush(), PushOutcome::Done));

    // Placement oracle: the same hash + weight-interval selection the unit
    // vectors pin (and the container suite proves against a live server).
    let expect_shard = |sensor: &str| {
        router.shard_for_hash(DistributedRouter::<Owned<SensorRow>>::hash_key(
            ShardKey::Str(sensor),
        ))
    };

    let mut seen = Vec::new();
    for (shard, rx) in rxs.iter_mut().enumerate() {
        let landed = drain_sensors(rx);
        assert!(
            !landed.is_empty(),
            "deterministic fixture fans out to both shards; shard {shard} is empty"
        );
        for sensor in landed {
            assert_eq!(
                expect_shard(&sensor),
                shard,
                "{sensor} landed on shard {shard}, not its parity shard"
            );
            seen.push(sensor);
        }
    }
    seen.sort();
    assert_eq!(seen, sensors, "every record reaches exactly one shard");
}
