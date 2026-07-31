//! Zero-copy acceptance test for the datum deserializer's flagship shape: one
//! Avro datum carrying a sensor batch (an array of events), decoded
//! **borrowed** by `AvroDatumDeserializer`, exploded with `flat_map`, and
//! encoded by a terminal sink stage — with zero-copy proven by pointer
//! provenance: every emitted event's `&str` must point *into the payload
//! buffer* the source handed the chain.
//!
//! Driven exactly the way a framework user's test drives the spate-test
//! mocks (see spate-test's own contract tests): memory source → lane poll →
//! `push_batch`. Copy this file as the template for your own borrowed
//! pipelines.

use apache_avro::types::Value;
use apache_avro::{Schema, to_avro_datum};
use spate_avro::{AvroDeserializerBuilder, AvroMode, AvroSettings, SchemaSource};
use spate_core::backpressure::InflightBudget;
use spate_core::checkpoint::{AckRef, Checkpointer};
use spate_core::deser::{Deserializer, EmitRecord, RecFamily};
use spate_core::error::{DeserError, SinkError};
use spate_core::ops::{ChunkConfig, Emitter, PushOutcome, chain};
use spate_core::record::{PartitionId, RawPayload, Record};
use spate_core::sink::{RowEncoder, ShardRouter, shard_queues};
use spate_core::source::{LaneId, Source, SourceCtx, SourceEvent, SourceLane};
use spate_test::memory_source;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

const SCHEMA: &str = r#"{"type":"record","name":"SensorBatch","fields":[
  {"name":"sensor","type":"string"},
  {"name":"events","type":{"type":"array","items":
    {"type":"record","name":"Event","fields":[
      {"name":"name","type":"string"},
      {"name":"value","type":"long"}]}}}]}"#;

// ---- the user-side types: borrowed records + their two-line families ------

#[derive(Debug, serde::Deserialize)]
struct SensorBatch<'a> {
    sensor: &'a str,
    events: Vec<Event<'a>>,
}

#[derive(Debug, serde::Deserialize)]
struct Event<'a> {
    name: &'a str,
    value: i64,
}

struct BatchFam;
impl RecFamily for BatchFam {
    type Rec<'buf> = SensorBatch<'buf>;
}

struct EventFam;
impl RecFamily for EventFam {
    type Rec<'buf> = Event<'buf>;
}

/// The `flat_map` explode: move each event out of the decoded batch. The
/// borrows keep pointing at the payload buffer, which outlives the whole
/// synchronous fan-out (a `fn` item, as borrowing families require).
fn explode<'buf>(batch: SensorBatch<'buf>, em: &mut Emitter<'_, EventFam>) {
    assert!(!batch.sensor.is_empty());
    for event in batch.events {
        em.emit(event);
    }
}

// ---- provenance fixtures ---------------------------------------------------

/// Shared payload-buffer address range, recorded by the deserializer tap and
/// checked by the encoder. Written and read inside one synchronous
/// `push_batch` call on one thread, so `Relaxed` suffices.
#[derive(Clone, Default)]
struct BufRange {
    lo: Arc<AtomicUsize>,
    hi: Arc<AtomicUsize>,
}

/// Wraps the datum deserializer to record each payload's buffer address range
/// before delegating.
#[derive(Clone)]
struct RangeTap<D> {
    inner: D,
    range: BufRange,
}

impl<D: Deserializer<BatchFam>> Deserializer<BatchFam> for RangeTap<D> {
    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, SensorBatch<'buf>>,
    ) -> Result<(), DeserError> {
        let ptrs = raw.bytes.as_ptr_range();
        self.range.lo.store(ptrs.start as usize, Ordering::Relaxed);
        self.range.hi.store(ptrs.end as usize, Ordering::Relaxed);
        self.inner.deserialize(raw, ack, out)
    }
}

/// Terminal encoder that asserts pointer provenance for every event, then
/// encodes it length-prefixed so the sealed chunks can be decoded and
/// checked for content.
#[derive(Clone)]
struct ProvenanceEncoder {
    range: BufRange,
    seen: Arc<AtomicUsize>,
}

impl RowEncoder<EventFam> for ProvenanceEncoder {
    fn encode<'buf>(
        &mut self,
        rec: &Record<Event<'buf>>,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), SinkError> {
        let lo = self.range.lo.load(Ordering::Relaxed);
        let hi = self.range.hi.load(Ordering::Relaxed);
        let start = rec.payload.name.as_ptr() as usize;
        let end = start + rec.payload.name.len();
        assert!(
            lo <= start && end <= hi,
            "event name {:?} ({start:#x}..{end:#x}) does not point into the \
             payload buffer ({lo:#x}..{hi:#x}) — the decode copied it",
            rec.payload.name,
        );
        buf.extend_from_slice(&(rec.payload.name.len() as u32).to_le_bytes());
        buf.extend_from_slice(rec.payload.name.as_bytes());
        buf.extend_from_slice(&rec.payload.value.to_le_bytes());
        self.seen.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct ToZero;
impl ShardRouter for ToZero {
    fn route(&self, _m: &spate_core::record::RecordMeta, _n: usize) -> usize {
        0
    }
}

// ---- test data ---------------------------------------------------------------

fn batch_datum(sensor: &str, events: &[(&str, i64)]) -> Vec<u8> {
    let schema = Schema::parse_str(SCHEMA).unwrap();
    let mut rec = apache_avro::types::Record::new(&schema).unwrap();
    rec.put("sensor", sensor);
    rec.put(
        "events",
        Value::Array(
            events
                .iter()
                .map(|(name, value)| {
                    Value::Record(vec![
                        ("name".into(), Value::String((*name).to_string())),
                        ("value".into(), Value::Long(*value)),
                    ])
                })
                .collect(),
        ),
    );
    to_avro_datum(&schema, rec).unwrap()
}

#[test]
fn borrowed_batch_explodes_zero_copy_through_the_chain() {
    const P0: PartitionId = PartitionId(0);
    const L0: LaneId = LaneId(0);
    const EVENTS: &[(&str, i64)] = &[
        ("temperature", 21),
        ("humidity", 63),
        ("pressure", 1013),
        ("co2", 417),
    ];

    // The borrowed datum deserializer, from ordinary settings (raw framing:
    // the whole payload is one datum for the fixed schema).
    let settings = AvroSettings {
        mode: AvroMode::Raw,
        schema: Some(SchemaSource::inline(SCHEMA)),
        ..AvroSettings::default()
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let deser = AvroDeserializerBuilder::from_settings(&settings, rt.handle())
        .unwrap()
        .build_datum::<BatchFam>()
        .unwrap();

    // Chain: borrowed decode → flat_map explode → provenance-checking sink.
    let range = BufRange::default();
    let seen = Arc::new(AtomicUsize::new(0));
    let (queues, mut rxs) = shard_queues(1, 4096);
    let mut chain = chain(RangeTap {
        inner: deser,
        range: range.clone(),
    })
    .flat_map::<EventFam, _>(explode)
    .sink(
        ProvenanceEncoder {
            range,
            seen: Arc::clone(&seen),
        },
        ToZero,
        ChunkConfig::default(),
        queues,
        Arc::new(InflightBudget::new()),
    )
    .build();

    // Drive it the way the runtime drives a source: spate-test memory source,
    // one lane, one pushed payload, lane poll → push_batch.
    let mut cp = Checkpointer::new();
    let (mut source, handle) = memory_source();
    source.open(SourceCtx::new(cp.handle())).unwrap();
    cp.begin_epoch(&[P0], 1);
    handle.assign_lanes(&[(L0, P0)]);
    let mut lanes = match source.poll_events(Duration::from_millis(100)).unwrap() {
        SourceEvent::LanesAssigned(lanes) => lanes,
        other => panic!("expected assignment, got {other:?}"),
    };
    handle.push(P0, None, &batch_datum("sensor-7", EVENTS));

    let mut batch = lanes[0]
        .poll(512, Duration::from_millis(100))
        .unwrap()
        .expect("one batch");
    match chain.push_batch(&mut batch, 0) {
        PushOutcome::Done => {}
        other => panic!("expected Done, got {other:?}"),
    }
    match chain.flush() {
        PushOutcome::Done => {}
        other => panic!("expected flushed chain, got {other:?}"),
    }

    // Every event passed the pointer-provenance assertion inside encode();
    // now check the count and the encoded contents that crossed the seam.
    assert_eq!(seen.load(Ordering::Relaxed), EVENTS.len());
    let mut encoded = Vec::new();
    while let Ok(chunk) = rxs[0].try_recv() {
        encoded.extend_from_slice(&chunk.frame);
    }
    let mut rest = encoded.as_slice();
    let mut decoded = Vec::new();
    while !rest.is_empty() {
        let len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
        let name = std::str::from_utf8(&rest[4..4 + len]).unwrap().to_string();
        let value = i64::from_le_bytes(rest[4 + len..12 + len].try_into().unwrap());
        decoded.push((name, value));
        rest = &rest[12 + len..];
    }
    let expected: Vec<(String, i64)> = EVENTS
        .iter()
        .map(|(name, value)| ((*name).to_string(), *value))
        .collect();
    assert_eq!(decoded, expected);
}
