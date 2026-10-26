//! The encode rig both the counted tier and the allocation snapshot drive:
//! one encoder, the records it encodes, and the buffer it writes into.
//!
//! Included with `#[path]`: a bench target is its own crate, so two targets
//! measure one region only by compiling the same source. Each includer also
//! declares `mod rows` from `support/rows.rs`.

use bytes::BytesMut;
use serde::Serialize;
use spate_clickhouse::{ClickHouseEncoder, NativeEncoder, NativeSchema};
use spate_core::checkpoint::AckRef;
use spate_core::deser::Owned;
use spate_core::record::{PartitionId, Record, RecordMeta};
use spate_core::sink::RowEncoder;
use std::hint::black_box;

use crate::rows::{self, EventRow, ExoticRow, MetricRow, ROWS};

/// A record carrying one row. The ack receiver is leaked rather than dropped
/// so that resolving a batch cannot enqueue on a live channel and make the
/// count depend on how many records a case built. Nothing here resolves a
/// batch anyway, since the encoders never touch the ack, and both the rows
/// and the receiver are built in setup, outside the collected region.
fn record<T>(payload: T) -> Record<T> {
    let (ack, rx) = AckRef::test_pair();
    std::mem::forget(rx);
    Record {
        payload,
        meta: RecordMeta {
            partition: PartitionId(0),
            offset: 0,
            event_time_ms: 0,
            key_hash: None,
        },
        ack,
    }
}

/// One encoder with the rows it encodes and a buffer sized to hold the result,
/// so the measurement is the encoder's work rather than the allocator growing
/// a `BytesMut` under it.
pub(crate) struct Rig<E, T> {
    enc: E,
    records: Vec<Record<T>>,
    buf: BytesMut,
}

/// The measured work: a whole chunk through one encoder.
pub(crate) fn encode_chunk<E, T>(rig: &mut Rig<E, T>)
where
    E: RowEncoder<Owned<T>>,
    T: Send + 'static,
{
    rig.buf.clear();
    for rec in &rig.records {
        rig.enc.encode(black_box(rec), &mut rig.buf).unwrap();
    }
    rig.enc.finish_chunk(&mut rig.buf).unwrap();
    assert!(!rig.buf.is_empty(), "encoder produced no bytes");
}

fn native_rig<T: Serialize + Send + 'static>(
    cols: &[(&str, &str)],
    payloads: Vec<T>,
) -> Rig<NativeEncoder<Owned<T>>, T> {
    let schema = NativeSchema::from_columns(cols).expect("native schema builds");
    Rig {
        enc: NativeEncoder::<Owned<T>>::new(schema),
        records: payloads.into_iter().map(record).collect(),
        buf: BytesMut::with_capacity(1 << 20),
    }
}

fn rowbinary_rig<T: Serialize + Send + 'static>(
    payloads: Vec<T>,
) -> Rig<ClickHouseEncoder<Owned<T>>, T> {
    Rig {
        enc: ClickHouseEncoder::<Owned<T>>::unchecked(),
        records: payloads.into_iter().map(record).collect(),
        buf: BytesMut::with_capacity(1 << 20),
    }
}

pub(crate) fn native_events() -> Rig<NativeEncoder<Owned<EventRow>>, EventRow> {
    native_rig(&rows::event_columns(), rows::events(ROWS))
}

pub(crate) fn native_metrics() -> Rig<NativeEncoder<Owned<MetricRow>>, MetricRow> {
    native_rig(&rows::metric_columns(), rows::metrics(ROWS))
}

pub(crate) fn native_exotic() -> Rig<NativeEncoder<Owned<ExoticRow>>, ExoticRow> {
    native_rig(&rows::exotic_columns(), rows::exotic(ROWS))
}

pub(crate) fn rowbinary_events() -> Rig<ClickHouseEncoder<Owned<EventRow>>, EventRow> {
    rowbinary_rig(rows::events(ROWS))
}

pub(crate) fn rowbinary_metrics() -> Rig<ClickHouseEncoder<Owned<MetricRow>>, MetricRow> {
    rowbinary_rig(rows::metrics(ROWS))
}
