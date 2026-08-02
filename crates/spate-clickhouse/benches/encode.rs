//! Criterion wall-clock benchmarks for the row encoders.
//!
//! The same shape the instruction-count bench measures — a whole chunk of
//! rows through one encoder, then `finish_chunk` — over the same corpora,
//! because the two only describe each other if they encode identical bytes.
//! What this adds is time: an instruction count cannot see a cache miss, and
//! the Native encoder's column buffers and the RowBinary encoder's single
//! output stream have very different locality at block scale.
//!
//! Every schema runs through both encoders, which is the arm the counted
//! bench leaves out for `exotic` — there it would measure serde rather than
//! this crate's column writers, but as a wall-clock reference for choosing a
//! wire format it is exactly the comparison a deployment makes.
//!
//! Throughput is declared in rows, so the report reads as time per row
//! amortised over a block rather than time per block.

use bytes::BytesMut;
use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main};
use serde::Serialize;
use spate_clickhouse::{ClickHouseEncoder, NativeEncoder, NativeSchema};
use spate_core::checkpoint::AckRef;
use spate_core::deser::Owned;
use spate_core::record::{PartitionId, Record, RecordMeta};
use spate_core::sink::RowEncoder;

#[path = "support/rows.rs"]
mod rows;

use rows::ROWS;

/// A record carrying one row. The ack receiver is leaked rather than dropped
/// so that resolving a batch cannot enqueue on a live channel; nothing here
/// resolves one, and the encoders never touch the ack.
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

/// Encode a whole chunk into a buffer already large enough to hold it, so the
/// measurement is the encoder's work rather than the allocator growing a
/// `BytesMut` under it.
fn encode_chunk<E, T>(enc: &mut E, records: &[Record<T>], buf: &mut BytesMut)
where
    E: RowEncoder<Owned<T>>,
    T: Send + 'static,
{
    buf.clear();
    for rec in records {
        enc.encode(std::hint::black_box(rec), buf).unwrap();
    }
    enc.finish_chunk(buf).unwrap();
}

/// Both encoders over one schema, as two cases in the group.
fn both_encoders<T: Serialize + Send + 'static>(
    g: &mut BenchmarkGroup<'_, WallTime>,
    schema: &str,
    cols: &[(&str, &str)],
    payloads: Vec<T>,
) {
    let records: Vec<Record<T>> = payloads.into_iter().map(record).collect();
    let mut buf = BytesMut::with_capacity(1 << 20);

    let native = NativeSchema::from_columns(cols).expect("native schema builds");
    let mut enc = NativeEncoder::<Owned<T>>::new(native);
    g.bench_function(format!("native_{schema}"), |b| {
        b.iter(|| encode_chunk(&mut enc, &records, &mut buf));
    });

    let mut enc = ClickHouseEncoder::<Owned<T>>::new();
    g.bench_function(format!("rowbinary_{schema}"), |b| {
        b.iter(|| encode_chunk(&mut enc, &records, &mut buf));
    });
}

fn clickhouse_encode(c: &mut Criterion) {
    let mut g = c.benchmark_group("clickhouse_encode");
    g.throughput(Throughput::Elements(ROWS as u64));
    both_encoders(&mut g, "events", &rows::event_columns(), rows::events(ROWS));
    both_encoders(
        &mut g,
        "metrics",
        &rows::metric_columns(),
        rows::metrics(ROWS),
    );
    both_encoders(
        &mut g,
        "exotic",
        &rows::exotic_columns(),
        rows::exotic(ROWS),
    );
    g.finish();
}

criterion_group!(benches, clickhouse_encode);
criterion_main!(benches);
