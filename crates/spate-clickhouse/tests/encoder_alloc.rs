//! Steady-state allocation of a schema-checked RowBinary encoder.
//!
//! Its own binary, because the global allocator is the binary's.

use bytes::BytesMut;
use clickhouse::test::{Mock, handlers};
use serde::Serialize;
use spate_clickhouse::ClickHouseRow;
use spate_clickhouse::ClickHouseSinkBuilder;
use spate_clickhouse::config::ClickHouseSinkConfig;
use spate_clickhouse::testing::{
    ClickHouseRowBinaryEncoder, rowbinary_format_encoder, system_columns,
};
use spate_core::deser::Owned;
use spate_core::record::Record;
use spate_core::sink::RowEncoder;
use spate_test::record;
use spate_test_support::alloc::{Counting, measure};

#[global_allocator]
static ALLOC: Counting = Counting;

#[derive(Debug, Clone, Serialize, ClickHouseRow)]
struct TestRow {
    id: u64,
    name: String,
    score: Option<f64>,
}

fn chunk<E: RowEncoder<Owned<TestRow>>>(enc: &mut E, rows: &[Record<TestRow>], buf: &mut BytesMut) {
    buf.clear();
    for r in rows {
        enc.encode(r, buf).unwrap();
    }
    enc.finish_chunk(buf).unwrap();
}

/// Pins that the first-record schema check runs once per encoder: after one
/// warm chunk, the sink's encoder allocates what the unchecked bench encoder
/// allocates.
#[tokio::test(flavor = "current_thread")]
async fn a_checked_rowbinary_encoder_allocates_what_the_unchecked_one_does() {
    let mock = Mock::new();
    mock.add(handlers::provide(system_columns(&[
        ("id", "UInt64", ""),
        ("name", "String", ""),
        ("score", "Nullable(Float64)", ""),
    ])));
    // The settings check that follows the schema read.
    mock.add(handlers::provide::<u8>([1u8]));
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        "table: orders\ncompression: none\nshards:\n  - replicas: [\"{}\"]\n",
        mock.url()
    ))
    .unwrap();
    let sink = ClickHouseSinkBuilder::new(cfg)
        .unwrap()
        .connect::<Owned<TestRow>>()
        .await
        .unwrap();
    let mut checked = sink.encoder::<Owned<TestRow>>();
    drop(sink);
    drop(mock);

    let rows: Vec<Record<TestRow>> = (0..1_000u64)
        .map(|i| {
            record(TestRow {
                id: i,
                name: format!("row-{i}"),
                score: (i % 3 == 0).then_some(i as f64),
            })
        })
        .collect();
    let mut buf = BytesMut::with_capacity(1 << 20);
    let mut unchecked =
        rowbinary_format_encoder(ClickHouseRowBinaryEncoder::<Owned<TestRow>>::unchecked());
    let c = measure(|| chunk(&mut checked, &rows, &mut buf)).unwrap();
    let u = measure(|| chunk(&mut unchecked, &rows, &mut buf)).unwrap();
    assert_eq!(c, u, "checked {c:?} vs unchecked {u:?}");
}
