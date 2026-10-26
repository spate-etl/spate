//! Steady-state allocation of a schema-checked RowBinary encoder.
//!
//! One test per binary, because `Counting` counts every thread in the process
//! while a case runs.

use bytes::BytesMut;
use clickhouse::test::{Mock, handlers};
use serde::Serialize;
use spate_clickhouse::config::{self, ClickHouseSinkConfig};
use spate_clickhouse::testing::system_columns;
use spate_clickhouse::{ClickHouseEncoder, ClickHouseRow};
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

fn chunk(
    enc: &mut ClickHouseEncoder<Owned<TestRow>>,
    rows: &[Record<TestRow>],
    buf: &mut BytesMut,
) {
    buf.clear();
    for r in rows {
        enc.encode(r, buf).unwrap();
    }
    enc.finish_chunk(buf).unwrap();
}

/// Pins that the first-record schema check runs once per encoder: after one
/// warm chunk, an encoder built from the sink's fetched schema allocates what
/// the unchecked bench encoder allocates.
#[tokio::test(flavor = "current_thread")]
async fn a_checked_rowbinary_encoder_allocates_what_the_unchecked_one_does() {
    let mock = Mock::new();
    mock.add(handlers::provide(system_columns(&[
        ("id", "UInt64", ""),
        ("name", "String", ""),
        ("score", "Nullable(Float64)", ""),
    ])));
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        "table: orders\ncompression: off\nshards:\n  - replicas: [\"{}\"]\n",
        mock.url()
    ))
    .unwrap();
    let sink = config::build(cfg)
        .unwrap()
        .with_row::<Owned<TestRow>>()
        .await
        .unwrap();
    let schema = sink.schema();
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
    let mut checked = ClickHouseEncoder::<Owned<TestRow>>::with_schema(schema);
    let mut unchecked = ClickHouseEncoder::<Owned<TestRow>>::unchecked();
    let c = measure(|| chunk(&mut checked, &rows, &mut buf)).unwrap();
    let u = measure(|| chunk(&mut unchecked, &rows, &mut buf)).unwrap();
    assert_eq!(c, u, "checked {c:?} vs unchecked {u:?}");
}
