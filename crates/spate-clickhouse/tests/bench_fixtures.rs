//! Both encoders accept both bench schemas.
//!
//! The Native encoder is driven by a column list written beside the row
//! struct, and a mismatch between the two — a column added without its field,
//! or a type that does not match — is a fatal error raised at encode time.
//! `cargo bench --no-run` cannot catch that: it compiles the bench without
//! running it, so the first sign would be a failed valgrind job minutes into
//! CI. This runs in milliseconds wherever `cargo test` does.

use bytes::BytesMut;
use spate_clickhouse::{ClickHouseEncoder, NativeEncoder, NativeSchema};
use spate_core::checkpoint::AckRef;
use spate_core::deser::Owned;
use spate_core::record::{PartitionId, Record, RecordMeta};
use spate_core::sink::RowEncoder;

#[path = "../benches/support/rows.rs"]
mod rows;

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

fn drive<E, T>(mut enc: E, payloads: Vec<T>) -> usize
where
    E: RowEncoder<Owned<T>>,
    T: Send + 'static,
{
    let mut buf = BytesMut::new();
    for p in payloads {
        enc.encode(&record(p), &mut buf).expect("row encodes");
    }
    enc.finish_chunk(&mut buf).expect("chunk finishes");
    buf.len()
}

#[test]
fn the_native_schemas_match_their_row_structs() {
    let events = NativeSchema::from_columns(&rows::event_columns()).expect("event schema builds");
    let metrics =
        NativeSchema::from_columns(&rows::metric_columns()).expect("metric schema builds");

    assert!(
        drive(
            NativeEncoder::<Owned<_>>::new(events),
            rows::events(rows::ROWS)
        ) > 0
    );
    assert!(
        drive(
            NativeEncoder::<Owned<_>>::new(metrics),
            rows::metrics(rows::ROWS)
        ) > 0
    );
}

#[test]
fn rowbinary_encodes_both_schemas() {
    assert!(
        drive(
            ClickHouseEncoder::<Owned<_>>::new(),
            rows::events(rows::ROWS)
        ) > 0
    );
    assert!(
        drive(
            ClickHouseEncoder::<Owned<_>>::new(),
            rows::metrics(rows::ROWS)
        ) > 0
    );
}

/// The encoded sizes, pinned.
///
/// An instruction count only compares across two legs that encoded identical
/// bytes, and the corpus is generated rather than stored — so a one-character
/// edit to a value formula in `rows.rs` re-baselines every comparison with
/// nothing to say it happened. These four numbers are what makes that edit
/// fail here instead. They are the sizes the pull request that added this
/// bench published, and changing one is a deliberate act: re-record it, and
/// treat every count from before the change as measuring a different corpus.
#[test]
fn the_corpora_are_pinned_across_revisions() {
    let events = NativeSchema::from_columns(&rows::event_columns()).expect("event schema builds");
    let metrics =
        NativeSchema::from_columns(&rows::metric_columns()).expect("metric schema builds");

    assert_eq!(
        drive(
            NativeEncoder::<Owned<_>>::new(events),
            rows::events(rows::ROWS)
        ),
        116_573,
        "native/events"
    );
    assert_eq!(
        drive(
            ClickHouseEncoder::<Owned<_>>::new(),
            rows::events(rows::ROWS)
        ),
        125_738,
        "rowbinary/events"
    );
    assert_eq!(
        drive(
            NativeEncoder::<Owned<_>>::new(metrics),
            rows::metrics(rows::ROWS)
        ),
        92_204,
        "native/metrics"
    );
    assert_eq!(
        drive(
            ClickHouseEncoder::<Owned<_>>::new(),
            rows::metrics(rows::ROWS)
        ),
        92_000,
        "rowbinary/metrics"
    );
}
