//! Wall-clock cases for the row encoders.
//!
//! The region `encode_gungraun.rs` counts, timed: a whole chunk of rows
//! through one encoder, then `finish_chunk`, driven through
//! `support/encode_rig.rs` over the corpora of `support/rows.rs`, so both
//! tiers encode identical bytes. Time adds what a count cannot see, such as a
//! cache miss: the Native encoder's column buffers and the RowBinary encoder's
//! single output stream have different locality at block scale.
//!
//! Every schema runs through both encoders. The counted tier leaves RowBinary
//! out for `exotic`, where it would measure serde more than this crate's
//! column writers; as a wall-clock reference for choosing a wire format it is
//! the comparison a deployment makes.
//!
//! The corpus digest folds in each schema's columns and its rows serialized
//! through serde, the same stream the encoders consume.
//!
//! One iteration encodes one chunk, and each case declares the chunk's rows
//! as items, so `records_per_s` reads as rows per second amortised over a
//! block.
//!
//! # Cases
//!
//! `encode_native_events`, `encode_native_metrics`, `encode_native_exotic`,
//! `encode_rowbinary_events`, `encode_rowbinary_metrics` and
//! `encode_rowbinary_exotic`. Within this package, `--filter encode_` selects
//! these six; the counted cases carry the same names without the prefix.
//!
//! Run: `cargo xtask bench ab --ref main --package spate-clickhouse --filter encode_`

use serde::Serialize;
use spate_bench::{Corpus, Suite, bench_main};
use spate_core::deser::Owned;
use spate_core::sink::RowEncoder;
use std::cell::RefCell;

#[path = "support/encode_rig.rs"]
mod encode_rig;
#[path = "support/rows.rs"]
mod rows;

use encode_rig::{
    Rig, encode_chunk, native_events, native_exotic, native_metrics, rowbinary_events,
    rowbinary_exotic, rowbinary_metrics,
};
use rows::ROWS;

/// Folds one schema's columns and rows into the corpus.
fn absorb<T: Serialize>(corpus: &mut Corpus, cols: &[(&str, &str)], rows: &[T]) {
    for (name, ty) in cols {
        corpus.absorb("column", format!("{name} {ty}").as_bytes());
    }
    let rows = serde_yaml::to_string(rows).expect("the rows serialize");
    corpus.absorb("rows", rows.as_bytes());
}

/// A case encoding one chunk per iteration through the rig `make` builds.
fn case<E, T>(
    suite: Suite,
    id: &str,
    cols: fn() -> Vec<(&'static str, &'static str)>,
    rows: fn(usize) -> Vec<T>,
    make: fn() -> Rig<E, T>,
) -> Suite
where
    E: RowEncoder<Owned<T>> + 'static,
    T: Serialize + Send + 'static,
{
    suite
        .case(
            id,
            move |corpus, _seed| {
                absorb(corpus, &cols(), &rows(ROWS));
                RefCell::new(make())
            },
            // The routine gets the setup's state by shared reference, and
            // encoding writes into the rig's buffer.
            |b, rig: &RefCell<Rig<E, T>>| {
                let mut rig = rig.borrow_mut();
                b.iter(|| encode_chunk(&mut rig));
            },
        )
        .items(ROWS as u64)
        .done()
}

fn suite() -> Suite {
    let suite = spate_bench::suite("spate-clickhouse");
    let suite = case(
        suite,
        "encode_native_events",
        rows::event_columns,
        rows::events,
        native_events,
    );
    let suite = case(
        suite,
        "encode_native_metrics",
        rows::metric_columns,
        rows::metrics,
        native_metrics,
    );
    let suite = case(
        suite,
        "encode_native_exotic",
        rows::exotic_columns,
        rows::exotic,
        native_exotic,
    );
    let suite = case(
        suite,
        "encode_rowbinary_events",
        rows::event_columns,
        rows::events,
        rowbinary_events,
    );
    let suite = case(
        suite,
        "encode_rowbinary_metrics",
        rows::metric_columns,
        rows::metrics,
        rowbinary_metrics,
    );
    case(
        suite,
        "encode_rowbinary_exotic",
        rows::exotic_columns,
        rows::exotic,
        rowbinary_exotic,
    )
}

bench_main!(suite);
