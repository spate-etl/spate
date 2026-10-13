//! Wall-clock cases for compressing insert frames.
//!
//! The work `frame_gungraun.rs` counts, timed: the eight frames of
//! `support/frames.rs` through the `clickhouse` client's own codec,
//! `CompressedData::new`, which `InsertFormatted::send` runs for every frame
//! when client compression is on, and through the sink's frame codec from
//! `testing::frame_codec`, with the copy of each block into an exact-size
//! `Bytes` that the terminal stage makes. The sink's codec is made and has
//! coded one frame before the measured region, as on a pipeline thread that
//! has sealed chunks before.
//!
//! One iteration compresses all eight frames, and each case declares eight
//! items, so `records_per_s` reads as frames per second and its inverse is the
//! time per frame. `bytes_per_s` counts the uncompressed frame bytes.
//!
//! # Cases
//!
//! `frame_upstream_lz4`, `frame_upstream_zstd1` and `frame_upstream_zstd3`,
//! then `frame_spate_lz4`, `frame_spate_zstd1` and `frame_spate_zstd3`.
//! Within this package, `--filter frame_` selects these six; the counted
//! cases carry the same names without the prefix.
//!
//! Run: `cargo xtask bench ab --ref main --package spate-clickhouse --filter frame_`

use bytes::Bytes;
use clickhouse::Compression;
use clickhouse::insert_formatted::CompressedData;
use spate_bench::{Suite, bench_main};
use std::hint::black_box;

#[allow(dead_code, reason = "this target compresses the events corpus only")]
#[path = "support/rows.rs"]
mod rows;

#[path = "support/frames.rs"]
mod frames;

use frames::{FRAMES, Frame};

fn case(suite: Suite, id: &str, compression: Compression) -> Suite {
    suite
        .case(
            id,
            |corpus, _seed| {
                let frames = frames::native_frames(FRAMES);
                for f in &frames {
                    corpus.absorb("frame", &f.bytes);
                }
                frames
            },
            move |b, frames: &Vec<Frame>| {
                b.iter(|| {
                    // Each output is dropped inside the region, as `send`
                    // drops it once written. `CompressedData` exposes no
                    // length, so the region returns the frames it compressed.
                    frames
                        .iter()
                        .map(|f| {
                            CompressedData::new(black_box(&f.bytes), compression)
                                .map(|_| 1u64)
                                .expect("compresses")
                        })
                        .sum::<u64>()
                });
            },
        )
        .items(FRAMES as u64)
        .bytes_of(|frames: &Vec<Frame>| frames.iter().map(|f| f.bytes.len() as u64).sum())
        .done()
}

/// A case coding the frames through the sink's codec for `compression`, a
/// `compression` key value.
fn spate_case(suite: Suite, id: &str, compression: &'static str) -> Suite {
    suite
        .case(
            id,
            |corpus, _seed| {
                let frames = frames::native_frames(FRAMES);
                for f in &frames {
                    corpus.absorb("frame", &f.bytes);
                }
                frames
            },
            move |b, frames: &Vec<Frame>| {
                let compression = compression.parse().expect("a compression value");
                let mut codec = spate_clickhouse::testing::frame_codec(compression)
                    .expect("a compressing codec")
                    .make();
                codec.encode_frame(&frames[0].bytes).expect("warms");
                b.iter(|| {
                    // Each copy is dropped inside the region, as the upstream
                    // cases drop their output.
                    frames
                        .iter()
                        .map(|f| {
                            let block = codec.encode_frame(black_box(&f.bytes)).expect("codes");
                            Bytes::copy_from_slice(block).len() as u64
                        })
                        .sum::<u64>()
                });
            },
        )
        .items(FRAMES as u64)
        .bytes_of(|frames: &Vec<Frame>| frames.iter().map(|f| f.bytes.len() as u64).sum())
        .done()
}

fn suite() -> Suite {
    let suite = spate_bench::suite("spate-clickhouse");
    let suite = case(suite, "frame_upstream_lz4", Compression::Lz4);
    let suite = case(suite, "frame_upstream_zstd1", Compression::Zstd(1));
    let suite = case(suite, "frame_upstream_zstd3", Compression::Zstd(3));
    let suite = spate_case(suite, "frame_spate_lz4", "lz4");
    let suite = spate_case(suite, "frame_spate_zstd1", "zstd:1");
    spate_case(suite, "frame_spate_zstd3", "zstd:3")
}

bench_main!(suite);
