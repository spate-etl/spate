//! Wall time for this crate's object framing.
//!
//! The wall-clock half of what `framing_gungraun.rs` counts. Every case drives
//! a rig from `support/framing_rig.rs` through the drive the counted tier and
//! the allocation snapshot also call, so a counted regression and a wall-clock
//! one are statements about one region. `framing_gungraun.rs` gives the full
//! account of each rig.
//!
//! Framed cases:
//!
//! - `frame_plain_whole` — one NDJSON object of about 4 MiB in fetch-sized
//!   chunks. The per-byte cost of framing alone, and the denominator for the
//!   rest.
//! - `frame_gzip_whole`, `frame_zstd_whole` — the same body stored compressed.
//!   Bodies are compressed in the fixture, so these time decompression and
//!   framing.
//! - `frame_gzip_multi_member`, `frame_zstd_multi_frame` — the same body
//!   stored as sixteen streams in one object. Each adds the per-stream decoder
//!   work to its `_whole` case.
//! - `frame_plain_range` — the second half of the object read as a byte-range
//!   split through `frame_range`. It also times the trim and one copy per GET
//!   window, the copy standing in for the fetch.
//!
//! Decoder floors, one per compressed case: `frame_floor_gzip_whole`,
//! `frame_floor_zstd_whole`, `frame_floor_gzip_multi_member` and
//! `frame_floor_zstd_multi_frame`. Each decodes the same stored bytes through
//! the decoder `framer.rs` builds, with no framer. A change in flate2 or zstd
//! moves a floor and its framed case together. A change in this crate moves
//! the framed case alone.
//!
//! Two counted cases are left out. By the counted tier's instruction counts,
//! `plain_mid_offset` costs the same per byte as `plain_whole`, and
//! `plain_many_small` adds about 2% per record. Both are below the driver's
//! 5% floor. Two kept cases sit near it. `frame_plain_range` is the only case
//! on `frame_range`, and its trim and window copy add about 4% per byte on
//! x86_64 and 5% on aarch64. The gzip multi-member term clears 5% on x86_64
//! only, and the zstd one clears it on both.
//!
//! Framing runs through `spate-json`'s `NdjsonFramer`, a dev-dependency, so a
//! change to that framer shows up here too.
//!
//! Each case's byte total is the stored input it reads, compressed where the
//! codec applies, because the codecs move different amounts of input for the
//! same records.
//!
//! Run it with `cargo xtask bench ab --ref main --package spate-s3 --filter frame_`.

use spate_bench::{Corpus, Suite, bench_main};

#[path = "support/framing_rig.rs"]
mod framing_rig;
#[path = "support/ndjson.rs"]
mod ndjson;

use framing_rig::{Decoder, RangeRig, Rig};

/// Folds everything a drive over `rig` reads into the corpus: the codec
/// setting and each key resolve the decoder, and the cap bounds the framer.
fn absorb_rig(corpus: &mut Corpus, rig: &Rig) {
    corpus.absorb("compression", format!("{:?}", rig.compression).as_bytes());
    corpus.absorb("max_record_bytes", &ndjson::MAX_RECORD_BYTES.to_le_bytes());
    for (key, chunks) in &rig.objects {
        corpus.absorb("key", key.as_bytes());
        for chunk in chunks {
            corpus.absorb("chunk", chunk);
        }
    }
}

/// The stored bytes a rig hands the decoder or framer.
fn stored_bytes(rig: &Rig) -> u64 {
    rig.objects
        .iter()
        .flat_map(|(_, chunks)| chunks)
        .map(|chunk| chunk.len() as u64)
        .sum()
}

/// A framed case over a run of objects.
fn objects(suite: Suite, id: &str, build: fn() -> Rig) -> Suite {
    suite
        .case(
            id,
            move |corpus, _seed| {
                let rig = build();
                absorb_rig(corpus, &rig);
                rig
            },
            |b, rig: &Rig| {
                b.iter(|| framing_rig::frame(rig));
            },
        )
        .items_of(|rig: &Rig| rig.expect_records as u64)
        .bytes_of(stored_bytes)
        .done()
}

/// The decoder floor beside the framed case over the same rig. The digest,
/// the record count and the byte total match that case, so the two report
/// rates over the same denominators.
fn floor(suite: Suite, id: &str, build: fn() -> Rig, decoder: Decoder) -> Suite {
    suite
        .case(
            id,
            move |corpus, _seed| {
                let rig = build();
                absorb_rig(corpus, &rig);
                rig
            },
            move |b, rig: &Rig| {
                b.iter(|| framing_rig::decode(rig, decoder));
            },
        )
        .items_of(|rig: &Rig| rig.expect_records as u64)
        .bytes_of(stored_bytes)
        .done()
}

/// A framed case over one byte range.
fn range(suite: Suite, id: &str, build: fn() -> RangeRig) -> Suite {
    suite
        .case(
            id,
            move |corpus, _seed| {
                let rig = build();
                corpus.absorb("body", &rig.body);
                corpus.absorb("start", &rig.start.to_le_bytes());
                corpus.absorb("delimiter", &[ndjson::DELIMITER]);
                corpus.absorb("range_bytes", &ndjson::RANGE_BYTES.to_le_bytes());
                corpus.absorb("chunk_bytes", &ndjson::CHUNK_BYTES.to_le_bytes());
                corpus.absorb("max_record_bytes", &ndjson::MAX_RECORD_BYTES.to_le_bytes());
                rig
            },
            |b, rig: &RangeRig| {
                b.iter(|| framing_rig::frame_one_range(rig));
            },
        )
        .items_of(|rig: &RangeRig| rig.expect_records as u64)
        .bytes_of(|rig: &RangeRig| rig.body.len() as u64 - rig.start)
        .done()
}

fn suite() -> Suite {
    let suite = spate_bench::suite("spate-s3");
    let suite = objects(suite, "frame_plain_whole", framing_rig::plain_whole);
    let suite = objects(suite, "frame_gzip_whole", framing_rig::gzip_whole);
    let suite = objects(suite, "frame_zstd_whole", framing_rig::zstd_whole);
    let suite = objects(
        suite,
        "frame_gzip_multi_member",
        framing_rig::gzip_multi_member,
    );
    let suite = objects(
        suite,
        "frame_zstd_multi_frame",
        framing_rig::zstd_multi_frame,
    );
    let suite = range(suite, "frame_plain_range", framing_rig::plain_range);
    let suite = floor(
        suite,
        "frame_floor_gzip_whole",
        framing_rig::gzip_whole,
        Decoder::Gzip,
    );
    let suite = floor(
        suite,
        "frame_floor_zstd_whole",
        framing_rig::zstd_whole,
        Decoder::Zstd,
    );
    let suite = floor(
        suite,
        "frame_floor_gzip_multi_member",
        framing_rig::gzip_multi_member,
        Decoder::Gzip,
    );
    floor(
        suite,
        "frame_floor_zstd_multi_frame",
        framing_rig::zstd_multi_frame,
        Decoder::Zstd,
    )
}

bench_main!(suite);
