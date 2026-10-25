//! The bench corpora are reproducible, and frame the way the benches claim.
//!
//! An instruction count only means something if both legs of a comparison ran
//! on byte-identical input, so "the corpus is a pure function of nothing" is a
//! property worth a test rather than an assumption. The benches themselves
//! cannot carry it: they need Linux, valgrind and a matching runner. This runs
//! everywhere `cargo test` does.

use spate_json::NdjsonFramer;
use spate_s3::Compression;
use spate_s3::bench_seams::{MakeFramer, frame_objects, frame_range};
use std::sync::Arc;

#[path = "../benches/support/descriptors.rs"]
mod descriptors;
#[path = "../benches/support/ndjson.rs"]
mod ndjson;

fn framer() -> MakeFramer {
    Arc::new(|| Box::new(NdjsonFramer::new(ndjson::MAX_RECORD_BYTES)))
}

#[test]
fn the_descriptor_corpora_are_reproducible() {
    assert_eq!(
        descriptors::single_member_plan(),
        descriptors::single_member_plan()
    );
}

/// Two calls in one process only prove the generator is pure; the benches
/// need the corpus to be identical *across revisions*, since the merge-base
/// leg and the head leg run different builds. Document count and total
/// encoded length pin that between them: any edit to a member count, a field
/// width or the document shape moves one or both. An edit that changed only
/// the *values*, holding every width, would slip past. That is the limit of
/// the pin rather than a hole in it, because what these benches spend is a
/// function of the bytes' length and layout, not of which hex digits are in
/// them.
#[test]
fn the_descriptor_corpus_is_pinned_across_revisions() {
    let plan = descriptors::single_member_plan();
    assert_eq!(
        plan.len(),
        descriptors::MEMBERS,
        "the corpus encoded a different document count"
    );
    let total: usize = plan.iter().map(Vec::len).sum();
    assert_eq!(
        total, 1_139_200,
        "the corpus encodes to {total} bytes, not 1,139,200 — if that is intended, \
         the comparison it anchors has been re-baselined and every recorded \
         count for this bench is against a different corpus"
    );
}

#[test]
fn every_codec_frames_the_whole_body() {
    let body = ndjson::whole_body();
    let expect = body.iter().filter(|&&b| b == b'\n').count();

    for (compression, suffix, stored) in [
        (Compression::Auto, "", body.clone()),
        (Compression::Auto, ".gz", ndjson::gzip(&body)),
        (Compression::Auto, ".zst", ndjson::zstd(&body)),
    ] {
        let objects = vec![(
            format!("part-000000.ndjson{suffix}"),
            ndjson::chunks(&stored),
        )];
        let records = frame_objects(compression, framer(), &objects).expect("frames cleanly");
        assert_eq!(records, expect, "{compression:?} framed a different count");
    }
}

/// The multi-part cases charge per-stream decoder work, and a fixture that
/// encoded one stream would measure what the `_whole` cases already do while
/// looking like a distinct profile. Their claim is structural (every part
/// carries its codec's own start-of-stream magic), so it is asserted
/// structurally rather than inferred from a byte count.
#[test]
fn the_multi_part_objects_are_really_multi_part() {
    /// gzip member header: the two-byte magic, then the deflate method.
    const GZIP_MAGIC: &[u8] = &[0x1f, 0x8b, 0x08];
    /// zstd frame magic, 0xFD2FB528 little-endian.
    const ZSTD_MAGIC: &[u8] = &[0x28, 0xb5, 0x2f, 0xfd];

    for (name, parts, magic) in [
        ("gzip", ndjson::members(ndjson::gzip), GZIP_MAGIC),
        ("zstd", ndjson::members(ndjson::zstd), ZSTD_MAGIC),
    ] {
        assert_eq!(
            parts.len(),
            ndjson::MEMBERS,
            "{name} encoded a different number of parts"
        );
        for (i, part) in parts.iter().enumerate() {
            assert!(
                part.starts_with(magic),
                "{name} part {i} does not begin a new stream, so the object is \
                 not the multi-stream shape the case is named for"
            );
        }
    }
}

/// Every part must be read: a decoder that stopped at the first would drop
/// fifteen sixteenths of the object, and the bench would report that as a
/// welcome fall in the count.
#[test]
fn a_multi_part_object_frames_every_record() {
    for (name, suffix, stored) in [
        ("gzip", ".gz", ndjson::concatenated(ndjson::gzip)),
        ("zstd", ".zst", ndjson::concatenated(ndjson::zstd)),
    ] {
        let objects = vec![(
            format!("part-000000.ndjson{suffix}"),
            ndjson::chunks(&stored),
        )];
        let records = frame_objects(Compression::Auto, framer(), &objects).expect("frames cleanly");
        assert_eq!(
            records,
            ndjson::RECORDS,
            "{name} framed {records} records, not the whole body's {}",
            ndjson::RECORDS
        );
    }
}

/// The mid-object entry point must land *inside* a record, or the bench case
/// is silently measuring an aligned read and the contract it pins is
/// untested.
#[test]
fn the_mid_offset_entry_lands_inside_a_record() {
    let body = ndjson::whole_body();
    let at = ndjson::offset_inside_a_record(&body, body.len() / 2);
    assert_ne!(body[at], b'\n', "the offset sits on a delimiter");
    assert_ne!(
        body[at - 1],
        b'\n',
        "the offset sits at the start of a record, not inside one"
    );
}

/// The framer frames whatever bytes it is given: entering part-way through a
/// record, it emits the leading partial line *as a record*.
///
/// Asserted here as well as inside the bench, which does not run under
/// `cargo test`.
#[test]
fn entering_mid_record_still_counts_the_leading_partial_line() {
    let body = ndjson::whole_body();
    let at = ndjson::offset_inside_a_record(&body, body.len() / 2);
    let tail = &body[at..];

    let complete = tail.iter().filter(|&&b| b == b'\n').count();
    let records = frame_objects(
        Compression::Auto,
        framer(),
        &[("part-000000.ndjson".to_owned(), ndjson::chunks(tail))],
    )
    .expect("frames cleanly");

    assert_eq!(
        records,
        complete + usize::from(!tail.ends_with(b"\n")),
        "the leading partial line is no longer counted as a record; if that \
         is intended, this is the contract being changed and the framing \
         bench's expectation moves with it"
    );
}

/// A byte-range split entering at the same offset owns only the records that
/// start inside its range, so it frames one record fewer than the raw tail:
/// the partial line belongs to the range before it.
#[test]
fn a_range_entered_mid_record_drops_the_leading_partial_line() {
    let body = ndjson::whole_body();
    let at = ndjson::offset_inside_a_record(&body, body.len() / 2);
    let complete = body[at..].iter().filter(|&&b| b == b'\n').count();

    let records = frame_range(
        framer(),
        &body,
        at as u64,
        body.len() as u64,
        b'\n',
        ndjson::RANGE_BYTES,
        ndjson::CHUNK_BYTES,
    )
    .expect("frames cleanly");

    assert_eq!(records, complete - 1);
}

#[test]
fn a_run_of_objects_frames_every_record() {
    let objects: Vec<_> = (0..ndjson::RUN_OBJECTS)
        .map(|i| {
            let body = ndjson::body(i * ndjson::RUN_RECORDS_EACH, ndjson::RUN_RECORDS_EACH);
            (format!("part-{i:06}.ndjson"), ndjson::chunks(&body))
        })
        .collect();
    let records = frame_objects(Compression::Auto, framer(), &objects).expect("frames cleanly");
    assert_eq!(records, ndjson::RUN_OBJECTS * ndjson::RUN_RECORDS_EACH);
}
