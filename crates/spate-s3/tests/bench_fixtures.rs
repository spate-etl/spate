//! The bench corpora are reproducible, and pack and frame the way the benches
//! claim.
//!
//! An instruction count only means something if both legs of a comparison ran
//! on byte-identical input, so "the corpus is a pure function of nothing" is a
//! property worth a test rather than an assumption. The benches themselves
//! cannot carry it: they need Linux, valgrind and a matching runner, and they
//! only run when a pull request selects the bench stage. This runs everywhere
//! `cargo test` does.

use spate_core::coordination::SplitId;
use spate_json::NdjsonFramer;
use spate_s3::bench_seams::{MakeFramer, frame_objects, plan_listing};
use spate_s3::{Compression, SplitDescriptor};
use std::collections::HashSet;
use std::sync::Arc;

#[path = "../benches/support/descriptors.rs"]
mod descriptors;
#[path = "../benches/support/listing.rs"]
mod listing;
#[path = "../benches/support/ndjson.rs"]
mod ndjson;

fn framer() -> MakeFramer {
    Arc::new(|| Box::new(NdjsonFramer::new(ndjson::MAX_RECORD_BYTES)))
}

#[test]
fn the_listing_corpora_are_reproducible() {
    assert_eq!(listing::uniform_small(), listing::uniform_small());
    assert_eq!(listing::big_objects(), listing::big_objects());
    assert_eq!(listing::mixed_tail(), listing::mixed_tail());
    assert_eq!(listing::deep_keys(), listing::deep_keys());
}

/// Two calls in one process only prove the generator is pure. The property
/// the benches need is stronger — that the corpus is the same *across
/// revisions*, since a merge-base leg and a head leg run different builds. A
/// pinned split count is the cheapest witness: any edit to a seed, a size
/// range, a key format or a count moves it, and moving it silently would
/// re-baseline every comparison without anything failing.
#[test]
fn the_corpora_are_pinned_across_revisions() {
    for (name, corpus, want) in [
        ("uniform_small", listing::uniform_small(), 625),
        ("big_objects", listing::big_objects(), 2_000),
        ("mixed_tail", listing::mixed_tail(), 407),
        ("deep_keys", listing::deep_keys(), 63),
    ] {
        let got = plan_listing(corpus, listing::TARGET_BYTES).len();
        assert_eq!(
            got, want,
            "{name} planned {got} splits, not {want} — if that is intended, \
             the comparison it anchors has been re-baselined and every \
             recorded count for this bench is against a different corpus"
        );
    }
}

/// `mixed_tail` exists to drive the open-bin deque to `PACKING_LOOKBACK`, and
/// that property is invisible in a split count — the profile it replaced
/// produced the same 407 splits while leaving the deque holding one bin, so
/// it measured nothing `uniform_small` did not.
///
/// The deque is private, so this asserts the shape that fills it: the large
/// objects must stay *below* the target. One at or above it closes its bin on
/// placement and the scan never runs.
#[test]
fn the_mixed_tail_large_objects_stay_under_the_target() {
    let corpus = listing::mixed_tail();
    let large: Vec<u64> = corpus
        .iter()
        .map(|(_, size, _)| *size)
        .filter(|s| *s > 64 * 1_024)
        .collect();
    assert!(!large.is_empty(), "the profile has no large objects at all");
    for size in large {
        assert!(
            size < listing::TARGET_BYTES,
            "a {size}-byte object is at or above the target, so it closes its \
             bin immediately and the lookback scan is never exercised"
        );
    }
}

/// The source sorts the listing by key before packing, so a corpus generated
/// out of key order is packed in an arrangement production never produces.
#[test]
fn the_corpora_are_generated_in_key_order() {
    for (name, corpus) in [
        ("uniform_small", listing::uniform_small()),
        ("big_objects", listing::big_objects()),
        ("mixed_tail", listing::mixed_tail()),
        ("deep_keys", listing::deep_keys()),
    ] {
        let keys: Vec<&str> = corpus.iter().map(|(k, _, _)| k.as_str()).collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        assert_eq!(keys, sorted, "{name} is not generated in sorted key order");
    }
}

#[test]
fn every_planned_split_has_a_distinct_id() {
    for corpus in [
        listing::uniform_small(),
        listing::big_objects(),
        listing::mixed_tail(),
        listing::deep_keys(),
    ] {
        let ids = plan_listing(corpus, listing::TARGET_BYTES);
        let unique: HashSet<&str> = ids.iter().map(SplitId::as_str).collect();
        assert!(!ids.is_empty());
        assert_eq!(unique.len(), ids.len(), "two splits minted the same id");
    }
}

/// The profiles have to stay *different* from each other, or the parameter is
/// decorative. `big_objects` in particular must keep landing one object per
/// split — that is the behavior a byte-range subdivision change alters, and
/// the reason the profile exists.
#[test]
fn the_profiles_pack_differently() {
    let uniform = listing::uniform_small();
    let big = listing::big_objects();

    let uniform_splits = plan_listing(uniform.clone(), listing::TARGET_BYTES).len();
    let big_splits = plan_listing(big.clone(), listing::TARGET_BYTES).len();

    assert_eq!(
        big_splits,
        big.len(),
        "an object at or above the target should close a bin on its own"
    );
    assert!(
        uniform_splits * 4 < uniform.len(),
        "small objects should share bins, got {uniform_splits} splits for {} objects",
        uniform.len()
    );
}

/// `deep_keys` exists to move key length and nothing else. If it drifted into
/// varying the sizes or the ETags too, its count would no longer attribute to
/// key length, and the profile would be measuring an unlabeled mixture.
#[test]
fn the_deep_key_profile_varies_only_the_key() {
    let deep = listing::deep_keys();
    let shallow = listing::uniform_small();
    assert!(
        deep.len() * 10 == shallow.len(),
        "deep_keys is meant to be uniform_small's first tenth, got {} against {}",
        deep.len(),
        shallow.len()
    );
    for (i, ((_, deep_size, deep_etag), (_, size, etag))) in
        deep.iter().zip(shallow.iter()).enumerate()
    {
        assert_eq!(deep_size, size, "object {i} draws a different size");
        assert_eq!(deep_etag, etag, "object {i} draws a different ETag");
    }
}

/// The deep keys have to actually be deep, and have to stay inside what an
/// object store will accept: a profile whose keys quietly shrank would report
/// a smaller count that read as an improvement in the code under measurement.
#[test]
fn the_deep_keys_sit_just_under_the_key_limit() {
    /// The object store's own cap on a key.
    const STORE_LIMIT: usize = 1024;

    for (key, _, _) in listing::deep_keys() {
        assert_eq!(
            key.len(),
            listing::DEEP_KEY_BYTES,
            "a deep key is {} bytes, not the {} the profile is built around: {key}",
            key.len(),
            listing::DEEP_KEY_BYTES
        );
        assert!(
            key.len() <= STORE_LIMIT,
            "a deep key exceeds the store's cap"
        );
    }
    let shallow = listing::uniform_small()[0].0.len();
    assert!(
        listing::DEEP_KEY_BYTES > shallow * 10,
        "deep keys ({}) are not meaningfully longer than ordinary ones ({shallow})",
        listing::DEEP_KEY_BYTES
    );
}

/// The descriptor corpora group members by a plain chunking because `pack` is
/// crate-private. This is what keeps that from being a guess: the real
/// planner packs a listing of uniformly small objects into exactly
/// `objects / MEMBERS_PER_SPLIT` splits, so a change to the open-cost floor —
/// the thing that caps members per split — fails here rather than leaving the
/// descriptor fixture describing a shape the planner no longer produces.
#[test]
fn the_chunked_grouping_matches_the_real_packer() {
    let corpus = listing::uniform_small();
    let objects = corpus.len();
    let splits = plan_listing(corpus, listing::TARGET_BYTES).len();
    assert_eq!(
        splits,
        objects / descriptors::MEMBERS_PER_SPLIT,
        "the planner packs {objects} uniformly small objects into {splits} splits, \
         not the {} the descriptor corpus assumes",
        objects / descriptors::MEMBERS_PER_SPLIT
    );
}

#[test]
fn the_descriptor_corpora_are_reproducible() {
    assert_eq!(
        descriptors::full_split_plan(),
        descriptors::full_split_plan()
    );
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
/// the *values*, holding every width, would slip past — and that is the
/// deliberate limit of the pin rather than a hole in it, because what these
/// benches spend is a function of the bytes' length and layout, not of which
/// hex digits are in them.
#[test]
fn the_descriptor_corpora_are_pinned_across_revisions() {
    for (name, plan, documents, bytes) in [
        (
            "full_splits",
            descriptors::full_split_plan(),
            descriptors::MEMBERS / descriptors::MEMBERS_PER_SPLIT,
            1_025_200_usize,
        ),
        (
            "single_member_splits",
            descriptors::single_member_plan(),
            descriptors::MEMBERS,
            1_139_200_usize,
        ),
    ] {
        assert_eq!(
            plan.len(),
            documents,
            "{name} encoded a different document count"
        );
        let total: usize = plan.iter().map(Vec::len).sum();
        assert_eq!(
            total, bytes,
            "{name} encodes to {total} bytes, not {bytes} — if that is intended, \
             the comparison it anchors has been re-baselined and every recorded \
             count for this bench is against a different corpus"
        );
    }
}

/// The two arrangements must carry the same members, or the pair is not a
/// comparison of document shape but of how much work each does.
#[test]
fn both_descriptor_arrangements_decode_to_the_same_members() {
    let members = |plan: Vec<Vec<u8>>| -> Vec<spate_s3::DescriptorObject> {
        plan.iter()
            .flat_map(|bytes| {
                SplitDescriptor::decode(bytes)
                    .expect("the corpus decodes")
                    .objects
            })
            .collect()
    };
    let full = members(descriptors::full_split_plan());
    let single = members(descriptors::single_member_plan());
    assert_eq!(full.len(), descriptors::MEMBERS);
    assert_eq!(full, single, "the arrangements carry different members");
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

/// The multi-part cases exist to charge per-stream decoder work, and a
/// fixture that encoded one stream would measure exactly what the `_whole`
/// cases already do while looking like a distinct profile. Their claim is
/// structural — every part carries its codec's own start-of-stream magic —
/// so it is asserted structurally rather than inferred from a byte count.
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
/// is silently measuring an aligned read and the contract it exists to pin is
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

/// The mid-object entry's record count is the contract the framing bench
/// exists to pin: entering part-way through a record, the framer emits the
/// leading partial line *as a record*, and a reader that discarded through
/// the first delimiter would emit one fewer.
///
/// Asserting it only inside the bench is not enough. The bench runs when a
/// maintainer applies `ci: bench` or on a push to `main` — so a pull request
/// changing the partial-line rule can pass `cargo test` and merge, with the
/// assertion first firing after the fact. Here it gates every pull request.
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
