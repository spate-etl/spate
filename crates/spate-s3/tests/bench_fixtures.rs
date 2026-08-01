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
use spate_s3::Compression;
use spate_s3::bench_seams::{MakeFramer, frame_objects, plan_listing};
use std::collections::HashSet;
use std::sync::Arc;

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
}

#[test]
fn every_planned_split_has_a_distinct_id() {
    for corpus in [
        listing::uniform_small(),
        listing::big_objects(),
        listing::mixed_tail(),
    ] {
        let ids = plan_listing(corpus, listing::TARGET_BYTES);
        let unique: HashSet<&str> = ids.iter().map(SplitId::as_str).collect();
        assert!(!ids.is_empty());
        assert_eq!(unique.len(), ids.len(), "two splits minted the same id");
    }
}

/// The profiles have to stay *different* from each other, or the parameter is
/// decorative. `big_objects` in particular must keep landing one object per
/// split — that is the behaviour a byte-range subdivision change alters, and
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

#[test]
fn every_codec_frames_the_whole_body() {
    let body = ndjson::whole_body();
    let expect = body.iter().filter(|&&b| b == b'\n').count();

    for (compression, suffix, stored) in [
        (Compression::None, "", body.clone()),
        (Compression::Gzip, ".gz", ndjson::gzip(&body)),
        (Compression::Zstd, ".zst", ndjson::zstd(&body)),
    ] {
        let objects = vec![(
            format!("part-000000.ndjson{suffix}"),
            ndjson::chunks(&stored),
        )];
        let records = frame_objects(compression, framer(), &objects).expect("frames cleanly");
        assert_eq!(records, expect, "{compression:?} framed a different count");
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

#[test]
fn a_run_of_objects_frames_every_record() {
    let objects: Vec<_> = (0..16)
        .map(|i| {
            let body = ndjson::body(i * 200, 200);
            (format!("part-{i:06}.ndjson"), ndjson::chunks(&body))
        })
        .collect();
    let records = frame_objects(Compression::None, framer(), &objects).expect("frames cleanly");
    assert_eq!(records, 16 * 200);
}
