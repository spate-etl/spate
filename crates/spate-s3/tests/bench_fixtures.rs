//! The bench corpora are reproducible, and pack the way the bench claims.
//!
//! An instruction count only means something if both legs of a comparison ran
//! on byte-identical input, so "the corpus is a pure function of nothing" is a
//! property worth a test rather than an assumption. The benches themselves
//! cannot carry it: they need Linux, valgrind and a matching runner, and they
//! only run when a pull request selects the bench stage. This runs everywhere
//! `cargo test` does.

use spate_core::coordination::SplitId;
use spate_s3::bench_seams::plan_listing;
use std::collections::HashSet;

#[path = "../benches/support/listing.rs"]
mod listing;

#[test]
fn the_listing_corpora_are_reproducible() {
    assert_eq!(listing::uniform_small(), listing::uniform_small());
    assert_eq!(listing::big_objects(), listing::big_objects());
    assert_eq!(listing::mixed_tail(), listing::mixed_tail());
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
