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
