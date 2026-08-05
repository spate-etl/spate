//! The bench rigs' corpora are reproducible, and are the corpora the cases
//! claim they are.
//!
//! Two tiers rest on these rigs. An instruction count only means something if
//! both legs of a comparison ran on byte-identical input, and the wall-clock
//! tier's corpus digest only demotes a mismatched pair *after* the fact — so
//! "the corpus is a pure function of its parameters" is a property worth a test
//! rather than an assumption. The benches themselves cannot carry it: the
//! counted tier needs Linux, valgrind and a matching runner and only runs when
//! a pull request selects it, and the wall tier runs on demand and never in CI.
//! This runs everywhere `cargo test` does.
//!
//! Reproducibility is the smaller half. Most of these cases rest on a claim
//! about *what the corpus does* — that the routing axis changes only the keys;
//! that a chunk target lands on a record boundary; that a keyed corpus really
//! spreads and a keyless one really does not; that a quarter-unrouted corpus is
//! a match-rate case rather than a three-branch one. Every one of those could
//! drift into its opposite while the benches went on running, reporting a
//! plausible number for the wrong path, and several are what a declared
//! `.items()` counter means. That is what is checked here.

use spate_core::ops::ChunkConfig;
use spate_core::record::{PartitionId, RawPayload, stable_key_hash};
use spate_core::sink::{KeyHashRouter, ShardRouter};
use std::collections::BTreeSet;

#[path = "../benches/support/chain_rig.rs"]
mod chain_rig;
#[path = "../benches/support/split_rig.rs"]
mod split_rig;

use chain_rig::{BATCH, BORROWED_BATCH_BYTES, Routing};
use split_rig::{PAYLOADS, Tags};

/// FNV-1a over a corpus.
///
/// Written out rather than taken from `DefaultHasher`, whose output is
/// explicitly not stable across releases — and a pin that could change under a
/// toolchain bump is not a pin.
fn digest(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

/// A corpus's length and digest.
///
/// The length alone is not enough to pin any of these. A changed index format,
/// a different filler or a reordered field list can leave the total untouched
/// while changing every byte a decoder reads — and the pin would then pass over
/// a corpus no recorded measurement was taken against. The digest is what
/// closes that.
fn pin(bytes: &[u8]) -> (usize, u64) {
    (bytes.len(), digest(bytes))
}

/// The chain rig's payloads as one blob, in the order the batch yields them —
/// which is also the order `chain_wall.rs` absorbs them into the harness's
/// corpus digest.
fn chain_payloads(routing: Routing) -> Vec<u8> {
    chain_rig::corpus(routing).payloads().concat()
}

/// The chain rig's message keys as one blob.
fn chain_keys(routing: Routing) -> Vec<u8> {
    chain_rig::corpus(routing).keys().concat()
}

/// The split rig's payloads as one blob.
fn split_payloads(tags: Tags) -> Vec<u8> {
    split_rig::corpus(tags).concat()
}

/// The `RecordMeta` a payload with this key would carry, which is where the
/// key is hashed on the production path.
fn meta_for(key: Option<&[u8]>) -> spate_core::record::RecordMeta {
    RawPayload {
        bytes: b"",
        key,
        partition: PartitionId(0),
        offset: 0,
        timestamp_ms: 0,
    }
    .meta()
}

// ---------------------------------------------------------------------------
// Reproducibility and pins
// ---------------------------------------------------------------------------

#[test]
fn the_corpora_are_reproducible() {
    for routing in [Routing::Fixed, Routing::KeyHash] {
        assert_eq!(chain_payloads(routing), chain_payloads(routing));
        assert_eq!(chain_keys(routing), chain_keys(routing));
    }
    for tags in [
        Tags::TwoBranches,
        Tags::FourBranches,
        Tags::FourBranchesQuarterUnrouted,
    ] {
        assert_eq!(split_payloads(tags), split_payloads(tags));
    }
}

/// Every corpus, pinned by length and digest.
///
/// Two calls in one process only prove the generators are pure. The property
/// both tiers need is stronger — that a corpus is the same *across revisions*,
/// since a merge-base leg and a head leg run different builds. A one-character
/// edit to the payload format, the key width or the record count would
/// otherwise re-baseline every comparison with nothing to say it happened.
/// These numbers are what makes that edit fail here instead. Changing one is a
/// deliberate act: re-record it, and treat every measurement from before the
/// change as describing a different corpus.
#[test]
fn the_corpora_are_pinned_across_revisions() {
    assert_eq!(
        pin(&chain_payloads(Routing::Fixed)),
        (19_968, 0x8945_1814_bc2e_0e09),
        "chain payloads"
    );
    assert_eq!(
        pin(&chain_keys(Routing::KeyHash)),
        (4_096, 0x12b9_8fcc_2436_7301),
        "chain keys"
    );
    assert!(
        chain_keys(Routing::Fixed).is_empty(),
        "the keyless corpus grew keys, so `Routing::Fixed` is no longer keyless"
    );
    assert_eq!(
        pin(&split_payloads(Tags::TwoBranches)),
        (229_376, 0x258a_6632_fbdb_f559),
        "split two_branches"
    );
    assert_eq!(
        pin(&split_payloads(Tags::FourBranches)),
        (229_376, 0x1102_1f17_7a54_6f19),
        "split four_branches"
    );
    assert_eq!(
        pin(&split_payloads(Tags::FourBranchesQuarterUnrouted)),
        (229_376, 0xab84_8eab_a083_7129),
        "split four_branches_quarter_unrouted"
    );
}

// ---------------------------------------------------------------------------
// The corpora have the shapes the cases' counters rest on
// ---------------------------------------------------------------------------

/// The routing axis changes the keys and nothing else.
///
/// This is what makes `chain_keyed_one_shard` readable against
/// `chain_borrowed` at all: same bytes through the same chain, with one added
/// cost. A payload shape that differed between the two would leave that pair
/// comparing two workloads and attributing the difference to routing.
#[test]
fn the_routing_axis_changes_only_the_keys() {
    assert_eq!(
        chain_payloads(Routing::Fixed),
        chain_payloads(Routing::KeyHash),
        "the two routings no longer share a payload corpus"
    );
}

/// Every element is the width the arithmetic assumes.
///
/// A length pin sees the total and not the split. `BORROWED_BATCH_BYTES` is
/// derived per payload, `chain_wall.rs`'s byte denominator is `BATCH * 39`, and
/// the split cases' is `PAYLOADS * 28` — all three are statements about an
/// element, so all three are checked as one.
#[test]
fn every_element_is_the_declared_width() {
    let payloads = chain_rig::corpus(Routing::KeyHash);
    for payload in payloads.payloads() {
        assert_eq!(payload.len(), 39, "a chain payload is not 39 bytes");
    }
    for key in payloads.keys() {
        assert_eq!(key.len(), 8, "a chain key is not 8 bytes");
    }
    assert_eq!(payloads.payloads().len(), BATCH);
    assert_eq!(payloads.keys().len(), BATCH);

    for tags in [
        Tags::TwoBranches,
        Tags::FourBranches,
        Tags::FourBranchesQuarterUnrouted,
    ] {
        let corpus = split_rig::corpus(tags);
        assert_eq!(corpus.len(), PAYLOADS);
        for payload in &corpus {
            assert_eq!(payload.len(), 28, "a split payload is not 28 bytes");
        }
    }
}

/// All three split corpora carry the same number of bytes.
///
/// That equality is what makes `bytes_per_s` the comparable figure across the
/// three cases: the same quantity of input, with only the distribution of route
/// arms changed. Constants nudged apart would leave the three silently
/// comparing different-sized corpora and attributing the difference to the
/// branch count.
#[test]
fn the_split_corpora_are_the_same_quantity_of_bytes() {
    let two = split_payloads(Tags::TwoBranches).len();
    assert_eq!(two, split_payloads(Tags::FourBranches).len());
    assert_eq!(two, split_payloads(Tags::FourBranchesQuarterUnrouted).len());
}

// ---------------------------------------------------------------------------
// The chains emit what the cases' counters declare
// ---------------------------------------------------------------------------

/// `chain_wall.rs` declares 1536 records for its borrowed arms and 512 for the
/// owned one.
///
/// Those are `flat_map`'s fan-out and its absence. If `split3` stopped emitting
/// three sub-records — a payload that lost a separator, a filter that started
/// rejecting — every `records_per_s` on the borrowed arms would be wrong by
/// the same factor, and nothing in the bench would notice: the case would still
/// run, still produce a number, and still compare cleanly against itself.
#[test]
fn the_chain_emits_the_row_counts_the_counters_declare() {
    assert_eq!(chain_rig::borrowed_rig().drive(), 1536, "borrowed rows");
    assert_eq!(chain_rig::owned_rig().drive(), 512, "owned rows");
}

/// The split rig's `expect_rows` is what a driven batch actually produces, for
/// every tag distribution.
///
/// `split_wall.rs` reads `expect_rows` for its `.items_of()` counter rather
/// than stating a literal, so this is the claim that counter rests on.
#[test]
fn the_split_rigs_produce_the_rows_they_expect() {
    let mut two = split_rig::two_branch_rig();
    assert_eq!(two.drive(), two.expect_rows, "two_branches");
    assert_eq!(two.expect_rows, PAYLOADS);

    let mut four = split_rig::four_branch_rig(Tags::FourBranches);
    assert_eq!(four.drive(), four.expect_rows, "four_branches");
    assert_eq!(four.expect_rows, PAYLOADS);

    let mut partial = split_rig::four_branch_rig(Tags::FourBranchesQuarterUnrouted);
    assert_eq!(partial.drive(), partial.expect_rows, "quarter_unrouted");
    assert_eq!(
        partial.expect_rows,
        PAYLOADS - PAYLOADS / 4,
        "the unrouted share is not the quarter the case name claims"
    );
}

// ---------------------------------------------------------------------------
// The swept parameters really sweep
// ---------------------------------------------------------------------------

/// Every chunk target a case names divides the batch's encoding exactly.
///
/// Building the rig is the check: `assert_encodes_to` and
/// `assert_seals_on_a_record_boundary` run inside the builder, so a payload
/// shape that drifted from the arithmetic fails here rather than leaving
/// `chain_chunk_sixteenth_batch` sealing some other number of chunks than its
/// name claims. Re-established from the test suite rather than only from a
/// bench that runs on demand.
#[test]
fn every_chunk_target_seals_on_a_record_boundary() {
    assert_eq!(
        BORROWED_BATCH_BYTES, 25_088,
        "the batch's encoding moved, so every chunk target divides something else"
    );
    for divisor in [2, 4, 16] {
        let mut rig =
            chain_rig::borrowed_rig_with(Routing::Fixed, 1, BORROWED_BATCH_BYTES / divisor);
        assert_eq!(rig.drive(), 1536, "chunk target 1/{divisor}");
    }
    // The default target is above the whole batch, so its single chunk seals at
    // `flush` — the baseline the three above are read against.
    assert!(
        ChunkConfig::default().target_bytes > BORROWED_BATCH_BYTES,
        "the default chunk target no longer clears a whole batch, so the \
         baseline cases seal mid-push like the chunk cases do"
    );
}

/// The keyed corpus spreads over every shard count a case names, and the
/// keyless one does not spread at all.
///
/// Both halves matter. The first is what stops a shard-count case measuring an
/// idle `Vec` of buffers — it is `assert_spreads`, run by building the rig. The
/// second is what makes `Routing::Fixed` a *controlled* baseline rather than an
/// accidental one: `KeyHashRouter` falls back to a hash of the source partition
/// for a keyless record, every payload here comes from partition 0, so the real
/// router over a keyless corpus places everything on shard 0 exactly as the
/// constant stub does. If that ever stopped holding, the keyless cases would
/// quietly become multi-shard ones.
#[test]
fn the_keyed_corpus_spreads_and_the_keyless_one_does_not() {
    for shards in [1, 4, 16] {
        let mut rig = chain_rig::borrowed_rig_with(
            Routing::KeyHash,
            shards,
            ChunkConfig::default().target_bytes,
        );
        assert_eq!(rig.drive(), 1536, "{shards} shards");
    }

    let keyless = meta_for(None);
    for shards in [1, 4, 16] {
        assert_eq!(
            KeyHashRouter.route(&keyless, shards),
            0,
            "a keyless record no longer routes to shard 0, so `Routing::Fixed` \
             is not the controlled baseline the keyed cases are read against"
        );
    }
}

/// The keys hash to 512 distinct values.
///
/// `chain_route_key_hash` folds a modulo over one `RecordMeta` per key. If the
/// keys collided, that case would be one residue computed 512 times and the
/// spread the router case exists to exercise would not be there — while the
/// number it reported stayed perfectly stable.
#[test]
fn the_keys_hash_to_distinct_values() {
    let corpus = chain_rig::corpus(Routing::KeyHash);
    let hashes: BTreeSet<u64> = corpus.keys().iter().map(|k| stable_key_hash(k)).collect();
    assert_eq!(hashes.len(), BATCH, "the keyed corpus has hash collisions");

    // And they reach every shard the standalone routing case divides by, which
    // is the property `assert_spreads` proves in situ.
    let residues: BTreeSet<usize> = corpus
        .keys()
        .iter()
        .map(|k| KeyHashRouter.route(&meta_for(Some(k)), 16))
        .collect();
    assert_eq!(residues.len(), 16, "the keys leave some of 16 shards empty");
}

/// Each split corpus distributes its tags the way its case name claims.
///
/// Strictly stronger than the rig's own `assert_hits_every_branch`, which only
/// proves a branch is non-empty. The failure this catches is the one
/// `QUARTER_UNROUTED_CYCLE`'s comment records having been caught once already:
/// aiming every unrouted payload at what would have been branch 3 leaves that
/// branch empty for the whole batch, and the case becomes three branches plus a
/// drop rather than four branches at a three-quarter hit rate. Non-emptiness
/// cannot tell those apart; the counts can.
#[test]
fn the_split_corpora_distribute_their_tags_as_claimed() {
    let tally = |tags: Tags| {
        let mut counts = [0usize; 5]; // branches 0..4, then unrouted
        for payload in split_rig::corpus(tags) {
            match payload[0] {
                b'0'..=b'3' => counts[usize::from(payload[0] - b'0')] += 1,
                _ => counts[4] += 1,
            }
        }
        counts
    };

    assert_eq!(
        tally(Tags::TwoBranches),
        [4096, 4096, 0, 0, 0],
        "two_branches is no longer an even split over exactly two branches"
    );
    assert_eq!(
        tally(Tags::FourBranches),
        [2048, 2048, 2048, 2048, 0],
        "four_branches is no longer an even split over exactly four branches"
    );
    assert_eq!(
        tally(Tags::FourBranchesQuarterUnrouted),
        [1536, 1536, 1536, 1536, 2048],
        "the unrouted payloads are not spread evenly, so this is not a \
         match-rate case over four branches"
    );
}
