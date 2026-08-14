//! The bench rigs' corpora are reproducible, and are the corpora the cases
//! claim they are.
//!
//! Two tiers rest on these rigs. An instruction count only means something if
//! both legs of a comparison ran on byte-identical input, and the wall-clock
//! tier's corpus digest only demotes a mismatched pair *after* the fact, so
//! "the corpus is a pure function of its parameters" is tested here rather
//! than assumed. The benches themselves cannot carry it: the counted tier
//! needs Linux, valgrind and a matching runner. This runs everywhere
//! `cargo test` does.
//!
//! Reproducibility is the smaller half. Most of these cases rest on a claim
//! about *what the corpus does*. The routing axis changes only the keys; a
//! chunk target lands on a record boundary; a keyed corpus spreads and a
//! keyless one does not; a quarter-unrouted corpus is a match-rate case rather
//! than a three-branch one. Every one of those could drift into its opposite
//! while the benches went on running, reporting a plausible number for the
//! wrong path, and several are what a declared `.items()` counter means. Those
//! claims are what this file checks.

use spate_core::ops::ChunkConfig;
use spate_core::record::{PartitionId, RawPayload, stable_key_hash};
use spate_core::sink::{KeyHashRouter, ShardRouter};
use std::collections::BTreeSet;

#[path = "../benches/support/ack_traffic.rs"]
mod ack_traffic;
#[path = "../benches/support/chain_rig.rs"]
mod chain_rig;
#[path = "../benches/support/poll_traffic.rs"]
mod poll_traffic;
#[path = "../benches/support/split_rig.rs"]
mod split_rig;

use ack_traffic::{BATCHES, Order, PARTITIONS};
use chain_rig::{BATCH, BORROWED_BATCH_BYTES, Routing};
use poll_traffic::{ITERATIONS, Profile};
use split_rig::{PAYLOADS, Tags};

/// FNV-1a over a corpus.
///
/// Written out rather than taken from `DefaultHasher`, whose output is not
/// stable across releases. A pin that could change under a toolchain bump is
/// not a pin.
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
/// while changing every byte a decoder reads. The pin would then pass over a
/// corpus no recorded measurement was taken against. The digest closes that.
fn pin(bytes: &[u8]) -> (usize, u64) {
    (bytes.len(), digest(bytes))
}

/// The chain rig's payloads as one blob, in the order the batch yields them,
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

/// The checkpoint rig's schedule of poll batches, as the wall tier absorbs it.
///
/// Read off a built rig rather than from a generator of its own, so what is
/// pinned here is what a drive issues. The tick width is passed because the
/// builder takes it, not because the schedule depends on it, which is itself
/// one of the claims below.
fn ack_schedule(per_tick: usize, order: Order) -> Vec<u8> {
    ack_traffic::rig(per_tick, order).corpus()
}

/// The backpressure rig's script of poll iterations.
fn poll_script(profile: Profile) -> Vec<u8> {
    poll_traffic::rig(profile, expected_transitions(profile)).corpus()
}

/// Transitions each profile's script produces, as `backpressure_gungraun.rs`
/// declares them.
///
/// Restated here rather than imported because the rig takes the expectation
/// from its caller. One that derived it from its own model of the state
/// machine would agree with itself however the machine changed.
fn expected_transitions(profile: Profile) -> usize {
    match profile {
        Profile::Quiet => 0,
        Profile::Congested => 1,
        Profile::Flapping => 2048,
    }
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
    for order in [Order::Issued, Order::Scrambled] {
        assert_eq!(ack_schedule(256, order), ack_schedule(256, order));
    }
    for profile in [Profile::Quiet, Profile::Congested, Profile::Flapping] {
        assert_eq!(poll_script(profile), poll_script(profile));
    }
}

/// Every corpus, pinned by length and digest.
///
/// Two calls in one process only prove the generators are pure. The property
/// both tiers need is stronger, that a corpus is the same *across revisions*,
/// since a merge-base leg and a head leg run different builds. A one-character
/// edit to the payload format, the key width or the record count would
/// otherwise re-baseline every comparison with nothing to say it happened.
/// These numbers make that edit fail here instead. Changing one is a
/// deliberate act. Re-record it, and treat every measurement from before the
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
    assert_eq!(
        pin(&ack_schedule(256, Order::Issued)),
        ACK_SCHEDULE_PIN,
        "ack schedule"
    );
    assert_eq!(
        pin(&poll_script(Profile::Quiet)),
        POLL_QUIET_PIN,
        "poll quiet script"
    );
    assert_eq!(
        pin(&poll_script(Profile::Congested)),
        POLL_CONGESTED_PIN,
        "poll congested script"
    );
    assert_eq!(
        pin(&poll_script(Profile::Flapping)),
        POLL_FLAPPING_PIN,
        "poll flapping script"
    );
}

/// The control-plane corpora's pins, named rather than written inline because
/// the length of each is also asserted from its own constants below. The
/// digest says the bytes did not move, and the arithmetic beside it says why
/// that length is the right one.
const ACK_SCHEDULE_PIN: (usize, u64) = (98_304, 0xb805_bf3f_d145_a605);
const POLL_QUIET_PIN: (usize, u64) = (589_824, 0x6605_f527_400e_e502);
const POLL_CONGESTED_PIN: (usize, u64) = (589_824, 0x79e5_ec3b_66f3_72c5);
const POLL_FLAPPING_PIN: (usize, u64) = (589_824, 0xe3fb_64f3_9e0d_2545);

/// Each corpus is the length its own constants imply.
///
/// The digests above would catch a changed corpus but say nothing about
/// *why* the length is what it is. A schedule is one entry per batch, and a
/// script one step per poll iteration, so these two products tie the pinned
/// lengths to `BATCHES` and `ITERATIONS`, the same counts the wall cases
/// declare as `.items()`. A corpus that grew while its `.items()` stayed
/// put would report a throughput per record that no longer had that many
/// records behind it.
#[test]
fn each_corpus_is_the_length_its_constants_imply() {
    assert_eq!(
        ACK_SCHEDULE_PIN.0,
        BATCHES * (4 + 8),
        "the schedule is one partition and one offset per batch"
    );
    assert_eq!(
        POLL_QUIET_PIN.0,
        ITERATIONS * (8 + 8 + 1 + 1),
        "the script is one add, sub, rejection and queue reading per iteration"
    );
    assert_eq!(POLL_QUIET_PIN.0, POLL_CONGESTED_PIN.0);
    assert_eq!(POLL_QUIET_PIN.0, POLL_FLAPPING_PIN.0);
}

/// Neither the tick width nor the driver changes which batches are issued.
///
/// The wall tier reads this as a license to compare `ack_wide_*` against
/// `ack_narrow_*` and against the threaded cases. The corpus digest that
/// travels in every record is the same for all of them, so what separates
/// those cases is how the batches are grouped and who issues them, not which
/// batches there are. A generator that started keying on either would demote
/// the pairs to *Not comparable*, but only after a full A/B run, and only if
/// the two legs happened to differ.
#[test]
fn the_ack_schedule_is_independent_of_tick_width_and_driver() {
    let wide = ack_schedule(256, Order::Issued);
    assert_eq!(wide, ack_schedule(16, Order::Issued), "tick width");
    assert_eq!(
        wide,
        ack_schedule(256, Order::Scrambled),
        "resolution order is a property of the driver, not the corpus"
    );
    for threads in [1, 2, 4] {
        assert_eq!(
            wide,
            ack_traffic::threaded(256, Order::Issued, threads).corpus(),
            "the {threads}-thread driver issues a different schedule"
        );
    }
}

// ---------------------------------------------------------------------------
// The corpora have the shapes the cases' counters rest on
// ---------------------------------------------------------------------------

/// The routing axis changes the keys and nothing else.
///
/// `chain_keyed_one_shard` is read against `chain_borrowed` as the same bytes
/// through the same chain, with one added cost. A payload shape that differed
/// between the two would leave that pair comparing two workloads and
/// attributing the difference to routing.
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
/// the split cases' is `PAYLOADS * 28`. All three are statements about an
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
/// That equality makes `bytes_per_s` the comparable figure across the three
/// cases, the same quantity of input with only the distribution of route arms
/// changed. Constants nudged apart would leave the three silently
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
/// three sub-records (a payload that lost a separator, a filter that started
/// rejecting), every `records_per_s` on the borrowed arms would be wrong by the
/// same factor, and nothing in the bench would notice. The case would still
/// run, still produce a number, and still compare cleanly against itself.
#[test]
fn the_chain_emits_the_row_counts_the_counters_declare() {
    assert_eq!(chain_rig::borrowed_rig().drive(), 1536, "borrowed rows");
    assert_eq!(chain_rig::owned_rig().drive(), 512, "owned rows");
}

/// The split rig's `expect_rows` is the row count a driven batch produces, for
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
// A drive is repeatable
// ---------------------------------------------------------------------------

/// Every control-plane rig performs the same work on its hundredth drive as
/// on its first.
///
/// The counted tier never needed this. gungraun drives a rig once and throws
/// it away. The wall harness calls a routine thousands of times against one
/// state, so a rig that drifts reports a case whose name stops describing what
/// it measures, and reports it as a clean, stable, wrong number.
///
/// The backpressure rig drifts unless [`poll_traffic::Rig::reset`] is called,
/// which is what that method does and what this pins. Its script holds
/// *relative* budget movements baked against a trajectory starting at zero, so
/// a second drive applies them to whatever the first left behind. `Quiet`
/// climbs out of its band, and `Congested` reports its one transition once and
/// zero thereafter because the controller is still paused. Both failures are
/// silent. `assert_in_band` runs in the builder, and the drive returns a
/// number either way.
#[test]
fn a_second_drive_is_the_same_work_as_the_first() {
    for per_tick in [16, 256] {
        for order in [Order::Issued, Order::Scrambled] {
            let mut rig = ack_traffic::rig(per_tick, order);
            for drive in 1..=3 {
                assert_eq!(
                    rig.drive(),
                    rig.expect_watermarks,
                    "ack drive {drive} at per_tick {per_tick}"
                );
                assert_eq!(rig.pending(), 0, "ack pending after drive {drive}");
            }
        }
    }

    for profile in [Profile::Quiet, Profile::Congested, Profile::Flapping] {
        let expect = expected_transitions(profile);
        let mut rig = poll_traffic::rig(profile, expect);
        let mut settled = Vec::new();
        for drive in 1..=3 {
            rig.reset();
            // Zero after a reset, and the same non-zero level at the end of
            // every drive. The transition count alone cannot carry this for
            // `Quiet`, whose expectation is zero and whose drive would satisfy
            // it by doing nothing.
            assert_eq!(rig.usage(), 0, "poll budget after reset {drive}");
            assert_eq!(rig.drive(), expect, "poll drive {drive}");
            settled.push(rig.usage());
        }
        assert!(
            settled[0] > 0,
            "a drive left the budget at zero, so it moved nothing"
        );
        assert!(
            settled.windows(2).all(|w| w[0] == w[1]),
            "a drive left the budget somewhere its predecessor did not: {settled:?}"
        );
    }
}

/// Skipping the reset breaks the backpressure rig.
///
/// The test above would also pass if `reset` were a no-op and the script had
/// quietly become idempotent on its own, leaving a method the wall target
/// calls every iteration with nothing to say for itself. This is the other
/// half. The drift is there, and the reset removes it.
#[test]
fn the_backpressure_rig_drifts_without_a_reset() {
    let mut congested = poll_traffic::rig(Profile::Congested, 1);
    assert_eq!(congested.drive(), 1, "the first drive crosses into a pause");
    assert_eq!(
        congested.drive(),
        0,
        "a second drive without a reset should find the controller still \
         paused; if this now reports 1, the controller's state no longer \
         survives a drive and `reset` may have stopped being needed"
    );

    // The quiet script nets 2,031,616 bytes upward per drive, so the fourth
    // drive opens above the high watermark and pauses on its first iteration.
    // It reports nothing on the drives after that; once paused it stays
    // paused, because a resume needs the usage back under the *low* watermark.
    // This looks for a transition anywhere in the run rather than on the last
    // drive.
    let mut quiet = poll_traffic::rig(Profile::Quiet, 0);
    let counts: Vec<usize> = (0..6).map(|_| quiet.drive()).collect();
    assert_eq!(
        counts[0], 0,
        "the first drive is the quiet profile by construction"
    );
    assert!(
        counts.iter().any(|&n| n > 0),
        "the quiet script should climb out of its band without a reset, and \
         did not in six drives ({counts:?}); if the budget's movements are no \
         longer relative, the reason `reset` exists has changed"
    );
}

// ---------------------------------------------------------------------------
// The threaded driver drives what the single-threaded one drives
// ---------------------------------------------------------------------------

/// Sharding the acking across threads changes who issues a batch, not which
/// batches are issued or what the checkpointer makes of them.
///
/// This is the claim the whole thread axis rests on. `AckIssuer` numbers
/// sequences per issuer and `PartitionTracker::register` panics on a gap, so a
/// partition issued from two threads inside one epoch is a crash. A partition
/// issued from *none* is a watermark that never arrives, which is silent. The
/// watermark count catches the second; running at all catches the first.
#[test]
fn the_threaded_driver_agrees_with_the_single_threaded_one() {
    let mut plain = ack_traffic::rig(256, Order::Issued);
    let expect = plain.drive();
    assert_eq!(expect, plain.expect_watermarks);

    for threads in [1, 2, 4] {
        let mut rig = ack_traffic::threaded(256, Order::Issued, threads);
        for drive in 1..=3 {
            assert_eq!(
                rig.drive(),
                expect,
                "{threads} threads, drive {drive}: a different number of \
                 watermark pairs than the single-threaded driver produces"
            );
            assert_eq!(rig.pending(), 0, "{threads} threads, drive {drive}");
        }
        assert_eq!(
            rig.expect_watermarks, plain.expect_watermarks,
            "the thread count moved the expected watermark total, so the \
             cases are no longer measuring one axis"
        );
    }
}

/// Only thread counts that divide the partitions are accepted.
///
/// Three threads over four partitions would leave one thread owning two and
/// the others one apiece, which still runs and reports a contention figure for
/// a workload that is unbalanced by construction rather than by the code.
#[test]
fn a_thread_count_must_divide_the_partitions() {
    let Err(panic) = std::panic::catch_unwind(|| ack_traffic::threaded(256, Order::Issued, 3))
    else {
        panic!("three threads over {PARTITIONS} partitions was accepted");
    };
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    // Check which panic, not only that one happened. Raising `PARTITIONS` to six
    // would make three threads legal, and a test that only counted panics
    // would go on passing while proving nothing about divisibility.
    assert!(
        message.contains("do not divide"),
        "the builder panicked for some other reason: {message}"
    );
}

/// A scrambled resolution order works when the window is a worker's share of
/// a tick rather than the whole tick.
///
/// The shipped cases pair `Order::Scrambled` only with the single-threaded
/// driver, so nothing else reaches the branch where `threaded` narrows the
/// scramble window by the thread count. Without this, replacing that
/// `per_tick / threads` with `per_tick` leaves every test green while the
/// walk stops being a permutation. Some batches resolve twice and others
/// never, which is a panic on a worker thread rather than a wrong number.
#[test]
fn the_scramble_window_is_a_workers_share_of_a_tick() {
    for threads in [1, 2, 4] {
        let mut rig = ack_traffic::threaded(256, Order::Scrambled, threads);
        for drive in 1..=2 {
            assert_eq!(
                rig.drive(),
                rig.expect_watermarks,
                "{threads} threads scrambled, drive {drive}"
            );
            assert_eq!(rig.pending(), 0, "{threads} threads scrambled");
        }
    }
}

// ---------------------------------------------------------------------------
// The swept parameters really sweep
// ---------------------------------------------------------------------------

/// Every chunk target a case names divides the batch's encoding exactly.
///
/// Building the rig is the check. `assert_encodes_to` and
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
    // `flush`, the baseline the three above are read against.
    assert!(
        ChunkConfig::default().target_bytes > BORROWED_BATCH_BYTES,
        "the default chunk target no longer clears a whole batch, so the \
         baseline cases seal mid-push like the chunk cases do"
    );
}

/// The keyed corpus spreads over every shard count a case names, and the
/// keyless one does not spread.
///
/// The first half stops a shard-count case measuring an idle `Vec` of buffers;
/// it is `assert_spreads`, run by building the rig. The second makes
/// `Routing::Fixed` a *controlled* baseline rather than an accidental one.
/// `KeyHashRouter` falls back to a hash of the source partition for a keyless
/// record, every payload here comes from partition 0, so the real router over a
/// keyless corpus places everything on shard 0 exactly as the constant stub
/// does. If that ever stopped holding, the keyless cases would quietly become
/// multi-shard ones.
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
/// spread the router case exercises would not be there, while the number it
/// reported stayed stable.
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
/// Stronger than the rig's own `assert_hits_every_branch`, which only proves a
/// branch is non-empty. The failure this catches is the one
/// `QUARTER_UNROUTED_CYCLE`'s comment records having been caught once already.
/// Aiming every unrouted payload at what would have been branch 3 leaves that
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
