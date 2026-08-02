//! Deterministic encoded-split-descriptor corpora for the decode bench.
//!
//! What a worker is handed is the leader's `encode` output, so the corpora
//! are **encoded here**, outside the measured region — the same discipline
//! the framing corpora follow by compressing in the fixture: a bench that
//! serialised inside the region would count the serialiser instead of the
//! parser it is supposed to be measuring.
//!
//! # Member grouping
//!
//! `pack` is crate-private, so members are grouped by a plain chunking rather
//! than by the real packer. That is not free-hand. The open-cost floor makes
//! every object cost at least `target / OPEN_COST_DIVISOR`, so a bin closes
//! at 16 members however small they are, and the planner packs a listing of
//! uniformly small objects into exactly that: 10,000 objects into 625 splits.
//! [`MEMBERS_PER_SPLIT`] is that same 16, and
//! `the_chunked_grouping_matches_the_real_packer` pins the equivalence
//! against `plan_listing` — so a change to the member cap fails a test rather
//! than silently reshaping this fixture. The single-member arrangement is
//! pinned the same way by `the_profiles_pack_differently`, which holds the
//! planner to one split per at-or-above-target object.
//!
//! # Determinism
//!
//! Nothing here varies between runs: keys, ETags, sizes and timestamps are
//! all functions of the member index. The listing corpus needs a generator
//! for its sizes because packing is sensitive to the size *distribution*;
//! decode is not — a descriptor costs what its bytes cost — so the values are
//! derived directly and there is no generator to seed.
//!
//! Key and ETag shapes mirror the listing corpus's, so a byte count here is
//! comparable with one there. They are restated rather than shared because a
//! `#[path]`-included support module has to be reachable from every file that
//! includes it, and the planning bench has no use for a descriptor corpus.

use spate_s3::{DescriptorObject, SplitDescriptor};

/// Members in every corpus below, whatever shape they are arranged into.
///
/// Holding it constant is what makes the profiles a comparison rather than
/// two unrelated numbers: they put the same member count — and so nearly the
/// same JSON byte volume — through the parser, and differ only in how many
/// documents it is spread over.
///
/// 6,400 is a plan's worth. The `uniform_small` listing profile packs into
/// 625 splits, so 400 full splits is the same order, and 6,400 members is
/// around 1 MB of descriptor JSON: enough that per-member field parsing
/// dominates the per-document version probe, and small enough to stay well
/// inside the instruction budget under emulation.
pub(crate) const MEMBERS: usize = 6_400;

/// Members per descriptor in the full-split arrangement — the cap the
/// open-cost floor imposes, and what an ordinary listing therefore produces.
pub(crate) const MEMBERS_PER_SPLIT: usize = 16;

/// A member key shaped like a real partitioned prefix, so the parsed string
/// is a realistic length rather than a two-character stub.
fn key(index: usize) -> String {
    const PER_HOUR: usize = 100;
    const PER_DAY: usize = PER_HOUR * 24;
    let day = index / PER_DAY % 28 + 1;
    let hour = index / PER_HOUR % 24;
    format!("year=2026/month=08/day={day:02}/hour={hour:02}/part-{index:08}.ndjson")
}

/// A quoted 32-character hexadecimal ETag — the shape an S3-compatible store
/// reports for a single-part upload, quotes included. Length matters: the
/// whole string is parsed and allocated per member.
fn etag(index: usize) -> String {
    // Both halves format to a fixed sixteen digits, so an ETag's length does
    // not depend on its value. Mixing the index through an odd multiplier is
    // only so the leading half varies across members the way a real digest
    // does, instead of being a run of zeros every member shares.
    let mixed = (index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    format!("\"{mixed:016x}{index:016x}\"")
}

/// One member, at the multi-megabyte size a backfill listing reports.
fn member(index: usize) -> DescriptorObject {
    DescriptorObject {
        key: key(index),
        size: 4 * 1024 * 1024 + (index as u64) % 997 * 1_024,
        etag: Some(etag(index)),
        last_modified_ms: 1_754_000_000_000 + index as i64,
    }
}

/// [`MEMBERS`] members encoded as descriptors of `members_each`.
fn plan(members_each: usize) -> Vec<Vec<u8>> {
    assert!(
        MEMBERS.is_multiple_of(members_each),
        "{MEMBERS} members do not divide into descriptors of {members_each}, so \
         the profiles would not carry the same member count"
    );
    let members: Vec<DescriptorObject> = (0..MEMBERS).map(member).collect();
    members
        .chunks(members_each)
        .map(|group| {
            SplitDescriptor::new(group.to_vec())
                .encode()
                .expect("a descriptor built by `new` carries the current version")
        })
        .collect()
}

/// A plan's worth of full splits: [`MEMBERS_PER_SPLIT`] members each, the
/// shape an ordinary listing produces.
pub(crate) fn full_split_plan() -> Vec<Vec<u8>> {
    plan(MEMBERS_PER_SPLIT)
}

/// The same members as one per descriptor — the shape a listing of
/// at-or-above-target objects produces, where each lands alone in its split.
pub(crate) fn single_member_plan() -> Vec<Vec<u8>> {
    plan(1)
}
