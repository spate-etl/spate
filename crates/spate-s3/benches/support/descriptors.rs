//! Deterministic encoded-descriptor corpus for the decode bench.
//!
//! What a reader is handed is the lister's `encode` output, so the corpus is
//! **encoded here**, outside the measured region, the same discipline the
//! framing corpora follow by compressing in the fixture: a bench that
//! serialized inside the region would count the serializer instead of the
//! parser it is supposed to be measuring.
//!
//! # Determinism
//!
//! Nothing here varies between runs: keys, ETags, sizes and timestamps are
//! all functions of the member index. Decode is not sensitive to a size
//! distribution, since a descriptor costs what its bytes cost, so the values
//! are derived directly and there is no generator to seed.

use spate_s3::{DescriptorObject, SplitDescriptor};

/// Members in the corpus, one descriptor each: the shape a unit of one object,
/// whole or a byte range of it, has.
///
/// 6,400 members is around 1 MB of descriptor JSON: enough that per-member
/// field parsing dominates the per-document version probe, and small enough to
/// stay well inside the instruction budget under emulation.
pub(crate) const MEMBERS: usize = 6_400;

/// A member key shaped like a real partitioned prefix, so the parsed string
/// is a realistic length rather than a two-character stub.
fn key(index: usize) -> String {
    const PER_HOUR: usize = 100;
    const PER_DAY: usize = PER_HOUR * 24;
    let day = index / PER_DAY % 28 + 1;
    let hour = index / PER_HOUR % 24;
    format!("year=2026/month=08/day={day:02}/hour={hour:02}/part-{index:08}.ndjson")
}

/// A quoted 32-character hexadecimal ETag, the shape an S3-compatible store
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

/// [`MEMBERS`] members, each encoded as its own descriptor.
pub(crate) fn single_member_plan() -> Vec<Vec<u8>> {
    (0..MEMBERS)
        .map(|index| {
            SplitDescriptor::new(vec![member(index)])
                .encode()
                .expect("a descriptor built by `new` carries the current version")
        })
        .collect()
}
