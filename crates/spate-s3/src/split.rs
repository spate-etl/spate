//! Split descriptors, deterministic split identity, and listing-order
//! packing. These are the shared vocabulary between the leader's planner and
//! every split reader.
//!
//! A **split** is one leasable unit of work: a small batch of whole objects,
//! or one byte range of a large object. The planner packs the sorted listing
//! into splits; the [`SplitDescriptor`] carries each split's member objects
//! (keys, sizes, ETags) and its range, if it has one, verbatim to whichever
//! worker gains it, so workers never list.
//!
//! # Identity
//!
//! [`split_id_for`] digests the member set (keys **and** ETags) plus the
//! packing-algorithm version into a stable [`SplitId`];
//! [`split_id_for_range`] digests one object's key, ETag and range the same
//! way. The consequences:
//!
//! - Replanning unchanged work reproduces the same ids, so re-submitting a
//!   plan is a store-side create-if-absent no-op.
//! - An overwritten object (new ETag) yields a **new** split id: the new
//!   content is new work, never silently skipped against stale progress.
//! - A change to the packing algorithm bumps the digested version, retiring
//!   every old id as an explicit epoch instead of silently re-reading a
//!   reshuffled listing against orphaned progress records.
//!
//! The digest is truncated SHA-256: split ids are persisted identity, and a
//! collision would silently drop one member set, so the digest must hold up
//! even for adversarially-named keys.
//!
//! # Packing
//!
//! [`pack`] walks the sorted listing in order and first-fits each object
//! into one of a bounded window of open bins. It never reorders by size, so
//! the output is a pure function of the listing order and objects sharing a
//! key prefix stay in the same split. Each object costs at least
//! `target / 16`, an open-cost floor that stops thousands of tiny objects
//! coalescing into one split, so a split holds at most ~16 members and its
//! descriptor stays far below backend value-size caps. An object above the
//! target that [`Packing::delimiter_for`] accepts is cut into byte ranges of
//! at most the target, one split each; any other object at or above the
//! target lands alone in its own split.

use crate::config::Compression;
use crate::fetch::ObjectEntry;
use crate::framer::Codec;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use spate_core::coordination::{CoordinationError, CoordinationErrorKind, SplitId};
use std::collections::VecDeque;

/// Version of the [`SplitDescriptor`] wire encoding. Bumped on any change
/// to the descriptor's schema; a worker refuses a descriptor written by an
/// incompatible release instead of misreading it.
pub const DESCRIPTOR_VERSION: u32 = 2;

/// Version of the packing algorithm, folded into every split id by
/// [`split_id_for`]. Bumping it retires all previously planned ids as an
/// explicit epoch (see the module docs).
pub(crate) const PACKING_VERSION: u32 = 2;

/// Largest object [`pack`] cuts into byte ranges: 50,000 GiB, the largest
/// object S3 can hold (10,000 parts of 5 GiB). A listing reporting a larger
/// size is read whole.
pub(crate) const MAX_SUBDIVIDABLE_BYTES: u64 = 50_000 << 30;

/// Maximum number of bins held open during packing. Bounds the open-bin
/// window and how far out of listing order a member can land. The listing
/// itself is resident for the whole plan, so planner memory scales with the
/// listing rather than with this.
pub(crate) const PACKING_LOOKBACK: usize = 10;

/// Denominator of the per-object open-cost floor: each member costs at
/// least `target / OPEN_COST_DIVISOR`, capping members per split at ~16.
pub(crate) const OPEN_COST_DIVISOR: u64 = 16;

/// One member object inside a [`SplitDescriptor`].
///
/// Mirrors what an object-store listing reports; everything a reader needs
/// to fetch and pin the object without a HEAD request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescriptorObject {
    /// Full object key.
    pub key: String,
    /// Object size in bytes, from the listing.
    pub size: u64,
    /// ETag from the listing, if the store reports one. Readers pin every
    /// GET to it (`If-Match`), so a concurrent overwrite surfaces as a
    /// precondition failure instead of a silent content splice.
    pub etag: Option<String>,
    /// Last-modified time (ms since epoch), used as the records' event time.
    pub last_modified_ms: i64,
}

/// A byte range `[start, end)` of one object, read as a split of its own.
///
/// The range owns the records whose first byte lies in `[start, end)`, where a
/// record starts at byte 0 and after every `delimiter` byte, so each record of
/// an object belongs to exactly one range of a tiling. The object must be
/// uncompressed: a reader whose `compression` setting decodes the key fails
/// the pipeline on a ranged split.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SplitRange {
    /// First byte of the range.
    pub start: u64,
    /// One past the last byte of the range.
    pub end: u64,
    /// The framer's [`resync_delimiter`](spate_core::framing::RecordFramer::resync_delimiter).
    /// A reader whose framer declares a different one refuses the split.
    pub delimiter: u8,
}

impl SplitRange {
    /// The range `[start, end)` of an object framed on `delimiter`.
    #[must_use]
    pub fn new(start: u64, end: u64, delimiter: u8) -> SplitRange {
        SplitRange {
            start,
            end,
            delimiter,
        }
    }
}

/// The opaque payload carried in a
/// [`SplitSpec::descriptor`](spate_core::coordination::SplitSpec): the
/// split's member objects, in listing order.
///
/// The encoding is versioned JSON ([`DESCRIPTOR_VERSION`]); member order is
/// meaningful (composite offsets index into it). Out-of-process producers
/// (an event-notification planner, a single-shot invocation minting one
/// split from an S3 event) construct via [`SplitDescriptor::new`] or
/// [`SplitDescriptor::with_range`] (which stamp the version;
/// [`encode`](SplitDescriptor::encode) refuses anything else) and mint ids
/// with [`split_id_for`] or [`split_id_for_range`] respectively, which
/// together are the whole cross-process contract. Fields are freely readable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitDescriptor {
    /// Encoding version; always [`DESCRIPTOR_VERSION`] at write. Private to
    /// construction ([`SplitDescriptor::new`]): a hand-written version
    /// would ship a descriptor every leasing worker fails on, fleet-wide.
    pub(crate) v: u32,
    /// Member objects, in listing (and therefore read) order.
    pub objects: Vec<DescriptorObject>,
    /// The byte range of the one member this split reads, or `None` when it
    /// reads every member whole.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<SplitRange>,
}

/// The version probe decoded before the full descriptor, so an
/// incompatible version is reported as such rather than as a parse error.
#[derive(Deserialize)]
struct VersionProbe {
    v: u32,
}

impl SplitDescriptor {
    /// Build a descriptor over `objects` (listing order, since ordinals
    /// index into it), stamped with the current [`DESCRIPTOR_VERSION`].
    /// [`with_range`](SplitDescriptor::with_range) builds a ranged one, and
    /// [`encode`](SplitDescriptor::encode) refuses any other version.
    #[must_use]
    pub fn new(objects: Vec<DescriptorObject>) -> SplitDescriptor {
        SplitDescriptor {
            v: DESCRIPTOR_VERSION,
            objects,
            range: None,
        }
    }

    /// Build a descriptor over the byte `range` of `object`, stamped with the
    /// current [`DESCRIPTOR_VERSION`]. [`encode`](SplitDescriptor::encode)
    /// refuses it unless `object` has an ETag and
    /// `range.start < range.end <= object.size`. `object` must be
    /// uncompressed, as [`SplitRange`] states.
    #[must_use]
    pub fn with_range(object: DescriptorObject, range: SplitRange) -> SplitDescriptor {
        SplitDescriptor {
            v: DESCRIPTOR_VERSION,
            objects: vec![object],
            range: Some(range),
        }
    }

    /// The encoding version this descriptor was constructed (or decoded)
    /// under.
    #[must_use]
    pub fn version(&self) -> u32 {
        self.v
    }

    /// Materialize the member objects as fetchable entries, preserving
    /// descriptor order (ordinals index into it).
    pub(crate) fn to_entries(&self) -> Vec<ObjectEntry> {
        self.objects
            .iter()
            .map(|o| ObjectEntry {
                key: o.key.clone(),
                size: o.size,
                etag: o.etag.clone(),
                last_modified_ms: o.last_modified_ms,
            })
            .collect()
    }

    /// Build a descriptor from listed entries, preserving their order.
    pub(crate) fn from_entries(entries: &[ObjectEntry]) -> SplitDescriptor {
        SplitDescriptor::new(
            entries
                .iter()
                .map(|e| DescriptorObject {
                    key: e.key.clone(),
                    size: e.size,
                    etag: e.etag.clone(),
                    last_modified_ms: e.last_modified_ms,
                })
                .collect(),
        )
    }

    /// Encode to the versioned wire form.
    ///
    /// # Errors
    ///
    /// [`Fatal`](CoordinationErrorKind::Fatal) when the descriptor's
    /// version is not [`DESCRIPTOR_VERSION`], or when it has a range and
    /// not exactly one member, a member without an ETag, or a range that is
    /// empty or extends past the member's size. A descriptor written under a
    /// wrong version fails pipeline-fatal on every worker that leases it.
    pub fn encode(&self) -> Result<Vec<u8>, CoordinationError> {
        if self.v != DESCRIPTOR_VERSION {
            return Err(CoordinationError::new(
                CoordinationErrorKind::Fatal,
                format!(
                    "descriptor version {} is not the supported {DESCRIPTOR_VERSION}; \
                     construct via SplitDescriptor::new",
                    self.v
                ),
            ));
        }
        self.validate_range()?;
        Ok(serde_json::to_vec(self).expect("descriptor serialization is infallible: no non-string map keys, no fallible Serialize impls"))
    }

    /// Decode a descriptor, probing the version first.
    ///
    /// # Errors
    ///
    /// [`Fatal`](CoordinationErrorKind::Fatal) when the bytes do not parse,
    /// were written under a different [`DESCRIPTOR_VERSION`], or carry a range
    /// [`encode`](SplitDescriptor::encode) would refuse. A worker must never
    /// guess at an incompatible descriptor.
    pub fn decode(bytes: &[u8]) -> Result<SplitDescriptor, CoordinationError> {
        let fatal = |reason: String| CoordinationError::new(CoordinationErrorKind::Fatal, reason);
        let probe: VersionProbe = serde_json::from_slice(bytes)
            .map_err(|e| fatal(format!("split descriptor is not valid JSON: {e}")))?;
        if probe.v != DESCRIPTOR_VERSION {
            return Err(fatal(format!(
                "split descriptor version {} is not this release's version \
                 {DESCRIPTOR_VERSION}; the split was planned by an incompatible release",
                probe.v
            )));
        }
        let descriptor: SplitDescriptor = serde_json::from_slice(bytes)
            .map_err(|e| fatal(format!("split descriptor failed to decode: {e}")))?;
        descriptor.validate_range()?;
        Ok(descriptor)
    }

    /// Refuse a ranged descriptor unless it has exactly one member, that
    /// member has an ETag, and `start < end <= size`.
    fn validate_range(&self) -> Result<(), CoordinationError> {
        let Some(range) = self.range else {
            return Ok(());
        };
        let fatal = |reason: String| CoordinationError::new(CoordinationErrorKind::Fatal, reason);
        let [object] = self.objects.as_slice() else {
            return Err(fatal(format!(
                "a ranged split descriptor has exactly one member, this one has {}",
                self.objects.len()
            )));
        };
        if object.etag.is_none() {
            return Err(fatal(format!(
                "ranged split descriptor over \"{}\" has no ETag to pin its reads to",
                object.key
            )));
        }
        if range.start >= range.end || range.end > object.size {
            return Err(fatal(format!(
                "ranged split descriptor over \"{}\" has range [{}, {}), which is empty or \
                 extends past the object's {} bytes",
                object.key, range.start, range.end, object.size
            )));
        }
        Ok(())
    }
}

/// Mint the deterministic split id for a member set.
///
/// `members` are `(key, etag)` pairs; order does not matter (they are
/// sorted by key before digesting). The id digests keys, ETags, and the
/// packing version; see the module docs for why each is included. The
/// result is always a valid [`SplitId`]: 25 bytes of `[A-Za-z0-9_-]`
/// regardless of what the keys contain.
///
/// Public so out-of-process producers mint byte-identical ids for the same
/// members. The digest preimage is wire format, precise enough to
/// reimplement in any language:
///
/// 1. Sort the members ascending by key (byte-wise comparison of the
///    UTF-8 key bytes).
/// 2. Feed SHA-256 with, in order:
///    - the domain tag: the 15 ASCII bytes `spate-s3-split\n`;
///    - the packing version as a little-endian `u32` (currently `2`, the
///      crate's `PACKING_VERSION`);
///    - for each member, in sorted order:
///      - the key's byte length as a little-endian `u32`, then the key's
///        UTF-8 bytes;
///      - the ETag presence byte: `0x01` followed by the ETag's byte
///        length as a little-endian `u32` and its UTF-8 bytes when
///        present, the single byte `0x00` when absent.
/// 3. Truncate the 32-byte digest to its first 16 bytes, encode them as
///    base64url without padding (RFC 4648 §5), and prefix `s3-`, giving a
///    25-character id over `[A-Za-z0-9_-]`.
///
/// ```
/// use spate_s3::split_id_for;
///
/// let id = split_id_for([
///     ("exports/2026/part-000.ndjson", Some("\"9b2cf5\"")),
///     ("exports/2026/part-001.ndjson", None),
/// ])
/// .expect("non-empty member set");
/// assert!(id.as_str().starts_with("s3-"));
/// assert_eq!(id.as_str().len(), 25);
/// ```
///
/// # Errors
///
/// [`Fatal`](CoordinationErrorKind::Fatal) for an empty member set; a
/// split with no members is meaningless.
pub fn split_id_for<'a, I>(members: I) -> Result<SplitId, CoordinationError>
where
    I: IntoIterator<Item = (&'a str, Option<&'a str>)>,
{
    split_id_with_version(members, PACKING_VERSION)
}

/// [`split_id_for`] with an explicit packing version. The seam lets tests
/// pin version sensitivity.
fn split_id_with_version<'a, I>(members: I, version: u32) -> Result<SplitId, CoordinationError>
where
    I: IntoIterator<Item = (&'a str, Option<&'a str>)>,
{
    let mut members: Vec<(&str, Option<&str>)> = members.into_iter().collect();
    if members.is_empty() {
        return Err(CoordinationError::new(
            CoordinationErrorKind::Fatal,
            "cannot mint a split id for an empty member set",
        ));
    }
    members.sort_unstable_by_key(|(key, _)| *key);

    let mut hasher = Sha256::new();
    hasher.update(b"spate-s3-split\n");
    hasher.update(version.to_le_bytes());
    for (key, etag) in members {
        digest_member(&mut hasher, key, etag);
    }
    id_from_digest(hasher)
}

/// Mint the deterministic split id for the byte `range` of one object.
///
/// The id digests the key, the ETag, the range and the packing version, and
/// never equals an id [`split_id_for`] mints. The digest preimage is wire
/// format:
///
/// 1. Feed SHA-256 with, in order:
///    - the domain tag: the 15 ASCII bytes `spate-s3-range\n`;
///    - the packing version as a little-endian `u32` (currently `2`, the
///      crate's `PACKING_VERSION`);
///    - the key's byte length as a little-endian `u32`, then the key's UTF-8
///      bytes;
///    - the byte `0x01`, the ETag's byte length as a little-endian `u32`,
///      then its UTF-8 bytes;
///    - `range.start` and `range.end`, each a little-endian `u64`, then the
///      single byte `range.delimiter`.
/// 2. Truncate and encode the digest as step 3 of [`split_id_for`] does.
///
/// ```
/// use spate_s3::{SplitRange, split_id_for_range};
///
/// let id = split_id_for_range(
///     "exports/2026/part-000.ndjson",
///     "\"9b2cf5\"",
///     SplitRange::new(0, 64 << 20, b'\n'),
/// )
/// .expect("non-empty range");
/// assert_eq!(id.as_str().len(), 25);
/// ```
///
/// # Errors
///
/// [`Fatal`](CoordinationErrorKind::Fatal) when `range.start >= range.end`.
pub fn split_id_for_range(
    key: &str,
    etag: &str,
    range: SplitRange,
) -> Result<SplitId, CoordinationError> {
    if range.start >= range.end {
        return Err(CoordinationError::new(
            CoordinationErrorKind::Fatal,
            format!(
                "cannot mint a split id for the empty range [{}, {})",
                range.start, range.end
            ),
        ));
    }
    let mut hasher = Sha256::new();
    hasher.update(b"spate-s3-range\n");
    hasher.update(PACKING_VERSION.to_le_bytes());
    digest_member(&mut hasher, key, Some(etag));
    hasher.update(range.start.to_le_bytes());
    hasher.update(range.end.to_le_bytes());
    hasher.update([range.delimiter]);
    id_from_digest(hasher)
}

/// Feed one member's key and ETag in the length-prefixed form both id
/// preimages share.
fn digest_member(hasher: &mut Sha256, key: &str, etag: Option<&str>) {
    hasher.update(u32::try_from(key.len()).unwrap_or(u32::MAX).to_le_bytes());
    hasher.update(key.as_bytes());
    match etag {
        Some(etag) => {
            hasher.update([0x01]);
            hasher.update(u32::try_from(etag.len()).unwrap_or(u32::MAX).to_le_bytes());
            hasher.update(etag.as_bytes());
        }
        None => hasher.update([0x00]),
    }
}

/// The `s3-` id over the first 16 bytes of the digest, base64url unpadded.
fn id_from_digest(hasher: Sha256) -> Result<SplitId, CoordinationError> {
    let digest = hasher.finalize();
    SplitId::new(format!("s3-{}", URL_SAFE_NO_PAD.encode(&digest[..16])))
}

/// What [`pack`] needs besides the listing.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Packing {
    /// The split target, in bytes. Never zero.
    pub(crate) target_bytes: u64,
    /// The configured compression, which decides each object's codec.
    pub(crate) compression: Compression,
    /// The framer's resync delimiter, or `None` to read every object whole.
    pub(crate) delimiter: Option<u8>,
}

impl Packing {
    /// The delimiter to cut `entry` on, or `None` when it is read whole.
    ///
    /// An object is cut when it is above the target, at most
    /// [`MAX_SUBDIVIDABLE_BYTES`], uncompressed, and has an ETag.
    pub(crate) fn delimiter_for(&self, entry: &ObjectEntry) -> Option<u8> {
        let delimiter = self.delimiter?;
        let cut = entry.size > self.target_bytes
            && entry.size <= MAX_SUBDIVIDABLE_BYTES
            && entry.etag.is_some()
            && Codec::resolve(self.compression, &entry.key) == Codec::Plain;
        cut.then_some(delimiter)
    }
}

/// One split [`pack`] produces.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Packed {
    /// Whole objects, in listing order.
    Objects(Vec<ObjectEntry>),
    /// One byte range of one object, which has an ETag.
    Range(ObjectEntry, SplitRange),
}

impl Packed {
    /// The split's deterministic id.
    pub(crate) fn id(&self) -> Result<SplitId, CoordinationError> {
        match self {
            Packed::Objects(members) => {
                split_id_for(members.iter().map(|m| (m.key.as_str(), m.etag.as_deref())))
            }
            Packed::Range(entry, range) => split_id_for_range(
                &entry.key,
                entry
                    .etag
                    .as_deref()
                    .expect("pack cuts only an object with an ETag"),
                *range,
            ),
        }
    }

    /// The split's descriptor.
    pub(crate) fn descriptor(&self) -> SplitDescriptor {
        match self {
            Packed::Objects(members) => SplitDescriptor::from_entries(members),
            Packed::Range(entry, range) => SplitDescriptor {
                range: Some(*range),
                ..SplitDescriptor::from_entries(std::slice::from_ref(entry))
            },
        }
    }

    /// The split's weight: the bytes it reads, at least 1.
    pub(crate) fn weight(&self) -> u64 {
        match self {
            // Saturating: sizes are remote listing data.
            Packed::Objects(members) => members
                .iter()
                .fold(0u64, |acc, m| acc.saturating_add(m.size)),
            Packed::Range(_, range) => range.end - range.start,
        }
        .max(1)
    }
}

/// Cut `[0, size)` into `ceil(size / target)` contiguous ranges, each
/// between `target / 2` and `target` bytes long when `size > target`.
fn tile(size: u64, target: u64, delimiter: u8) -> impl Iterator<Item = SplitRange> {
    let n = size.div_ceil(target);
    // u128: `i * size` reaches about 2^71 for the largest object at the 1 MiB
    // target floor.
    let boundary = move |i: u64| (u128::from(i) * u128::from(size) / u128::from(n)) as u64;
    (0..n).map(move |i| SplitRange::new(boundary(i), boundary(i + 1), delimiter))
}

/// Pack the sorted listing into splits of roughly `packing.target_bytes`
/// each.
///
/// A pure function of `(entries, packing)`: walking the listing in order,
/// each object costs `max(size, target_bytes / 16)` and first-fits into the
/// oldest of at most [`PACKING_LOOKBACK`] open bins with room; a bin at or
/// above the target closes, so an object costing the whole target lands
/// alone in its own split. An object [`Packing::delimiter_for`] accepts is
/// instead cut into `ceil(size / target)` byte ranges, emitted at its listing
/// position in ascending order. It evicts an open bin as an oversized object
/// does, so the other objects' bins are the same whether or not it is cut.
/// Returned splits preserve listing order both across splits (by first
/// member) and within each split.
pub(crate) fn pack(entries: Vec<ObjectEntry>, packing: &Packing) -> Vec<Packed> {
    let target_bytes = packing.target_bytes;
    debug_assert!(
        target_bytes > 0,
        "config validation rejects a zero split target"
    );
    struct Bin {
        members: Vec<ObjectEntry>,
        cost: u64,
        /// Set on a byte-range bin, which has one member and is never open.
        range: Option<SplitRange>,
    }
    let floor = (target_bytes / OPEN_COST_DIVISOR).max(1);
    let mut bins: Vec<Bin> = Vec::new();
    // Indexes into `bins` still accepting members, oldest first.
    let mut open: VecDeque<usize> = VecDeque::new();
    for entry in entries {
        if let Some(delimiter) = packing.delimiter_for(&entry) {
            if open.len() == PACKING_LOOKBACK {
                open.pop_front();
            }
            bins.extend(tile(entry.size, target_bytes, delimiter).map(|range| Bin {
                members: vec![entry.clone()],
                cost: range.end - range.start,
                range: Some(range),
            }));
            continue;
        }
        let cost = entry.size.max(floor);
        // Saturating: sizes are remote listing data and may be
        // adversarially close to u64::MAX; the fit test must not overflow.
        let idx = match open
            .iter()
            .position(|&i| bins[i].cost.saturating_add(cost) <= target_bytes)
        {
            Some(pos) => open[pos],
            None => {
                if open.len() == PACKING_LOOKBACK {
                    open.pop_front();
                }
                bins.push(Bin {
                    members: Vec::new(),
                    cost: 0,
                    range: None,
                });
                let idx = bins.len() - 1;
                open.push_back(idx);
                idx
            }
        };
        bins[idx].members.push(entry);
        bins[idx].cost = bins[idx].cost.saturating_add(cost);
        if bins[idx].cost >= target_bytes
            && let Some(pos) = open.iter().position(|&i| i == idx)
        {
            open.remove(pos);
        }
    }
    bins.into_iter()
        .map(
            |Bin {
                 mut members, range, ..
             }| match range {
                Some(range) => Packed::Range(members.swap_remove(0), range),
                None => Packed::Objects(members),
            },
        )
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn entry(key: &str, size: u64) -> ObjectEntry {
        ObjectEntry {
            key: key.to_string(),
            size,
            etag: Some(format!("\"etag-{key}\"")),
            last_modified_ms: 1_760_000_000_000,
        }
    }

    const MB: u64 = 1024 * 1024;

    /// Packing that reads every object whole.
    fn whole(target: u64) -> Packing {
        Packing {
            target_bytes: target,
            compression: Compression::Auto,
            delimiter: None,
        }
    }

    /// Packing that cuts a large plain object on `\n`.
    fn cut(target: u64) -> Packing {
        Packing {
            delimiter: Some(b'\n'),
            ..whole(target)
        }
    }

    /// The members of every whole-object split, in order.
    fn object_bins(packed: &[Packed]) -> Vec<Vec<ObjectEntry>> {
        packed
            .iter()
            .filter_map(|p| match p {
                Packed::Objects(members) => Some(members.clone()),
                Packed::Range(..) => None,
            })
            .collect()
    }

    // --- identity ---

    #[test]
    fn digest_id_is_pinned() {
        // The id is persisted identity: accidental drift of the digest
        // algorithm, preimage layout, or encoding must fail this test, and
        // a deliberate change must bump PACKING_VERSION.
        let id = split_id_for([
            ("exports/2026/part-000.ndjson", Some("\"9b2cf5\"")),
            ("exports/2026/part-001.ndjson", None),
        ])
        .unwrap();
        assert_eq!(id.as_str(), "s3-xje_MfL-wSNqMCft5ellCg");
    }

    #[test]
    fn digest_is_order_insensitive_but_content_sensitive() {
        let forward = split_id_for([("a", Some("1")), ("b", Some("2"))]).unwrap();
        let reversed = split_id_for([("b", Some("2")), ("a", Some("1"))]).unwrap();
        assert_eq!(forward, reversed);

        let other_key = split_id_for([("a", Some("1")), ("c", Some("2"))]).unwrap();
        assert_ne!(forward, other_key);
    }

    #[test]
    fn digest_changes_when_an_etag_changes() {
        let before = split_id_for([("a", Some("v1")), ("b", Some("x"))]).unwrap();
        let overwritten = split_id_for([("a", Some("v2")), ("b", Some("x"))]).unwrap();
        let dropped = split_id_for([("a", None), ("b", Some("x"))]).unwrap();
        assert_ne!(before, overwritten);
        assert_ne!(before, dropped);
    }

    #[test]
    fn digest_folds_in_the_packing_version() {
        let v1 = split_id_with_version([("a", Some("1"))], 1).unwrap();
        let v2 = split_id_with_version([("a", Some("1"))], 2).unwrap();
        assert_ne!(v1, v2);
    }

    #[test]
    fn empty_member_set_is_rejected() {
        assert!(split_id_for(std::iter::empty()).is_err());
    }

    #[test]
    fn ambiguous_concatenations_do_not_collide() {
        // Length prefixes and etag presence tags keep distinct member sets
        // from concatenating to one preimage.
        let a = split_id_for([("ab", Some("c"))]).unwrap();
        let b = split_id_for([("a", Some("bc"))]).unwrap();
        let c = split_id_for([("abc", None)]).unwrap();
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(b, c);
    }

    // --- descriptor ---

    #[test]
    fn descriptor_round_trips() {
        let desc = SplitDescriptor::from_entries(&[
            entry("exports/part-000.ndjson.gz", 52 * MB),
            ObjectEntry {
                key: "exports/part-001.ndjson.gz".to_string(),
                size: 9,
                etag: None,
                last_modified_ms: 1,
            },
        ]);
        let decoded = SplitDescriptor::decode(&desc.encode().unwrap()).unwrap();
        assert_eq!(decoded, desc);
        assert_eq!(decoded.v, DESCRIPTOR_VERSION);
    }

    #[test]
    fn descriptor_encoding_is_pinned() {
        // The descriptor is a persisted document; its field names and
        // shape are wire format. A deliberate change bumps
        // DESCRIPTOR_VERSION and updates this pin.
        let desc = SplitDescriptor::from_entries(&[entry("k", 5)]);
        assert_eq!(
            String::from_utf8(desc.encode().unwrap()).unwrap(),
            r#"{"v":2,"objects":[{"key":"k","size":5,"etag":"\"etag-k\"","last_modified_ms":1760000000000}]}"#,
        );
    }

    #[test]
    fn encode_refuses_a_descriptor_not_built_by_new() {
        // A hand-written version would ship a descriptor every leasing
        // worker fails pipeline-fatal on; refuse at the producer instead.
        let rogue = SplitDescriptor {
            v: 0,
            objects: vec![],
            range: None,
        };
        let err = rogue.encode().unwrap_err();
        assert_eq!(err.kind, CoordinationErrorKind::Fatal);
        assert!(
            err.reason.contains("SplitDescriptor::new"),
            "reason: {}",
            err.reason
        );
        assert_eq!(SplitDescriptor::new(vec![]).version(), DESCRIPTOR_VERSION);
    }

    /// A version-1 descriptor, written before descriptors could carry a
    /// range, is refused.
    #[test]
    fn a_version_1_descriptor_is_refused() {
        let err = SplitDescriptor::decode(
            br#"{"v":1,"objects":[{"key":"k","size":5,"etag":null,"last_modified_ms":1}]}"#,
        )
        .unwrap_err();
        assert_eq!(err.kind, CoordinationErrorKind::Fatal);
        assert!(err.reason.contains("version 1"), "reason: {}", err.reason);
    }

    #[test]
    fn unknown_descriptor_version_is_rejected_actionably() {
        let err = SplitDescriptor::decode(br#"{"v":999,"objects":[]}"#).unwrap_err();
        assert_eq!(err.kind, CoordinationErrorKind::Fatal);
        assert!(err.reason.contains("version 999"), "reason: {}", err.reason);
        assert!(
            err.reason.contains("incompatible release"),
            "reason: {}",
            err.reason
        );

        let garbage = SplitDescriptor::decode(b"not json").unwrap_err();
        assert_eq!(garbage.kind, CoordinationErrorKind::Fatal);
    }

    fn ranged_object(size: u64, etag: Option<&str>) -> DescriptorObject {
        DescriptorObject {
            key: "big.ndjson".to_string(),
            size,
            etag: etag.map(str::to_owned),
            last_modified_ms: 7,
        }
    }

    #[test]
    fn ranged_descriptor_round_trips_and_its_encoding_is_pinned() {
        let desc = SplitDescriptor::with_range(
            ranged_object(100, Some("\"e\"")),
            SplitRange::new(40, 80, b'\n'),
        );
        let encoded = desc.encode().unwrap();
        assert_eq!(
            String::from_utf8(encoded.clone()).unwrap(),
            r#"{"v":2,"objects":[{"key":"big.ndjson","size":100,"etag":"\"e\"","last_modified_ms":7}],"range":{"start":40,"end":80,"delimiter":10}}"#,
        );
        assert_eq!(SplitDescriptor::decode(&encoded).unwrap(), desc);
    }

    /// `encode` and `decode` refuse a ranged descriptor with other than one
    /// member, without an ETag, or with an empty or out-of-bounds range.
    #[test]
    fn invalid_ranged_descriptors_are_refused_on_both_sides() {
        let pinned = || ranged_object(100, Some("\"e\""));
        let two_members = SplitDescriptor {
            objects: vec![pinned(), pinned()],
            ..SplitDescriptor::with_range(pinned(), SplitRange::new(0, 10, b'\n'))
        };
        let no_members = SplitDescriptor {
            objects: vec![],
            ..SplitDescriptor::with_range(pinned(), SplitRange::new(0, 10, b'\n'))
        };
        let cases = [
            ("member", two_members),
            ("member", no_members),
            (
                "ETag",
                SplitDescriptor::with_range(
                    ranged_object(100, None),
                    SplitRange::new(0, 10, b'\n'),
                ),
            ),
            (
                "empty",
                SplitDescriptor::with_range(pinned(), SplitRange::new(10, 10, b'\n')),
            ),
            (
                "empty",
                SplitDescriptor::with_range(pinned(), SplitRange::new(11, 10, b'\n')),
            ),
            (
                "past the object",
                SplitDescriptor::with_range(pinned(), SplitRange::new(0, 101, b'\n')),
            ),
        ];
        for (needle, desc) in cases {
            let err = desc.encode().unwrap_err();
            assert_eq!(err.kind, CoordinationErrorKind::Fatal);
            assert!(err.reason.contains(needle), "encode: {}", err.reason);
            // The same bytes written by a producer that skipped validation.
            let raw = serde_json::to_vec(&desc).unwrap();
            let err = SplitDescriptor::decode(&raw).unwrap_err();
            assert_eq!(err.kind, CoordinationErrorKind::Fatal);
            assert!(err.reason.contains(needle), "decode: {}", err.reason);
        }
        // A range covering the whole object is valid.
        let whole = SplitDescriptor::with_range(pinned(), SplitRange::new(0, 100, b'\n'));
        SplitDescriptor::decode(&whole.encode().unwrap()).unwrap();
    }

    #[test]
    fn ranged_id_is_pinned() {
        // Persisted identity, like `digest_id_is_pinned`; the value was
        // recomputed from the documented preimage outside Rust.
        let id = split_id_for_range(
            "exports/2026/part-000.ndjson",
            "\"9b2cf5\"",
            SplitRange::new(0, 64 * MB, b'\n'),
        )
        .unwrap();
        assert_eq!(id.as_str(), "s3-fA0wNRW2MyDxquwFvNy0JA");
    }

    /// Every input of a ranged id moves it, and no ranged id equals the
    /// member-set id of the same object.
    #[test]
    fn ranged_ids_digest_every_input_and_never_equal_member_set_ids() {
        let base = split_id_for_range("k", "e", SplitRange::new(0, 10, b'\n')).unwrap();
        for other in [
            split_id_for_range("k2", "e", SplitRange::new(0, 10, b'\n')),
            split_id_for_range("k", "e2", SplitRange::new(0, 10, b'\n')),
            split_id_for_range("k", "e", SplitRange::new(1, 10, b'\n')),
            split_id_for_range("k", "e", SplitRange::new(0, 11, b'\n')),
            split_id_for_range("k", "e", SplitRange::new(0, 10, b';')),
        ] {
            assert_ne!(other.unwrap(), base);
        }
        assert_ne!(split_id_for([("k", Some("e"))]).unwrap(), base);
        assert!(split_id_for_range("k", "e", SplitRange::new(10, 10, b'\n')).is_err());
    }

    // --- packing ---

    /// A listing entry whose shape `kind` picks: plain, gzip, zstd, or plain
    /// without an ETag.
    fn shaped(i: usize, size: u64, kind: u8) -> ObjectEntry {
        let key = format!(
            "k{i:04}.ndjson{}",
            ["", ".gz", ".zst", ""][kind as usize % 4]
        );
        ObjectEntry {
            etag: (kind % 4 != 3).then(|| format!("\"etag-{i}\"")),
            ..entry(&key, size)
        }
    }

    #[test]
    fn packing_is_deterministic_for_a_fixed_listing() {
        let listing: Vec<ObjectEntry> = (0..200)
            .map(|i| entry(&format!("k{i:04}"), (i % 90) * MB))
            .collect();
        let a = pack(listing.clone(), &cut(64 * MB));
        let b = pack(listing, &cut(64 * MB));
        assert_eq!(a, b);
    }

    #[test]
    fn tiny_objects_coalesce_under_the_open_cost_floor() {
        // 64 tiny objects at a 64 MB target: each costs the 4 MB floor, so
        // exactly 16 fill a bin.
        let listing: Vec<ObjectEntry> = (0..64).map(|i| entry(&format!("k{i:02}"), 1)).collect();
        let bins = object_bins(&pack(listing, &whole(64 * MB)));
        assert_eq!(bins.len(), 4);
        assert!(bins.iter().all(|b| b.len() == 16));
    }

    #[test]
    fn adversarial_listing_sizes_do_not_overflow_the_fit_test() {
        // Sizes come from remote listing metadata; a u64::MAX entry must
        // neither panic in debug nor share a bin, and is read whole.
        let listing = vec![
            entry("a", 10 * MB),
            entry("huge", u64::MAX),
            entry("z", 10 * MB),
        ];
        let packed = pack(listing, &cut(64 * MB));
        let bins = object_bins(&packed);
        assert_eq!(bins.len(), packed.len(), "nothing is cut");
        let huge_bin = bins
            .iter()
            .find(|b| b.iter().any(|e| e.key == "huge"))
            .unwrap();
        assert_eq!(huge_bin.len(), 1, "the oversized object lands alone");
        let total: usize = bins.iter().map(Vec::len).sum();
        assert_eq!(total, 3, "nothing lost, nothing duplicated");
    }

    /// An object above [`MAX_SUBDIVIDABLE_BYTES`] is read whole, and one at
    /// the limit, or of 6 TiB, is cut.
    #[test]
    fn an_object_above_the_subdivision_limit_stays_whole() {
        let target = 1 << 30;
        let six_tib = pack(vec![entry("6tib", 6 << 40)], &cut(target));
        assert_eq!(six_tib.len(), 6 << 10);
        let over = entry("over", MAX_SUBDIVIDABLE_BYTES + 1);
        assert_eq!(
            pack(vec![over.clone()], &cut(target)),
            [Packed::Objects(vec![over])]
        );
        let at = pack(vec![entry("at", MAX_SUBDIVIDABLE_BYTES)], &cut(target));
        assert_eq!(at.len() as u64, MAX_SUBDIVIDABLE_BYTES / target);
        assert!(at.iter().all(|p| matches!(p, Packed::Range(..))));
    }

    /// An object is read whole when it is compressed by extension or by
    /// configuration, has no ETag, meets a framer with no delimiter, or sits
    /// exactly at the target.
    #[test]
    fn objects_that_cannot_be_cut_stay_whole() {
        let target = 64 * MB;
        let plain = entry("plain.ndjson", 3 * target);
        let gzip_config = Packing {
            compression: Compression::Gzip,
            ..cut(target)
        };
        let no_etag = ObjectEntry {
            etag: None,
            ..plain.clone()
        };
        for (packing, object) in [
            (cut(target), entry("gz.ndjson.gz", 3 * target)),
            (cut(target), entry("zst.ndjson.zst", 3 * target)),
            (gzip_config, plain.clone()),
            (cut(target), no_etag),
            (whole(target), plain.clone()),
            (cut(target), entry("at.ndjson", target)),
        ] {
            assert_eq!(
                pack(vec![object.clone()], &packing),
                [Packed::Objects(vec![object.clone()])],
                "{packing:?} {object:?}"
            );
        }
        assert_eq!(
            pack(vec![entry("over.ndjson", target + 1)], &cut(target)).len(),
            2,
            "one byte above the target is cut"
        );
    }

    #[test]
    fn oversized_object_gets_its_own_split() {
        let listing = vec![
            entry("a", 10 * MB),
            entry("huge", 500 * MB),
            entry("b", 10 * MB),
        ];
        let bins = object_bins(&pack(listing, &whole(64 * MB)));
        let huge_bin = bins
            .iter()
            .find(|b| b.iter().any(|e| e.key == "huge"))
            .unwrap();
        assert_eq!(
            huge_bin.len(),
            1,
            "an oversized object never shares a split"
        );
    }

    /// A large plain object is cut into ranges emitted at its listing
    /// position, between its neighbors' splits.
    #[test]
    fn a_large_plain_object_is_cut_in_place() {
        let listing = vec![
            entry("a", 60 * MB),
            entry("huge", 500 * MB),
            entry("z", 60 * MB),
        ];
        let packed = pack(listing, &cut(64 * MB));
        let keys: Vec<&str> = packed
            .iter()
            .map(|p| match p {
                Packed::Objects(members) => members[0].key.as_str(),
                Packed::Range(entry, _) => entry.key.as_str(),
            })
            .collect();
        let mut want = vec!["a"];
        want.extend(["huge"; 8]);
        want.push("z");
        assert_eq!(keys, want);
    }

    /// Every size from one byte above the target to eight times it, and the
    /// largest object at the smallest target, tiles `[0, size)` with ranges
    /// of `target / 2` to `target` bytes.
    #[test]
    fn ranges_tile_the_object_within_half_to_one_target() {
        for target in 1..=64u64 {
            for size in target + 1..=8 * target {
                assert_tiles(size, target);
            }
        }
        assert_tiles(MAX_SUBDIVIDABLE_BYTES, MB);
        assert_tiles(MAX_SUBDIVIDABLE_BYTES - 1, MB);
    }

    fn assert_tiles(size: u64, target: u64) {
        let mut count = 0;
        let mut next = 0;
        for range in tile(size, target, b'\n') {
            count += 1;
            assert_eq!(range.start, next, "size {size} target {target}");
            let len = range.end - range.start;
            assert!(
                target / 2 <= len && len <= target,
                "size {size} target {target}: range {range:?} is {len} bytes"
            );
            next = range.end;
        }
        assert_eq!(count, size.div_ceil(target), "size {size} target {target}");
        assert_eq!(next, size, "size {size} target {target}");
    }

    #[test]
    fn packing_preserves_listing_order_within_and_across_bins() {
        let listing: Vec<ObjectEntry> = (0..50)
            .map(|i| {
                entry(
                    &format!("k{i:02}"),
                    if i % 7 == 0 { 60 * MB } else { 3 * MB },
                )
            })
            .collect();
        let bins = object_bins(&pack(listing, &whole(64 * MB)));
        for bin in &bins {
            let keys: Vec<&str> = bin.iter().map(|e| e.key.as_str()).collect();
            let mut sorted = keys.clone();
            sorted.sort_unstable();
            assert_eq!(keys, sorted, "members stay in listing order within a bin");
        }
        let firsts: Vec<&str> = bins.iter().map(|b| b[0].key.as_str()).collect();
        let mut sorted = firsts.clone();
        sorted.sort_unstable();
        assert_eq!(
            firsts, sorted,
            "bins emerge in listing order of their first member"
        );
    }

    #[test]
    fn lookback_bounds_how_long_a_bin_stays_open() {
        // A bin that never fills is force-closed once PACKING_LOOKBACK
        // newer bins have opened: no unbounded open-bin growth.
        let mut listing = vec![entry("a-half-full", 40 * MB)];
        // Each of these fills a fresh bin exactly (nothing fits alongside
        // 40 MB in a 64 MB bin except <= 24 MB; use 60 MB so none fit).
        for i in 0..PACKING_LOOKBACK + 2 {
            listing.push(entry(&format!("b{i:02}"), 60 * MB));
        }
        listing.sort_unstable_by(|a, b| a.key.cmp(&b.key));
        let bins = object_bins(&pack(listing, &whole(64 * MB)));
        // The half-full bin closed with its single member; every 60 MB
        // object got its own bin.
        assert_eq!(bins.len(), PACKING_LOOKBACK + 3);
        assert!(bins.iter().all(|b| b.len() == 1));
    }

    proptest! {
        /// Every object lands whole in exactly one split, or is tiled in
        /// order by ranged splits and appears in no whole-object split.
        #[test]
        fn prop_packing_partitions_the_listing_exactly(
            objects in proptest::collection::vec((0u64..300 * MB, 0u8..4), 0..120),
            target_mb in 1u64..129,
        ) {
            let listing: Vec<ObjectEntry> = objects
                .iter()
                .enumerate()
                .map(|(i, &(size, kind))| shaped(i, size, kind))
                .collect();
            let packed = pack(listing.clone(), &cut(target_mb * MB));
            for object in &listing {
                let whole_count = object_bins(&packed)
                    .iter()
                    .flatten()
                    .filter(|e| *e == object)
                    .count();
                let ranges: Vec<SplitRange> = packed
                    .iter()
                    .filter_map(|p| match p {
                        Packed::Range(e, range) if e == object => Some(*range),
                        _ => None,
                    })
                    .collect();
                if ranges.is_empty() {
                    prop_assert_eq!(whole_count, 1, "{:?}", object);
                } else {
                    prop_assert_eq!(whole_count, 0, "{:?}", object);
                    let mut next = 0;
                    for range in ranges {
                        prop_assert_eq!(range.start, next);
                        next = range.end;
                    }
                    prop_assert_eq!(next, object.size);
                }
            }
            prop_assert!(object_bins(&packed).iter().all(|b| !b.is_empty()));
        }

        /// Cutting an object replaces its whole-object split with its ranges
        /// and changes no other split.
        #[test]
        fn prop_small_object_bins_do_not_depend_on_cutting(
            objects in proptest::collection::vec((0u64..300 * MB, 0u8..4), 0..120),
            target_mb in 1u64..129,
        ) {
            let listing: Vec<ObjectEntry> = objects
                .iter()
                .enumerate()
                .map(|(i, &(size, kind))| shaped(i, size, kind))
                .collect();
            let packing = cut(target_mb * MB);
            let expanded: Vec<Packed> = pack(listing.clone(), &whole(target_mb * MB))
                .into_iter()
                .flat_map(|split| match &split {
                    Packed::Objects(members) if members.len() == 1 => {
                        match packing.delimiter_for(&members[0]) {
                            Some(delimiter) => tile(members[0].size, packing.target_bytes, delimiter)
                                .map(|range| Packed::Range(members[0].clone(), range))
                                .collect(),
                            None => vec![split],
                        }
                    }
                    _ => vec![split],
                })
                .collect();
            prop_assert_eq!(pack(listing, &packing), expanded);
        }

        #[test]
        fn prop_ranges_tile_large_objects(
            target in 1u64..=1 << 32,
            n in 2u64..=1 << 12,
            extra in any::<u64>(),
        ) {
            // A size in `((n - 1) * target, n * target]`, and its upper end.
            assert_tiles((n - 1) * target + 1 + extra % target, target);
            assert_tiles(n * target, target);
        }

        #[test]
        fn prop_member_count_is_bounded_by_the_floor(
            sizes in proptest::collection::vec(0u64..300 * MB, 0..120),
        ) {
            let target = 64 * MB;
            let listing: Vec<ObjectEntry> = sizes
                .iter()
                .enumerate()
                .map(|(i, &s)| entry(&format!("k{i:04}"), s))
                .collect();
            let bins = object_bins(&pack(listing, &cut(target)));
            // floor = target/16 divides target exactly, so a bin never
            // holds more than 16 members, the structural descriptor bound.
            prop_assert!(bins.iter().all(|b| b.len() <= 16));
        }

        #[test]
        fn prop_split_ids_are_valid_for_arbitrary_keys(
            keys in proptest::collection::btree_set("[ -~]{1,64}", 1..8),
            etag in proptest::option::of("[ -~]{1,16}"),
        ) {
            // S3 keys contain '/', '.', spaces, '%', anything printable —
            // none of it may leak into the id (charset [A-Za-z0-9_-]).
            let members: Vec<(&str, Option<&str>)> =
                keys.iter().map(|k| (k.as_str(), etag.as_deref())).collect();
            let id = split_id_for(members).unwrap();
            prop_assert!(id.as_str().starts_with("s3-"));
            prop_assert_eq!(id.as_str().len(), 25);
        }

        #[test]
        fn prop_packing_and_ids_are_deterministic(
            sizes in proptest::collection::vec(0u64..200 * MB, 1..60),
        ) {
            let listing: Vec<ObjectEntry> = sizes
                .iter()
                .enumerate()
                .map(|(i, &s)| entry(&format!("k{i:04}"), s))
                .collect();
            let ids = |packed: &[Packed]| -> Vec<SplitId> {
                packed.iter().map(|p| p.id().unwrap()).collect()
            };
            let a = pack(listing.clone(), &cut(32 * MB));
            let b = pack(listing, &cut(32 * MB));
            prop_assert_eq!(ids(&a), ids(&b));
        }
    }
}
