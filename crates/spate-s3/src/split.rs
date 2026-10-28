//! Unit descriptors and deterministic unit identity.
//!
//! A unit of work is one object, one byte range of one object, or several
//! objects read whole in turn. The
//! [`SplitDescriptor`] carries the unit's member objects (keys, sizes, ETags)
//! and its range, if it has one, verbatim to whichever worker receives the
//! unit, so workers never list.
//!
//! # Identity
//!
//! [`split_id_for`] digests the member set (keys **and** ETags) plus a
//! version into a stable id; [`split_id_for_range`] digests one object's key,
//! ETag and range the same way. The consequences:
//!
//! - Publishing unchanged work again reproduces the same ids.
//! - An overwritten object (new ETag) yields a **new** id: the new content is
//!   new work, never silently skipped against stale progress.
//! - A change to the version retires every old id as an explicit epoch.
//!
//! The digest is truncated SHA-256: ids are persisted identity, and a
//! collision would silently drop one member set, so the digest must hold up
//! even for adversarially-named keys.
//!
//! # Cutting
//!
//! An object above the target that [`Packing::delimiter_for`] accepts is cut
//! by [`tile`] into byte ranges of at most the target, one unit each; any
//! other object is read whole.

use crate::config::Compression;
use crate::fetch::ObjectEntry;
use crate::framer::Codec;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

/// Version of the [`SplitDescriptor`] wire encoding. Bumped on any change
/// to the descriptor's schema; a worker refuses a descriptor written by an
/// incompatible release instead of misreading it.
pub const DESCRIPTOR_VERSION: u32 = 2;

/// The most member objects a descriptor may name for the source to read it.
/// A unit naming more is failed back. Far below the composite offset's
/// ordinal space, so no unit's ordinal overflows it.
pub const MAX_UNIT_MEMBERS: usize = 4096;

/// Version folded into every id by [`split_id_for`] and
/// [`split_id_for_range`]. It is part of the persisted identity, so a change
/// retires every id already minted.
pub(crate) const PACKING_VERSION: u32 = 2;

/// Largest object a unit is cut into byte ranges: 50,000 GiB, the largest
/// object S3 can hold (10,000 parts of 5 GiB). A listing reporting a larger
/// size is read whole.
pub(crate) const MAX_SUBDIVIDABLE_BYTES: u64 = 50_000 << 30;

/// Why a unit descriptor or id could not be built or decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SplitError {
    /// What was wrong, naming the object or range where there is one.
    pub reason: String,
}

impl SplitError {
    fn new(reason: impl Into<String>) -> SplitError {
        SplitError {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for SplitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

impl std::error::Error for SplitError {}

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

/// A byte range `[start, end)` of one object, read as a unit of its own.
///
/// The range owns the records whose first byte lies in `[start, end)`, where a
/// record starts at byte 0 and after every `delimiter` byte, so each record of
/// an object belongs to exactly one range of a tiling. The object must be
/// uncompressed. When the reader's `compression` setting decodes the key, the
/// reader fails the unit back to the feed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SplitRange {
    /// First byte of the range.
    pub start: u64,
    /// One past the last byte of the range.
    pub end: u64,
    /// The framer's [`resync_delimiter`](spate_core::framing::RecordFramer::resync_delimiter).
    /// A reader whose framer declares a different one refuses the unit.
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

/// The body of a feed unit that names objects to read: the unit's member
/// objects, in listing order.
///
/// The encoding is versioned JSON ([`DESCRIPTOR_VERSION`]); member order is
/// meaningful (composite offsets index into it). Out-of-process producers
/// (an event-notification handler, a single-shot invocation minting one
/// unit from an S3 event) construct via [`SplitDescriptor::new`] or
/// [`SplitDescriptor::with_range`] (which stamp the version;
/// [`encode`](SplitDescriptor::encode) refuses anything else) and mint ids
/// with [`split_id_for`] or [`split_id_for_range`] respectively, which
/// together are the whole cross-process contract. Fields are freely readable.
///
/// The source reads a descriptor of one member whole, in byte ranges when the
/// member qualifies for cutting, or as the byte range the descriptor names. It
/// reads a descriptor of two or more members whole, member by member in order,
/// on one lane, and a member named twice is read once per mention. It fails
/// back a descriptor that names no member or more than [`MAX_UNIT_MEMBERS`],
/// carries an ETag `If-Match` cannot send, or names a byte range cut on a
/// delimiter other than the framer's or of an object the source decompresses.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitDescriptor {
    /// Encoding version; always [`DESCRIPTOR_VERSION`] at write. Private to
    /// construction ([`SplitDescriptor::new`]): a hand-written version
    /// would ship a descriptor every leasing worker fails on, fleet-wide.
    pub(crate) v: u32,
    /// Member objects, in listing (and therefore read) order.
    pub objects: Vec<DescriptorObject>,
    /// The byte range of the one member this unit reads, or `None` when it
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
    #[cfg(test)]
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
    /// When the descriptor's
    /// version is not [`DESCRIPTOR_VERSION`], or when it has a range and
    /// not exactly one member, a member without an ETag, or a range that is
    /// empty or extends past the member's size.
    pub fn encode(&self) -> Result<Vec<u8>, SplitError> {
        if self.v != DESCRIPTOR_VERSION {
            return Err(SplitError::new(format!(
                "descriptor version {} is not the supported {DESCRIPTOR_VERSION}; \
                     construct via SplitDescriptor::new",
                self.v
            )));
        }
        self.validate_range()?;
        Ok(serde_json::to_vec(self).expect("descriptor serialization is infallible: no non-string map keys, no fallible Serialize impls"))
    }

    /// Decode a descriptor, probing the version first.
    ///
    /// # Errors
    ///
    /// When the bytes do not parse,
    /// were written under a different [`DESCRIPTOR_VERSION`], or carry a range
    /// [`encode`](SplitDescriptor::encode) would refuse. A worker must never
    /// guess at an incompatible descriptor.
    pub fn decode(bytes: &[u8]) -> Result<SplitDescriptor, SplitError> {
        let fatal = SplitError::new;
        let probe: VersionProbe = serde_json::from_slice(bytes)
            .map_err(|e| fatal(format!("unit descriptor is not valid JSON: {e}")))?;
        if probe.v != DESCRIPTOR_VERSION {
            return Err(fatal(format!(
                "unit descriptor version {} is not this release's version \
                 {DESCRIPTOR_VERSION}; the unit was written by an incompatible release",
                probe.v
            )));
        }
        let descriptor: SplitDescriptor = serde_json::from_slice(bytes)
            .map_err(|e| fatal(format!("unit descriptor failed to decode: {e}")))?;
        descriptor.validate_range()?;
        Ok(descriptor)
    }

    /// Refuse a ranged descriptor unless it has exactly one member, that
    /// member has an ETag, and `start < end <= size`.
    fn validate_range(&self) -> Result<(), SplitError> {
        let Some(range) = self.range else {
            return Ok(());
        };
        let fatal = SplitError::new;
        let [object] = self.objects.as_slice() else {
            return Err(fatal(format!(
                "a ranged unit descriptor has exactly one member, this one has {}",
                self.objects.len()
            )));
        };
        if object.etag.is_none() {
            return Err(fatal(format!(
                "ranged unit descriptor over \"{}\" has no ETag to pin its reads to",
                object.key
            )));
        }
        if range.start >= range.end || range.end > object.size {
            return Err(fatal(format!(
                "ranged unit descriptor over \"{}\" has range [{}, {}), which is empty or \
                 extends past the object's {} bytes",
                object.key, range.start, range.end, object.size
            )));
        }
        Ok(())
    }
}

/// Mint the deterministic unit id for a member set.
///
/// `members` are `(key, etag)` pairs; order does not matter (they are
/// sorted by key before digesting). The id digests keys, ETags, and the
/// version; see the module docs for why each is included. The
/// result is always a valid id: 25 bytes of `[A-Za-z0-9_-]`
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
/// assert!(id.starts_with("s3-"));
/// assert_eq!(id.len(), 25);
/// ```
///
/// # Errors
///
/// When the member set is empty; a unit with no members is meaningless.
pub fn split_id_for<'a, I>(members: I) -> Result<String, SplitError>
where
    I: IntoIterator<Item = (&'a str, Option<&'a str>)>,
{
    split_id_with_version(members, PACKING_VERSION)
}

/// [`split_id_for`] with an explicit packing version. The seam lets tests
/// pin version sensitivity.
fn split_id_with_version<'a, I>(members: I, version: u32) -> Result<String, SplitError>
where
    I: IntoIterator<Item = (&'a str, Option<&'a str>)>,
{
    let mut members: Vec<(&str, Option<&str>)> = members.into_iter().collect();
    if members.is_empty() {
        return Err(SplitError::new(
            "cannot mint a unit id for an empty member set",
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

/// Mint the deterministic unit id for the byte `range` of one object.
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
/// assert_eq!(id.len(), 25);
/// ```
///
/// # Errors
///
/// When `range.start >= range.end`.
pub fn split_id_for_range(key: &str, etag: &str, range: SplitRange) -> Result<String, SplitError> {
    if range.start >= range.end {
        return Err(SplitError::new(format!(
            "cannot mint a unit id for the empty range [{}, {})",
            range.start, range.end
        )));
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
fn id_from_digest(hasher: Sha256) -> Result<String, SplitError> {
    let digest = hasher.finalize();
    Ok(format!("s3-{}", URL_SAFE_NO_PAD.encode(&digest[..16])))
}

/// What cutting a unit into ranges needs besides the object.
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

/// Cut `[0, size)` into `ceil(size / target)` contiguous ranges, each
/// between `target / 2` and `target` bytes long when `size > target`.
pub(crate) fn tile(size: u64, target: u64, delimiter: u8) -> impl Iterator<Item = SplitRange> {
    let n = size.div_ceil(target);
    // u128: `i * size` reaches about 2^71 for the largest object at the 1 MiB
    // target floor.
    let boundary = move |i: u64| (u128::from(i) * u128::from(size) / u128::from(n)) as u64;
    (0..n).map(move |i| SplitRange::new(boundary(i), boundary(i + 1), delimiter))
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
        assert_eq!(id, "s3-xje_MfL-wSNqMCft5ellCg");
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
        let err = split_id_for(std::iter::empty()).unwrap_err();
        assert_eq!(err.to_string(), err.reason);
        assert!(err.reason.contains("empty member set"), "{}", err.reason);
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
        assert!(err.reason.contains("version 1"), "reason: {}", err.reason);
    }

    #[test]
    fn unknown_descriptor_version_is_rejected_actionably() {
        let err = SplitDescriptor::decode(br#"{"v":999,"objects":[]}"#).unwrap_err();
        assert!(err.reason.contains("version 999"), "reason: {}", err.reason);
        assert!(
            err.reason.contains("incompatible release"),
            "reason: {}",
            err.reason
        );

        let garbage = SplitDescriptor::decode(b"not json").unwrap_err();
        assert!(
            garbage.reason.contains("not valid JSON"),
            "{}",
            garbage.reason
        );
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
            assert!(err.reason.contains(needle), "encode: {}", err.reason);
            // The same bytes written by a producer that skipped validation.
            let raw = serde_json::to_vec(&desc).unwrap();
            let err = SplitDescriptor::decode(&raw).unwrap_err();
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
        assert_eq!(id, "s3-fA0wNRW2MyDxquwFvNy0JA");
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

    /// An object above [`MAX_SUBDIVIDABLE_BYTES`] is read whole, and one at
    /// the limit, or of 6 TiB, is cut.
    #[test]
    fn an_object_above_the_subdivision_limit_stays_whole() {
        let packing = cut(1 << 30);
        assert_eq!(packing.delimiter_for(&entry("6tib", 6 << 40)), Some(b'\n'));
        let at = entry("at", MAX_SUBDIVIDABLE_BYTES);
        assert_eq!(packing.delimiter_for(&at), Some(b'\n'));
        let over = entry("over", MAX_SUBDIVIDABLE_BYTES + 1);
        assert_eq!(packing.delimiter_for(&over), None);
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
                packing.delimiter_for(&object),
                None,
                "{packing:?} {object:?}"
            );
        }
        assert_eq!(
            cut(target).delimiter_for(&entry("over.ndjson", target + 1)),
            Some(b'\n'),
            "one byte above the target is cut"
        );
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

    proptest! {
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
        fn prop_split_ids_are_valid_for_arbitrary_keys(
            keys in proptest::collection::btree_set("[ -~]{1,64}", 1..8),
            etag in proptest::option::of("[ -~]{1,16}"),
        ) {
            // S3 keys contain '/', '.', spaces, '%', anything printable —
            // none of it may leak into the id (charset [A-Za-z0-9_-]).
            let members: Vec<(&str, Option<&str>)> =
                keys.iter().map(|k| (k.as_str(), etag.as_deref())).collect();
            let id = split_id_for(members).unwrap();
            prop_assert!(id.starts_with("s3-"));
            prop_assert_eq!(id.len(), 25);
        }

    }
}
