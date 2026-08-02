//! Split descriptors, deterministic split identity, and listing-order
//! packing — the shared vocabulary between the leader's planner and every
//! split reader.
//!
//! A **split** is a small batch of whole objects read as one leasable unit
//! of work. The planner packs the sorted listing into splits; the
//! [`SplitDescriptor`] carries each split's member objects (keys, sizes,
//! ETags) verbatim to whichever worker gains it, so workers never list.
//!
//! # Identity
//!
//! [`split_id_for`] digests the member set (keys **and** ETags) plus the
//! packing-algorithm version into a stable [`SplitId`]. The consequences
//! are load-bearing:
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
//! into one of a bounded window of open bins (no sorting by size: packing
//! stays a pure, streamable function of the listing and preserves prefix
//! locality). Each object costs at least `target / 16` — the open-cost
//! floor that stops thousands of tiny objects coalescing into one split —
//! so a split holds at most ~16 members and its descriptor stays far below
//! backend value-size caps. An object at or above the target lands alone in
//! its own split.

use crate::fetch::ObjectEntry;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use spate_core::coordination::{CoordinationError, CoordinationErrorKind, SplitId};
use std::collections::VecDeque;

/// Version of the [`SplitDescriptor`] wire encoding. Bumped on any change
/// to the descriptor's schema; a worker refuses a descriptor written by an
/// incompatible release instead of misreading it.
pub const DESCRIPTOR_VERSION: u32 = 1;

/// Version of the packing algorithm, folded into every split id by
/// [`split_id_for`]. Bumping it retires all previously planned ids as an
/// explicit epoch (see the module docs).
pub(crate) const PACKING_VERSION: u32 = 1;

/// Maximum number of bins held open during packing. Bounds planner memory
/// and how far out of listing order a member can land.
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
    /// Last-modified time (ms since epoch) — the records' event time.
    pub last_modified_ms: i64,
}

/// The opaque payload carried in a
/// [`SplitSpec::descriptor`](spate_core::coordination::SplitSpec): the
/// split's member objects, in listing order.
///
/// The encoding is versioned JSON ([`DESCRIPTOR_VERSION`]); member order is
/// meaningful (composite offsets index into it). Out-of-process producers —
/// an event-notification planner, a single-shot invocation minting one
/// split from an S3 event — construct via [`SplitDescriptor::new`] (which
/// stamps the version; [`encode`](SplitDescriptor::encode) refuses anything
/// else) and mint ids with [`split_id_for`], which together are the whole
/// cross-process contract. Fields are freely readable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitDescriptor {
    /// Encoding version; always [`DESCRIPTOR_VERSION`] at write. Private to
    /// construction ([`SplitDescriptor::new`]): a hand-written version
    /// would ship a descriptor every leasing worker fails on, fleet-wide.
    pub(crate) v: u32,
    /// Member objects, in listing (and therefore read) order.
    pub objects: Vec<DescriptorObject>,
}

/// The version probe decoded before the full descriptor, so an
/// incompatible version is reported as such rather than as a parse error.
#[derive(Deserialize)]
struct VersionProbe {
    v: u32,
}

impl SplitDescriptor {
    /// Build a descriptor over `objects` (listing order — ordinals index
    /// into it), stamped with the current [`DESCRIPTOR_VERSION`]. The only
    /// way to construct one; [`encode`](SplitDescriptor::encode) refuses
    /// any other version.
    #[must_use]
    pub fn new(objects: Vec<DescriptorObject>) -> SplitDescriptor {
        SplitDescriptor {
            v: DESCRIPTOR_VERSION,
            objects,
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
    /// version is not [`DESCRIPTOR_VERSION`] — a descriptor written under a
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
        Ok(serde_json::to_vec(self).expect("descriptor serialization is infallible: no non-string map keys, no fallible Serialize impls"))
    }

    /// Decode a descriptor, probing the version first.
    ///
    /// # Errors
    ///
    /// [`Fatal`](CoordinationErrorKind::Fatal) when the bytes do not parse
    /// or were written under a different [`DESCRIPTOR_VERSION`] — a worker
    /// must never guess at an incompatible descriptor.
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
        serde_json::from_slice(bytes)
            .map_err(|e| fatal(format!("split descriptor failed to decode: {e}")))
    }
}

/// Mint the deterministic split id for a member set.
///
/// `members` are `(key, etag)` pairs; order does not matter (they are
/// sorted by key before digesting). The id digests keys, ETags, and the
/// packing version — see the module docs for why each is included. The
/// result is always a valid [`SplitId`]: 25 bytes of `[A-Za-z0-9_-]`
/// regardless of what the keys contain.
///
/// Public so out-of-process producers mint byte-identical ids for the same
/// members. The digest preimage is wire format — precise enough to
/// reimplement in any language:
///
/// 1. Sort the members ascending by key (byte-wise comparison of the
///    UTF-8 key bytes).
/// 2. Feed SHA-256 with, in order:
///    - the domain tag: the 15 ASCII bytes `spate-s3-split\n`;
///    - the packing version as a little-endian `u32` (currently `1` —
///      the crate's `PACKING_VERSION`);
///    - for each member, in sorted order:
///      - the key's byte length as a little-endian `u32`, then the key's
///        UTF-8 bytes;
///      - the ETag presence byte: `0x01` followed by the ETag's byte
///        length as a little-endian `u32` and its UTF-8 bytes when
///        present, the single byte `0x00` when absent.
/// 3. Truncate the 32-byte digest to its first 16 bytes, encode them as
///    base64url without padding (RFC 4648 §5), and prefix `s3-` — a
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
/// [`Fatal`](CoordinationErrorKind::Fatal) for an empty member set — a
/// split with no members is meaningless.
pub fn split_id_for<'a, I>(members: I) -> Result<SplitId, CoordinationError>
where
    I: IntoIterator<Item = (&'a str, Option<&'a str>)>,
{
    split_id_with_version(members, PACKING_VERSION)
}

/// [`split_id_for`] with an explicit packing version — the seam that lets
/// tests pin version sensitivity.
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
    let digest = hasher.finalize();
    SplitId::new(format!("s3-{}", URL_SAFE_NO_PAD.encode(&digest[..16])))
}

/// Pack the sorted listing into splits of roughly `target_bytes` each.
///
/// A pure function of `(entries, target_bytes)`: walking the listing in
/// order, each object costs `max(size, target_bytes / 16)` and first-fits
/// into the oldest of at most [`PACKING_LOOKBACK`] open bins with room; a
/// bin at or above the target closes. An object costing the whole target
/// therefore lands alone in its own split. Returned bins preserve listing
/// order both across bins (by first member) and within each bin.
pub(crate) fn pack(entries: Vec<ObjectEntry>, target_bytes: u64) -> Vec<Vec<ObjectEntry>> {
    debug_assert!(
        target_bytes > 0,
        "config validation rejects a zero split target"
    );
    struct Bin {
        members: Vec<ObjectEntry>,
        cost: u64,
    }
    let floor = (target_bytes / OPEN_COST_DIVISOR).max(1);
    let mut bins: Vec<Bin> = Vec::new();
    // Indexes into `bins` still accepting members, oldest first.
    let mut open: VecDeque<usize> = VecDeque::new();
    for entry in entries {
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
    bins.into_iter().map(|b| b.members).collect()
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
        assert_eq!(id.as_str(), "s3-_rD2rPZklAFVV4pYEYaWxg");
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
            r#"{"v":1,"objects":[{"key":"k","size":5,"etag":"\"etag-k\"","last_modified_ms":1760000000000}]}"#,
        );
    }

    #[test]
    fn encode_refuses_a_descriptor_not_built_by_new() {
        // A hand-written version would ship a descriptor every leasing
        // worker fails pipeline-fatal on; refuse at the producer instead.
        let rogue = SplitDescriptor {
            v: 0,
            objects: vec![],
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

    // --- packing ---

    #[test]
    fn packing_is_deterministic_for_a_fixed_listing() {
        let listing: Vec<ObjectEntry> = (0..200)
            .map(|i| entry(&format!("k{i:04}"), (i % 40) * MB))
            .collect();
        let a = pack(listing.clone(), 64 * MB);
        let b = pack(listing, 64 * MB);
        assert_eq!(a, b);
    }

    #[test]
    fn tiny_objects_coalesce_under_the_open_cost_floor() {
        // 64 tiny objects at a 64 MB target: each costs the 4 MB floor, so
        // exactly 16 fill a bin.
        let listing: Vec<ObjectEntry> = (0..64).map(|i| entry(&format!("k{i:02}"), 1)).collect();
        let bins = pack(listing, 64 * MB);
        assert_eq!(bins.len(), 4);
        assert!(bins.iter().all(|b| b.len() == 16));
    }

    #[test]
    fn adversarial_listing_sizes_do_not_overflow_the_fit_test() {
        // Sizes come from remote listing metadata; a u64::MAX entry must
        // neither panic in debug nor share a bin.
        let listing = vec![
            entry("a", 10 * MB),
            entry("huge", u64::MAX),
            entry("z", 10 * MB),
        ];
        let bins = pack(listing, 64 * MB);
        let huge_bin = bins
            .iter()
            .find(|b| b.iter().any(|e| e.key == "huge"))
            .unwrap();
        assert_eq!(huge_bin.len(), 1, "the oversized object lands alone");
        let total: usize = bins.iter().map(Vec::len).sum();
        assert_eq!(total, 3, "nothing lost, nothing duplicated");
    }

    #[test]
    fn oversized_object_gets_its_own_split() {
        let listing = vec![
            entry("a", 10 * MB),
            entry("huge", 500 * MB),
            entry("b", 10 * MB),
        ];
        let bins = pack(listing, 64 * MB);
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
        let bins = pack(listing, 64 * MB);
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
        let bins = pack(listing, 64 * MB);
        // The half-full bin closed with its single member; every 60 MB
        // object got its own bin.
        assert_eq!(bins.len(), PACKING_LOOKBACK + 3);
        assert!(bins.iter().all(|b| b.len() == 1));
    }

    proptest! {
        #[test]
        fn prop_packing_partitions_the_listing_exactly(
            sizes in proptest::collection::vec(0u64..300 * MB, 0..120),
            target_mb in 1u64..129,
        ) {
            let listing: Vec<ObjectEntry> = sizes
                .iter()
                .enumerate()
                .map(|(i, &s)| entry(&format!("k{i:04}"), s))
                .collect();
            let bins = pack(listing.clone(), target_mb * MB);
            let repacked: Vec<ObjectEntry> = {
                let mut all: Vec<ObjectEntry> = bins.iter().flatten().cloned().collect();
                all.sort_unstable_by(|a, b| a.key.cmp(&b.key));
                all
            };
            // Union of members == listing: nothing lost, nothing duplicated.
            prop_assert_eq!(repacked, listing);
            prop_assert!(bins.iter().all(|b| !b.is_empty()));
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
            let bins = pack(listing, target);
            // floor = target/16 divides target exactly, so a bin never
            // holds more than 16 members — the structural descriptor bound.
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
            let ids = |bins: &[Vec<ObjectEntry>]| -> Vec<SplitId> {
                bins.iter()
                    .map(|b| {
                        split_id_for(b.iter().map(|e| (e.key.as_str(), e.etag.as_deref())))
                            .unwrap()
                    })
                    .collect()
            };
            let a = pack(listing.clone(), 32 * MB);
            let b = pack(listing, 32 * MB);
            prop_assert_eq!(ids(&a), ids(&b));
        }
    }
}
