//! Work units of the feed path: each names one object, or one byte range of
//! one object.
//!
//! [`parts_of`] decides whether an object is read whole or cut, by the rule
//! the planner applies to a listing ([`Packing::delimiter_for`] and
//! [`tile`]), and [`units`] mints one feed unit per part. The reader cuts a
//! whole-object unit through the same [`Cursor`], so a unit's ranges are the
//! same whichever side cuts them.

use crate::fetch::ObjectEntry;
use crate::listing::addressable;
use crate::split::{
    DescriptorObject, Packing, SplitDescriptor, SplitRange, split_id_for, split_id_for_range, tile,
};
use spate_core::coordination::CoordinationError;
use spate_core::feed::Unit;
use std::iter::Peekable;
use std::sync::Arc;

/// One part of a unit: the object, whole or in one byte range.
pub(crate) struct Part {
    /// The one object the part reads, shared by every part of the object.
    pub(crate) object: Arc<Vec<ObjectEntry>>,
    /// The byte range read, or `None` for the whole object.
    pub(crate) range: Option<SplitRange>,
}

/// The parts of one object, yielded lazily so an object cut into many
/// ranges holds one range at a time.
pub(crate) struct Cursor {
    /// The one object every part clones.
    pub(crate) object: Arc<Vec<ObjectEntry>>,
    /// The ranges still to yield, or `None` when the object is read whole.
    ranges: Option<Peekable<Box<dyn Iterator<Item = SplitRange> + Send>>>,
    /// The whole part is still to yield.
    whole: bool,
}

impl Cursor {
    /// A cursor yielding `object` whole, once.
    fn whole(object: Arc<Vec<ObjectEntry>>) -> Cursor {
        Cursor {
            object,
            ranges: None,
            whole: true,
        }
    }

    /// A cursor yielding the one byte `range` of `object`.
    pub(crate) fn range(object: Arc<Vec<ObjectEntry>>, range: SplitRange) -> Cursor {
        Cursor::ranges(object, Box::new(std::iter::once(range)))
    }

    fn ranges(
        object: Arc<Vec<ObjectEntry>>,
        ranges: Box<dyn Iterator<Item = SplitRange> + Send>,
    ) -> Cursor {
        Cursor {
            object,
            ranges: Some(ranges.peekable()),
            whole: false,
        }
    }

    /// Whether every part has been yielded.
    pub(crate) fn is_exhausted(&mut self) -> bool {
        match &mut self.ranges {
            Some(ranges) => ranges.peek().is_none(),
            None => !self.whole,
        }
    }
}

impl Iterator for Cursor {
    type Item = Part;

    fn next(&mut self) -> Option<Part> {
        let range = match &mut self.ranges {
            Some(ranges) => Some(ranges.next()?),
            None => {
                if !std::mem::take(&mut self.whole) {
                    return None;
                }
                None
            }
        };
        Some(Part {
            object: Arc::clone(&self.object),
            range,
        })
    }
}

/// The parts of the one object in `object`: the ranges [`tile`] cuts when
/// [`Packing::delimiter_for`] accepts the object and its key is
/// [`addressable`], and the whole object otherwise.
pub(crate) fn parts_of(object: Arc<Vec<ObjectEntry>>, packing: Packing) -> Cursor {
    debug_assert_eq!(object.len(), 1, "a unit names one object");
    let entry = &object[0];
    match packing
        .delimiter_for(entry)
        .filter(|_| addressable(&entry.key))
    {
        Some(delimiter) => {
            let ranges = tile(entry.size, packing.target_bytes, delimiter);
            Cursor::ranges(object, Box::new(ranges))
        }
        None => Cursor::whole(object),
    }
}

/// One feed unit per part of each listed object, in listing order.
///
/// A whole part's id is [`split_id_for`] over its key and ETag, and a range
/// part's is [`split_id_for_range`], so an id depends only on the key, the
/// ETag, the range and the range configuration.
///
/// # Errors
///
/// [`Fatal`](spate_core::coordination::CoordinationErrorKind::Fatal) when a
/// descriptor or an id cannot be built, which no listed object causes.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "no lister builds feed units yet")
)]
pub(crate) fn units(
    entries: Vec<ObjectEntry>,
    packing: &Packing,
) -> Result<Vec<Unit>, CoordinationError> {
    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        let object = DescriptorObject {
            key: entry.key.clone(),
            size: entry.size,
            etag: entry.etag.clone(),
            last_modified_ms: entry.last_modified_ms,
        };
        for part in parts_of(Arc::new(vec![entry]), *packing) {
            let (id, descriptor) = match part.range {
                None => (
                    split_id_for([(object.key.as_str(), object.etag.as_deref())])?,
                    SplitDescriptor::new(vec![object.clone()]),
                ),
                Some(range) => (
                    split_id_for_range(
                        &object.key,
                        object
                            .etag
                            .as_deref()
                            .expect("an object is cut only when it has an ETag"),
                        range,
                    )?,
                    SplitDescriptor::with_range(object.clone(), range),
                ),
            };
            out.push(Unit::new(id.as_str(), descriptor.encode()?));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Compression;
    use crate::split::MAX_SUBDIVIDABLE_BYTES;
    use proptest::prelude::*;

    const MIB: u64 = 1 << 20;
    const GIB: u64 = 1 << 30;

    const EXTENSIONS: [&str; 6] = ["", ".ndjson", ".gz", ".gzip", ".zst", ".zstd"];

    /// Key shapes: one `Path::parse` keeps, three it rejects and two it
    /// rewrites.
    fn key(i: usize, shape: u8, ext: &str) -> (String, bool) {
        match shape {
            0 => (format!("a/k{i}{ext}"), false),
            1 => (format!("a//k{i}{ext}"), true),
            2 => (format!("a/../k{i}{ext}"), true),
            3 => (format!("a/k\u{1}{i}{ext}"), true),
            4 => (format!("/a/k{i}{ext}"), true),
            _ => (format!("a/k{i}{ext}/"), true),
        }
    }

    fn compression(n: u8) -> Compression {
        match n {
            0 => Compression::Auto,
            1 => Compression::None,
            2 => Compression::Gzip,
            _ => Compression::Zstd,
        }
    }

    /// Whether the object is read uncompressed, decided without
    /// `Codec::resolve`.
    fn plain(compression: Compression, key: &str) -> bool {
        match compression {
            Compression::None => true,
            Compression::Auto => ![".gz", ".gzip", ".zst", ".zstd"]
                .iter()
                .any(|ext| key.ends_with(ext)),
            _ => false,
        }
    }

    fn size(target: u64, shape: u8, random: u64) -> u64 {
        match shape {
            0 => target,
            1 => target + 1,
            2 if target >= GIB => MAX_SUBDIVIDABLE_BYTES - 1,
            3 if target >= GIB => MAX_SUBDIVIDABLE_BYTES,
            4 if target >= GIB => MAX_SUBDIVIDABLE_BYTES + 1,
            5 => 0,
            _ => random % (4 * target),
        }
    }

    fn target() -> impl Strategy<Value = u64> {
        prop_oneof![MIB..=64 * MIB, 16 * GIB..=64 * GIB]
    }

    /// Pins that an object is cut lazily. At a one-byte target the largest
    /// object the cut accepts has more ranges than memory can hold at once,
    /// and its cursor still yields the first two.
    #[test]
    fn the_cut_yields_one_range_at_a_time() {
        let packing = Packing {
            target_bytes: 1,
            compression: Compression::None,
            delimiter: Some(b'\n'),
        };
        let entry = ObjectEntry {
            key: "a.ndjson".into(),
            size: MAX_SUBDIVIDABLE_BYTES,
            etag: Some("\"etag\"".into()),
            last_modified_ms: 1,
        };
        let mut cursor = parts_of(Arc::new(vec![entry]), packing);
        let first: Vec<(u64, u64)> = cursor
            .by_ref()
            .take(2)
            .map(|part| {
                let range = part.range.expect("the object is cut");
                (range.start, range.end)
            })
            .collect();
        assert_eq!(first, [(0, 1), (1, 2)]);
        assert!(!cursor.is_exhausted());
    }

    /// Pins that the cut marks each range with the framer's delimiter.
    #[test]
    fn the_cut_marks_each_range_with_the_framers_delimiter() {
        let packing = Packing {
            target_bytes: MIB,
            compression: Compression::None,
            delimiter: Some(b'\r'),
        };
        let entry = ObjectEntry {
            key: "a.csv".into(),
            size: 3 * MIB,
            etag: Some("\"etag\"".into()),
            last_modified_ms: 1,
        };
        let delimiters: Vec<u8> = parts_of(Arc::new(vec![entry]), packing)
            .map(|part| part.range.expect("the object is cut").delimiter)
            .collect();
        assert_eq!(delimiters, [b'\r'; 3]);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Pins that every listed object yields one whole unit, or ranges
        /// that tile `[0, size)` with each between half the target and the
        /// target, and that each id is the one `split_id_for` or
        /// `split_id_for_range` mints, the same on every call and different
        /// under another ETag.
        #[test]
        fn prop_units_tile_the_listing_one_object_or_range_each(
            objects in proptest::collection::vec(
                (0u8..7, any::<u64>(), 0usize..6, 0u8..6, any::<bool>()),
                0..12,
            ),
            target in target(),
            codec in 0u8..4,
            delimiter in proptest::option::of(Just(b'\n')),
        ) {
            let compression = compression(codec);
            let packing = Packing { target_bytes: target, compression, delimiter };
            let listing: Vec<(ObjectEntry, bool)> = objects
                .iter()
                .enumerate()
                .map(|(i, &(size_shape, random, ext, key_shape, etag))| {
                    let (key, unaddressable) = key(i, key_shape, EXTENSIONS[ext]);
                    let entry = ObjectEntry {
                        etag: etag.then(|| format!("\"etag-{i}\"")),
                        key,
                        size: size(target, size_shape, random),
                        last_modified_ms: 1,
                    };
                    (entry, unaddressable)
                })
                .collect();
            let entries: Vec<ObjectEntry> = listing.iter().map(|(e, _)| e.clone()).collect();
            let got = units(entries.clone(), &packing).unwrap();
            let again = units(entries.clone(), &packing).unwrap();
            prop_assert_eq!(&got, &again, "units differ between two calls");

            let mut seen = 0;
            for (entry, unaddressable) in &listing {
                let mine: Vec<(String, SplitDescriptor)> = got
                    .iter()
                    .map(|u| (u.id.clone(), SplitDescriptor::decode(&u.body).unwrap()))
                    .filter(|(_, d)| d.objects[0].key == entry.key)
                    .collect();
                seen += mine.len();
                for (_, d) in &mine {
                    prop_assert_eq!(d.objects.len(), 1);
                    prop_assert_eq!(&d.to_entries()[0], entry);
                }
                let cut = delimiter.is_some()
                    && entry.size > target
                    && entry.size <= MAX_SUBDIVIDABLE_BYTES
                    && entry.etag.is_some()
                    && plain(compression, &entry.key)
                    && !unaddressable;
                if cut {
                    let mut next = 0;
                    for (id, d) in &mine {
                        let range = d.range.expect("a cut object yields only ranges");
                        prop_assert_eq!(range.start, next);
                        let len = range.end - range.start;
                        prop_assert!(target / 2 <= len && len <= target, "{:?}", range);
                        prop_assert_eq!(Some(range.delimiter), delimiter);
                        let want = split_id_for_range(
                            &entry.key,
                            entry.etag.as_deref().unwrap(),
                            range,
                        )
                        .unwrap();
                        prop_assert_eq!(id.as_str(), want.as_str());
                        next = range.end;
                    }
                    prop_assert_eq!(next, entry.size, "the ranges do not tile the object");
                } else {
                    prop_assert_eq!(mine.len(), 1, "{:?} is not one whole unit", entry);
                    let (id, d) = &mine[0];
                    prop_assert_eq!(d.range, None);
                    let want =
                        split_id_for([(entry.key.as_str(), entry.etag.as_deref())]).unwrap();
                    prop_assert_eq!(id.as_str(), want.as_str());
                }
            }
            prop_assert_eq!(seen, got.len(), "a unit names no listed object");

            let retagged: Vec<ObjectEntry> = entries
                .iter()
                .map(|e| ObjectEntry {
                    etag: e.etag.as_ref().map(|t| format!("{t}-2")),
                    ..e.clone()
                })
                .collect();
            let other = units(retagged, &packing).unwrap();
            for (a, b) in got.iter().zip(&other) {
                let tagged = SplitDescriptor::decode(&a.body).unwrap().objects[0].etag.is_some();
                if tagged {
                    prop_assert_ne!(&a.id, &b.id, "the id ignores the ETag");
                }
            }
        }
    }
}
