//! Work units from a listing: [`plan`] turns a sorted listing into one unit
//! per object, or one per byte range of an object cut as ADR-0054 states.
//!
//! A unit's id depends only on its object's key and ETag, its range, and the
//! configuration that cuts ranges, through the derivations of
//! [`split_id_for`](crate::split_id_for) and
//! [`split_id_for_range`](crate::split_id_for_range). The same listing under
//! the same configuration mints the same units, whatever the listing's page
//! size and wherever a resumed listing starts. Nothing is packed, so a unit
//! never names two objects.

use crate::fetch::ObjectEntry;
use crate::split::{Packed, Packing, tile};
use spate_core::coordination::CoordinationError;
use spate_core::feed::Unit;
use std::rc::Rc;

/// The units covering `entries`, in listing order and, within an object, in
/// range order.
///
/// An entry `packing` cuts becomes one unit per range, and any other becomes
/// one whole-object unit. Each of `unaddressable`, the objects whose keys the
/// store cannot address, becomes one whole-object unit under its raw key, so
/// its read poisons that unit alone. One with no bytes holds no records and
/// gets no unit, and keys listed twice get one unit, from the largest.
///
/// # Errors
///
/// [`Fatal`](spate_core::coordination::CoordinationErrorKind::Fatal) when a
/// unit's id or descriptor cannot be built, which a listing of non-empty
/// ranges and ETag-pinned cut objects never causes.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "this crate publishes no units of its own")
)]
pub(crate) fn plan(
    entries: Vec<ObjectEntry>,
    mut unaddressable: Vec<ObjectEntry>,
    packing: &Packing,
) -> Result<Vec<Unit>, CoordinationError> {
    // XML end-of-line handling decodes keys that differ only in CR, LF or
    // CRLF to one key. The largest is kept, so an empty one cannot hide one
    // with records.
    unaddressable.sort_unstable_by(|a, b| a.key.cmp(&b.key).then(b.size.cmp(&a.size)));
    unaddressable.dedup_by(|a, b| a.key == b.key);

    let mut units = Vec::with_capacity(entries.len() + unaddressable.len());
    let mut push = |packed: Packed| -> Result<(), CoordinationError> {
        let id = packed.id()?;
        units.push(Unit::new(id.as_str(), packed.descriptor().encode()?));
        Ok(())
    };
    for entry in entries {
        match packing.delimiter_for(&entry) {
            Some(delimiter) => {
                let ranges = tile(entry.size, packing.target_bytes, delimiter);
                let entry = Rc::new(entry);
                for range in ranges {
                    push(Packed::Range(Rc::clone(&entry), range))?;
                }
            }
            None => push(Packed::Objects(vec![entry]))?,
        }
    }
    for entry in unaddressable {
        if entry.size == 0 {
            tracing::debug!(key = %entry.key, "not planning an empty object whose key the store cannot address");
            continue;
        }
        push(Packed::Objects(vec![entry]))?;
    }
    Ok(units)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Compression;
    use crate::split::{SplitDescriptor, SplitRange, split_id_for, split_id_for_range};
    use proptest::prelude::*;

    const MB: u64 = 1024 * 1024;

    fn cut(target_bytes: u64) -> Packing {
        Packing {
            target_bytes,
            compression: Compression::Auto,
            delimiter: Some(b'\n'),
        }
    }

    /// A listing entry whose shape `kind` picks: plain, gzip, zstd, or plain
    /// without an ETag.
    fn shaped(i: usize, size: u64, kind: u8) -> ObjectEntry {
        ObjectEntry {
            key: format!(
                "k{i:04}.ndjson{}",
                ["", ".gz", ".zst", ""][kind as usize % 4]
            ),
            size,
            etag: (kind % 4 != 3).then(|| format!("\"etag-{i}\"")),
            last_modified_ms: 1_760_000_000_000,
        }
    }

    /// The object a unit names and its range, decoded from its body.
    fn decoded(unit: &Unit) -> (ObjectEntry, Option<SplitRange>) {
        let descriptor = SplitDescriptor::decode(&unit.body).unwrap();
        let [object] = <[ObjectEntry; 1]>::try_from(descriptor.to_entries()).unwrap();
        (object, descriptor.range)
    }

    proptest! {
        /// Pins the unit shape: every object of the listing is named by one
        /// whole-object unit, or by units tiling it in order with ranges, and
        /// by no other unit. Each id is the public derivation over the
        /// unit's key, ETag and range, and planning again mints the same
        /// units.
        #[test]
        fn prop_units_tile_the_listing_one_object_or_range_each(
            objects in proptest::collection::vec((0u64..300 * MB, 0u8..4), 0..120),
            unaddressable in proptest::collection::vec(0u64..300 * MB, 0..4),
            target_mb in 1u64..129,
        ) {
            let listing: Vec<ObjectEntry> = objects
                .iter()
                .enumerate()
                .map(|(i, &(size, kind))| shaped(i, size, kind))
                .collect();
            let odd: Vec<ObjectEntry> = unaddressable
                .iter()
                .enumerate()
                .map(|(i, &size)| ObjectEntry {
                    key: format!("/odd-{i}"),
                    ..shaped(i, size, 0)
                })
                .collect();
            let packing = cut(target_mb * MB);
            let units = plan(listing.clone(), odd.clone(), &packing).unwrap();
            let again = plan(listing.clone(), odd.clone(), &packing).unwrap();
            prop_assert_eq!(&units, &again);

            let reads: Vec<(ObjectEntry, Option<SplitRange>)> = units.iter().map(decoded).collect();
            for (unit, (object, range)) in units.iter().zip(&reads) {
                let id = match range {
                    Some(range) => split_id_for_range(
                        &object.key,
                        object.etag.as_deref().unwrap(),
                        *range,
                    ),
                    None => split_id_for([(object.key.as_str(), object.etag.as_deref())]),
                };
                let id = id.unwrap();
                prop_assert_eq!(unit.id.as_str(), id.as_str());
            }
            let expected = listing
                .iter()
                .chain(odd.iter().filter(|o| o.size > 0));
            let mut at = 0;
            for object in expected {
                let named: Vec<Option<SplitRange>> = reads
                    .iter()
                    .filter(|(o, _)| o == object)
                    .map(|(_, range)| *range)
                    .collect();
                prop_assert!(!named.is_empty(), "{:?} has no unit", object);
                // Units come in listing order, an object's units together.
                prop_assert!(reads[at..at + named.len()].iter().all(|(o, _)| o == object));
                at += named.len();
                if named == [None] {
                    prop_assert!(
                        packing.delimiter_for(object).is_none() || object.key.starts_with('/'),
                        "{:?} qualifies for cutting but is read whole",
                        object
                    );
                    continue;
                }
                let mut next = 0;
                for range in named {
                    let range = range.expect("a cut object has no whole-object unit");
                    prop_assert_eq!(range.start, next);
                    next = range.end;
                }
                prop_assert_eq!(next, object.size);
            }
            prop_assert_eq!(at, reads.len(), "a unit names no listed object");
        }
    }

    /// Pins that a unit's id follows its object's ETag: an object rewritten
    /// in place mints new ids for its whole-object unit and for its ranges.
    #[test]
    fn units_for_a_changed_etag_get_new_ids() {
        let small = shaped(0, MB, 0);
        let large = shaped(1, 3 * MB, 0);
        let ids = |etag: &str| -> Vec<String> {
            let listing = [&small, &large]
                .into_iter()
                .map(|e| ObjectEntry {
                    etag: Some(etag.to_owned()),
                    ..e.clone()
                })
                .collect();
            plan(listing, Vec::new(), &cut(2 * MB))
                .unwrap()
                .into_iter()
                .map(|u| u.id)
                .collect()
        };
        let before = ids("\"a\"");
        let after = ids("\"b\"");
        assert_eq!(before.len(), 3, "one whole unit and two ranges");
        assert!(
            before.iter().all(|id| !after.contains(id)),
            "{before:?} and {after:?} share an id"
        );
    }

    /// Pins that an unaddressable object is never cut, a key listed twice
    /// gets one unit from its largest listing, and an empty one gets none.
    #[test]
    fn unaddressable_objects_are_planned_whole_once_and_never_empty() {
        let odd = |key: &str, size: u64| ObjectEntry {
            key: key.to_owned(),
            ..shaped(0, size, 0)
        };
        let units = plan(
            Vec::new(),
            vec![odd("/a", 3 * MB), odd("/a", 1), odd("b/", 0)],
            &cut(MB),
        )
        .unwrap();
        let reads: Vec<(ObjectEntry, Option<SplitRange>)> = units.iter().map(decoded).collect();
        assert_eq!(reads, [(odd("/a", 3 * MB), None)]);
    }
}
