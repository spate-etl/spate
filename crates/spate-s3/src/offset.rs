//! Composite record offsets: (object ordinal, record index) packed into the
//! framework's monotonic `i64` offset space.
//!
//! The checkpoint tracker keeps a single monotonic `i64` watermark per
//! partition and advances it to `last_offset + 1` when a batch resolves, so
//! a lane's offsets must be strictly increasing across object boundaries
//! **and** `+ 1` must always land on a decodable position. The layout:
//!
//! ```text
//! bit 63      bits 62..40           bits 39..0
//! [ 0 ][ object ordinal: 23 bits ][ record index: 40 bits ]
//! ```
//!
//! - The sign bit stays clear: every encoded position is a valid,
//!   non-negative `i64`.
//! - Ordinals order objects within one lane's slice of the listing; record
//!   indexes order records within one object. Lexicographic packing makes
//!   the composite ordering match (ordinal, index) ordering exactly.
//! - A record is never emitted at index `MAX_RECORD_INDEX + 1 = 2^40 - 1`
//!   (the lane's emit guard fails fatally first). That index is reserved so
//!   a watermark — "one past the last acknowledged record" — can always be
//!   expressed *inside* the same ordinal, and the `+ 1` never carries into
//!   the ordinal field claiming a later object started.

use std::fmt;

/// Bits reserved for the record index within an object.
const RECORD_BITS: u32 = 40;
/// Bits available for the object ordinal within a lane (63 − 40).
const ORDINAL_BITS: u32 = 23;

/// Mask of the record-index field.
const RECORD_MASK: i64 = (1 << RECORD_BITS) - 1;

/// Highest ordinal a lane may start an object at (`2^23 − 1`).
pub(crate) const MAX_ORDINAL: u32 = (1 << ORDINAL_BITS) - 1;

/// Highest record index that may be *emitted* (`2^40 − 2`). Index
/// `2^40 − 1` is reserved for the end-of-object watermark.
pub(crate) const MAX_RECORD_INDEX: u64 = RECORD_MASK as u64 - 1;

/// A decoded position within a lane's slice: which object (by ordinal in
/// the lane's deterministic listing order) and which record within it.
///
/// As a *watermark* decode, `record` is the index of the **next** record to
/// read: `record == n` means `n` records of the object are already
/// committed. `record` may then equal `MAX_RECORD_INDEX + 1`, the reserved
/// end-of-object index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Position {
    /// Object ordinal within the lane's slice (0-based).
    pub(crate) ordinal: u32,
    /// Record index within the object (0-based).
    pub(crate) record: u64,
}

/// A position that cannot be encoded: the object or the lane outgrew the
/// bit budget. Always fatal — continuing would corrupt watermarks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OffsetOverflow {
    what: &'static str,
    at: Position,
}

impl fmt::Display for OffsetOverflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "composite offset overflow: {} exhausted at ordinal {} record {} \
             (limits: {} objects per lane, {} records per object)",
            self.what,
            self.at.ordinal,
            self.at.record,
            MAX_ORDINAL as u64 + 1,
            MAX_RECORD_INDEX + 1,
        )
    }
}

impl std::error::Error for OffsetOverflow {}

impl Position {
    /// Encode into the framework's `i64` offset space. Fails if either
    /// field is out of range (the emit-side guards make this unreachable
    /// for record offsets; watermarks may use the reserved index).
    pub(crate) fn encode(self) -> Result<i64, OffsetOverflow> {
        if self.ordinal > MAX_ORDINAL {
            return Err(OffsetOverflow {
                what: "object ordinal space",
                at: self,
            });
        }
        // The reserved end-of-object index is encodable (watermarks use it);
        // one past it is not.
        if self.record > MAX_RECORD_INDEX + 1 {
            return Err(OffsetOverflow {
                what: "record index space",
                at: self,
            });
        }
        Ok(((self.ordinal as i64) << RECORD_BITS) | self.record as i64)
    }

    /// Decode a non-negative `i64` offset (or watermark) back into a
    /// position.
    pub(crate) fn decode(offset: i64) -> Position {
        debug_assert!(offset >= 0, "composite offsets are non-negative");
        Position {
            ordinal: (offset >> RECORD_BITS) as u32,
            record: (offset & RECORD_MASK) as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn origin_is_zero_and_roundtrips() {
        let origin = Position {
            ordinal: 0,
            record: 0,
        };
        assert_eq!(origin.encode().unwrap(), 0);
        assert_eq!(Position::decode(0), origin);
    }

    #[test]
    fn watermark_after_last_emittable_record_stays_in_its_object() {
        // A record emitted at MAX_RECORD_INDEX yields watermark E + 1, which
        // must decode to the *reserved* index of the same ordinal — never to
        // (ordinal + 1, 0).
        let last = Position {
            ordinal: 7,
            record: MAX_RECORD_INDEX,
        };
        let watermark = last.encode().unwrap() + 1;
        let decoded = Position::decode(watermark);
        assert_eq!(decoded.ordinal, 7, "no carry into the ordinal field");
        assert_eq!(decoded.record, MAX_RECORD_INDEX + 1, "reserved index");
    }

    #[test]
    fn out_of_range_fields_refuse_to_encode() {
        assert!(
            Position {
                ordinal: MAX_ORDINAL + 1,
                record: 0,
            }
            .encode()
            .is_err()
        );
        assert!(
            Position {
                ordinal: 0,
                record: MAX_RECORD_INDEX + 2,
            }
            .encode()
            .is_err(),
            "one past the reserved watermark index is unencodable"
        );
    }

    #[test]
    fn maximum_watermark_is_i64_max() {
        // The largest expressible watermark — reserved index of the last
        // ordinal — is exactly i64::MAX, and is never itself incremented
        // (nothing can be emitted at the reserved index).
        let max = Position {
            ordinal: MAX_ORDINAL,
            record: MAX_RECORD_INDEX + 1,
        };
        assert_eq!(max.encode().unwrap(), i64::MAX);
    }

    proptest! {
        #[test]
        fn encode_decode_roundtrips(
            ordinal in 0..=MAX_ORDINAL,
            record in 0..=MAX_RECORD_INDEX + 1,
        ) {
            let pos = Position { ordinal, record };
            let encoded = pos.encode().unwrap();
            prop_assert!(encoded >= 0);
            prop_assert_eq!(Position::decode(encoded), pos);
        }

        #[test]
        fn encoding_orders_exactly_like_positions(
            a_ord in 0..=MAX_ORDINAL, a_rec in 0..=MAX_RECORD_INDEX + 1,
            b_ord in 0..=MAX_ORDINAL, b_rec in 0..=MAX_RECORD_INDEX + 1,
        ) {
            let a = Position { ordinal: a_ord, record: a_rec };
            let b = Position { ordinal: b_ord, record: b_rec };
            prop_assert_eq!(
                a.encode().unwrap().cmp(&b.encode().unwrap()),
                a.cmp(&b),
                "i64 ordering must match (ordinal, record) ordering"
            );
        }

        #[test]
        fn watermark_decode_is_the_inverse_of_plus_one(
            ordinal in 0..=MAX_ORDINAL,
            record in 0..=MAX_RECORD_INDEX,
        ) {
            // For any emittable record, watermark = offset + 1 decodes to
            // (same ordinal, record + 1): "next record of the same object".
            let emitted = Position { ordinal, record };
            let watermark = Position::decode(emitted.encode().unwrap() + 1);
            prop_assert_eq!(watermark.ordinal, ordinal);
            prop_assert_eq!(watermark.record, record + 1);
        }

        #[test]
        fn crossing_an_object_boundary_is_strictly_monotonic(
            ordinal in 0..MAX_ORDINAL,
            record in 0..=MAX_RECORD_INDEX + 1,
        ) {
            // The first record of the next object encodes strictly above
            // every position (record offsets and watermarks alike) of the
            // previous object.
            let in_prev = Position { ordinal, record };
            let next = Position { ordinal: ordinal + 1, record: 0 };
            prop_assert!(next.encode().unwrap() > in_prev.encode().unwrap());
        }
    }
}
