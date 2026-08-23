//! The composite offset codec over arbitrary offsets and positions.
//!
//! The decode arm takes an arbitrary non-negative `i64` and asserts it decodes
//! to a position whose fields are both in range and which re-encodes to the
//! offset it came from. The encode arm takes an arbitrary position and asserts
//! it encodes exactly when both fields are in range, and that the offset
//! decodes back to it. A third arm asserts two offsets order the way the
//! positions they carry do.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use spate_s3::fuzz_seams::{MAX_ORDINAL, MAX_RECORD_INDEX, decode_position, encode_position};

#[derive(Arbitrary, Debug)]
struct Input {
    /// Offsets as a store returns them. The sign bit is cleared below.
    offsets: [i64; 2],
    /// Positions as `(ordinal, record)`, in range or out of it.
    positions: [(u32, u64); 2],
}

fuzz_target!(|input: Input| {
    // `decode_position` takes non-negative offsets only.
    let offsets = input.offsets.map(|offset| offset & i64::MAX);

    for offset in offsets {
        let (ordinal, record) = decode_position(offset);
        assert!(
            ordinal <= MAX_ORDINAL,
            "offset {offset} decoded to ordinal {ordinal}, above {MAX_ORDINAL}"
        );
        assert!(
            record <= MAX_RECORD_INDEX + 1,
            "offset {offset} decoded to record {record}, above the reserved index"
        );
        assert_eq!(
            encode_position(ordinal, record),
            Some(offset),
            "offset {offset} decoded to ({ordinal}, {record}), which encodes elsewhere"
        );
    }

    assert_eq!(
        offsets[0].cmp(&offsets[1]),
        decode_position(offsets[0]).cmp(&decode_position(offsets[1])),
        "offsets {offsets:?} order differently from the positions they carry"
    );

    for (ordinal, record) in input.positions {
        let in_range = ordinal <= MAX_ORDINAL && record <= MAX_RECORD_INDEX + 1;
        match encode_position(ordinal, record) {
            Some(offset) => {
                assert!(in_range, "({ordinal}, {record}) encoded from out of range");
                assert!(offset >= 0, "({ordinal}, {record}) encoded to {offset}");
                assert_eq!(
                    decode_position(offset),
                    (ordinal, record),
                    "({ordinal}, {record}) changed across encode and decode"
                );
            }
            None => assert!(!in_range, "({ordinal}, {record}) is in range and refused"),
        }
    }
});
