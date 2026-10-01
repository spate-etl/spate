//! Ranged reads over arbitrary object bytes.
//!
//! A split reading a byte range of an object forwards the records whose first
//! byte lies inside the range. The target tiles an object into fuzzer-chosen
//! ranges, reads each under two fuzzer-chosen window schedules, and asserts
//! that the two schedules forward the same bytes, that each non-empty stream
//! starts on a record, and that the streams concatenate to the object.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use spate_s3::fuzz_seams::read_range;

#[derive(Arbitrary, Debug)]
struct Input {
    object: Vec<u8>,
    delimiter: u8,
    /// Range boundaries, taken modulo the object's length.
    cuts: Vec<u16>,
    /// Two window schedules as `(range_bytes, chunk_bytes)`.
    left: (u8, u8),
    right: (u8, u8),
}

fuzz_target!(|input: Input| {
    let object = input.object.as_slice();
    if object.is_empty() {
        return;
    }
    let len = object.len();
    let mut bounds: Vec<usize> = input
        .cuts
        .iter()
        .map(|cut| usize::from(*cut) % len)
        .chain([0, len])
        .collect();
    bounds.sort_unstable();
    bounds.dedup();

    let mut joined = Vec::with_capacity(len);
    for pair in bounds.windows(2) {
        let (start, end) = (pair[0] as u64, pair[1] as u64);
        let read = |(range_bytes, chunk_bytes): (u8, u8)| {
            read_range(
                object,
                start,
                end,
                input.delimiter,
                u64::from(range_bytes),
                u64::from(chunk_bytes),
            )
            .expect("a non-empty range inside the object")
        };
        let owned = read(input.left);
        assert_eq!(
            owned,
            read(input.right),
            "range [{start}, {end}) forwarded different bytes under two window schedules"
        );
        if !owned.is_empty() {
            let at = joined.len();
            assert!(
                at == 0 || object[at - 1] == input.delimiter,
                "range [{start}, {end}) forwarded bytes from mid-record at {at}"
            );
        }
        joined.extend_from_slice(&owned);
    }
    assert_eq!(joined, object, "the ranges did not tile the object");
});
