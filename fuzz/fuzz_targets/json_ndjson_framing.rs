//! NDJSON framing under arbitrary chunking.
//!
//! `RecordFramer` requires the record sequence to be independent of how bytes
//! are split across `push` calls, which is what makes resume-by-record-index
//! deterministic. The target frames one byte stream twice: once in a single
//! `push`, and once split at fuzzer-chosen offsets, then compares the two
//! outcomes. The record-size cap comes from the input as well, so the cap
//! error is reached under both chunkings or neither.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use spate_core::framing::RecordFramer;
use spate_json::NdjsonFramer;

#[derive(Arbitrary, Debug)]
struct Input {
    /// One below the framer's `max_record_bytes`, so the cap is at least 1.
    max_record_bytes_minus_one: u16,
    stream: Vec<u8>,
    /// Offsets into `stream`, taken modulo its length plus one.
    cuts: Vec<u16>,
}

/// Feed `chunks` through a fresh framer and collect every record. `None` when
/// a push exceeded the record-size cap.
fn frame(max_record_bytes: usize, chunks: &[&[u8]]) -> Option<Vec<Vec<u8>>> {
    let mut framer: Box<dyn RecordFramer> = Box::new(NdjsonFramer::new(max_record_bytes));
    for chunk in chunks {
        framer.push(chunk).ok()?;
    }
    framer.finish().ok()?;
    let mut records = Vec::new();
    while let Some(record) = framer.pop() {
        records.push(record);
    }
    Some(records)
}

fuzz_target!(|input: Input| {
    let cap = usize::from(input.max_record_bytes_minus_one) + 1;
    let stream = input.stream.as_slice();

    let mut cuts: Vec<usize> = input
        .cuts
        .iter()
        .map(|cut| usize::from(*cut) % (stream.len() + 1))
        .collect();
    cuts.sort_unstable();

    let mut chunks: Vec<&[u8]> = Vec::with_capacity(cuts.len() + 1);
    let mut start = 0;
    for cut in cuts {
        chunks.push(&stream[start..cut]);
        start = cut;
    }
    chunks.push(&stream[start..]);

    assert_eq!(
        frame(cap, &[stream]),
        frame(cap, &chunks),
        "framing of {stream:02x?} at cap {cap} depends on the chunking"
    );
});
