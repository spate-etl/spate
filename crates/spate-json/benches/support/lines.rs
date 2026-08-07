//! Deterministic newline-delimited byte streams for the framing benches,
//! counted and wall-clock alike.
//!
//! [`NdjsonFramer`](spate_json::NdjsonFramer) knows nothing about JSON beyond
//! the newline convention, so what decides its cost is the *shape* of the byte
//! stream: how many lines, how wide each one is, how the chunk boundaries fall
//! against them, which terminator the producer wrote, and how much of the
//! stream is blank. Every fixture here is a pure function of those parameters,
//! so a case names its regime rather than describing a payload.
//!
//! Lines are written by hand rather than serialized. This bench measures
//! framing, and pulling a serializer into the fixture builder would put its
//! cost in the corpus rather than leave the corpus a fixed quantity of bytes —
//! and, more importantly, a serializer cannot be asked for a line of an exact
//! width, which is what makes "the chunk is smaller than a line" a statement
//! about the framer rather than about the record generator.
//!
//! Every line is nonetheless valid JSON: the framer is fed what a JSON source
//! would really carry, and `tests/bench_fixtures.rs` holds it to that.

// Each target that includes this module uses a different subset of it, so an
// item is legitimately dead in one while live in another. A module-wide
// `allow` rather than per-item `expect`, which would itself go unfulfilled in
// whichever target does use the item.
#![allow(dead_code, reason = "each bench target uses a different subset")]

/// Lines in a stream. Sized so that per-chunk and per-line work dominates the
/// one-off cost of opening a framer: at [`LINE_BYTES`] this is a 1.6 MB
/// stream, twenty-five fetch-sized chunks, which is the regime a source lane
/// runs in rather than the regime a unit test does.
pub(crate) const RECORDS: usize = 8_000;

/// The standard line width, in bytes, not counting the terminator. The same
/// order as the flat records the decode benches carry.
pub(crate) const LINE_BYTES: usize = 200;

/// The wide-line width. Eight times [`LINE_BYTES`], and the stream carries an
/// eighth as many lines, so the two corpora are the same total number of bytes
/// and differ only in how many record boundaries are in them.
pub(crate) const WIDE_LINE_BYTES: usize = LINE_BYTES * 8;

/// Lines in the wide-line stream, chosen to hold the byte total fixed.
pub(crate) const WIDE_RECORDS: usize = RECORDS / 8;

/// A fetch-sized chunk: many lines arrive per `push`, which is what a
/// streaming source's read loop hands the framer.
pub(crate) const FETCH_CHUNK_BYTES: usize = 64 * 1024;

/// A chunk far smaller than one line, so every line is assembled from several
/// `push` calls and the framer's partial-line buffer carries state across
/// almost every one of them.
pub(crate) const SPLIT_CHUNK_BYTES: usize = 32;

/// Which terminator the producer wrote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Eol {
    /// `\n` — the NDJSON convention.
    Lf,
    /// `\r\n` — what a Windows producer, or an HTTP-framed export, writes. The
    /// framer strips exactly one trailing `\r`, and that strip is the only
    /// difference between the two streams beyond one byte per line.
    Crlf,
}

impl Eol {
    fn bytes(self) -> &'static [u8] {
        match self {
            Eol::Lf => b"\n",
            Eol::Crlf => b"\r\n",
        }
    }
}

/// Deterministic filler bytes, from the same 64-bit LCG the other bench
/// corpora in this workspace use.
///
/// The constants are Knuth's MMIX multiplier and increment; the high bits are
/// taken because the low bits of an LCG have short periods. Varied rather than
/// constant content so no case is measuring a run of identical bytes, which
/// neither a real payload nor a compressor-fed stream would give it.
fn filler(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            // Lowercase ASCII only: nothing that needs escaping inside a JSON
            // string, so the line is valid JSON without the fixture having to
            // reason about escapes.
            b'a' + u8::try_from((state >> 33) % 26).expect("a value below 26")
        })
        .collect()
}

/// One line of exactly `width` bytes, not counting the terminator.
///
/// The padding is what makes the width exact, and the width is what makes a
/// chunk size expressible as a ratio against a line. `width` must leave room
/// for the fixed part of the document.
pub(crate) fn line(index: usize, width: usize) -> Vec<u8> {
    let head = format!(
        "{{\"id\":{index},\"ts_ms\":{},\"pad\":\"",
        1_754_000_000_000_u64 + index as u64
    );
    let tail = "\"}";
    let overhead = head.len() + tail.len();
    assert!(
        width >= overhead,
        "a {width}-byte line cannot hold the {overhead}-byte document frame"
    );
    let mut out = Vec::with_capacity(width);
    out.extend_from_slice(head.as_bytes());
    out.extend_from_slice(&filler(index as u64, width - overhead));
    out.extend_from_slice(tail.as_bytes());
    debug_assert_eq!(out.len(), width);
    out
}

/// A stream of `records` lines of exactly `width` bytes each.
///
/// `blank_every` interleaves a bare terminator after every nth record (0 for
/// none). A blank line is skipped by the framer and consumes no record index,
/// so the record count is `records` whatever the density — which is what
/// `expect_bytes` below relies on, and what makes the blank case a controlled
/// comparison rather than a smaller corpus.
pub(crate) fn stream(records: usize, width: usize, eol: Eol, blank_every: usize) -> Vec<u8> {
    let eol = eol.bytes();
    let mut out = Vec::with_capacity(records * (width + eol.len() + 1));
    for i in 0..records {
        out.extend_from_slice(&line(i, width));
        out.extend_from_slice(eol);
        if blank_every != 0 && (i + 1).is_multiple_of(blank_every) {
            out.extend_from_slice(eol);
        }
    }
    out
}

/// Split a stream into chunks of `size` bytes, as a source's read loop hands
/// them to the framer. The last chunk is short unless the size divides the
/// stream.
pub(crate) fn chunks(bytes: &[u8], size: usize) -> Vec<Vec<u8>> {
    bytes.chunks(size).map(<[u8]>::to_vec).collect()
}

/// The decoded bytes a stream of `records` lines of `width` must frame into.
///
/// Terminators and blank lines are not part of a record, so this is
/// independent of both — which is why asserting it in the bench is worth
/// something: a CRLF stream whose `\r` stopped being stripped, or a blank line
/// that started counting, fails here rather than reading as a slightly larger
/// number.
pub(crate) const fn expect_bytes(records: usize, width: usize) -> usize {
    records * width
}
