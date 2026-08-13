//! Wall time for streaming record framing.
//!
//! [`NdjsonFramer`] cuts a byte *stream* into newline-delimited records so a
//! streaming source can hand the deserializer one document per payload. This
//! is the wall-clock half of what `framing_gungraun.rs` counts, over the same
//! streams through the same loop — both drive `frame_stream` from
//! `support/frame_rig.rs`, so a counted regression and a wall-clock one are
//! statements about one region.
//!
//! Four cases, where the counted tier runs six. Wall time cannot resolve what
//! separates the two that are left out. `lf_line_chunks` sits between
//! `lf_fetch_chunks` and `lf_split_chunks` on one axis both of those already
//! fix, and `lf_blank_interleaved` is one branch per line — a per-line term
//! the counted tier prices exactly and a 5% wall-clock floor would not see:
//!
//! - `frame_lf_fetch_chunks` — many lines per chunk, which is what a
//!   fetch-sized read hands the framer. The regime production runs in, and the
//!   denominator for the rest.
//! - `frame_lf_split_chunks` — the same bytes in chunks far smaller than a
//!   line, so almost every record is assembled across several pushes through
//!   the partial-line buffer. The other end of that axis, and what a
//!   small-read source or a chunked transfer encoding puts the framer in.
//! - `frame_crlf_fetch_chunks` — the baseline stream terminated `\r\n`. The
//!   framer strips exactly one trailing `\r`, so the delta is that strip plus
//!   one more byte to scan per line. Both of those are inside the declared
//!   byte total, which counts what the framer scanned rather than what it
//!   emitted.
//! - `frame_lf_wide_lines` — an eighth as many lines, eight times as wide.
//!   The framed output is the same 1.6 MB either way; the scanned input is
//!   0.4% smaller here, being the seven thousand terminators an eighth as many
//!   lines do not carry. Read against the baseline it separates the framer's
//!   per-byte cost from its per-record cost, which no other pair here can do.
//!
//! No metrics recorder: the framer registers nothing, so this binary depends
//! on none of the instrumentation `decode_wall.rs` installs. That is the
//! one-target-per-subject rule of thumb rather than a hard requirement here —
//! the harness runs one process per case per replicate, so two subjects in one
//! binary could not collide on a recorder anyway.
//!
//! Nothing pins an iteration count; see `decode_wall.rs` for why.
//!
//! Run it with `make bench-ab REF=main FILTER=frame_`.
//!
//! [`NdjsonFramer`]: spate_json::NdjsonFramer

use spate_bench::{Suite, bench_main};

#[path = "support/frame_rig.rs"]
mod frame_rig;
#[path = "support/lines.rs"]
mod lines;

use frame_rig::{Rig, frame_stream};
use lines::Eol;

/// The bytes a rig hands the framer, terminators included.
///
/// Not the same quantity as `expect_bytes`, which is the framed *output* —
/// records times width, with the terminators stripped. The framer scans the
/// terminators, and the LF and CRLF streams differ by exactly the extra byte
/// per line, so charging both against their output would hide half of that
/// difference in the denominator and leave `bytes_per_s` describing a corpus
/// neither case reads.
fn input_bytes(rig: &Rig) -> u64 {
    rig.chunks.iter().map(|chunk| chunk.len() as u64).sum()
}

/// A rig over the standard stream, chunked at `chunk_bytes`.
fn standard(eol: Eol, blank_every: usize, chunk_bytes: usize) -> Rig {
    let stream = lines::stream(lines::RECORDS, lines::LINE_BYTES, eol, blank_every);
    Rig {
        chunks: lines::chunks(&stream, chunk_bytes),
        expect_records: lines::RECORDS,
        expect_bytes: lines::expect_bytes(lines::RECORDS, lines::LINE_BYTES),
    }
}

fn wide() -> Rig {
    let stream = lines::stream(lines::WIDE_RECORDS, lines::WIDE_LINE_BYTES, Eol::Lf, 0);
    Rig {
        chunks: lines::chunks(&stream, lines::FETCH_CHUNK_BYTES),
        expect_records: lines::WIDE_RECORDS,
        expect_bytes: lines::expect_bytes(lines::WIDE_RECORDS, lines::WIDE_LINE_BYTES),
    }
}

/// One case over one rig.
///
/// The state is a plain [`Rig`] rather than a `RefCell`: [`frame_stream`]
/// takes it by shared reference and builds its own framer, so nothing the
/// routine touches survives the call and the thousandth drive is the first
/// drive. `tests/bench_fixtures.rs` holds that as a property rather than
/// leaving it to be inferred from this comment.
///
/// The assertion is what keeps the case honest under a name. A framer that
/// stopped splitting, stopped stripping a `\r`, or started counting blank
/// lines would otherwise report a large improvement rather than a failure —
/// and the returned pair is also what `black_box` holds, so the loop cannot be
/// optimized away.
fn case(suite: Suite, id: &str, build: fn() -> Rig) -> Suite {
    suite
        .case(
            id,
            move |corpus, _seed| {
                let rig = build();
                // The chunks are absorbed one at a time rather than
                // concatenated: `absorb` folds each input's length in, so a
                // stream re-cut at a different chunk size digests differently.
                // That is wanted — the chunking is half of what the case is.
                for chunk in &rig.chunks {
                    corpus.absorb("chunk", chunk);
                }
                // The framer does not decode, so the backend cannot change
                // what this measures. Declared anyway, so that both of this
                // crate's wall targets state the arm they compiled on the same
                // two tripwires rather than on one each.
                corpus.declare("backend", spate_json::BACKEND_ID.as_bytes());
                rig
            },
            |b, rig: &Rig| {
                b.iter(|| {
                    let got = frame_stream(rig);
                    assert_eq!(
                        got,
                        (rig.expect_records, rig.expect_bytes),
                        "the framing changed; if that is intended, the fixture's \
                         expectation is the contract being edited"
                    );
                    got
                });
            },
        )
        .items_of(|rig: &Rig| rig.expect_records as u64)
        .bytes_of(input_bytes)
        .done()
}

fn suite() -> Suite {
    let suite = spate_bench::suite("spate-json");
    let suite = case(suite, "frame_lf_fetch_chunks", || {
        standard(Eol::Lf, 0, lines::FETCH_CHUNK_BYTES)
    });
    let suite = case(suite, "frame_lf_split_chunks", || {
        standard(Eol::Lf, 0, lines::SPLIT_CHUNK_BYTES)
    });
    let suite = case(suite, "frame_crlf_fetch_chunks", || {
        standard(Eol::Crlf, 0, lines::FETCH_CHUNK_BYTES)
    });
    case(suite, "frame_lf_wide_lines", wide)
}

bench_main!(suite);
