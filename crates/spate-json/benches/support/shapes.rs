//! Payload shapes for the decode-path benches, counted and wall-clock alike.
//!
//! Every other corpus in this crate is a flat record of seven to fifteen
//! fields, which is one point on an axis a JSON decoder is read along. These
//! are the rest of it: an object far wider than any struct, a document nested
//! far deeper than one, an array that is almost entirely numbers, and a
//! document that is almost entirely one string. Each isolates a different part
//! of a parser — key lookup, recursion, number conversion, string handling —
//! and each is a shape a real payload takes.
//!
//! Two of them do a second job. The duplicate-key guard parses a document a
//! *second* time through a structural visitor that recurses over objects and
//! arrays, so what it costs depends on the same width and depth these
//! documents vary; the guard cases reuse [`wide_flat`] and [`deep_nested`]
//! verbatim, which makes the guard-on and guard-off counts a controlled pair
//! whose difference is the guard and nothing else.
//!
//! Written as bytes rather than serialised from a type: none of these shapes
//! has a Rust type to serialise from — that is what makes them the shapes a
//! struct cannot express — and building them by hand keeps the corpus a fixed
//! quantity of bytes rather than whatever a serialiser happens to emit.

// Each target that includes this module uses a different subset of it, so an
// item is legitimately dead in one while live in another. A module-wide
// `allow` rather than per-item `expect`, which would itself go unfulfilled in
// whichever target does use the item.
#![allow(dead_code, reason = "each bench target uses a different subset")]

/// Fields in the wide flat object.
///
/// Wide enough that per-field work — a key string, a map insertion, one value
/// — is what the count is made of, rather than the per-document setup a
/// fifteen-field record is dominated by.
pub(crate) const WIDE_FIELDS: usize = 4_000;

/// Documents in the deeply-nested array, and how deep each one nests.
///
/// Depth alone cannot size this corpus, and that is the decoder's doing rather
/// than a preference: `serde_json` refuses to recurse past 128 levels and the
/// duplicate-key guard inherits the limit, so no single document can be made
/// large by nesting. The array is what gets the corpus into the instruction
/// band while every document in it is still deep — which is also the shape a
/// real nested export takes.
///
/// Half the limit rather than as close to it as the array wrapper allows, so
/// the fixture is not one nesting level away from turning into a decode error
/// and measuring the recursion guard instead of the recursion.
pub(crate) const DEEP_DEPTH: usize = 64;
pub(crate) const DEEP_DOCS: usize = 40;

/// Scalar fields beside the recursive one at each level of a deep document.
pub(crate) const DEEP_WIDTH: usize = 4;

/// Elements in the numeric array.
pub(crate) const NUMBERS: usize = 40_000;

/// Bytes of text in the large string field.
///
/// Half a megabyte: large enough that whatever a backend does per byte
/// dominates whatever it does per document, which is the claim this case
/// exists to test. The `simd` backend must copy the whole payload into its
/// scratch buffer before it can parse destructively, and this is the case
/// where that copy is big enough to see.
pub(crate) const TEXT_BYTES: usize = 512 * 1024;

/// One escape sequence per this many bytes of text.
///
/// Not zero, deliberately. A string with no escapes is one the parser can hand
/// over as a single copy, and a case built that way would be measuring
/// `memcpy` in the C runtime rather than either parser — including on the
/// backend that has no scratch copy to make, which would leave the comparison
/// saying nothing. Text carrying the odd quote and newline is both the honest
/// corpus and the one whose count belongs to the decoder.
pub(crate) const ESCAPE_EVERY: usize = 64;

/// Deterministic filler bytes from a 64-bit LCG (Knuth's MMIX constants),
/// taking the high bits because an LCG's low bits have short periods.
fn lcg(seed: u64) -> impl FnMut() -> u64 {
    let mut state = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        state >> 33
    }
}

/// A flat object of `WIDE_FIELDS` fields, cycling through the four scalar JSON
/// types so no case is measuring one value kind repeated.
///
/// Keys are fixed-width and ordered, which is the shape an exploded column
/// dump or a wide feature vector has, and the shape that makes the
/// duplicate-key variant below differ from this one in exactly one place.
pub(crate) fn wide_flat() -> Vec<u8> {
    wide_flat_with_duplicate(false)
}

/// The same object with its **last** key repeating its first.
///
/// Late rather than early on purpose: an early duplicate is found before the
/// guard has walked anything, and a case built that way would report the cost
/// of rejecting a document rather than the cost of checking one. Here the
/// guard pays for the whole object and then rejects, which is the worst case
/// and the one worth knowing.
///
/// The decode itself would succeed — `serde_json` is last-value-wins on a
/// repeated key — so a case over this corpus emitting a record is a guard that
/// stopped guarding, not a fixture that stopped being duplicated.
pub(crate) fn wide_flat_duplicate_key() -> Vec<u8> {
    wide_flat_with_duplicate(true)
}

fn wide_flat_with_duplicate(duplicate_last: bool) -> Vec<u8> {
    let mut next = lcg(0x5EED_0001);
    let mut out = Vec::with_capacity(WIDE_FIELDS * 32);
    out.push(b'{');
    for i in 0..WIDE_FIELDS {
        if i > 0 {
            out.push(b',');
        }
        let key = if duplicate_last && i == WIDE_FIELDS - 1 {
            0
        } else {
            i
        };
        let v = next();
        let field = match i % 4 {
            0 => format!("\"f{key:06}\":{}", v % 1_000_000),
            1 => format!("\"f{key:06}\":{}.{:04}", v % 1_000, v % 10_000),
            2 => format!("\"f{key:06}\":\"v-{:08}\"", v % 100_000_000),
            _ => format!("\"f{key:06}\":{}", v.is_multiple_of(2)),
        };
        out.extend_from_slice(field.as_bytes());
    }
    out.push(b'}');
    out
}

/// An array of `DEEP_DOCS` documents, each nesting `DEEP_DEPTH` levels deep
/// with `DEEP_WIDTH` scalar fields beside the recursive one at every level.
///
/// The recursive key is last at each level, so a visitor walking the object in
/// order reads the scalars before it descends — the order a real nested
/// document is written in, and the one that keeps the recursion from being the
/// first thing every level does.
pub(crate) fn deep_nested() -> Vec<u8> {
    let mut next = lcg(0x5EED_0002);
    let mut out = Vec::with_capacity(DEEP_DOCS * DEEP_DEPTH * DEEP_WIDTH * 24);
    out.push(b'[');
    for doc in 0..DEEP_DOCS {
        if doc > 0 {
            out.push(b',');
        }
        for level in 0..DEEP_DEPTH {
            out.push(b'{');
            for field in 0..DEEP_WIDTH {
                if field > 0 {
                    out.push(b',');
                }
                let v = next();
                let rendered = match field % 4 {
                    0 => format!("\"a{field}\":{}", v % 1_000_000),
                    1 => format!("\"a{field}\":\"s-{:06}\"", v % 1_000_000),
                    2 => format!("\"a{field}\":{}.{:03}", v % 100, v % 1_000),
                    _ => format!("\"a{field}\":{}", v.is_multiple_of(2)),
                };
                out.extend_from_slice(rendered.as_bytes());
            }
            // The recursive field, except at the innermost level.
            if level + 1 < DEEP_DEPTH {
                out.extend_from_slice(b",\"n\":");
            }
        }
        out.extend(std::iter::repeat_n(b'}', DEEP_DEPTH));
    }
    out.push(b']');
    out
}

/// An array of `NUMBERS` numeric literals: integers, negatives, decimals and
/// one in ten in exponent form.
///
/// Almost nothing but number conversion — no keys to hash, no strings to
/// unescape — which is the one part of a parser the record-shaped corpora
/// never put under load.
pub(crate) fn numeric_array() -> Vec<u8> {
    let mut next = lcg(0x5EED_0003);
    let mut out = Vec::with_capacity(NUMBERS * 12);
    out.push(b'[');
    for i in 0..NUMBERS {
        if i > 0 {
            out.push(b',');
        }
        let v = next();
        let rendered = match i % 10 {
            0..=3 => format!("{}", v % 1_000_000_000),
            4..=5 => format!("-{}", v % 100_000),
            6..=8 => format!("{}.{:06}", v % 10_000, v % 1_000_000),
            _ => format!("{}.{:03}e{}", v % 10, v % 1_000, v % 12),
        };
        out.extend_from_slice(rendered.as_bytes());
    }
    out.push(b']');
    out
}

/// A small record whose one `text` field holds `TEXT_BYTES` of escaped text.
///
/// The other fields are there so the document is a record rather than a bare
/// string: what the case isolates is one field being three orders of magnitude
/// larger than its neighbours, which is what a log line, a stack trace or an
/// embedded document looks like on the wire.
pub(crate) fn large_string() -> Vec<u8> {
    let mut next = lcg(0x5EED_0004);
    let mut text = Vec::with_capacity(TEXT_BYTES + TEXT_BYTES / ESCAPE_EVERY);
    let mut plain = 0usize;
    while plain < TEXT_BYTES {
        if plain > 0 && plain.is_multiple_of(ESCAPE_EVERY) {
            // Two bytes on the wire, one byte in the decoded string. Both
            // parsers have to leave their fast copy path to handle it.
            text.extend_from_slice(if plain.is_multiple_of(ESCAPE_EVERY * 2) {
                b"\\\""
            } else {
                b"\\n"
            });
        } else {
            let v = next();
            text.push(b'a' + u8::try_from(v % 26).expect("a value below 26"));
        }
        plain += 1;
    }
    let mut out = Vec::with_capacity(text.len() + 64);
    out.extend_from_slice(b"{\"id\":918273645,\"kind\":\"log\",\"text\":\"");
    out.extend_from_slice(&text);
    out.extend_from_slice(b"\"}");
    out
}
