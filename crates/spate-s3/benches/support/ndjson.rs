//! Deterministic NDJSON object bodies for the framing bench.
//!
//! Every fixture is built outside the measured region and, where a codec
//! applies, **compressed here** — a bench that compressed inside the measured
//! region would count the compressor instead of the decompressor it is
//! supposed to be measuring.
//!
//! The records are shaped like the `orders` payload the other decode benches
//! use, so a byte count here is comparable with one there, but they are
//! written by hand rather than serialized: this bench measures framing, and
//! pulling in a serializer would put its cost in the fixture builder.

use std::io::Write as _;

/// Bytes per fetched chunk — the source's default (`chunk.target_bytes`,
/// 512 KiB), so the framer's work is split across `push_chunk` calls at the
/// granularity production actually uses.
pub(crate) const CHUNK_BYTES: usize = 512 * 1024;

/// Records per object in the single-object profiles. Chosen so the object
/// lands around 4 MiB, which is ~16 chunks: enough that per-chunk work
/// dominates per-object setup, and small enough to stay well inside the
/// instruction budget under emulation.
pub(crate) const RECORDS: usize = 16_000;

/// Independently-encoded streams inside one compressed object — gzip calls
/// them members, zstd frames, and the decoders treat them alike: read to the
/// end of one, validate its trailer, reinitialize, continue.
///
/// Sixteen because that is what a run of upload sessions appending to one
/// export key produces, and because it makes the multi-part cases the
/// per-member counterpart of `plain_many_small`'s sixteen *objects*: one
/// charges sixteen framer resets across object boundaries, the other sixteen
/// decoder reinitializations inside a single object.
pub(crate) const MEMBERS: usize = 16;

/// Objects in the multi-object profile, and records in each. Shared with the
/// test that pins the profile's record count: two copies of these would let
/// the bench's corpus drift while the test went on passing against its own.
pub(crate) const RUN_OBJECTS: usize = 16;
pub(crate) const RUN_RECORDS_EACH: usize = 200;

/// The framer's per-record cap. Generous relative to the fixtures, so no
/// case is measuring the cap check rejecting anything.
pub(crate) const MAX_RECORD_BYTES: usize = 1024 * 1024;

/// One NDJSON line, deterministic in `index` and around 250 bytes — the
/// same order as the flat record the Avro and JSON decode benches use.
fn line(index: usize) -> String {
    format!(
        "{{\"id\":{index},\"sku\":\"SKU-{index:08}\",\"customer\":\"customer-{:05}\",\
         \"qty\":{},\"price\":{}.{:02},\"currency\":\"GBP\",\"ts_ms\":{},\
         \"region\":\"eu-west-{}\",\"priority\":{},\"discount\":0.{:02},\
         \"notes\":\"order {index} placed through the standard channel\",\
         \"paid\":{},\"channel\":\"web\",\"warehouse\":\"wh-{:03}\"}}\n",
        index % 100_000,
        index % 40 + 1,
        index % 900 + 10,
        index % 100,
        1_754_000_000_000_u64 + index as u64,
        index % 3 + 1,
        index % 5,
        index % 100,
        index.is_multiple_of(2),
        index % 250,
    )
}

/// An NDJSON body of `records` lines, starting at `first`.
pub(crate) fn body(first: usize, records: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(records * 256);
    for i in first..first + records {
        out.extend_from_slice(line(i).as_bytes());
    }
    out
}

/// The standard single-object body: one object's worth of lines.
pub(crate) fn whole_body() -> Vec<u8> {
    body(0, RECORDS)
}

/// The standard single-object body encoded as [`MEMBERS`] independent
/// streams, each covering an equal run of records.
///
/// Every part is a complete stream in its own right — its own header, its own
/// trailer — so the returned pieces concatenate into an object a decoder must
/// re-enter [`MEMBERS`] times. `the_multi_part_objects_are_really_multi_part`
/// pins that: without it the fixture could quietly encode one stream and the
/// case would measure the same thing `gzip_whole` already does.
pub(crate) fn members(store: impl Fn(&[u8]) -> Vec<u8>) -> Vec<Vec<u8>> {
    assert!(
        RECORDS.is_multiple_of(MEMBERS),
        "{RECORDS} records do not divide into {MEMBERS} equal parts, so the \
         parts would not reassemble into the whole body"
    );
    let each = RECORDS / MEMBERS;
    (0..MEMBERS).map(|m| store(&body(m * each, each))).collect()
}

/// [`members`] concatenated: one stored object, many streams. Byte-for-byte
/// the same decoded content as [`whole_body`], which is what makes the pair a
/// controlled comparison — the only difference is how many streams it took.
pub(crate) fn concatenated(store: impl Fn(&[u8]) -> Vec<u8>) -> Vec<u8> {
    members(store).concat()
}

/// Split a body into fetch-sized chunks.
pub(crate) fn chunks(bytes: &[u8]) -> Vec<Vec<u8>> {
    bytes.chunks(CHUNK_BYTES).map(<[u8]>::to_vec).collect()
}

/// gzip the body, as the object would be stored.
pub(crate) fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(bytes).expect("gzip a vec");
    enc.finish().expect("finish gzip")
}

/// zstd the body, as the object would be stored.
pub(crate) fn zstd(bytes: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(bytes, 3).expect("zstd a vec")
}

/// The byte offset of the first record boundary at or after `at`, plus a few
/// bytes — so a chunk list starting here begins **inside** a record, which is
/// what a reader entering a subdivided object at an arbitrary byte range
/// faces.
pub(crate) fn offset_inside_a_record(bytes: &[u8], at: usize) -> usize {
    let boundary = bytes[at..]
        .iter()
        .position(|&b| b == b'\n')
        .expect("the fixture has a newline after the midpoint")
        + at
        + 1;
    // Land a third of the way into the following record rather than at a
    // fixed byte count, so the offset does not accidentally sit on a
    // delimiter if the record shape changes.
    let next = bytes[boundary..]
        .iter()
        .position(|&b| b == b'\n')
        .expect("the fixture has a further newline")
        / 3;
    boundary + next
}
