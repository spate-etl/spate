//! Deterministic NDJSON object bodies for the framing bench.
//!
//! Every fixture is built outside the measured region and, where a codec
//! applies, **compressed here** — a bench that compressed inside the measured
//! region would count the compressor instead of the decompressor it is
//! supposed to be measuring.
//!
//! The records are shaped like the `orders` payload the other decode benches
//! use, so a byte count here is comparable with one there, but they are
//! written by hand rather than serialised: this bench measures framing, and
//! pulling in a serialiser would put its cost in the fixture builder.

use std::io::Write as _;

/// Bytes per fetched chunk, matching the source's default chunk sizing — the
/// framer's work is split across `push_chunk` calls at exactly this
/// granularity in production.
pub(crate) const CHUNK_BYTES: usize = 256 * 1024;

/// Records per object in the single-object profiles. Chosen so the object
/// lands around 4 MiB, which is ~16 chunks: enough that per-chunk work
/// dominates per-object setup, and small enough to stay well inside the
/// instruction budget under emulation.
const RECORDS: usize = 16_000;

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
