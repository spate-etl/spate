//! The framing rigs the counted tier, the allocation snapshot and the
//! wall-clock tier drive: the objects one framing job reads, the codec they
//! are stored under, and the record count the framer must emit. The drives
//! that run a rig through the crate live here too, so every target measures
//! the same call.
//!
//! Included with `#[path]`: a bench target is its own crate, so two targets
//! measure one workload only by compiling the same source. Each includer also
//! declares `mod ndjson` from `support/ndjson.rs`.

#![allow(dead_code, reason = "each bench target uses a different subset")]

use flate2::write::MultiGzDecoder;
use spate_json::NdjsonFramer;
use spate_s3::Compression;
use spate_s3::bench_seams::{MakeFramer, frame_objects, frame_range};
use std::hint::black_box;
use std::io::{self, Write as _};
use std::sync::Arc;
use zstd::stream::{raw, zio};

use crate::ndjson;

/// One framing job: the codec the objects are stored under, and the objects
/// themselves as `(key, chunks)`.
pub(crate) struct Rig {
    pub(crate) compression: Compression,
    pub(crate) objects: Vec<(String, Vec<bytes::Bytes>)>,
    /// What the framer must emit. Asserted rather than returned so a fixture
    /// that silently stops producing records cannot pass as a fast one.
    pub(crate) expect_records: usize,
    /// The bytes the objects decode to, which a decoder floor must produce.
    pub(crate) expect_bytes: usize,
}

pub(crate) fn make_framer() -> MakeFramer {
    Arc::new(|| Box::new(NdjsonFramer::new(ndjson::MAX_RECORD_BYTES)))
}

/// One object whose body is transformed by `store` (identity for plain, a
/// compressor otherwise) under a key whose extension matches.
///
/// Every case runs `Compression::Auto`, which is the shipped default and the
/// only setting that consults the key at all: under an explicit `Gzip` or
/// `Zstd` the resolver returns immediately and the extension is dead weight.
/// Passing `Auto` is what makes the per-object codec resolution these cases
/// claim to measure the same work production does.
fn one_object(compression: Compression, suffix: &str, store: impl Fn(&[u8]) -> Vec<u8>) -> Rig {
    let body = ndjson::whole_body();
    let expect_records = body.iter().filter(|&&b| b == b'\n').count();
    let stored = store(&body);
    Rig {
        compression,
        objects: vec![(
            format!("part-000000.ndjson{suffix}"),
            ndjson::chunks(&stored),
        )],
        expect_records,
        expect_bytes: body.len(),
    }
}

pub(crate) fn plain_whole() -> Rig {
    one_object(Compression::Auto, "", <[u8]>::to_vec)
}

pub(crate) fn gzip_whole() -> Rig {
    one_object(Compression::Auto, ".gz", ndjson::gzip)
}

pub(crate) fn zstd_whole() -> Rig {
    one_object(Compression::Auto, ".zst", ndjson::zstd)
}

/// One object whose body is stored as several independent streams.
///
/// The decoded bytes are the whole body either way, so `expect_records` is
/// the same count the `_whole` case asserts, which is the assertion that
/// matters here: a decoder that stopped at the first stream would return one
/// sixteenth of the records and fail rather than report a smaller number.
fn multi_part_object(suffix: &str, store: impl Fn(&[u8]) -> Vec<u8>) -> Rig {
    let stored = ndjson::concatenated(store);
    Rig {
        compression: Compression::Auto,
        objects: vec![(
            format!("part-000000.ndjson{suffix}"),
            ndjson::chunks(&stored),
        )],
        expect_records: ndjson::RECORDS,
        expect_bytes: ndjson::whole_body().len(),
    }
}

pub(crate) fn gzip_multi_member() -> Rig {
    multi_part_object(".gz", ndjson::gzip)
}

pub(crate) fn zstd_multi_frame() -> Rig {
    multi_part_object(".zst", ndjson::zstd)
}

/// The same object entered part-way through a record.
///
/// The expected count is derived from the fixture rather than written down,
/// so it tracks the record shape. The rule it encodes: the leading partial
/// line counts as a record, because the framer has no way to know it entered
/// mid-record. A byte-range split drops that line before framing, which
/// [`plain_range`] asserts.
pub(crate) fn plain_mid_offset() -> Rig {
    let body = ndjson::whole_body();
    let at = ndjson::offset_inside_a_record(&body, body.len() / 2);
    let tail = &body[at..];
    let expect_records =
        tail.iter().filter(|&&b| b == b'\n').count() + usize::from(!tail.ends_with(b"\n"));
    Rig {
        compression: Compression::Auto,
        objects: vec![("part-000000.ndjson".to_owned(), ndjson::chunks(tail))],
        expect_records,
        expect_bytes: tail.len(),
    }
}

/// One byte range of an object, from the offset [`plain_mid_offset`] enters
/// at to the object's end.
pub(crate) struct RangeRig {
    pub(crate) body: Vec<u8>,
    pub(crate) start: u64,
    /// What the range owns: the records starting at or after `start`, one
    /// fewer than the raw tail frames.
    pub(crate) expect_records: usize,
}

pub(crate) fn plain_range() -> RangeRig {
    let body = ndjson::whole_body();
    let at = ndjson::offset_inside_a_record(&body, body.len() / 2);
    let expect_records = body[at..].iter().filter(|&&b| b == b'\n').count() - 1;
    RangeRig {
        body,
        start: at as u64,
        expect_records,
    }
}

/// Sixteen small members through one framer.
pub(crate) fn plain_many_small() -> Rig {
    let (objects_n, records_each) = (ndjson::RUN_OBJECTS, ndjson::RUN_RECORDS_EACH);
    let mut expect_bytes = 0;
    let objects = (0..objects_n)
        .map(|i| {
            let body = ndjson::body(i * records_each, records_each);
            expect_bytes += body.len();
            (format!("part-{i:06}.ndjson"), ndjson::chunks(&body))
        })
        .collect();
    Rig {
        compression: Compression::Auto,
        objects,
        expect_records: objects_n * records_each,
        expect_bytes,
    }
}

/// Frames `rig` through `frame_objects` and returns the record count, which
/// must be the rig's `expect_records`.
pub(crate) fn frame(rig: &Rig) -> usize {
    let records = frame_objects(rig.compression, make_framer(), black_box(&rig.objects))
        .expect("the fixture frames cleanly");
    assert_eq!(
        records, rig.expect_records,
        "framed record count changed; if that is intended, the fixture's \
         expectation is the contract being edited"
    );
    records
}

/// Reads `rig`'s range through `frame_range` in the source's default GET
/// windows and chunks, and returns the record count, which must be the rig's
/// `expect_records`.
pub(crate) fn frame_one_range(rig: &RangeRig) -> usize {
    let records = frame_range(
        make_framer(),
        black_box(&rig.body),
        rig.start,
        rig.body.len() as u64,
        ndjson::DELIMITER,
        ndjson::RANGE_BYTES,
        ndjson::CHUNK_BYTES,
    )
    .expect("the fixture frames cleanly");
    assert_eq!(
        records, rig.expect_records,
        "a range framed a different record count; if that is intended, the \
         fixture's expectation is the contract being edited"
    );
    records
}

/// The library a decoder floor runs: the one `framer.rs` builds for the
/// object's codec.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Decoder {
    /// `flate2::write::MultiGzDecoder`.
    Gzip,
    /// `zstd::stream::zio::Writer` over `zstd::stream::raw::Decoder`.
    Zstd,
}

/// Counts the bytes written to it and keeps none, so a floor pays for
/// decoding and nothing after it.
struct Count(usize);

impl io::Write for Count {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Decodes each of `rig`'s objects through `decoder` with no framer, and
/// returns the decoded byte count, which must be the rig's `expect_bytes`.
///
/// The decoder types follow `ObjectFramer`'s in `crates/spate-s3/src/framer.rs`,
/// fed one chunk at a time and finished the same way, so the floor is the
/// library's share of the framed case over the same bytes. Library defaults
/// apply: the framer's zstd window cap is libzstd's default limit.
pub(crate) fn decode(rig: &Rig, decoder: Decoder) -> usize {
    let mut decoded = 0;
    for (_, chunks) in black_box(&rig.objects) {
        decoded += decode_object(chunks, decoder).expect("the fixture decodes cleanly");
    }
    assert_eq!(
        decoded, rig.expect_bytes,
        "the floor decoded a different byte count, so it is not reading every \
         stream of the object"
    );
    decoded
}

fn decode_object(chunks: &[bytes::Bytes], decoder: Decoder) -> io::Result<usize> {
    match decoder {
        Decoder::Gzip => {
            let mut gz = MultiGzDecoder::new(Count(0));
            for chunk in chunks {
                gz.write_all(chunk)?;
            }
            gz.try_finish()?;
            Ok(gz.get_ref().0)
        }
        Decoder::Zstd => {
            let decoder = raw::Decoder::new()?;
            let mut zst = zio::Writer::new(Count(0), decoder);
            for chunk in chunks {
                zst.write_all(chunk)?;
            }
            zst.finish()?;
            Ok(zst.writer().0)
        }
    }
}
