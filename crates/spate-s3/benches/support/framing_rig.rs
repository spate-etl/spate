//! The framing rigs both the counted tier and the allocation snapshot drive:
//! the objects one framing job reads, the codec they are stored under, and
//! the record count the framer must emit.
//!
//! Included with `#[path]`: a bench target is its own crate, so two targets
//! measure one workload only by compiling the same source. Each includer also
//! declares `mod ndjson` from `support/ndjson.rs`.

use spate_json::NdjsonFramer;
use spate_s3::Compression;
use spate_s3::bench_seams::MakeFramer;
use std::sync::Arc;

use crate::ndjson;

/// One framing job: the codec the objects are stored under, and the objects
/// themselves as `(key, chunks)`.
pub(crate) struct Rig {
    pub(crate) compression: Compression,
    pub(crate) objects: Vec<(String, Vec<Vec<u8>>)>,
    /// What the framer must emit. Asserted rather than returned so a fixture
    /// that silently stops producing records cannot pass as a fast one.
    pub(crate) expect_records: usize,
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
    let objects = (0..objects_n)
        .map(|i| {
            let body = ndjson::body(i * records_each, records_each);
            (format!("part-{i:06}.ndjson"), ndjson::chunks(&body))
        })
        .collect();
    Rig {
        compression: Compression::Auto,
        objects,
        expect_records: objects_n * records_each,
    }
}
