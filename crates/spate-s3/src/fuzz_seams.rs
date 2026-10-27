//! Entry points into code that reads bytes this process did not write: the
//! composite offset codec, the object framer, the trim of a ranged read, and
//! the decoder of a feed delivery's body.
//!
//! Follows the first two rules [`bench_seams`](crate::bench_seams) states:
//! the `testing` feature with `#[doc(hidden)]`, and functions with no type of
//! this crate's own. Beside the entry points, a target may call one predicate
//! the source applies when the rule behind it belongs to a dependency. A
//! target that predicts with such a predicate checks that the source applies
//! it, and the predicate's own tests pin its rule.

use crate::config::Compression;
use crate::framer::{Codec, ObjectFramer};
use crate::offset::{self, Position};
use crate::split::SplitRange;
use crate::trim::RangeTrim;
use bytes::Bytes;
use spate_core::framing::RecordFramer;
use std::io;
use std::sync::Arc;

/// Highest object ordinal a lane's composite offset carries.
pub const MAX_ORDINAL: u32 = offset::MAX_ORDINAL;

/// Highest record index a composite offset carries for an emitted record. A
/// watermark reaches one above it.
pub const MAX_RECORD_INDEX: u64 = offset::MAX_RECORD_INDEX;

/// Pack an object ordinal and a record index into the framework's `i64`
/// offset space. `None` when either field is out of range.
#[must_use]
pub fn encode_position(ordinal: u32, record: u64) -> Option<i64> {
    Position { ordinal, record }.encode().ok()
}

/// Unpack an offset into its object ordinal and record index.
///
/// `offset` is a position this crate encoded or a watermark one past one, so
/// it is non-negative. A debug build asserts that.
#[must_use]
pub fn decode_position(offset: i64) -> (u32, u64) {
    let position = Position::decode(offset);
    (position.ordinal, position.record)
}

/// Frame one object's chunks into records, resolving the codec from `key`
/// under `compression` and decompressing as a lane does. Returns how many
/// records the object produced and what the run reported, stopping at the
/// first error the way a lane does.
///
/// The count covers a failing run too, up to the records the framer had
/// completed when the error surfaced. A run reports whatever the decompressor
/// or the framer gave it: a truncated stream, a corrupt frame, or a record
/// over the framer's cap.
///
/// `chunks` is one object's bytes in delivery order, already compressed when
/// the resolved codec says so. Each record is dropped as it is popped, so a
/// stream that decompresses to far more than the framer holds is framed in
/// constant memory.
pub fn frame_object<M>(
    compression: Compression,
    key: &str,
    chunks: &[&[u8]],
    make_framer: M,
) -> (usize, io::Result<()>)
where
    M: Fn() -> Box<dyn RecordFramer> + Send + Sync + 'static,
{
    let mut framer = ObjectFramer::new(Arc::new(make_framer));
    let mut records = 0;
    let mut outcome = framer.begin_object(Codec::resolve(compression, key));
    for chunk in chunks {
        if outcome.is_err() {
            break;
        }
        outcome = framer.push_chunk(chunk);
        while framer.pop_record().is_some() {
            records += 1;
        }
    }
    if outcome.is_ok() {
        outcome = framer.finish_object();
        while framer.pop_record().is_some() {
            records += 1;
        }
    }
    (records, outcome)
}

/// The bytes a unit reading `[start, end)` of `object` forwards, fetched in
/// the windows the fetcher schedules for `range_bytes` and `chunk_bytes`.
/// `None` when the range is empty or extends past the object.
///
/// # Panics
///
/// If the schedule asks for an empty window.
#[must_use]
pub fn read_range(
    object: &[u8],
    start: u64,
    end: u64,
    delimiter: u8,
    range_bytes: u64,
    chunk_bytes: u64,
) -> Option<Vec<u8>> {
    let size = object.len() as u64;
    if start >= end || end > size {
        return None;
    }
    let mut trim = RangeTrim::new(SplitRange::new(start, end, delimiter), size);
    let mut pos = trim.first_byte();
    let mut owned = Vec::new();
    while let Some(window_end) = trim.next_window(pos, range_bytes, chunk_bytes) {
        assert!(window_end > pos, "an empty window at byte {pos}");
        let window = Bytes::copy_from_slice(&object[pos as usize..window_end as usize]);
        owned.extend_from_slice(&trim.feed(pos, window));
        pos = window_end;
    }
    Some(owned)
}

/// One part a delivery's body decodes to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedPart {
    /// The object key.
    pub key: String,
    /// The ETag sent in `If-Match`, quoted.
    pub etag: Option<String>,
    /// The byte range read, `None` for the whole object.
    pub range: Option<(u64, u64)>,
    /// The object version a notification names.
    pub version: Option<String>,
    /// The re-enqueue count a notification carries; 0 for a descriptor.
    pub re_enqueues: u32,
}

/// What the source does with a delivery's body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodedUnit {
    /// Read these parts.
    Read(Vec<DecodedPart>),
    /// Acknowledge it with nothing to read.
    Acknowledge,
    /// Fail it back to the feed.
    Fail,
}

/// Decode `body` as the source over `bucket` and `prefix` does, with no
/// object cut into ranges, so the parts number at most one per record.
#[must_use]
pub fn decode_unit(body: &[u8], bucket: &str, prefix: &str) -> DecodedUnit {
    let scope = crate::unit::Scope {
        bucket: Some(bucket.to_owned()),
        prefix: object_store::path::Path::from(prefix),
    };
    let packing = crate::split::Packing {
        target_bytes: u64::MAX,
        compression: Compression::None,
        delimiter: None,
    };
    match crate::unit::decode(body, &scope, packing) {
        crate::unit::Decoded::Read(cursors) => DecodedUnit::Read(
            cursors
                .into_iter()
                .flatten()
                .map(|part| {
                    let (version, re_enqueues) = match part.origin {
                        crate::unit::Origin::Notification {
                            version,
                            re_enqueues,
                        } => (version, re_enqueues),
                        crate::unit::Origin::Lister => (None, 0),
                    };
                    DecodedPart {
                        key: part.object[0].key.clone(),
                        etag: part.object[0].etag.clone(),
                        range: part.range.map(|r| (r.start, r.end)),
                        version,
                        re_enqueues,
                    }
                })
                .collect(),
        ),
        crate::unit::Decoded::Acknowledge(_) => DecodedUnit::Acknowledge,
        crate::unit::Decoded::Fail(_) => DecodedUnit::Fail,
    }
}

/// The body of the unit a worker re-enqueues for `key` in `bucket` after an
/// overwrite, at the ETag, size and time a HEAD reported, counting
/// `re_enqueues`.
#[must_use]
pub fn re_enqueue_body(
    bucket: &str,
    key: &str,
    etag: Option<&str>,
    size: u64,
    last_modified_ms: i64,
    re_enqueues: u32,
) -> Vec<u8> {
    let overwrite = crate::fetch::Overwrite {
        key: key.to_owned(),
        etag: etag.map(str::to_owned),
        size,
        last_modified_ms,
    };
    crate::unit::re_enqueue_body(bucket, &overwrite, re_enqueues)
}

/// `etag` in the quoted form `If-Match` compares.
#[must_use]
pub fn quoted(etag: &str) -> String {
    crate::unit::quoted(etag.to_owned())
}

/// A storage notification in `bucket` with one `ObjectCreated:Put` record
/// carrying each field given and leaving out each one that is `None`.
#[must_use]
pub fn notification_body(
    bucket: &str,
    event_source: Option<&str>,
    key: &str,
    size: Option<u64>,
    etag: Option<&str>,
    version: Option<&str>,
    re_enqueues: Option<u32>,
) -> Vec<u8> {
    let mut object = serde_json::json!({ "key": key });
    if let Some(size) = size {
        object["size"] = size.into();
    }
    if let Some(etag) = etag {
        object["eTag"] = etag.into();
    }
    if let Some(version) = version {
        object["versionId"] = version.into();
    }
    let mut record = serde_json::json!({
        "eventName": "ObjectCreated:Put",
        "s3": { "bucket": { "name": bucket }, "object": object },
    });
    if let Some(n) = re_enqueues {
        record["spateReEnqueues"] = n.into();
    }
    if let Some(source) = event_source {
        record["eventSource"] = source.into();
    }
    serde_json::to_vec(&serde_json::json!({ "Records": [record] }))
        .expect("a JSON value always serialises")
}

/// Whether the source can read `key` under its own name through
/// `object_store`.
#[must_use]
pub fn addressable(key: &str) -> bool {
    crate::listing::addressable(key)
}
