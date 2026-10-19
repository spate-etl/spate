//! Work units of the feed path and their parts: a unit a lister publishes
//! names one object, or one byte range of one object, and a storage
//! notification is one unit with one part per non-empty created object.
//!
//! [`parts_of`] decides whether an object is read whole or cut, by the rule
//! the planner applies to a listing ([`Packing::delimiter_for`] and
//! [`tile`]), and [`units`] mints one feed unit per part. The reader cuts a
//! whole-object unit through the same [`Cursor`], so a unit's ranges are the
//! same whichever side cuts them. [`decode`] reads a delivery's body, a
//! descriptor or a notification, into the parts the reader opens.

use crate::fetch::ObjectEntry;
use crate::framer::Codec;
use crate::listing::addressable;
use crate::split::{
    DescriptorObject, Packing, SplitDescriptor, SplitRange, split_id_for, split_id_for_range, tile,
};
use object_store::path::Path;
use serde::Deserialize;
use spate_core::coordination::CoordinationError;
use spate_core::feed::Unit;
use std::iter::Peekable;
use std::sync::Arc;

/// One part of a unit: the object, whole or in one byte range.
pub(crate) struct Part {
    /// The one object the part reads, shared by every part of the object.
    pub(crate) object: Arc<Vec<ObjectEntry>>,
    /// The byte range read, or `None` for the whole object.
    pub(crate) range: Option<SplitRange>,
}

/// The parts of one object, yielded lazily so an object cut into many
/// ranges holds one range at a time.
pub(crate) struct Cursor {
    /// The one object every part clones.
    pub(crate) object: Arc<Vec<ObjectEntry>>,
    /// The ranges still to yield, or `None` when the object is read whole.
    ranges: Option<Peekable<Box<dyn Iterator<Item = SplitRange> + Send>>>,
    /// The whole part is still to yield.
    whole: bool,
}

impl Cursor {
    /// A cursor yielding `object` whole, once.
    fn whole(object: Arc<Vec<ObjectEntry>>) -> Cursor {
        Cursor {
            object,
            ranges: None,
            whole: true,
        }
    }

    /// A cursor yielding the one byte `range` of `object`.
    pub(crate) fn range(object: Arc<Vec<ObjectEntry>>, range: SplitRange) -> Cursor {
        Cursor::ranges(object, Box::new(std::iter::once(range)))
    }

    fn ranges(
        object: Arc<Vec<ObjectEntry>>,
        ranges: Box<dyn Iterator<Item = SplitRange> + Send>,
    ) -> Cursor {
        Cursor {
            object,
            ranges: Some(ranges.peekable()),
            whole: false,
        }
    }

    /// Whether every part has been yielded.
    pub(crate) fn is_exhausted(&mut self) -> bool {
        match &mut self.ranges {
            Some(ranges) => ranges.peek().is_none(),
            None => !self.whole,
        }
    }
}

impl Iterator for Cursor {
    type Item = Part;

    fn next(&mut self) -> Option<Part> {
        let range = match &mut self.ranges {
            Some(ranges) => Some(ranges.next()?),
            None => {
                if !std::mem::take(&mut self.whole) {
                    return None;
                }
                None
            }
        };
        Some(Part {
            object: Arc::clone(&self.object),
            range,
        })
    }
}

/// The parts of the one object in `object`: the ranges [`tile`] cuts when
/// [`Packing::delimiter_for`] accepts the object and its key is
/// [`addressable`], and the whole object otherwise.
pub(crate) fn parts_of(object: Arc<Vec<ObjectEntry>>, packing: Packing) -> Cursor {
    debug_assert_eq!(object.len(), 1, "a part names one object");
    let entry = &object[0];
    match packing
        .delimiter_for(entry)
        .filter(|_| addressable(&entry.key))
    {
        Some(delimiter) => {
            let ranges = tile(entry.size, packing.target_bytes, delimiter);
            Cursor::ranges(object, Box::new(ranges))
        }
        None => Cursor::whole(object),
    }
}

/// One feed unit per part of each listed object, in listing order.
///
/// A whole part's id is [`split_id_for`] over its key and ETag, and a range
/// part's is [`split_id_for_range`], so an id depends only on the key, the
/// ETag, the range and the range configuration.
///
/// # Errors
///
/// [`Fatal`](spate_core::coordination::CoordinationErrorKind::Fatal) when a
/// descriptor or an id cannot be built, which no listed object causes.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "no lister builds feed units yet")
)]
pub(crate) fn units(
    entries: Vec<ObjectEntry>,
    packing: &Packing,
) -> Result<Vec<Unit>, CoordinationError> {
    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        let object = DescriptorObject {
            key: entry.key.clone(),
            size: entry.size,
            etag: entry.etag.clone(),
            last_modified_ms: entry.last_modified_ms,
        };
        for part in parts_of(Arc::new(vec![entry]), *packing) {
            let (id, descriptor) = match part.range {
                None => (
                    split_id_for([(object.key.as_str(), object.etag.as_deref())])?,
                    SplitDescriptor::new(vec![object.clone()]),
                ),
                Some(range) => (
                    split_id_for_range(
                        &object.key,
                        object
                            .etag
                            .as_deref()
                            .expect("an object is cut only when it has an ETag"),
                        range,
                    )?,
                    SplitDescriptor::with_range(object.clone(), range),
                ),
            };
            out.push(Unit::new(id.as_str(), descriptor.encode()?));
        }
    }
    Ok(out)
}

/// Where a notification's records must point for the source to read them.
#[derive(Clone, Debug)]
pub(crate) struct Scope {
    /// The source's bucket. `None` matches no record.
    pub(crate) bucket: Option<String>,
    /// The source's prefix within the bucket; empty for the whole bucket.
    pub(crate) prefix: Path,
}

/// What the reader does with one delivery.
pub(crate) enum Decoded {
    /// Read these parts, then acknowledge the delivery once.
    Read(Vec<Cursor>),
    /// Acknowledge the delivery with nothing to read, for the reason given.
    Acknowledge(&'static str),
    /// Fail the delivery back to the feed, for the reason given.
    Fail(String),
}

/// Read a delivery's `body`: a [`SplitDescriptor`] a lister published, or a
/// storage notification.
///
/// A notification is an S3 `Records[]` message, the same records inside the
/// `{"EventName","Key","Records"}` envelope, or the S3 test event, which is
/// acknowledged. A record with no `eventName`, or one that is not a string,
/// fails the whole message. Each record whose `eventName` is not
/// `ObjectCreated:*`, with or without an `s3:` prefix, is dropped whatever its
/// bucket. A created record that does
/// not deserialise, or one for another bucket, for a key outside `scope`'s
/// prefix, with no size, or with a key that does not decode fails the whole
/// message, as does any other body. A created object of size 0 is then
/// dropped. Every other record becomes one part, its key form-decoded (`+` is
/// a space, then percent-decoding as UTF-8) and its ETag quoted, unless
/// [`addressable`] refuses its key or its ETag cannot be an `If-Match` value,
/// either of which fails the whole message. A message with no part left is
/// acknowledged.
pub(crate) fn decode(body: &[u8], scope: &Scope, packing: Packing) -> Decoded {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return Decoded::Fail(
            "the unit's body is neither a descriptor nor a storage notification".to_owned(),
        );
    };
    if value.get("v").is_some() {
        return match descriptor_parts(body, packing) {
            Ok(cursor) => Decoded::Read(vec![cursor]),
            Err(reason) => Decoded::Fail(reason),
        };
    }
    if value.get("Event").and_then(serde_json::Value::as_str) == Some("s3:TestEvent") {
        return Decoded::Acknowledge("a storage test event");
    }
    let Some(records) = value.get("Records").and_then(serde_json::Value::as_array) else {
        return Decoded::Fail(
            "the unit's body is neither a descriptor nor a storage notification".to_owned(),
        );
    };
    let mut parts = Vec::with_capacity(records.len());
    for (i, record) in records.iter().enumerate() {
        match record_part(record, scope, packing) {
            Ok(Some(cursor)) => parts.push(cursor),
            Ok(None) => {}
            Err(reason) => return Decoded::Fail(format!("notification record {i}: {reason}")),
        }
    }
    if parts.is_empty() {
        Decoded::Acknowledge("a notification that names no object to read")
    } else {
        Decoded::Read(parts)
    }
}

/// The fields of a notification record the source reads.
#[derive(Deserialize)]
struct Record {
    #[serde(rename = "eventTime", default)]
    event_time: Option<String>,
    s3: RecordS3,
}

#[derive(Deserialize)]
struct RecordS3 {
    bucket: RecordBucket,
    object: RecordObject,
}

#[derive(Deserialize)]
struct RecordBucket {
    name: String,
}

#[derive(Deserialize)]
struct RecordObject {
    key: String,
    #[serde(default)]
    size: Option<u64>,
    #[serde(rename = "eTag", default)]
    etag: Option<String>,
}

/// The part one notification record names, `None` for a record dropped
/// without one, or why the record fails its message.
fn record_part(
    record: &serde_json::Value,
    scope: &Scope,
    packing: Packing,
) -> Result<Option<Cursor>, String> {
    let Some(event) = record.get("eventName").and_then(serde_json::Value::as_str) else {
        return Err("the record's `eventName` is missing or not a string".to_owned());
    };
    if !event
        .strip_prefix("s3:")
        .unwrap_or(event)
        .starts_with("ObjectCreated:")
    {
        return Ok(None);
    }
    let record =
        Record::deserialize(record).map_err(|e| format!("the record is unreadable: {e}"))?;
    let bucket = record.s3.bucket.name;
    if scope.bucket.as_deref() != Some(bucket.as_str()) {
        return Err(format!(
            "the record names the bucket \"{bucket}\", which this source does not read"
        ));
    }
    let key = form_decoded(&record.s3.object.key)?;
    let prefix = scope.prefix.as_ref();
    if !prefix.is_empty()
        && !key
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
    {
        return Err(format!(
            "the key \"{key}\" is outside the source's prefix \"{prefix}/\""
        ));
    }
    let Some(size) = record.s3.object.size else {
        return Err(format!("the record for \"{key}\" has no size"));
    };
    // An empty object has no records, and is read without a request.
    if size == 0 {
        return Ok(None);
    }
    if !addressable(&key) {
        return Err(format!(
            "the key \"{key}\" cannot be read under its own name through `object_store`"
        ));
    }
    if record
        .s3
        .object
        .etag
        .as_deref()
        .is_some_and(|etag| !sendable(etag))
    {
        return Err(format!(
            "the record for \"{key}\" has an ETag that cannot be sent in `If-Match`"
        ));
    }
    let last_modified_ms = record
        .event_time
        .as_deref()
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .map_or(0, |t| t.timestamp_millis());
    let entry = ObjectEntry {
        key,
        size,
        etag: record.s3.object.etag.map(quoted),
        last_modified_ms,
    };
    Ok(Some(parts_of(Arc::new(vec![entry]), packing)))
}

/// `etag` in the quoted form a listing reports and `If-Match` compares,
/// adding the quotes a notification leaves out.
fn quoted(etag: String) -> String {
    if etag.len() >= 2 && etag.starts_with('"') && etag.ends_with('"') {
        etag
    } else {
        format!("\"{etag}\"")
    }
}

/// Whether `etag` can be an `If-Match` header value: `http` refuses a
/// control byte other than a tab, and `object_store` panics building a
/// request with a refused header.
fn sendable(etag: &str) -> bool {
    etag.bytes().all(|b| b == b'\t' || (b >= 0x20 && b != 0x7f))
}

/// `key` decoded as `application/x-www-form-urlencoded`: `+` is a space, and
/// each `%HH` is a byte, the bytes read as UTF-8.
fn form_decoded(key: &str) -> Result<String, String> {
    let hex = |b: u8| char::from(b).to_digit(16);
    let bytes = key.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' => {
                let byte = match (
                    bytes.get(i + 1).copied().and_then(hex),
                    bytes.get(i + 2).copied().and_then(hex),
                ) {
                    (Some(high), Some(low)) => (high * 16 + low) as u8,
                    _ => return Err(format!("the key \"{key}\" has a malformed `%` escape")),
                };
                out.push(byte);
                i += 2;
            }
            b => out.push(b),
        }
        // Decoding never makes the key longer. The check turns a loop that
        // stops advancing `i`, as a cargo-mutants mutant of either step does,
        // into a panic before it exhausts the test host's memory.
        debug_assert!(out.len() <= bytes.len());
        i += 1;
    }
    String::from_utf8(out).map_err(|_| format!("the key \"{key}\" does not decode as UTF-8"))
}

/// The parts of the one object a descriptor body names, or why the unit
/// cannot be read.
///
/// A ranged unit is refused when its delimiter is not the framer's or its
/// object is compressed, the refusals the coordinated path makes when it
/// opens a ranged split.
fn descriptor_parts(body: &[u8], packing: Packing) -> Result<Cursor, String> {
    let descriptor = SplitDescriptor::decode(body)
        .map_err(|e| format!("the unit's body is not a descriptor: {}", e.reason))?;
    let object = match <[ObjectEntry; 1]>::try_from(descriptor.to_entries()) {
        Ok([object]) => object,
        Err(entries) => {
            return Err(format!(
                "the unit names {} objects; this source reads one object per unit",
                entries.len()
            ));
        }
    };
    if object.etag.as_deref().is_some_and(|etag| !sendable(etag)) {
        return Err(format!(
            "the unit's ETag for \"{}\" cannot be sent in `If-Match`",
            object.key
        ));
    }
    let Some(range) = descriptor.range else {
        return Ok(parts_of(Arc::new(vec![object]), packing));
    };
    if packing.delimiter != Some(range.delimiter) {
        return Err(format!(
            "the unit splits its object on byte {:#04x}, but the source's framer {}",
            range.delimiter,
            match packing.delimiter {
                Some(d) => format!("resyncs on byte {d:#04x}"),
                None => "declares no resync delimiter".to_owned(),
            }
        ));
    }
    let codec = Codec::resolve(packing.compression, &object.key);
    if codec != Codec::Plain {
        return Err(format!(
            "the unit names a byte range of \"{}\", which the source decodes as {}; a byte \
             range needs an uncompressed object",
            object.key,
            format!("{codec:?}").to_lowercase()
        ));
    }
    Ok(Cursor::range(Arc::new(vec![object]), range))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Compression;
    use crate::split::MAX_SUBDIVIDABLE_BYTES;
    use proptest::prelude::*;

    const MIB: u64 = 1 << 20;
    const GIB: u64 = 1 << 30;

    const EXTENSIONS: [&str; 6] = ["", ".ndjson", ".gz", ".gzip", ".zst", ".zstd"];

    /// Key shapes: one `Path::parse` keeps, three it rejects and two it
    /// rewrites.
    fn key(i: usize, shape: u8, ext: &str) -> (String, bool) {
        match shape {
            0 => (format!("a/k{i}{ext}"), false),
            1 => (format!("a//k{i}{ext}"), true),
            2 => (format!("a/../k{i}{ext}"), true),
            3 => (format!("a/k\u{1}{i}{ext}"), true),
            4 => (format!("/a/k{i}{ext}"), true),
            _ => (format!("a/k{i}{ext}/"), true),
        }
    }

    fn compression(n: u8) -> Compression {
        match n {
            0 => Compression::Auto,
            1 => Compression::None,
            2 => Compression::Gzip,
            _ => Compression::Zstd,
        }
    }

    /// Whether the object is read uncompressed, decided without
    /// `Codec::resolve`.
    fn plain(compression: Compression, key: &str) -> bool {
        match compression {
            Compression::None => true,
            Compression::Auto => ![".gz", ".gzip", ".zst", ".zstd"]
                .iter()
                .any(|ext| key.ends_with(ext)),
            _ => false,
        }
    }

    fn size(target: u64, shape: u8, random: u64) -> u64 {
        match shape {
            0 => target,
            1 => target + 1,
            2 if target >= GIB => MAX_SUBDIVIDABLE_BYTES - 1,
            3 if target >= GIB => MAX_SUBDIVIDABLE_BYTES,
            4 if target >= GIB => MAX_SUBDIVIDABLE_BYTES + 1,
            5 => 0,
            _ => random % (4 * target),
        }
    }

    fn target() -> impl Strategy<Value = u64> {
        prop_oneof![MIB..=64 * MIB, 16 * GIB..=64 * GIB]
    }

    /// Pins that an object is cut lazily. At a one-byte target the largest
    /// object the cut accepts has more ranges than memory can hold at once,
    /// and its cursor still yields the first two.
    #[test]
    fn the_cut_yields_one_range_at_a_time() {
        let packing = Packing {
            target_bytes: 1,
            compression: Compression::None,
            delimiter: Some(b'\n'),
        };
        let entry = ObjectEntry {
            key: "a.ndjson".into(),
            size: MAX_SUBDIVIDABLE_BYTES,
            etag: Some("\"etag\"".into()),
            last_modified_ms: 1,
        };
        let mut cursor = parts_of(Arc::new(vec![entry]), packing);
        let first: Vec<(u64, u64)> = cursor
            .by_ref()
            .take(2)
            .map(|part| {
                let range = part.range.expect("the object is cut");
                (range.start, range.end)
            })
            .collect();
        assert_eq!(first, [(0, 1), (1, 2)]);
        assert!(!cursor.is_exhausted());
    }

    /// Pins that the cut marks each range with the framer's delimiter.
    #[test]
    fn the_cut_marks_each_range_with_the_framers_delimiter() {
        let packing = Packing {
            target_bytes: MIB,
            compression: Compression::None,
            delimiter: Some(b'\r'),
        };
        let entry = ObjectEntry {
            key: "a.csv".into(),
            size: 3 * MIB,
            etag: Some("\"etag\"".into()),
            last_modified_ms: 1,
        };
        let delimiters: Vec<u8> = parts_of(Arc::new(vec![entry]), packing)
            .map(|part| part.range.expect("the object is cut").delimiter)
            .collect();
        assert_eq!(delimiters, [b'\r'; 3]);
    }

    /// The scope of a source over `s3://bucket/data/`.
    fn scope() -> Scope {
        Scope {
            bucket: Some("bucket".to_owned()),
            prefix: Path::from("data"),
        }
    }

    /// The scope of a source over the whole of `s3://bucket/`.
    fn whole_bucket() -> Scope {
        Scope {
            bucket: Some("bucket".to_owned()),
            prefix: Path::from(""),
        }
    }

    /// The packing of a source whose range target is 100 bytes.
    fn packing() -> Packing {
        Packing {
            target_bytes: 100,
            compression: Compression::Auto,
            delimiter: Some(b'\n'),
        }
    }

    /// One notification record, as AWS writes it.
    fn record(event: &str, bucket: &str, key: &str, size: Option<u64>) -> serde_json::Value {
        serde_json::json!({
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "eventTime": "2026-10-19T12:00:00.000Z",
            "eventName": event,
            "s3": {
                "bucket": { "name": bucket },
                "object": { "key": key, "size": size, "eTag": "0123abcd", "sequencer": "0A1B" },
            },
        })
    }

    /// A created record for `key` in the source's bucket.
    fn created(key: &str, size: u64) -> serde_json::Value {
        record("ObjectCreated:Put", "bucket", key, Some(size))
    }

    fn message(records: &[serde_json::Value]) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({ "Records": records })).unwrap()
    }

    /// One part as `(key, etag, range)`.
    type Read = (String, Option<String>, Option<(u64, u64)>);

    /// Every part of every cursor `decoded` reads.
    fn read(decoded: Decoded) -> Vec<Read> {
        match decoded {
            Decoded::Read(cursors) => cursors
                .into_iter()
                .flatten()
                .map(|part| {
                    let entry = &part.object[0];
                    (
                        entry.key.clone(),
                        entry.etag.clone(),
                        part.range.map(|r| (r.start, r.end)),
                    )
                })
                .collect(),
            Decoded::Acknowledge(reason) => panic!("acknowledged: {reason}"),
            Decoded::Fail(reason) => panic!("failed: {reason}"),
        }
    }

    fn keys(decoded: Decoded) -> Vec<String> {
        read(decoded).into_iter().map(|(key, _, _)| key).collect()
    }

    fn failed(decoded: Decoded) -> String {
        match decoded {
            Decoded::Fail(reason) => reason,
            Decoded::Read(cursors) => panic!("read {} parts", cursors.len()),
            Decoded::Acknowledge(reason) => panic!("acknowledged: {reason}"),
        }
    }

    fn acknowledged(decoded: Decoded) -> &'static str {
        match decoded {
            Decoded::Acknowledge(reason) => reason,
            Decoded::Read(cursors) => panic!("read {} parts", cursors.len()),
            Decoded::Fail(reason) => panic!("failed: {reason}"),
        }
    }

    fn decoded(records: &[serde_json::Value]) -> Decoded {
        decode(&message(records), &scope(), packing())
    }

    /// Pins the key decoding: `+` is a space, `%2F` and a raw `/` give the
    /// same key, a percent-encoded UTF-8 sequence decodes, and a malformed
    /// escape or bytes that are not UTF-8 fail the message.
    #[test]
    fn a_notification_key_is_form_decoded() {
        for key in [
            "data/dir%2Fhello+world.ndjson",
            "data/dir/hello+world.ndjson",
            "data%2Fdir%2Fhello%20world.ndjson",
        ] {
            assert_eq!(
                keys(decoded(&[created(key, 10)])),
                ["data/dir/hello world.ndjson"],
                "{key}"
            );
        }
        assert_eq!(
            keys(decoded(&[created("data/caf%C3%A9+%2B.ndjson", 10)])),
            ["data/café +.ndjson"]
        );
        for key in [
            "data/a%2",
            "data/a%zz.ndjson",
            "data/a%+1.ndjson",
            "data/a%",
        ] {
            let reason = failed(decoded(&[created(key, 10)]));
            assert!(reason.contains("malformed `%` escape"), "{key}: {reason}");
        }
        let reason = failed(decoded(&[created("data/%FF.ndjson", 10)]));
        assert!(reason.contains("does not decode as UTF-8"), "{reason}");
    }

    /// Pins that a created record is read whether or not its `eventName`
    /// carries the `s3:` prefix, and that the envelope RustFS sends is read
    /// as the bare `Records[]` message is.
    #[test]
    fn an_event_name_with_or_without_the_s3_prefix_is_accepted() {
        for event in [
            "s3:ObjectCreated:Put",
            "ObjectCreated:Copy",
            "ObjectCreated:CompleteMultipartUpload",
        ] {
            let message = [record(event, "bucket", "data/a.ndjson", Some(10))];
            assert_eq!(keys(decoded(&message)), ["data/a.ndjson"], "{event}");
        }
        let envelope = serde_json::json!({
            "EventName": "s3:ObjectCreated:Put",
            "Key": "bucket/data/a.ndjson",
            "Records": [record("s3:ObjectCreated:Put", "bucket", "data%2Fa.ndjson", Some(10))],
        });
        let body = serde_json::to_vec(&envelope).unwrap();
        assert_eq!(keys(decode(&body, &scope(), packing())), ["data/a.ndjson"]);
    }

    /// Pins the scope check: a record for another bucket, or for a key
    /// outside `prefix/` (one that only shares the prefix's characters
    /// included), fails the whole message, as does any record when the
    /// source names no bucket; a whole-bucket source reads any key.
    #[test]
    fn a_record_for_another_bucket_or_prefix_is_dead_lettered() {
        let other = record("ObjectCreated:Put", "elsewhere", "data/a.ndjson", Some(10));
        let reason = failed(decoded(&[created("data/a.ndjson", 10), other]));
        assert!(reason.contains("notification record 1"), "{reason}");
        assert!(reason.contains("the bucket \"elsewhere\""), "{reason}");
        for key in ["other/a.ndjson", "database/a.ndjson", "data"] {
            let reason = failed(decoded(&[created(key, 10)]));
            assert!(
                reason.contains("outside the source's prefix \"data/\""),
                "{key}: {reason}"
            );
        }
        let nameless = Scope {
            bucket: None,
            prefix: Path::from("data"),
        };
        let reason = failed(decode(
            &message(&[created("data/a.ndjson", 10)]),
            &nameless,
            packing(),
        ));
        assert!(
            reason.contains("which this source does not read"),
            "{reason}"
        );
        let body = message(&[created("any/where.ndjson", 10)]);
        assert_eq!(
            keys(decode(&body, &whole_bucket(), packing())),
            ["any/where.ndjson"]
        );
    }

    /// Pins that a created object of size 0 is acknowledged with no part,
    /// even under a key `object_store` cannot address, but not under a key
    /// that does not decode, for another bucket or outside the prefix, and
    /// that an object of one byte is read.
    #[test]
    fn a_size_zero_record_is_acknowledged_without_a_unit() {
        for key in ["data/empty.ndjson", "data//empty.ndjson", "data/folder/"] {
            acknowledged(decoded(&[created(key, 0)]));
        }
        let reason = failed(decoded(&[created("data/a%zz", 0)]));
        assert!(reason.contains("malformed `%` escape"), "{reason}");
        let reason = failed(decoded(&[record(
            "ObjectCreated:Put",
            "elsewhere",
            "data/empty.ndjson",
            Some(0),
        )]));
        assert!(reason.contains("the bucket \"elsewhere\""), "{reason}");
        let reason = failed(decoded(&[created("other/empty.ndjson", 0)]));
        assert!(reason.contains("outside the source's prefix"), "{reason}");
        assert_eq!(
            keys(decoded(&[created("data/one.ndjson", 1)])),
            ["data/one.ndjson"]
        );
        let reason = failed(decoded(&[record(
            "ObjectCreated:Put",
            "bucket",
            "data/a",
            None,
        )]));
        assert!(reason.contains("has no size"), "{reason}");
    }

    /// Pins that the S3 test event and a record that creates nothing, for
    /// any bucket, are acknowledged with no part, an event name that only
    /// begins with `ObjectCreated` included.
    #[test]
    fn a_test_event_and_a_removal_are_acknowledged_without_a_unit() {
        let test_event = serde_json::json!({
            "Service": "Amazon S3",
            "Event": "s3:TestEvent",
            "Time": "2026-10-19T12:00:00.000Z",
            "Bucket": "bucket",
            "RequestId": "5582815E1AEA5ADF",
            "HostId": "8cLeGAmw098X5cv4Zkwcmo8vvZa3eH3eKxsPzbB9wrR+YstdA6Knx4Ip8EXAMPLE",
        });
        acknowledged(decode(
            &serde_json::to_vec(&test_event).unwrap(),
            &scope(),
            packing(),
        ));
        for event in [
            "ObjectRemoved:Delete",
            "s3:ObjectRemoved:DeleteMarkerCreated",
            "ObjectCreatedButNot",
        ] {
            acknowledged(decoded(&[record(
                event,
                "elsewhere",
                "other/a.ndjson",
                None,
            )]));
        }
        acknowledged(decoded(&[]));
    }

    /// Pins that a key `Path::parse` rejects or rewrites fails the message,
    /// including one whose encoded `/` decodes to an empty segment.
    #[test]
    fn a_key_path_parse_rejects_or_rewrites_is_dead_lettered() {
        for key in [
            "a//b.ndjson",
            "/a.ndjson",
            "a/",
            "a/%2Fb.ndjson",
            "a/./b.ndjson",
        ] {
            let body = message(&[created(key, 10)]);
            let reason = failed(decode(&body, &whole_bucket(), packing()));
            assert!(
                reason.contains("cannot be read under its own name"),
                "{key}: {reason}"
            );
        }
    }

    /// Pins that a body that is neither a descriptor nor a notification, a
    /// body with a descriptor's `v` that does not decode as one, or a created
    /// record the source cannot read, fails the message, and that a
    /// descriptor still reads.
    #[test]
    fn an_unknown_notification_shape_is_dead_lettered() {
        for body in [
            b"not json".to_vec(),
            b"{}".to_vec(),
            br#"{"Records":{}}"#.to_vec(),
            br#"["Records"]"#.to_vec(),
        ] {
            let reason = failed(decode(&body, &scope(), packing()));
            assert!(
                reason.contains("neither a descriptor nor a storage notification"),
                "{reason}"
            );
        }
        let no_s3 = serde_json::json!({ "eventName": "ObjectCreated:Put" });
        let no_name = serde_json::json!({ "s3": { "bucket": { "name": "bucket" } } });
        let mut negative = created("data/a.ndjson", 10);
        negative["s3"]["object"]["size"] = serde_json::json!(-1);
        let mut fractional = created("data/a.ndjson", 10);
        fractional["s3"]["object"]["size"] = serde_json::json!(1.5);
        for record in [no_s3, no_name, negative, fractional] {
            failed(decoded(&[record]));
        }
        for body in [br#"{"v":1,"objects":[]}"#.to_vec(), br#"{"v":2}"#.to_vec()] {
            let reason = failed(decode(&body, &scope(), packing()));
            assert!(reason.contains("not a descriptor"), "{reason}");
        }
        let object = DescriptorObject {
            key: "data/a.ndjson".into(),
            size: 10,
            etag: None,
            last_modified_ms: 1,
        };
        let body = SplitDescriptor::new(vec![object]).encode().unwrap();
        assert_eq!(keys(decode(&body, &scope(), packing())), ["data/a.ndjson"]);
    }

    /// Pins that a notification's ETag is sent quoted, as a listing reports
    /// it, that a quoted one is kept, the empty `""` included, that one with
    /// only a leading quote is wrapped, and that an absent one stays absent.
    #[test]
    fn an_unquoted_event_etag_is_sent_quoted() {
        let etag = |value: serde_json::Value| {
            let mut created = created("data/a.ndjson", 10);
            created["s3"]["object"]["eTag"] = value;
            read(decoded(&[created])).remove(0).1
        };
        assert_eq!(etag(serde_json::json!("abc")), Some("\"abc\"".to_owned()));
        assert_eq!(
            etag(serde_json::json!("\"abc\"")),
            Some("\"abc\"".to_owned())
        );
        assert_eq!(etag(serde_json::json!("\"")), Some("\"\"\"".to_owned()));
        assert_eq!(
            etag(serde_json::json!("\"abc")),
            Some("\"\"abc\"".to_owned())
        );
        assert_eq!(etag(serde_json::json!("\"\"")), Some("\"\"".to_owned()));
        assert_eq!(etag(serde_json::Value::Null), None);
    }

    /// Pins that an ETag `If-Match` cannot carry fails the message, from a
    /// notification or a descriptor, and that a tab, a space, `~` and a
    /// non-ASCII character are sent.
    #[test]
    fn an_etag_that_cannot_be_a_header_value_is_dead_lettered() {
        let with_etag = |etag: &str| {
            let mut created = created("data/a.ndjson", 10);
            created["s3"]["object"]["eTag"] = serde_json::json!(etag);
            decoded(&[created])
        };
        for etag in ["abc\u{1}", "abc\u{1f}", "abc\u{7f}", "a\nb"] {
            let reason = failed(with_etag(etag));
            assert!(reason.contains("cannot be sent in `If-Match`"), "{reason}");
        }
        for etag in ["a\tb", "a b", "a~b", "a\u{e9}b"] {
            assert_eq!(keys(with_etag(etag)), ["data/a.ndjson"], "{etag:?}");
        }
        let object = |etag: &str| DescriptorObject {
            key: "data/a.ndjson".into(),
            size: 10,
            etag: Some(etag.to_owned()),
            last_modified_ms: 1,
        };
        let body = SplitDescriptor::new(vec![object("\"abc\u{1}\"")])
            .encode()
            .unwrap();
        let reason = failed(decode(&body, &scope(), packing()));
        assert!(reason.contains("cannot be sent in `If-Match`"), "{reason}");
        let body = SplitDescriptor::new(vec![object("\"abc\"")])
            .encode()
            .unwrap();
        assert_eq!(keys(decode(&body, &scope(), packing())), ["data/a.ndjson"]);
    }

    /// Pins that a created object of size 0 is dropped before its ETag is
    /// checked: an ETag `If-Match` cannot carry on an empty object leaves the
    /// message's other records to be read.
    #[test]
    fn an_empty_object_with_an_unsendable_etag_is_dropped() {
        let mut empty = created("data/empty.ndjson", 0);
        empty["s3"]["object"]["eTag"] = serde_json::json!("abc\u{1}");
        assert_eq!(
            keys(decoded(&[empty, created("data/a.ndjson", 10)])),
            ["data/a.ndjson"]
        );
    }

    /// Pins that the ETag check covers a descriptor naming a byte range, as
    /// it does a whole-object descriptor.
    #[test]
    fn a_ranged_descriptor_with_an_unsendable_etag_fails() {
        let object = |etag: &str| DescriptorObject {
            key: "data/a.ndjson".into(),
            size: 300,
            etag: Some(etag.to_owned()),
            last_modified_ms: 1,
        };
        let range = SplitRange::new(0, 100, b'\n');
        let body = SplitDescriptor::with_range(object("\"abc\u{1}\""), range)
            .encode()
            .unwrap();
        let reason = failed(decode(&body, &scope(), packing()));
        assert!(reason.contains("cannot be sent in `If-Match`"), "{reason}");
        let body = SplitDescriptor::with_range(object("\"abc\""), range)
            .encode()
            .unwrap();
        assert_eq!(keys(decode(&body, &scope(), packing())), ["data/a.ndjson"]);
    }

    /// Pins that a record with no `eventName`, or one that is not a string,
    /// fails its message even when its `s3` fields name an object to read.
    #[test]
    fn a_record_without_an_event_name_fails_its_message() {
        for name in [None, Some(serde_json::json!(1))] {
            let mut record = created("data/a.ndjson", 10);
            match name {
                None => {
                    record.as_object_mut().unwrap().remove("eventName");
                }
                Some(value) => record["eventName"] = value,
            }
            let reason = failed(decoded(&[created("data/b.ndjson", 10), record]));
            assert!(reason.contains("notification record 1"), "{reason}");
            assert!(
                reason.contains("`eventName` is missing or not a string"),
                "{reason}"
            );
        }
    }

    /// Pins that a dropped record leaves the records after it to be read: a
    /// removal, an event that creates nothing and an empty object ahead of a
    /// created object each give the message that object's part alone.
    #[test]
    fn a_dropped_record_leaves_the_records_after_it() {
        for dropped in [
            record("ObjectRemoved:Delete", "bucket", "data/gone.ndjson", None),
            record(
                "ObjectRestore:Completed",
                "elsewhere",
                "other/a.ndjson",
                Some(10),
            ),
            created("data/empty.ndjson", 0),
        ] {
            assert_eq!(
                keys(decoded(&[dropped, created("data/a.ndjson", 10)])),
                ["data/a.ndjson"]
            );
        }
    }

    /// Pins that a message's created records give one part each, in record
    /// order, that a qualifying object is cut into ranges, and that
    /// `eventTime` becomes the object's time, 0 when absent or unreadable.
    #[test]
    fn a_message_with_several_records_gives_one_part_each() {
        let parts = read(decoded(&[
            created("data/small.ndjson", 10),
            created("data/large.ndjson", 300),
        ]));
        assert_eq!(
            parts,
            [
                (
                    "data/small.ndjson".to_owned(),
                    Some("\"0123abcd\"".to_owned()),
                    None
                ),
                (
                    "data/large.ndjson".to_owned(),
                    Some("\"0123abcd\"".to_owned()),
                    Some((0, 100))
                ),
                (
                    "data/large.ndjson".to_owned(),
                    Some("\"0123abcd\"".to_owned()),
                    Some((100, 200))
                ),
                (
                    "data/large.ndjson".to_owned(),
                    Some("\"0123abcd\"".to_owned()),
                    Some((200, 300))
                ),
            ]
        );
        let Decoded::Read(cursors) = decoded(&[created("data/a.ndjson", 10)]) else {
            panic!("not read");
        };
        let expected = chrono::DateTime::parse_from_rfc3339("2026-10-19T12:00:00.000Z")
            .unwrap()
            .timestamp_millis();
        assert_eq!(cursors[0].object[0].last_modified_ms, expected);
        for time in [serde_json::Value::Null, serde_json::json!("yesterday")] {
            let mut record = created("data/a.ndjson", 10);
            record["eventTime"] = time;
            let Decoded::Read(cursors) = decoded(&[record]) else {
                panic!("not read");
            };
            assert_eq!(cursors[0].object[0].last_modified_ms, 0);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Pins that every listed object yields one whole unit, or ranges
        /// that tile `[0, size)` with each between half the target and the
        /// target, and that each id is the one `split_id_for` or
        /// `split_id_for_range` mints, the same on every call and different
        /// under another ETag.
        #[test]
        fn prop_units_tile_the_listing_one_object_or_range_each(
            objects in proptest::collection::vec(
                (0u8..7, any::<u64>(), 0usize..6, 0u8..6, any::<bool>()),
                0..12,
            ),
            target in target(),
            codec in 0u8..4,
            delimiter in proptest::option::of(Just(b'\n')),
        ) {
            let compression = compression(codec);
            let packing = Packing { target_bytes: target, compression, delimiter };
            let listing: Vec<(ObjectEntry, bool)> = objects
                .iter()
                .enumerate()
                .map(|(i, &(size_shape, random, ext, key_shape, etag))| {
                    let (key, unaddressable) = key(i, key_shape, EXTENSIONS[ext]);
                    let entry = ObjectEntry {
                        etag: etag.then(|| format!("\"etag-{i}\"")),
                        key,
                        size: size(target, size_shape, random),
                        last_modified_ms: 1,
                    };
                    (entry, unaddressable)
                })
                .collect();
            let entries: Vec<ObjectEntry> = listing.iter().map(|(e, _)| e.clone()).collect();
            let got = units(entries.clone(), &packing).unwrap();
            let again = units(entries.clone(), &packing).unwrap();
            prop_assert_eq!(&got, &again, "units differ between two calls");

            let mut seen = 0;
            for (entry, unaddressable) in &listing {
                let mine: Vec<(String, SplitDescriptor)> = got
                    .iter()
                    .map(|u| (u.id.clone(), SplitDescriptor::decode(&u.body).unwrap()))
                    .filter(|(_, d)| d.objects[0].key == entry.key)
                    .collect();
                seen += mine.len();
                for (_, d) in &mine {
                    prop_assert_eq!(d.objects.len(), 1);
                    prop_assert_eq!(&d.to_entries()[0], entry);
                }
                let cut = delimiter.is_some()
                    && entry.size > target
                    && entry.size <= MAX_SUBDIVIDABLE_BYTES
                    && entry.etag.is_some()
                    && plain(compression, &entry.key)
                    && !unaddressable;
                if cut {
                    let mut next = 0;
                    for (id, d) in &mine {
                        let range = d.range.expect("a cut object yields only ranges");
                        prop_assert_eq!(range.start, next);
                        let len = range.end - range.start;
                        prop_assert!(target / 2 <= len && len <= target, "{:?}", range);
                        prop_assert_eq!(Some(range.delimiter), delimiter);
                        let want = split_id_for_range(
                            &entry.key,
                            entry.etag.as_deref().unwrap(),
                            range,
                        )
                        .unwrap();
                        prop_assert_eq!(id.as_str(), want.as_str());
                        next = range.end;
                    }
                    prop_assert_eq!(next, entry.size, "the ranges do not tile the object");
                } else {
                    prop_assert_eq!(mine.len(), 1, "{:?} is not one whole unit", entry);
                    let (id, d) = &mine[0];
                    prop_assert_eq!(d.range, None);
                    let want =
                        split_id_for([(entry.key.as_str(), entry.etag.as_deref())]).unwrap();
                    prop_assert_eq!(id.as_str(), want.as_str());
                }
            }
            prop_assert_eq!(seen, got.len(), "a unit names no listed object");

            let retagged: Vec<ObjectEntry> = entries
                .iter()
                .map(|e| ObjectEntry {
                    etag: e.etag.as_ref().map(|t| format!("{t}-2")),
                    ..e.clone()
                })
                .collect();
            let other = units(retagged, &packing).unwrap();
            for (a, b) in got.iter().zip(&other) {
                let tagged = SplitDescriptor::decode(&a.body).unwrap().objects[0].etag.is_some();
                if tagged {
                    prop_assert_ne!(&a.id, &b.id, "the id ignores the ETag");
                }
            }
        }
    }
}
