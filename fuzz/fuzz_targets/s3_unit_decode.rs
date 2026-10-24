//! Feed delivery decoding over arbitrary bytes, and the round trip of a
//! re-enqueued unit's body.
//!
//! A delivery's body is whatever a writer to the queue sent: a descriptor, a
//! storage notification, or neither. The decode arms assert that every part
//! the source would read carries a quoted ETag that `If-Match` can send, a
//! version that is non-empty, not `null`, at most 1,024 bytes and free of
//! spaces and control bytes, and a key under the source's prefix. One decode
//! arm takes raw bytes; the other builds a notification record from arbitrary
//! fields, so every run reaches the record parser, and asserts that a record
//! read names the version and count it carried. The round-trip arm asserts
//! that a re-enqueued unit's body is ASCII when its ETag is, and that a body
//! the source reads names the key, the quoted ETag and the count it was
//! written with, and no version.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use spate_s3::fuzz_seams::{
    DecodedPart, DecodedUnit, decode_unit, notification_body, quoted, re_enqueue_body,
};

const BUCKET: &str = "bucket";
const PREFIX: &str = "data";

#[derive(Arbitrary, Debug)]
struct Input {
    body: Vec<u8>,
    record: Record,
    overwrite: Overwrite,
}

/// One notification record's fields, any of which may be absent.
#[derive(Arbitrary, Debug)]
struct Record {
    key: String,
    size: Option<u64>,
    etag: Option<String>,
    version: Option<String>,
    re_enqueues: Option<u32>,
}

/// What a HEAD reported after an overwrite.
#[derive(Arbitrary, Debug)]
struct Overwrite {
    key: String,
    etag: Option<String>,
    size: u64,
    last_modified_ms: i64,
    re_enqueues: u32,
}

fuzz_target!(|input: Input| {
    if let DecodedUnit::Read(parts) = decode_unit(&input.body, BUCKET, PREFIX) {
        check(&parts);
    }
    record(&input.record);
    round_trip(&input.overwrite);
});

/// Decode a notification built from `record`'s fields.
fn record(record: &Record) {
    let body = notification_body(
        BUCKET,
        &format!("{PREFIX}/{}", record.key),
        record.size,
        record.etag.as_deref(),
        record.version.as_deref(),
        record.re_enqueues,
    );
    if let DecodedUnit::Read(parts) = decode_unit(&body, BUCKET, PREFIX) {
        check(&parts);
        let named = record
            .version
            .as_deref()
            .filter(|v| !v.is_empty() && *v != "null");
        for part in &parts {
            assert_eq!(part.version.as_deref(), named, "{record:?}");
            assert_eq!(part.re_enqueues, record.re_enqueues.unwrap_or(0));
        }
    }
}

/// Encode a re-enqueued unit's body and decode it back.
fn round_trip(overwrite: &Overwrite) {
    let key = format!("{PREFIX}/{}", overwrite.key);
    let body = re_enqueue_body(
        BUCKET,
        &key,
        overwrite.etag.as_deref(),
        overwrite.size,
        overwrite.last_modified_ms,
        overwrite.re_enqueues,
    );
    if overwrite.etag.as_deref().is_none_or(|etag| etag.is_ascii()) {
        assert!(body.is_ascii(), "a non-ASCII body for an ASCII ETag");
    }
    if let DecodedUnit::Read(parts) = decode_unit(&body, BUCKET, PREFIX) {
        assert_eq!(parts.len(), 1, "{parts:?}");
        let part = &parts[0];
        assert_eq!(part.key, key);
        assert_eq!(part.etag, overwrite.etag.as_deref().map(quoted));
        assert_eq!(part.version, None);
        assert_eq!(part.re_enqueues, overwrite.re_enqueues);
    }
}

/// The properties every part a delivery decodes to holds.
fn check(parts: &[DecodedPart]) {
    for part in parts {
        if let Some(etag) = &part.etag {
            assert!(
                etag.len() >= 2 && etag.starts_with('"') && etag.ends_with('"'),
                "an unquoted ETag {etag:?}"
            );
            assert!(
                etag.bytes().all(|b| b == b'\t' || (b >= 0x20 && b != 0x7f)),
                "an ETag `If-Match` cannot send {etag:?}"
            );
        }
        if let Some(version) = &part.version {
            assert!(!version.is_empty() && version != "null", "{version:?}");
            assert!(
                version.len() <= 1024,
                "a version of {} bytes",
                version.len()
            );
            assert!(
                !version.bytes().any(|b| b == b' ' || b.is_ascii_control()),
                "{version:?}"
            );
        }
        if part.re_enqueues > 0 || part.version.is_some() {
            assert!(
                part.key.starts_with("data/"),
                "a notification part outside the prefix: {:?}",
                part.key
            );
        }
    }
}
