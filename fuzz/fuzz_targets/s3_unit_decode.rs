//! Feed delivery decoding over arbitrary bytes, and the round trip of a
//! re-enqueued unit's body.
//!
//! A delivery's body is whatever a writer to the queue sent: a descriptor, a
//! storage notification, or neither. The decode arm asserts that every part
//! the source would read carries a quoted ETag that `If-Match` can send, a
//! version that is non-empty, not `null`, at most 1,024 bytes and free of
//! spaces and control bytes, and a key under the source's prefix. The
//! round-trip arm asserts that a re-enqueued unit's body is ASCII when its
//! ETag is, and that a body the source reads names the key, the quoted ETag
//! and the count it was written with, and no version.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use spate_s3::fuzz_seams::{DecodedUnit, decode_unit, quoted, re_enqueue_body};

const BUCKET: &str = "bucket";
const PREFIX: &str = "data";

#[derive(Arbitrary, Debug)]
struct Input {
    body: Vec<u8>,
    key: String,
    etag: Option<String>,
    size: u64,
    last_modified_ms: i64,
    re_enqueues: u32,
}

fuzz_target!(|input: Input| {
    if let DecodedUnit::Read(parts) = decode_unit(&input.body, BUCKET, PREFIX) {
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

    let key = format!("{PREFIX}/{}", input.key);
    let body = re_enqueue_body(
        BUCKET,
        &key,
        input.etag.as_deref(),
        input.size,
        input.last_modified_ms,
        input.re_enqueues,
    );
    if input.etag.as_deref().is_none_or(|etag| etag.is_ascii()) {
        assert!(body.is_ascii(), "a non-ASCII body for an ASCII ETag");
    }
    if let DecodedUnit::Read(parts) = decode_unit(&body, BUCKET, PREFIX) {
        assert_eq!(parts.len(), 1, "{parts:?}");
        let part = &parts[0];
        assert_eq!(part.key, key);
        assert_eq!(part.etag, input.etag.as_deref().map(quoted));
        assert_eq!(part.version, None);
        assert_eq!(part.re_enqueues, input.re_enqueues);
    }
});
