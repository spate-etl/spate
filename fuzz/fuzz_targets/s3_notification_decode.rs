//! Feed-unit decoding over storage notifications built from arbitrary records.
//!
//! A unit's body comes from a queue or topic, so whoever can publish there
//! chooses its bytes. The target builds a `Records` message for a source
//! reading `bucket` under one of a few prefixes. Its records' event names,
//! keys, sizes, ETags, versions and re-enqueue counts take the shapes the
//! decoder's rules distinguish, and the target predicts each record's outcome
//! from how it was built: dropped, read, or failing its message. A message
//! with a failing record must read nothing. Any other message must read
//! exactly the predicted parts, in order. Each part's key is under the prefix,
//! accepted by `addressable` and equal to the key before it was encoded. Each
//! ETag is quoted and accepted by `http` as a header value. Raw bytes are left
//! to `s3_unit_decode`.
//!
//! The prediction follows the decoder's order where it decides between
//! dropping a record and failing its message. A record whose fields do not
//! deserialise fails even at size 0. A size of 0 then drops the record after
//! the bucket, key decoding, prefix and size checks, and before the
//! `addressable`, ETag and `versionId` checks.

#![no_main]

use arbitrary::Arbitrary;
use http::HeaderValue;
use libfuzzer_sys::fuzz_target;
use serde_json::{Map, Value, json};
use spate_s3::fuzz_seams::{DecodedUnit, addressable, decode_unit};
use std::fmt::Write as _;

const BUCKET: &str = "bucket";
const PREFIXES: [&str; 3] = ["", "data", "a/b"];
/// The longest `versionId` the source reads.
const MAX_VERSION_BYTES: usize = 1024;

#[derive(Arbitrary, Debug)]
struct Input {
    prefix: u8,
    records: Vec<Record>,
    envelope: bool,
}

#[derive(Arbitrary, Debug)]
struct Record {
    event: Event,
    /// `s3` is not an object, so the record does not deserialise.
    unreadable: bool,
    other_bucket: bool,
    key: Key,
    size: Size,
    etag: Etag,
    version: Version,
    re_enqueues: ReEnqueues,
    event_time: Option<String>,
}

#[derive(Arbitrary, Debug)]
enum Event {
    Created { s3_prefix: bool, suffix: String },
    Removed { s3_prefix: bool },
    Other(String),
    NotString(i64),
    Missing,
}

/// A key as the target means it, and how it is encoded into the record.
#[derive(Arbitrary, Debug)]
struct Key {
    /// Start the key with the source's prefix and a `/`.
    under_prefix: bool,
    chars: Vec<(Char, Encoding)>,
    tail: Tail,
}

#[derive(Arbitrary, Debug)]
enum Char {
    /// One of the characters the key rules single out.
    Special(u8),
    Any(char),
}

#[derive(Arbitrary, Debug)]
enum Encoding {
    Literal,
    PercentUpper,
    PercentLower,
}

/// Bytes appended after the encoded key, each of which fails its decoding.
#[derive(Arbitrary, Debug)]
enum Tail {
    None,
    LonePercent,
    NonHexEscape,
    InvalidUtf8,
    TruncatedUtf8,
}

#[derive(Arbitrary, Debug)]
enum Size {
    Absent,
    Null,
    Zero,
    Small(u32),
    Max,
}

#[derive(Arbitrary, Debug)]
enum Etag {
    Absent,
    Value(String),
    Quoted(String),
}

/// A record's `versionId`.
#[derive(Arbitrary, Debug)]
enum Version {
    Absent,
    Null,
    NullText,
    Value(String),
    /// A run of `v` from 1,000 to 1,049 bytes, either side of the cap.
    Long(u8),
    NotString(i64),
}

/// A record's `spateReEnqueues`.
#[derive(Arbitrary, Debug)]
enum ReEnqueues {
    Absent,
    Null,
    Count(u32),
    /// More than `u32::MAX`.
    Over(u32),
    Text(String),
}

/// One part as `(key, etag, version, re_enqueues)`.
type Part = (String, Option<String>, Option<String>, u32);

/// What the decoder must do with one record.
#[derive(Debug)]
enum Outcome {
    Drop,
    Read(Part),
    Fail,
}

fuzz_target!(|input: Input| {
    let prefix = PREFIXES[usize::from(input.prefix) % PREFIXES.len()];
    let mut values = Vec::with_capacity(input.records.len());
    let mut outcomes = Vec::with_capacity(input.records.len());
    for record in &input.records {
        let (value, outcome) = build(record, prefix);
        values.push(value);
        outcomes.push(outcome);
    }
    let mut message = json!({ "Records": values });
    if input.envelope {
        message["EventName"] = json!("s3:ObjectCreated:Put");
        message["Key"] = json!("bucket/key");
    }
    let body = serde_json::to_vec(&message).expect("a JSON value serialises");
    let decoded = decode_unit(&body, BUCKET, prefix);

    if outcomes.iter().any(|o| matches!(o, Outcome::Fail)) {
        assert_eq!(
            decoded,
            DecodedUnit::Fail,
            "a message with a failing record must fail; expected {outcomes:?}"
        );
        return;
    }
    let parts: Vec<Part> = match decoded {
        DecodedUnit::Read(parts) => parts
            .into_iter()
            .map(|p| (p.key, p.etag, p.version, p.re_enqueues))
            .collect(),
        DecodedUnit::Acknowledge => Vec::new(),
        DecodedUnit::Fail => panic!("failed; expected {outcomes:?}"),
    };
    for (key, etag, _, _) in &parts {
        assert!(
            prefix.is_empty() || key.starts_with(&format!("{prefix}/")),
            "the part {key:?} is outside the prefix {prefix:?}"
        );
        assert!(addressable(key), "the part {key:?} is not addressable");
        if let Some(etag) = etag {
            assert!(
                etag.len() >= 2 && etag.starts_with('"') && etag.ends_with('"'),
                "the ETag {etag:?} of {key:?} is not quoted"
            );
            assert!(
                HeaderValue::from_str(etag).is_ok(),
                "the ETag {etag:?} of {key:?} cannot be sent"
            );
        }
    }
    let expected: Vec<Part> = outcomes
        .into_iter()
        .filter_map(|o| match o {
            Outcome::Read(part) => Some(part),
            Outcome::Drop | Outcome::Fail => None,
        })
        .collect();
    assert_eq!(
        parts, expected,
        "the message read other parts than predicted"
    );
});

/// One record's JSON and what the decoder must do with it.
fn build(record: &Record, prefix: &str) -> (Value, Outcome) {
    let (encoded, decoded) = record.key.encode(prefix);
    let size = match record.size {
        Size::Absent | Size::Null => None,
        Size::Zero => Some(0),
        Size::Small(n) => Some(u64::from(n.max(1))),
        Size::Max => Some(u64::MAX),
    };
    let etag = match &record.etag {
        Etag::Absent => None,
        Etag::Value(e) => Some(e.clone()),
        Etag::Quoted(e) => Some(format!("\"{e}\"")),
    };

    let mut object = Map::new();
    object.insert("key".to_owned(), json!(encoded));
    match record.size {
        Size::Absent => {}
        Size::Null => {
            object.insert("size".to_owned(), Value::Null);
        }
        _ => {
            object.insert("size".to_owned(), json!(size));
        }
    }
    if let Some(etag) = &etag {
        object.insert("eTag".to_owned(), json!(etag));
    }
    if let Some(version) = record.version.json() {
        object.insert("versionId".to_owned(), version);
    }
    let bucket = if record.other_bucket { "other" } else { BUCKET };
    let s3 = if record.unreadable {
        json!(7)
    } else {
        json!({ "bucket": { "name": bucket }, "object": object })
    };
    let mut value = json!({ "eventVersion": "2.1", "s3": s3 });
    if let Some(time) = &record.event_time {
        value["eventTime"] = json!(time);
    }
    if let Some(count) = record.re_enqueues.json() {
        value["spateReEnqueues"] = count;
    }
    let name = match &record.event {
        Event::Created { s3_prefix, suffix } => {
            Some(format!("{}ObjectCreated:{suffix}", s3_tag(*s3_prefix)))
        }
        Event::Removed { s3_prefix } => Some(format!("{}ObjectRemoved:Delete", s3_tag(*s3_prefix))),
        Event::Other(name) => Some(name.clone()),
        Event::NotString(n) => {
            value["eventName"] = json!(n);
            None
        }
        Event::Missing => None,
    };
    if let Some(name) = &name {
        value["eventName"] = json!(name);
    }

    let outcome = match name {
        None => Outcome::Fail,
        Some(name) if !created(&name) => Outcome::Drop,
        Some(_) => predict(record, prefix, decoded, size, etag),
    };
    (value, outcome)
}

/// The outcome of a created record that names an object.
fn predict(
    record: &Record,
    prefix: &str,
    decoded: Option<String>,
    size: Option<u64>,
    etag: Option<String>,
) -> Outcome {
    let (Some(version), Some(re_enqueues)) = (record.version.text(), record.re_enqueues.count())
    else {
        return Outcome::Fail;
    };
    if record.unreadable || record.other_bucket {
        return Outcome::Fail;
    }
    let Some(key) = decoded else {
        return Outcome::Fail;
    };
    if !prefix.is_empty()
        && !key
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
    {
        return Outcome::Fail;
    }
    match size {
        None => return Outcome::Fail,
        Some(0) => return Outcome::Drop,
        Some(_) => {}
    }
    if !addressable(&key)
        || etag
            .as_deref()
            .is_some_and(|e| HeaderValue::from_str(e).is_err())
    {
        return Outcome::Fail;
    }
    let version = version.filter(|v| !v.is_empty() && v != "null");
    if version.as_deref().is_some_and(|v| {
        v.len() > MAX_VERSION_BYTES || v.bytes().any(|b| b == b' ' || b.is_ascii_control())
    }) {
        return Outcome::Fail;
    }
    let etag = etag.map(|e| {
        if e.len() >= 2 && e.starts_with('"') && e.ends_with('"') {
            e
        } else {
            format!("\"{e}\"")
        }
    });
    Outcome::Read((key, etag, version, re_enqueues))
}

impl Key {
    /// The key as a notification carries it, and as the decoder must read it
    /// back, `None` when the encoding is malformed.
    fn encode(&self, prefix: &str) -> (String, Option<String>) {
        let mut decoded = String::new();
        let mut encoded = String::new();
        if self.under_prefix && !prefix.is_empty() {
            decoded.push_str(prefix);
            decoded.push('/');
            encoded.push_str(&decoded);
        }
        for (c, encoding) in &self.chars {
            let c = c.get();
            decoded.push(c);
            match (c, encoding) {
                (' ', Encoding::Literal) => encoded.push('+'),
                ('+' | '%', Encoding::Literal) | (_, Encoding::PercentUpper) => {
                    percent(&mut encoded, c, true);
                }
                (_, Encoding::PercentLower) => percent(&mut encoded, c, false),
                (_, Encoding::Literal) => encoded.push(c),
            }
        }
        let malformed = match self.tail {
            Tail::None => None,
            Tail::LonePercent => Some("%"),
            Tail::NonHexEscape => Some("%G0"),
            Tail::InvalidUtf8 => Some("%FF"),
            Tail::TruncatedUtf8 => Some("%C3"),
        };
        match malformed {
            None => (encoded, Some(decoded)),
            Some(tail) => (encoded + tail, None),
        }
    }
}

impl Char {
    fn get(&self) -> char {
        const SPECIAL: [char; 10] = ['a', '/', '.', ' ', '+', '%', '\t', '\0', 'é', '中'];
        match self {
            Char::Special(i) => SPECIAL[usize::from(*i) % SPECIAL.len()],
            Char::Any(c) => *c,
        }
    }
}

impl Version {
    /// The `versionId` written, `None` to leave the field out.
    fn json(&self) -> Option<Value> {
        match self {
            Version::Absent => None,
            Version::Null => Some(Value::Null),
            Version::NullText => Some(json!("null")),
            Version::Value(v) => Some(json!(v)),
            Version::Long(n) => Some(json!("v".repeat(1000 + usize::from(*n) % 50))),
            Version::NotString(n) => Some(json!(n)),
        }
    }

    /// The string the record carries, `Some(None)` for none, and `None` when
    /// the field does not deserialise as a string.
    fn text(&self) -> Option<Option<String>> {
        match self {
            Version::Absent | Version::Null => Some(None),
            Version::NotString(_) => None,
            _ => Some(self.json().and_then(|v| v.as_str().map(str::to_owned))),
        }
    }
}

impl ReEnqueues {
    /// The `spateReEnqueues` written, `None` to leave the field out.
    fn json(&self) -> Option<Value> {
        match self {
            ReEnqueues::Absent => None,
            ReEnqueues::Null => Some(Value::Null),
            ReEnqueues::Count(n) => Some(json!(n)),
            ReEnqueues::Over(n) => Some(json!(u64::from(u32::MAX) + 1 + u64::from(*n))),
            ReEnqueues::Text(s) => Some(json!(s)),
        }
    }

    /// The count the record carries, `None` when it is not a `u32`.
    fn count(&self) -> Option<u32> {
        match self {
            ReEnqueues::Absent | ReEnqueues::Null => Some(0),
            ReEnqueues::Count(n) => Some(*n),
            ReEnqueues::Over(_) | ReEnqueues::Text(_) => None,
        }
    }
}

/// Append `c`'s UTF-8 bytes as `%HH` escapes.
fn percent(out: &mut String, c: char, upper: bool) {
    let mut buf = [0; 4];
    for b in c.encode_utf8(&mut buf).bytes() {
        let _ = if upper {
            write!(out, "%{b:02X}")
        } else {
            write!(out, "%{b:02x}")
        };
    }
}

/// Whether `name` is an `ObjectCreated:*` event, with or without `s3:`.
fn created(name: &str) -> bool {
    name.strip_prefix("s3:")
        .unwrap_or(name)
        .starts_with("ObjectCreated:")
}

fn s3_tag(prefixed: bool) -> &'static str {
    if prefixed { "s3:" } else { "" }
}
