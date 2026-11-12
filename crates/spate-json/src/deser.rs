//! The JSON deserializers.
//!
//! Both deserializers share [`DecoderCore`]: framing → per-item decode →
//! emit. Framing splits one payload into 0..N JSON documents; each document is
//! decoded into the record type and emitted with the payload's metadata and a
//! clone of the batch ack. Empty or whitespace-only payloads decode to zero
//! records (source tombstones).
//!
//! Backpressure is handled by the chain *between* payloads, so the decoder
//! emits every record for a payload and ignores the [`Flow`] return (like the
//! framework's other decoders); the driver stops pulling the next payload when
//! downstream is full.

use crate::backend;
use crate::config::{JsonFraming, OnError};
use crate::metrics::JsonDeserMetrics;
use serde::de::DeserializeOwned;
use serde_json::Value;
use spate_core::checkpoint::AckRef;
use spate_core::deser::{Deserializer, EmitRecord, Owned};
use spate_core::error::DeserError;
use spate_core::record::{RawPayload, Record, RecordMeta};
use spate_core::telemetry::RateLimit;
use std::marker::PhantomData;

/// Rate-limits the per-record skip warning so a poison storm can't flood the
/// logs; the exact drop count is always in `spate_json_deser_records_dropped_total`.
static SKIP_WARN: RateLimit = RateLimit::hot_path();

/// Why a record was dropped, for the metric label and log field.
#[derive(Clone, Copy)]
enum Reason {
    Malformed,
    DuplicateKey,
}

impl Reason {
    fn label(self) -> &'static str {
        match self {
            Reason::Malformed => "malformed",
            Reason::DuplicateKey => "duplicate_key",
        }
    }
}

/// Classify an error returned by the duplicate-key structural pass.
/// [`DupGuard`](backend) accepts every JSON shape, so a *data* error is our
/// injected duplicate-key rejection, while a *syntax*/EOF error is just
/// malformed input the structural pass happened to reach before the decode
/// did, which must still be reported as `malformed`, not `duplicate_key`.
fn dup_check_reason(e: &backend::DecodeError) -> Reason {
    if e.is_data {
        Reason::DuplicateKey
    } else {
        Reason::Malformed
    }
}

/// Shared framing + error-policy state behind every JSON deserializer.
#[derive(Clone, Debug)]
pub(crate) struct DecoderCore {
    pub(crate) framing: JsonFraming,
    pub(crate) on_error: OnError,
    pub(crate) reject_duplicate_keys: bool,
    pub(crate) metrics: Option<JsonDeserMetrics>,
}

impl DecoderCore {
    /// Decode `raw` into 0..N records of type `P` and emit them.
    fn run<'buf, P>(
        &self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, P>,
    ) -> Result<(), DeserError>
    where
        P: DeserializeOwned,
    {
        if is_blank(raw.bytes) {
            return Ok(()); // tombstone
        }
        // Every record from one payload shares its metadata, and computing it
        // hashes the whole key.
        let meta = raw.meta();
        match self.framing {
            JsonFraming::Single => self.run_single(raw.bytes, meta, ack, out),
            JsonFraming::Ndjson => self.run_ndjson(raw.bytes, meta, ack, out),
            JsonFraming::Array => self.run_array(raw.bytes, meta, ack, out),
        }
    }

    fn run_single<'buf, P: DeserializeOwned>(
        &self,
        bytes: &'buf [u8],
        meta: RecordMeta,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, P>,
    ) -> Result<(), DeserError> {
        if let Some(payload) = self.decode_item::<P>(bytes)? {
            let _ = out.emit(Record {
                payload,
                meta,
                ack: ack.clone(),
            });
        }
        Ok(())
    }

    fn run_ndjson<'buf, P: DeserializeOwned>(
        &self,
        bytes: &'buf [u8],
        meta: RecordMeta,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, P>,
    ) -> Result<(), DeserError> {
        // One JSON value per `\n`-separated line (NDJSON forbids embedded
        // newlines), so a line split isolates each record.
        match self.on_error {
            // Skip streams line by line: a malformed line is dropped on its own
            // and the good lines around it still flow.
            OnError::Skip => {
                for line in ndjson_lines(bytes) {
                    if let Some(payload) = self.decode_item::<P>(line)? {
                        let _ = out.emit(Record {
                            payload,
                            meta,
                            ack: ack.clone(),
                        });
                    }
                }
                Ok(())
            }
            // Fail is atomic per payload: decode every line before emitting any
            // record, so the first bad line returns the error with nothing yet
            // emitted. Emitting a prefix and *then* failing would let the
            // chain's Skip deserializer policy commit the source offset past the
            // un-emitted tail, silently losing the records after the bad line.
            OnError::Fail => {
                let mut decoded = Vec::new();
                for line in ndjson_lines(bytes) {
                    if let Some(payload) = self.decode_item::<P>(line)? {
                        decoded.push(payload);
                    }
                }
                for payload in decoded {
                    let _ = out.emit(Record {
                        payload,
                        meta,
                        ack: ack.clone(),
                    });
                }
                Ok(())
            }
        }
    }

    fn run_array<'buf, P: DeserializeOwned>(
        &self,
        bytes: &'buf [u8],
        meta: RecordMeta,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, P>,
    ) -> Result<(), DeserError> {
        // The array is decoded in one pass, so its error handling is atomic:
        // a malformed element fails the whole payload (per `on_error`). Use
        // `ndjson` when you need per-record isolation.
        if self.reject_duplicate_keys
            && let Err(e) = backend::check_no_duplicate_keys(bytes)
        {
            return self.on_item_error(&e, dup_check_reason(&e));
        }
        match backend::decode_one::<Vec<P>>(bytes) {
            Ok(items) => {
                for payload in items {
                    let _ = out.emit(Record {
                        payload,
                        meta,
                        ack: ack.clone(),
                    });
                }
                Ok(())
            }
            Err(e) => self.on_item_error(&e, Reason::Malformed),
        }
    }

    /// Decode one JSON document into `P`. `Ok(Some)` = decoded, `Ok(None)` =
    /// dropped under `on_error: skip`, `Err` = `on_error: fail`.
    fn decode_item<P: DeserializeOwned>(&self, bytes: &[u8]) -> Result<Option<P>, DeserError> {
        if self.reject_duplicate_keys
            && let Err(e) = backend::check_no_duplicate_keys(bytes)
        {
            self.on_item_error(&e, dup_check_reason(&e))?;
            return Ok(None);
        }
        match backend::decode_one::<P>(bytes) {
            Ok(payload) => Ok(Some(payload)),
            Err(e) => {
                self.on_item_error(&e, Reason::Malformed)?;
                Ok(None)
            }
        }
    }

    /// Apply the error policy to a decode failure: `Skip` counts it and
    /// returns `Ok`, `Fail` returns [`DeserError::Malformed`].
    fn on_item_error(&self, e: &backend::DecodeError, reason: Reason) -> Result<(), DeserError> {
        match self.on_error {
            OnError::Skip => {
                if let Some(m) = &self.metrics {
                    match reason {
                        Reason::Malformed => m.dropped_malformed(),
                        Reason::DuplicateKey => m.dropped_duplicate_key(),
                    }
                }
                spate_core::rate_limited_warn!(
                    SKIP_WARN,
                    reason = reason.label(),
                    error = %e,
                    "json record skipped by on_error policy"
                );
                Ok(())
            }
            OnError::Fail => Err(DeserError::Malformed {
                reason: format!("{}: {e}", reason.label()),
            }),
        }
    }
}

/// True when `bytes` is empty or only JSON whitespace, a tombstone that
/// yields no records.
fn is_blank(bytes: &[u8]) -> bool {
    bytes.iter().all(u8::is_ascii_whitespace)
}

/// The lines of an NDJSON payload that are not blank, split on `\n`.
fn ndjson_lines(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut start = 0;
    memchr::memchr_iter(b'\n', bytes)
        .chain(std::iter::once(bytes.len()))
        .map(move |end| {
            let line = &bytes[start..end];
            start = end + 1;
            line
        })
        .filter(|line| !is_blank(line))
}

/// Typed deserializer: decodes each JSON document into your own
/// `T: serde::de::DeserializeOwned`. No `serde_json` types appear in the
/// pipeline. This is the flagship path.
pub struct JsonSerdeDeserializer<T> {
    core: DecoderCore,
    _t: PhantomData<fn() -> T>,
}

// Manual `Clone`/`Debug` so the record type `T` need not be `Clone`/`Debug`.
// The only state is the `DecoderCore`; `T` is a type tag (`fn() -> T`).
impl<T> Clone for JsonSerdeDeserializer<T> {
    fn clone(&self) -> Self {
        JsonSerdeDeserializer {
            core: self.core.clone(),
            _t: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for JsonSerdeDeserializer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonSerdeDeserializer")
            .field("core", &self.core)
            .finish_non_exhaustive()
    }
}

impl<T> JsonSerdeDeserializer<T> {
    pub(crate) fn new(core: DecoderCore) -> Self {
        JsonSerdeDeserializer {
            core,
            _t: PhantomData,
        }
    }
}

impl<T> Deserializer<Owned<T>> for JsonSerdeDeserializer<T>
where
    T: DeserializeOwned + Send + 'static,
{
    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, T>,
    ) -> Result<(), DeserError> {
        self.core.run::<T>(raw, ack, out)
    }
}

/// Dynamically-typed deserializer: emits [`serde_json::Value`] records, for
/// pipelines that inspect or route on structure not known at compile time.
#[derive(Clone, Debug)]
pub struct JsonValueDeserializer {
    core: DecoderCore,
}

impl JsonValueDeserializer {
    pub(crate) fn new(core: DecoderCore) -> Self {
        JsonValueDeserializer { core }
    }
}

impl Deserializer<Owned<Value>> for JsonValueDeserializer {
    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, Value>,
    ) -> Result<(), DeserError> {
        self.core.run::<Value>(raw, ack, out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{JsonDeserializerBuilder, JsonSettings};
    use crate::framing::tests::arb_stream;
    use proptest::prelude::*;
    use serde::Deserialize;
    use spate_core::record::{Flow, PartitionId, stable_key_hash};
    use spate_test::render_metrics;

    #[derive(Debug, Deserialize, PartialEq)]
    struct Ev {
        id: i64,
        name: String,
    }

    struct Collected<T>(Vec<Record<T>>);
    impl<'buf, T> EmitRecord<'buf, T> for Collected<T> {
        fn emit(&mut self, rec: Record<T>) -> Flow {
            self.0.push(rec);
            Flow::Continue
        }
    }

    fn raw(bytes: &[u8]) -> RawPayload<'_> {
        RawPayload {
            offset: 42,
            ..spate_test::raw_payload(bytes)
        }
    }

    use spate_test::test_ack;

    fn builder(framing: JsonFraming, on_error: OnError, dup: bool) -> JsonDeserializerBuilder {
        JsonDeserializerBuilder::from_settings(JsonSettings {
            framing,
            on_error,
            reject_duplicate_keys: dup,
        })
    }

    #[test]
    fn single_round_trip_and_meta() {
        let mut d = builder(JsonFraming::Single, OnError::Skip, false).build_serde::<Ev>();
        let mut out = Collected(Vec::new());
        d.deserialize(&raw(br#"{"id":7,"name":"orders"}"#), &test_ack(), &mut out)
            .unwrap();
        assert_eq!(out.0.len(), 1);
        assert_eq!(out.0[0].meta.offset, 42);
        assert_eq!(
            out.0[0].payload,
            Ev {
                id: 7,
                name: "orders".into()
            }
        );
    }

    #[test]
    fn ndjson_emits_a_record_per_line() {
        let mut d = builder(JsonFraming::Ndjson, OnError::Skip, false).build_serde::<Ev>();
        let mut out = Collected(Vec::new());
        let payload =
            b"{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}\n{\"id\":3,\"name\":\"c\"}";
        d.deserialize(&raw(payload), &test_ack(), &mut out).unwrap();
        assert_eq!(out.0.len(), 3);
        assert_eq!(out.0[2].payload.id, 3);
        // every derived record carries the payload's offset (one Kafka message)
        assert!(out.0.iter().all(|r| r.meta.offset == 42));
    }

    #[test]
    fn array_explodes_elements() {
        let mut d = builder(JsonFraming::Array, OnError::Skip, false).build_serde::<Ev>();
        let mut out = Collected(Vec::new());
        d.deserialize(
            &raw(br#"[{"id":1,"name":"a"},{"id":2,"name":"b"}]"#),
            &test_ack(),
            &mut out,
        )
        .unwrap();
        assert_eq!(out.0.len(), 2);
    }

    #[test]
    fn empty_and_blank_payloads_are_tombstones() {
        let mut d = builder(JsonFraming::Single, OnError::Skip, false).build_value();
        let mut out = Collected(Vec::new());
        d.deserialize(&raw(b""), &test_ack(), &mut out).unwrap();
        d.deserialize(&raw(b"  \n  "), &test_ack(), &mut out)
            .unwrap();
        assert!(out.0.is_empty());
    }

    #[test]
    fn every_record_carries_the_payload_meta() {
        let key: &[u8] = b"user-7";
        let cases: [(JsonFraming, OnError, &[u8], usize); 4] = [
            (
                JsonFraming::Single,
                OnError::Skip,
                br#"{"id":1,"name":"a"}"#,
                1,
            ),
            (
                JsonFraming::Ndjson,
                OnError::Skip,
                b"{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}",
                2,
            ),
            (
                JsonFraming::Ndjson,
                OnError::Fail,
                b"{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}",
                2,
            ),
            (
                JsonFraming::Array,
                OnError::Skip,
                br#"[{"id":1,"name":"a"},{"id":2,"name":"b"}]"#,
                2,
            ),
        ];
        for (framing, on_error, bytes, records) in cases {
            let payload = RawPayload {
                bytes,
                key: Some(key),
                partition: PartitionId(3),
                offset: 42,
                timestamp_ms: 1_700_000_000_000,
            };
            let mut d = builder(framing, on_error, false).build_serde::<Ev>();
            let mut out = Collected(Vec::new());
            d.deserialize(&payload, &test_ack(), &mut out).unwrap();
            assert_eq!(out.0.len(), records, "{framing:?} {on_error:?}");
            for rec in &out.0 {
                assert_eq!(rec.meta.partition, PartitionId(3), "{framing:?}");
                assert_eq!(rec.meta.offset, 42, "{framing:?}");
                assert_eq!(rec.meta.event_time_ms, 1_700_000_000_000, "{framing:?}");
                assert_eq!(
                    rec.meta.key_hash,
                    Some(stable_key_hash(key)),
                    "{framing:?} {on_error:?}"
                );
            }
        }
    }

    #[test]
    fn a_blank_payload_emits_and_counts_nothing_under_any_framing() {
        const PIPELINE: &str = "blank-payloads";
        let rendered = render_metrics(|| {
            for framing in [JsonFraming::Single, JsonFraming::Ndjson, JsonFraming::Array] {
                for on_error in [OnError::Skip, OnError::Fail] {
                    let mut d = builder(framing, on_error, false)
                        .with_metrics(PIPELINE, "main")
                        .build_value();
                    for bytes in [&b""[..], b"  \n  "] {
                        let payload = RawPayload {
                            key: Some(b"user-7"),
                            ..raw(bytes)
                        };
                        let mut out = Collected(Vec::new());
                        d.deserialize(&payload, &test_ack(), &mut out)
                            .unwrap_or_else(|e| panic!("{framing:?} {on_error:?}: {e}"));
                        assert!(out.0.is_empty(), "{framing:?} {on_error:?}");
                    }
                }
            }
        });
        let dropped: Vec<&str> = rendered
            .lines()
            .filter(|l| l.contains("spate_json_deser_records_dropped_total"))
            .filter(|l| l.contains(PIPELINE))
            .filter(|l| !l.ends_with(" 0"))
            .collect();
        assert!(
            dropped.is_empty(),
            "a blank payload was counted: {dropped:?}"
        );
    }

    #[test]
    fn malformed_single_skips_or_fails() {
        let mut skip = builder(JsonFraming::Single, OnError::Skip, false).build_serde::<Ev>();
        let mut out = Collected(Vec::new());
        skip.deserialize(&raw(b"{not json"), &test_ack(), &mut out)
            .unwrap();
        assert!(out.0.is_empty(), "skip drops the bad payload");

        let mut fail = builder(JsonFraming::Single, OnError::Fail, false).build_serde::<Ev>();
        let err = fail
            .deserialize(&raw(b"{not json"), &test_ack(), &mut out)
            .unwrap_err();
        assert!(matches!(err, DeserError::Malformed { .. }), "{err}");
    }

    #[test]
    fn ndjson_skip_isolates_the_bad_line() {
        let mut d = builder(JsonFraming::Ndjson, OnError::Skip, false).build_serde::<Ev>();
        let mut out = Collected(Vec::new());
        let payload = b"{\"id\":1,\"name\":\"a\"}\nGARBAGE\n{\"id\":3,\"name\":\"c\"}";
        d.deserialize(&raw(payload), &test_ack(), &mut out).unwrap();
        assert_eq!(out.0.len(), 2, "good lines flow, bad line dropped");
        assert_eq!(out.0[0].payload.id, 1);
        assert_eq!(out.0[1].payload.id, 3);
    }

    #[test]
    fn ndjson_fail_stops_on_the_bad_line() {
        let mut d = builder(JsonFraming::Ndjson, OnError::Fail, false).build_serde::<Ev>();
        let mut out = Collected(Vec::new());
        let payload = b"{\"id\":1,\"name\":\"a\"}\nGARBAGE\n{\"id\":3,\"name\":\"c\"}";
        let err = d
            .deserialize(&raw(payload), &test_ack(), &mut out)
            .unwrap_err();
        assert!(matches!(err, DeserError::Malformed { .. }), "{err}");
        // Fail is atomic: the good line before GARBAGE must NOT be emitted, or
        // the chain's Skip policy would commit the offset past the lost tail.
        assert!(
            out.0.is_empty(),
            "no record may be emitted when the payload fails"
        );
    }

    #[test]
    fn array_error_is_atomic() {
        let mut d = builder(JsonFraming::Array, OnError::Skip, false).build_serde::<Ev>();
        let mut out = Collected(Vec::new());
        // The second element does not match `Ev`; the whole array is dropped.
        d.deserialize(
            &raw(br#"[{"id":1,"name":"a"},{"oops":true}]"#),
            &test_ack(),
            &mut out,
        )
        .unwrap();
        assert!(out.0.is_empty(), "array error handling is atomic");
    }

    #[test]
    fn duplicate_keys_last_wins_by_default_reject_when_configured() {
        // Default: serde_json is last-value-wins, one record emitted.
        let mut allow = builder(JsonFraming::Single, OnError::Skip, false).build_value();
        let mut out = Collected(Vec::new());
        allow
            .deserialize(&raw(br#"{"id":1,"id":2}"#), &test_ack(), &mut out)
            .unwrap();
        assert_eq!(out.0.len(), 1);
        assert_eq!(out.0[0].payload["id"], serde_json::json!(2));

        // reject_duplicate_keys + fail: hard error, labeled duplicate_key.
        let mut reject = builder(JsonFraming::Single, OnError::Fail, true).build_value();
        let err = reject
            .deserialize(&raw(br#"{"id":1,"id":2}"#), &test_ack(), &mut out)
            .unwrap_err();
        match err {
            DeserError::Malformed { reason } => assert!(
                reason.starts_with("duplicate_key:"),
                "a real duplicate key must be labeled duplicate_key, got `{reason}`"
            ),
            other => panic!("expected Malformed, got {other}"),
        }
    }

    #[test]
    fn nested_duplicate_keys_are_rejected() {
        let mut d = builder(JsonFraming::Single, OnError::Fail, true).build_value();
        let mut out = Collected(Vec::new());
        let err = d
            .deserialize(&raw(br#"{"outer":{"a":1,"a":2}}"#), &test_ack(), &mut out)
            .unwrap_err();
        assert!(matches!(err, DeserError::Malformed { .. }), "{err}");
    }

    #[test]
    fn malformed_with_dedup_is_labeled_malformed_not_duplicate_key() {
        // With reject_duplicate_keys on, the structural pass runs first; a plain
        // syntax error (no duplicate key) must still be reported as `malformed`.
        let mut d = builder(JsonFraming::Single, OnError::Fail, true).build_serde::<Ev>();
        let mut out = Collected(Vec::new());
        let err = d
            .deserialize(&raw(b"{not valid json"), &test_ack(), &mut out)
            .unwrap_err();
        match err {
            DeserError::Malformed { reason } => assert!(
                reason.starts_with("malformed:"),
                "a syntax error must be labeled malformed, got `{reason}`"
            ),
            other => panic!("expected Malformed, got {other}"),
        }
    }

    #[test]
    fn skipped_records_are_counted_in_metrics() {
        const STD: &str = r#"pipeline="orders",component="main",component_type="deserializer""#;
        let rendered = render_metrics(|| {
            let mut d = builder(JsonFraming::Ndjson, OnError::Skip, false)
                .with_metrics("orders", "main")
                .build_serde::<Ev>();
            let mut out = Collected(Vec::new());
            let payload =
                b"{\"id\":1,\"name\":\"a\"}\nGARBAGE\nALSO BAD\n{\"id\":4,\"name\":\"d\"}";
            d.deserialize(&raw(payload), &test_ack(), &mut out).unwrap();
            assert_eq!(out.0.len(), 2);
        });
        let needle =
            format!(r#"spate_json_deser_records_dropped_total{{{STD},reason="malformed"}} 2"#);
        assert!(
            rendered.contains(&needle),
            "missing `{needle}`:\n{rendered}"
        );
    }

    proptest! {
        /// `ndjson_lines` yields what `split` on `\n` yields once blank lines
        /// are dropped, including the segment after the last `\n`.
        #[test]
        fn ndjson_lines_match_a_split_on_newline(
            stream in arb_stream(),
        ) {
            let expected: Vec<&[u8]> = stream
                .split(|&b| b == b'\n')
                .filter(|line| !is_blank(line))
                .collect();
            let lines: Vec<&[u8]> = ndjson_lines(&stream).collect();
            prop_assert_eq!(lines, expected);
        }
    }
}
