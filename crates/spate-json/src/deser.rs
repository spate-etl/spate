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
use crate::config::{JsonFraming, JsonOnError, TAG};
use crate::metrics::JsonDeserMetrics;
use serde::de::DeserializeOwned;
use serde_json::Value;
use spate_core::checkpoint::AckRef;
use spate_core::config::{ConfigError, SectionPath};
use spate_core::deser::{DeserMetricsCtx, Deserializer, EmitRecord, Owned};
use spate_core::error::DeserError;
use spate_core::framing::FramingContract;
use spate_core::record::{RawPayload, Record, RecordMeta};
use spate_core::telemetry::RateLimit;
use std::marker::PhantomData;

/// Rate-limits the per-record skip warning so a poison storm can't flood the
/// logs. A chain built with `with_metrics` counts every drop in
/// `spate_json_deser_records_dropped_total`.
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
    pub(crate) on_error: JsonOnError,
    pub(crate) reject_duplicate_keys: bool,
    pub(crate) metrics: Option<JsonDeserMetrics>,
}

impl DecoderCore {
    /// Resolve the drop counters from the chain's `Meter`, or count nothing
    /// when it hands none.
    fn attach_metrics(&mut self, cx: DeserMetricsCtx) {
        self.metrics = cx.meter.as_ref().map(JsonDeserMetrics::new);
    }

    /// Refuse a source that frames one record per payload unless each
    /// payload decodes as one document.
    fn check_source_framing(&self, source: FramingContract) -> Result<(), ConfigError> {
        match (source, self.framing) {
            (FramingContract::PerRecord, JsonFraming::Lines | JsonFraming::Array) => {
                Err(SectionPath::deserializer(TAG).error(format!(
                    "splits each payload as `JsonFraming::{:?}`, but the source already frames \
                     one record per payload; build the deserializer with \
                     `JsonDeserializerBuilder::framing(JsonFraming::Single)`, or use a source \
                     that leaves framing to the deserializer",
                    self.framing
                )))
            }
            _ => Ok(()),
        }
    }

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
            JsonFraming::Lines => self.run_ndjson(raw.bytes, meta, ack, out),
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
        if let Some(payload) = self.decode_item::<P>(bytes, &meta)? {
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
            JsonOnError::Skip => {
                for line in ndjson_lines(bytes) {
                    if let Some(payload) = self.decode_item::<P>(line, &meta)? {
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
            JsonOnError::Fail => {
                let mut decoded = Vec::new();
                for line in ndjson_lines(bytes) {
                    if let Some(payload) = self.decode_item::<P>(line, &meta)? {
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
        // `Lines` with `on_error: skip` when you need per-record isolation.
        if self.reject_duplicate_keys
            && let Err(e) = backend::check_no_duplicate_keys(bytes)
        {
            return self.on_item_error(&e, dup_check_reason(&e), &meta);
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
            Err(e) => self.on_item_error(&e, Reason::Malformed, &meta),
        }
    }

    /// Decode one JSON document into `P`. `Ok(Some)` = decoded, `Ok(None)` =
    /// dropped under `on_error: skip`, `Err` = `on_error: fail`. `meta` is
    /// the payload's, for the skip warning.
    fn decode_item<P: DeserializeOwned>(
        &self,
        bytes: &[u8],
        meta: &RecordMeta,
    ) -> Result<Option<P>, DeserError> {
        if self.reject_duplicate_keys
            && let Err(e) = backend::check_no_duplicate_keys(bytes)
        {
            self.on_item_error(&e, dup_check_reason(&e), meta)?;
            return Ok(None);
        }
        match backend::decode_one::<P>(bytes) {
            Ok(payload) => Ok(Some(payload)),
            Err(e) => {
                self.on_item_error(&e, Reason::Malformed, meta)?;
                Ok(None)
            }
        }
    }

    /// Apply the error policy to a decode failure: `Skip` counts it and returns
    /// `Ok`, `Fail` returns
    /// [`DeserErrorKind::Malformed`](spate_core::error::DeserErrorKind::Malformed).
    fn on_item_error(
        &self,
        e: &backend::DecodeError,
        reason: Reason,
        meta: &RecordMeta,
    ) -> Result<(), DeserError> {
        match self.on_error {
            JsonOnError::Skip => {
                if let Some(m) = &self.metrics {
                    match reason {
                        Reason::Malformed => m.dropped_malformed(),
                        Reason::DuplicateKey => m.dropped_duplicate_key(),
                    }
                }
                spate_core::rate_limited_warn!(
                    SKIP_WARN,
                    reason = reason.label(),
                    partition = meta.partition.0,
                    offset = meta.offset,
                    error = %e,
                    "json record skipped by on_error policy"
                );
                Ok(())
            }
            JsonOnError::Fail => Err(DeserError::malformed(format!("{}: {e}", reason.label()))),
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

    fn component_type(&self) -> &str {
        TAG
    }

    fn attach_metrics(&mut self, cx: DeserMetricsCtx) {
        self.core.attach_metrics(cx);
    }

    fn check_source_framing(&self, source: FramingContract) -> Result<(), ConfigError> {
        self.core.check_source_framing(source)
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

    fn component_type(&self) -> &str {
        TAG
    }

    fn attach_metrics(&mut self, cx: DeserMetricsCtx) {
        self.core.attach_metrics(cx);
    }

    fn check_source_framing(&self, source: FramingContract) -> Result<(), ConfigError> {
        self.core.check_source_framing(source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{JsonDeserializerBuilder, JsonDeserializerConfig};
    use crate::framing::tests::arb_stream;
    use proptest::prelude::*;
    use serde::Deserialize;
    use spate_core::error::DeserErrorKind;
    use spate_core::metrics::Meter;
    use spate_core::record::{PartitionId, stable_key_hash};
    use spate_test::{EmitCollector, render_metrics};

    /// The namespace of the `Meter` these tests hand a deserializer in place of
    /// a chain's.
    const TEST_NS: &str = "json_probe";

    /// A `Meter` like the one a chain hands its deserializer, under [`TEST_NS`].
    fn test_metrics(pipeline: &str) -> DeserMetricsCtx {
        let meter = Meter::with_namespace(TEST_NS, pipeline.to_owned(), "main.deserializer", TAG);
        DeserMetricsCtx::new(Some(meter))
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct Ev {
        id: i64,
        name: String,
    }

    fn raw(bytes: &[u8]) -> RawPayload<'_> {
        RawPayload {
            offset: 42,
            ..spate_test::raw_payload(bytes)
        }
    }

    use spate_test::test_ack;

    fn builder(framing: JsonFraming, on_error: JsonOnError, dup: bool) -> JsonDeserializerBuilder {
        JsonDeserializerBuilder::new(JsonDeserializerConfig {
            on_error,
            reject_duplicate_keys: dup,
        })
        .expect("the config validates")
        .framing(framing)
    }

    /// The refusal for a framing that splits payloads under a source that
    /// frames one record per payload.
    fn refusal(variant: &str) -> String {
        format!(
            "deserializer.json: splits each payload as `JsonFraming::{variant}`, but the source \
             already frames one record per payload; build the deserializer with \
             `JsonDeserializerBuilder::framing(JsonFraming::Single)`, or use a source that \
             leaves framing to the deserializer"
        )
    }

    /// Each framing against each source contract, through the typed and the
    /// dynamically-typed deserializer, so neither may skip the check.
    #[test]
    fn a_per_record_source_is_refused_unless_framing_is_single() {
        for (framing, variant) in [
            (JsonFraming::Single, None),
            (JsonFraming::Lines, Some("Lines")),
            (JsonFraming::Array, Some("Array")),
        ] {
            let b = builder(framing, JsonOnError::Skip, false);
            let serde = b.build_serde::<Ev>();
            let value = b.build_value();
            for got in [
                serde.check_source_framing(FramingContract::PerRecord),
                value.check_source_framing(FramingContract::PerRecord),
            ] {
                match variant {
                    None => got.unwrap(),
                    Some(v) => assert_eq!(got.unwrap_err().to_string(), refusal(v)),
                }
            }
            serde
                .check_source_framing(FramingContract::WholePayload)
                .unwrap();
            value
                .check_source_framing(FramingContract::WholePayload)
                .unwrap();
        }
    }

    #[test]
    fn single_round_trip_and_meta() {
        let mut d = builder(JsonFraming::Single, JsonOnError::Skip, false).build_serde::<Ev>();
        let mut out = EmitCollector::new();
        d.deserialize(&raw(br#"{"id":7,"name":"orders"}"#), &test_ack(), &mut out)
            .unwrap();
        assert_eq!(out.records.len(), 1);
        assert_eq!(out.records[0].meta.offset, 42);
        assert_eq!(
            out.records[0].payload,
            Ev {
                id: 7,
                name: "orders".into()
            }
        );
    }

    #[test]
    fn ndjson_emits_a_record_per_line() {
        let mut d = builder(JsonFraming::Lines, JsonOnError::Skip, false).build_serde::<Ev>();
        let mut out = EmitCollector::new();
        let payload =
            b"{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}\n{\"id\":3,\"name\":\"c\"}";
        d.deserialize(&raw(payload), &test_ack(), &mut out).unwrap();
        assert_eq!(out.records.len(), 3);
        assert_eq!(out.records[2].payload.id, 3);
        // every derived record carries the payload's offset (one Kafka message)
        assert!(out.records.iter().all(|r| r.meta.offset == 42));
    }

    #[test]
    fn array_explodes_elements() {
        let mut d = builder(JsonFraming::Array, JsonOnError::Skip, false).build_serde::<Ev>();
        let mut out = EmitCollector::new();
        d.deserialize(
            &raw(br#"[{"id":1,"name":"a"},{"id":2,"name":"b"}]"#),
            &test_ack(),
            &mut out,
        )
        .unwrap();
        assert_eq!(out.records.len(), 2);
    }

    #[test]
    fn empty_and_blank_payloads_are_tombstones() {
        let mut d = builder(JsonFraming::Single, JsonOnError::Skip, false).build_value();
        let mut out = EmitCollector::new();
        d.deserialize(&raw(b""), &test_ack(), &mut out).unwrap();
        d.deserialize(&raw(b"  \n  "), &test_ack(), &mut out)
            .unwrap();
        assert!(out.records.is_empty());
    }

    #[test]
    fn every_record_carries_the_payload_meta() {
        let key: &[u8] = b"user-7";
        let cases: [(JsonFraming, JsonOnError, &[u8], usize); 4] = [
            (
                JsonFraming::Single,
                JsonOnError::Skip,
                br#"{"id":1,"name":"a"}"#,
                1,
            ),
            (
                JsonFraming::Lines,
                JsonOnError::Skip,
                b"{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}",
                2,
            ),
            (
                JsonFraming::Lines,
                JsonOnError::Fail,
                b"{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}",
                2,
            ),
            (
                JsonFraming::Array,
                JsonOnError::Skip,
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
            let mut out = EmitCollector::new();
            d.deserialize(&payload, &test_ack(), &mut out).unwrap();
            assert_eq!(out.records.len(), records, "{framing:?} {on_error:?}");
            for rec in &out.records {
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
            for framing in [JsonFraming::Single, JsonFraming::Lines, JsonFraming::Array] {
                for on_error in [JsonOnError::Skip, JsonOnError::Fail] {
                    let mut d = builder(framing, on_error, false).build_value();
                    d.attach_metrics(test_metrics(PIPELINE));
                    for bytes in [&b""[..], b"  \n  "] {
                        let payload = RawPayload {
                            key: Some(b"user-7"),
                            ..raw(bytes)
                        };
                        let mut out = EmitCollector::new();
                        d.deserialize(&payload, &test_ack(), &mut out)
                            .unwrap_or_else(|e| panic!("{framing:?} {on_error:?}: {e}"));
                        assert!(out.records.is_empty(), "{framing:?} {on_error:?}");
                    }
                }
            }
        });
        let family = format!("spate_{TEST_NS}_records_dropped_total");
        let dropped: Vec<&str> = rendered
            .lines()
            .filter(|l| l.contains(&family))
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
        let mut skip = builder(JsonFraming::Single, JsonOnError::Skip, false).build_serde::<Ev>();
        let mut out = EmitCollector::new();
        skip.deserialize(&raw(b"{not json"), &test_ack(), &mut out)
            .unwrap();
        assert!(out.records.is_empty(), "skip drops the bad payload");

        let mut fail = builder(JsonFraming::Single, JsonOnError::Fail, false).build_serde::<Ev>();
        let err = fail
            .deserialize(&raw(b"{not json"), &test_ack(), &mut out)
            .unwrap_err();
        assert_eq!(err.kind(), DeserErrorKind::Malformed, "{err}");
    }

    #[test]
    fn ndjson_skip_isolates_the_bad_line() {
        let mut d = builder(JsonFraming::Lines, JsonOnError::Skip, false).build_serde::<Ev>();
        let mut out = EmitCollector::new();
        let payload = b"{\"id\":1,\"name\":\"a\"}\nGARBAGE\n{\"id\":3,\"name\":\"c\"}";
        d.deserialize(&raw(payload), &test_ack(), &mut out).unwrap();
        assert_eq!(out.records.len(), 2, "good lines flow, bad line dropped");
        assert_eq!(out.records[0].payload.id, 1);
        assert_eq!(out.records[1].payload.id, 3);
    }

    #[test]
    fn ndjson_fail_stops_on_the_bad_line() {
        let mut d = builder(JsonFraming::Lines, JsonOnError::Fail, false).build_serde::<Ev>();
        let mut out = EmitCollector::new();
        let payload = b"{\"id\":1,\"name\":\"a\"}\nGARBAGE\n{\"id\":3,\"name\":\"c\"}";
        let err = d
            .deserialize(&raw(payload), &test_ack(), &mut out)
            .unwrap_err();
        assert_eq!(err.kind(), DeserErrorKind::Malformed, "{err}");
        // Fail is atomic: the good line before GARBAGE must NOT be emitted, or
        // the chain's Skip policy would commit the offset past the lost tail.
        assert!(
            out.records.is_empty(),
            "no record may be emitted when the payload fails"
        );
    }

    #[test]
    fn array_error_is_atomic() {
        let mut d = builder(JsonFraming::Array, JsonOnError::Skip, false).build_serde::<Ev>();
        let mut out = EmitCollector::new();
        // The second element does not match `Ev`; the whole array is dropped.
        d.deserialize(
            &raw(br#"[{"id":1,"name":"a"},{"oops":true}]"#),
            &test_ack(),
            &mut out,
        )
        .unwrap();
        assert!(out.records.is_empty(), "array error handling is atomic");
    }

    #[test]
    fn array_fail_emits_nothing() {
        let mut d = builder(JsonFraming::Array, JsonOnError::Fail, false).build_serde::<Ev>();
        let mut out = EmitCollector::new();
        let err = d
            .deserialize(
                &raw(br#"[{"id":1,"name":"a"},{"oops":true}]"#),
                &test_ack(),
                &mut out,
            )
            .unwrap_err();
        assert_eq!(err.kind(), DeserErrorKind::Malformed, "{err}");
        // The good first element must not be emitted ahead of the error.
        assert!(
            out.records.is_empty(),
            "no record may be emitted when the payload fails"
        );
    }

    #[test]
    fn duplicate_keys_last_wins_by_default_reject_when_configured() {
        // Default: serde_json is last-value-wins, one record emitted.
        let mut allow = builder(JsonFraming::Single, JsonOnError::Skip, false).build_value();
        let mut out = EmitCollector::new();
        allow
            .deserialize(&raw(br#"{"id":1,"id":2}"#), &test_ack(), &mut out)
            .unwrap();
        assert_eq!(out.records.len(), 1);
        assert_eq!(out.records[0].payload["id"], serde_json::json!(2));

        // reject_duplicate_keys + fail: hard error, labeled duplicate_key.
        let mut reject = builder(JsonFraming::Single, JsonOnError::Fail, true).build_value();
        let err = reject
            .deserialize(&raw(br#"{"id":1,"id":2}"#), &test_ack(), &mut out)
            .unwrap_err();
        match err {
            e if e.kind() == DeserErrorKind::Malformed => assert!(
                e.reason().starts_with("duplicate_key:"),
                "a real duplicate key must be labeled duplicate_key, got `{}`",
                e.reason()
            ),
            other => panic!("expected Malformed, got {other}"),
        }
    }

    #[test]
    fn nested_duplicate_keys_are_rejected() {
        let mut d = builder(JsonFraming::Single, JsonOnError::Fail, true).build_value();
        let mut out = EmitCollector::new();
        let err = d
            .deserialize(&raw(br#"{"outer":{"a":1,"a":2}}"#), &test_ack(), &mut out)
            .unwrap_err();
        assert_eq!(err.kind(), DeserErrorKind::Malformed, "{err}");
    }

    #[test]
    fn malformed_with_dedup_is_labeled_malformed_not_duplicate_key() {
        // With reject_duplicate_keys on, the structural pass runs first; a plain
        // syntax error (no duplicate key) must still be reported as `malformed`.
        let mut d = builder(JsonFraming::Single, JsonOnError::Fail, true).build_serde::<Ev>();
        let mut out = EmitCollector::new();
        let err = d
            .deserialize(&raw(b"{not valid json"), &test_ack(), &mut out)
            .unwrap_err();
        match err {
            e if e.kind() == DeserErrorKind::Malformed => assert!(
                e.reason().starts_with("malformed:"),
                "a syntax error must be labeled malformed, got `{}`",
                e.reason()
            ),
            other => panic!("expected Malformed, got {other}"),
        }
    }

    #[test]
    fn skipped_records_are_counted_in_metrics() {
        const STD: &str =
            r#"pipeline="orders",component="main.deserializer",component_type="json""#;
        let rendered = render_metrics(|| {
            let mut d = builder(JsonFraming::Lines, JsonOnError::Skip, false).build_serde::<Ev>();
            d.attach_metrics(test_metrics("orders"));
            let mut out = EmitCollector::new();
            let payload =
                b"{\"id\":1,\"name\":\"a\"}\nGARBAGE\nALSO BAD\n{\"id\":4,\"name\":\"d\"}";
            d.deserialize(&raw(payload), &test_ack(), &mut out).unwrap();
            assert_eq!(out.records.len(), 2);
        });
        let needle =
            format!(r#"spate_{TEST_NS}_records_dropped_total{{{STD},reason="malformed"}} 2"#);
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
