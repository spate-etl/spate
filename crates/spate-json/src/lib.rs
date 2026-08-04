//! JSON deserialization for the Spate framework.
//!
//! Decodes JSON payloads carried by a source (a Kafka message, a file line,
//! an HTTP body) into either your own serde types ([`JsonSerdeDeserializer`])
//! or dynamically-typed [`JsonValue`] records ([`JsonValueDeserializer`]).
//! Three framings map onto the framework's "one payload → 0..N records"
//! contract:
//!
//! - **`single`** — the whole payload is one JSON document → one record (the
//!   Kafka-message default). An empty or whitespace-only payload is a
//!   tombstone: zero records, no error.
//! - **`ndjson`** — newline-delimited JSON (JSON Lines): one JSON value per
//!   `\n`-separated line → one record per line. Blank lines are skipped.
//!   This is the framing with **per-line** error isolation.
//! - **`array`** — a top-level JSON array → one record per element, decoded in
//!   a single pass. A malformed array is handled atomically by the configured
//!   [error policy](JsonSettings); use `ndjson` when you need per-record
//!   isolation.
//!
//! Those three frame a payload already resident in memory (a Kafka message).
//! For a **streaming** source that never holds a whole object in RAM — an
//! `spate-s3` object, an HTTP body — the connector also owns the byte-stream
//! framer: [`NdjsonFramer`] is a chunk-fed, bounded
//! [`RecordFramer`](spate_core::framing::RecordFramer) the source runs to cut the
//! stream into records, then hands each to the deserializer in `single` mode.
//! Framing this way is a format concern, so it lives here rather than in the
//! source.
//!
//! # Errors and metrics
//!
//! A document that does not parse (or does not match the target type) is a
//! record-level error. Under `on_error: skip` (the default) it is dropped and
//! counted in `spate_json_deser_records_dropped_total{reason="malformed"}` and
//! the decode continues; under `on_error: fail` it surfaces a decode error on
//! the first bad record, which the chain's deserializer error policy then
//! either replays (stop the pipeline, at-least-once) or drops as a whole
//! payload. The connector-owned
//! `spate_json_deser_*` families are minted from a [`Meter`](spate_core::metrics::Meter)
//! when the builder is given a metrics scope (see
//! [`JsonDeserializerBuilder::with_metrics`]); they sit alongside the
//! framework's generic `spate_deser_*` stage metrics, which wrap every decoder.
//!
//! # Fidelity knobs
//!
//! `reject_duplicate_keys` (config) turns serde_json's silent last-value-wins
//! on duplicate object keys into a hard error — a guard against upstream
//! corruption. The optional Cargo features `float-roundtrip`,
//! `arbitrary-precision`, and `raw-value` pass straight through to
//! `serde_json`; `arbitrary-precision` in particular is crate-wide and
//! interacts with `flatten`/`untagged`/`RawValue`, so enable it deliberately.
//!
//! # Backends
//!
//! Decoding uses `serde_json` (stable 1.x) by default. The byte-slice → value
//! step sits behind an internal seam, so the opt-in **`simd`** Cargo feature
//! swaps in `simd-json` (SIMD-accelerated parsing) with no change to this API —
//! [`BACKEND_ID`] reports which is compiled. `simd` is a decode speedup over
//! serde_json on the single-document and array paths (the Kafka-message
//! default), by a margin that depends on the payload and the host
//! architecture. It is off by default and excluded from the facade's `full`
//! feature. `simd` is *not* byte-for-byte identical to serde_json on every
//! input — it rejects integer literals outside the `i64`/`u64` range that
//! serde_json accepts as `f64`, and does not honor serde_json's
//! `arbitrary_precision` / `raw_value` / `float_roundtrip` features; see the
//! JSON connector guide's Backends section. `from_reader` is never used on the
//! hot path — decoding always operates on the in-memory payload slice.

mod backend;
mod config;
mod deser;
mod framing;
mod metrics;

pub use backend::BACKEND_ID;
pub use config::{JsonConfigError, JsonDeserializerBuilder, JsonFraming, JsonSettings, OnError};
pub use deser::{JsonSerdeDeserializer, JsonValueDeserializer};
pub use framing::NdjsonFramer;

/// Dynamically-typed JSON record, re-exported for the [`JsonValueDeserializer`]
/// path. Unlike Avro's value type this is a stable 1.x dependency, so it does
/// not widen the framework's semver surface.
pub type JsonValue = serde_json::Value;
