//! JSON deserialization for the `etl-rs` framework.
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
//! # Errors and metrics
//!
//! A document that does not parse (or does not match the target type) is a
//! record-level error. Under `on_error: skip` (the default) it is dropped and
//! counted in `etl_json_deser_records_dropped_total{reason="malformed"}` and
//! the decode continues; under `on_error: fail` it surfaces a decode error on
//! the first bad record, which the chain's deserializer error policy then
//! either replays (stop the pipeline, at-least-once) or drops as a whole
//! payload. The connector-owned
//! `etl_json_deser_*` families are minted from a [`Meter`](etl_core::metrics::Meter)
//! when the builder is given a metrics scope (see
//! [`JsonDeserializerBuilder::with_metrics`]); they sit alongside the
//! framework's generic `etl_deser_*` stage metrics, which wrap every decoder.
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
//! Decoding uses `serde_json` (stable 1.x). The byte-slice → value step sits
//! behind an internal seam so a SIMD backend can be added later behind a Cargo
//! feature without changing this API. `from_reader` is never used on the hot
//! path — decoding always operates on the in-memory payload slice.

mod backend;
mod config;
mod deser;
mod metrics;

pub use config::{JsonConfigError, JsonDeserializerBuilder, JsonFraming, JsonSettings, OnError};
pub use deser::{JsonSerdeDeserializer, JsonValueDeserializer};

/// Dynamically-typed JSON record, re-exported for the [`JsonValueDeserializer`]
/// path. Unlike Avro's value type this is a stable 1.x dependency, so it does
/// not widen the framework's semver surface.
pub type JsonValue = serde_json::Value;
