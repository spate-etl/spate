//! JSON deserialization for the Spate framework.
//!
//! Decodes JSON payloads carried by a source (a Kafka message, a file line,
//! an HTTP body) into either your own serde types ([`JsonSerdeDeserializer`])
//! or dynamically-typed [`JsonValue`] records ([`JsonValueDeserializer`]).
//! Three framings, set with
//! [`JsonDeserializerBuilder::framing`], map onto the framework's "one
//! payload → 0..N records" contract:
//!
//! - **`Single`** — the whole payload is one JSON document → one record (the
//!   Kafka-message default). An empty or whitespace-only payload is a
//!   tombstone: zero records, no error.
//! - **`Lines`** — newline-delimited JSON (JSON Lines): one JSON value per
//!   `\n`-separated line → one record per line. Blank lines are skipped.
//!   Under `on_error: skip` this is the framing with **per-line** error
//!   isolation.
//! - **`Array`** — a top-level JSON array → one record per element, decoded in
//!   a single pass. A malformed array is handled atomically by the configured
//!   [error policy](JsonDeserializerConfig); use `Lines` with `on_error: skip` when you
//!   need per-record isolation.
//!
//! Those three frame a payload already resident in memory (a Kafka message).
//! For a **streaming** source that delivers an object in chunks (an
//! `spate-s3` object, an HTTP body) the connector also owns the byte-stream
//! framer: [`JsonLinesFramer`] is a chunk-fed, bounded
//! [`RecordFramer`](spate_core::framing::RecordFramer) the source runs to cut the
//! stream into records, then hands each to the deserializer under `Single`.
//! The deserializer refuses `Lines` and `Array` under such a source when the
//! pipeline starts.
//! Framing this way is a format concern, so it lives here rather than in the
//! source.
//!
//! # Errors and metrics
//!
//! A document that does not parse (or does not match the target type) is a
//! record-level error. Under `on_error: skip` (the default) it is dropped,
//! counted in `spate_json_deser_records_dropped_total{reason="malformed"}` on a
//! chain built with `with_metrics`, and the decode continues. Under `Array` one
//! bad element drops the whole array. Under `on_error: fail` it surfaces a
//! decode error on the first bad record, which the chain's deserializer error
//! policy then
//! either replays (stop the pipeline, at-least-once) or drops as a whole
//! payload. The error text holds the kind of failure, and its position where the
//! parser reports one, with no content from the record, and the `WARN` line for a skipped record carries
//! its partition and offset. The connector-owned
//! `spate_json_deser_*` families are minted from the
//! [`Meter`](spate_core::metrics::Meter) that
//! [`ChainBuilder::with_metrics`](spate_core::ops::ChainBuilder::with_metrics)
//! hands the deserializer; they sit alongside the
//! framework's generic `spate_deser_*` stage metrics, which wrap every decoder.
//!
//! # Fidelity knobs
//!
//! `reject_duplicate_keys` (config) turns serde_json's silent last-value-wins
//! on duplicate object keys into a hard error, guarding against upstream
//! corruption. The optional Cargo features `float-roundtrip`,
//! `arbitrary-precision`, and `raw-value` pass straight through to
//! `serde_json`; `arbitrary-precision` in particular is crate-wide and
//! interacts with `flatten`/`untagged`/`RawValue`, so enable it deliberately.
//!
//! # Backends
//!
//! Decoding uses `serde_json` (stable 1.x) by default. The byte-slice → value
//! step sits behind an internal seam, so the opt-in **`simd`** Cargo feature
//! swaps in `simd-json` (SIMD-accelerated parsing) with no change to this
//! API, and [`BACKEND_ID`] reports which is compiled. `simd` is faster on some
//! payloads and slower on others, depending on the CPU, and the JSON connector
//! guide's Backends section gives the measured figures. It is off by default.
//! `simd` rejects integer literals outside
//! the `i64`/`u64` range that serde_json accepts as `f64` and normalizes `-0`
//! to `0`. Both backends reject NUL bytes after numbers and underscore separators
//! in numbers. `serde_json` has a recursion limit of 128; `simd-json` limits
//! nesting to 1024. With `reject_duplicate_keys`, the `serde_json` guard applies
//! its limit before decoding. `simd` does not honor serde_json's
//! `arbitrary_precision` / `raw_value` / `float_roundtrip` features; see the
//! JSON connector guide's Backends section. `from_reader` is
//! never used on the hot path, and decoding always operates on the in-memory
//! payload slice.

mod backend;
mod config;
mod deser;
mod framing;
mod metrics;

pub use backend::BACKEND_ID;
pub use config::{JsonDeserializerBuilder, JsonDeserializerConfig, JsonFraming, JsonOnError};
pub use deser::{JsonSerdeDeserializer, JsonValueDeserializer};
pub use framing::JsonLinesFramer;

/// Dynamically-typed JSON record, re-exported for the [`JsonValueDeserializer`]
/// path. Unlike Avro's value type this is a stable 1.x dependency, so it does
/// not widen the framework's semver surface.
pub type JsonValue = serde_json::Value;
