//! Avro deserialization for the `etl-rs` framework.
//!
//! Decodes bare Avro datums as carried by Kafka messages — including the
//! Confluent wire format (magic byte + 4-byte schema id + datum) — with a
//! per-thread compiled-schema cache and asynchronous schema-registry
//! fetching off the hot path.
//!
//! Two codec backends are provided: the default backend built on
//! `apache-avro` (spec-complete reader/writer schema resolution) and an
//! opt-in zero-copy backend (`fast` feature) built on `serde_avro_fast`.
