//! Avro deserialization for the `etl-rs` framework.
//!
//! Decodes bare Avro datums as carried by Kafka messages — Confluent wire
//! format (magic byte + schema id + datum), Avro single-object encoding,
//! or raw datums with a fixed schema — into either dynamically-typed
//! [`AvroValue`] records or your own serde types
//! ([`AvroSerdeDeserializer`]).
//!
//! # Never block the pipeline thread
//!
//! Registry schemas are fetched by an asynchronous task on the I/O
//! runtime and cached per process. A payload whose schema is not cached
//! yet reports "not ready": the operator chain holds the batch (the
//! source is paused by backpressure if needed) and replays the payload
//! once the fetch lands — records are never dropped or duplicated, and
//! the CPU-pinned pipeline threads never perform I/O. Ids the registry
//! cannot serve are negatively cached (with a TTL) and handled by the
//! deserializer's `ErrorPolicy` like any other poison payload.
//!
//! # Schema evolution
//!
//! Writer schemas come from the payload (registry id or fingerprint) or
//! configuration; an optional `reader_schema` pins the shape records are
//! resolved into, using Avro's schema-resolution rules (field reordering,
//! defaults, promotions, aliases). Registry schemas using references are
//! not supported yet and are surfaced as unavailable.
//!
//! # The `fast` backend (opt-in feature)
//!
//! The `fast` feature adds a `serde_avro_fast`-based backend
//! (`AvroFastDeserializer`, built by
//! [`AvroDeserializerBuilder::build_serde_fast`] /
//! [`AvroDeserializerBuilder::build_fast`]): single-pass datum→`T` decoding
//! several times faster than the apache-avro paths, and the only backend
//! able to emit **borrowed** (zero-copy) records. Its evolution model
//! differs — serde attributes against each writer schema instead of
//! reader-schema resolution — see the type's docs. Backends are chosen per
//! pipeline; both coexist in one build.
//!
//! **License note:** the feature pulls in `serde_avro_fast`, whose crates.io
//! metadata declares `LGPL-3.0-only` even though the upstream repository
//! (`github.com/Ten0/serde_avro_fast`) is MPL-2.0 — the fix is merged
//! upstream but unreleased as of 2026-07 — and one of its dependencies,
//! `serde_serializer_quick_unsupported` (a ~300-line `no_std` macro-only
//! helper by the same author), which **is** genuinely LGPL-3.0-only. The
//! feature is off by default and the default build contains no trace of
//! either crate; enabling it is your project's own compliance call.

mod cache;
mod config;
mod deser;
mod registry;
mod wire;

pub use config::{
    AvroConfigError, AvroDeserializerBuilder, AvroMode, AvroSettings, RegistrySection, SchemaSource,
};
#[cfg(feature = "fast")]
pub use deser::AvroFastDeserializer;
pub use deser::{AvroSerdeDeserializer, AvroValue, AvroValueDeserializer};
pub use wire::{parse_confluent, parse_single_object};
