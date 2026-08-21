//! Avro deserialization for the Spate framework.
//!
//! Decodes bare Avro datums as carried by Kafka messages (Confluent wire
//! format of magic byte + schema id + datum, Avro single-object encoding,
//! or raw datums with a fixed schema) three ways: single-pass into your
//! own serde types ([`AvroDatumDeserializer`], the throughput path, with
//! optional zero-copy borrowed records), two-pass into serde types with
//! Avro schema resolution ([`AvroSerdeDeserializer`]), or into
//! dynamically-typed [`AvroValue`] records ([`AvroValueDeserializer`]).
//!
//! # Never block the pipeline thread
//!
//! Registry schemas are fetched by an asynchronous task on the I/O
//! runtime and cached per process. A payload whose schema is not cached
//! yet reports "not ready": the operator chain holds the batch (the
//! source is paused by backpressure if needed) and replays the payload
//! once the fetch lands. Records are never dropped or duplicated, and
//! the CPU-pinned pipeline threads never perform I/O. Ids the registry
//! cannot serve are negatively cached (with a TTL) and handled by the
//! deserializer's `ErrorPolicy` like any other poison payload.
//!
//! # Schema evolution
//!
//! Writer schemas come from the payload (registry id or fingerprint) or
//! configuration; an optional `reader_schema` pins the shape records are
//! resolved into, using Avro's schema-resolution rules (field reordering,
//! defaults, promotions). Registry schemas using references are not
//! supported yet and are surfaced as unavailable.
//!
//! A reader **field** alias is not applied. A reader field that renames a
//! writer's field and lists the old name in `aliases` fails every payload;
//! rename with `#[serde(alias)]` on the record type instead. Record names
//! are not compared during resolution, so a record-level alias has no
//! effect either way.

mod cache;
mod config;
mod datum;
mod de;
mod deser;
mod registry;
mod wire;

pub use config::{
    AvroConfigError, AvroDeserializerBuilder, AvroMode, AvroSettings, RegistrySection, SchemaSource,
};
pub use datum::AvroDatumDeserializer;
pub use deser::{AvroSerdeDeserializer, AvroValue, AvroValueDeserializer};
pub use wire::{parse_confluent, parse_single_object};
