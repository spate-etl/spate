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
//! deserializer's `ErrorPolicy` like any other poison payload. The cache
//! holds at most 1,024 compiled schemas and an estimated 64 MiB of them, and
//! an evicted id is fetched again on its next payload. A schema estimated
//! to hold more than that once compiled is refused and negatively cached. A
//! registry that rejects the credentials or the TLS handshake, or whose
//! certificate fails verification, makes the next cache miss stop the
//! pipeline, whatever the `ErrorPolicy`.
//!
//! # Schema evolution
//!
//! Writer schemas come from the payload (registry id or fingerprint) or
//! configuration; an optional `reader_schema` pins the shape records are
//! resolved into, using Avro's schema-resolution rules (field reordering,
//! defaults, promotions). Registry schemas using references are not
//! supported yet and are surfaced as unavailable.
//!
//! A reader schema that recurses through a union limits how deep a datum may
//! recurse: see [`AvroValueDeserializer`].
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
mod resolve;
mod wire;

#[cfg(feature = "testing")]
#[doc(hidden)]
pub use cache::{InsertRig, nested_schema, schema_weight};
/// The schema cache's bounds, for tests that derive their counts.
#[cfg(feature = "testing")]
#[doc(hidden)]
pub const MAX_READY_ENTRIES: usize = cache::MAX_READY_ENTRIES;
#[cfg(feature = "testing")]
#[doc(hidden)]
pub const MAX_READY_BYTES: usize = cache::MAX_READY_BYTES;
pub use config::{
    AvroDeserializerBuilder, AvroDeserializerConfig, AvroMode, RegistrySection, SchemaSource,
    TlsSection,
};
pub use datum::AvroDatumDeserializer;
pub use deser::{AvroSerdeDeserializer, AvroValue, AvroValueDeserializer};
#[cfg(any(test, feature = "testing"))]
#[doc(hidden)]
pub use wire::single_object_frame;
pub use wire::{parse_confluent, parse_single_object};

#[cfg(test)]
mod tests {
    use std::panic::{RefUnwindSafe, UnwindSafe};

    /// The public deserializer types stay `Send`, `Sync`, `UnwindSafe` and
    /// `RefUnwindSafe` while they hold the schema cache's clock.
    #[test]
    fn public_types_keep_their_auto_traits() {
        fn auto_traits<T: Send + Sync + UnwindSafe + RefUnwindSafe>() {}
        auto_traits::<crate::AvroDeserializerBuilder>();
        auto_traits::<crate::AvroValueDeserializer>();
        auto_traits::<crate::AvroSerdeDeserializer<u8>>();
        auto_traits::<crate::AvroDatumDeserializer<spate_core::deser::Owned<u8>>>();
    }
}
