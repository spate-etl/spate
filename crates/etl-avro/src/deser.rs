//! The Avro deserializers.
//!
//! Every deserializer shares [`DecoderCore`]: framing → schema resolution →
//! datum decode, with the backend-specific datum decode swapped on top of
//! [`DecoderCore::resolve`]. Schema resolution never blocks a pipeline
//! thread: on a registry cache miss the core triggers an asynchronous fetch
//! and returns [`DeserError::NotReady`], which the chain converts into a
//! retriable `Blocked` (see `etl-core`'s deserializer contract).
//!
//! Empty payloads (Kafka tombstones) decode to **zero records** in every
//! mode.

use crate::cache::{CacheSnapshot, CompiledSchema, Lookup};
use crate::registry::RegistryHandle;
use crate::wire;
use apache_avro::Schema;
use apache_avro::from_avro_datum;
use etl_core::checkpoint::AckRef;
#[cfg(feature = "fast")]
use etl_core::deser::RecFamily;
use etl_core::deser::{Deserializer, EmitRecord, Owned};
use etl_core::error::DeserError;
use etl_core::record::{RawPayload, Record};
use std::marker::PhantomData;
use std::sync::Arc;

/// The generic Avro value type emitted by [`AvroValueDeserializer`].
///
/// **Stability exemption:** this is a re-export of
/// `apache_avro::types::Value` (a 0.x dependency). Pipelines built on the
/// dynamically-typed path opt into that crate's stability; the typed
/// [`AvroSerdeDeserializer`] path has no such exposure.
pub type AvroValue = apache_avro::types::Value;

/// Where the writer schema for a payload comes from.
#[derive(Clone, Debug)]
pub(crate) enum SchemaSourceMode {
    /// Confluent wire format: writer schema fetched from the registry by
    /// the id embedded in each payload.
    Confluent {
        registry: RegistryHandle,
        /// Per-deserializer lock-free memo of the shared cache. Consulted
        /// first on every payload so a repeated (already-`Ready`) schema id
        /// costs no shared-lock acquisition; refreshed only on a miss.
        memo: CacheSnapshot,
    },
    /// The whole payload is a bare datum; the writer schema is fixed.
    Raw { schema: Arc<CompiledSchema> },
    /// Avro single-object encoding; the header fingerprint must match the
    /// configured schema's Rabin fingerprint.
    SingleObject {
        schema: Arc<CompiledSchema>,
        fingerprint: u64,
    },
}

/// Shared framing + schema resolution + decode.
#[derive(Clone, Debug)]
pub(crate) struct DecoderCore {
    pub(crate) mode: SchemaSourceMode,
    /// Optional reader schema: pins the shape records are resolved into
    /// (Avro schema-resolution rules: field reordering, defaults, type
    /// promotions, aliases).
    pub(crate) reader_schema: Option<Arc<Schema>>,
}

/// A resolved payload: the writer schema plus the datum slice, which borrows
/// the payload buffer. `None` is a tombstone (empty payload, zero records).
type Resolved<'buf> = Option<(Arc<CompiledSchema>, &'buf [u8])>;

impl DecoderCore {
    /// The backend-agnostic prefix of every decode: the tombstone rule,
    /// framing, and schema resolution. The returned datum slice borrows the
    /// payload buffer (`'buf`), so a borrowed-record backend can decode
    /// straight out of it.
    fn resolve<'buf>(&mut self, raw: &RawPayload<'buf>) -> Result<Resolved<'buf>, DeserError> {
        if raw.bytes.is_empty() {
            return Ok(None);
        }
        let resolved: (Arc<CompiledSchema>, &'buf [u8]) = match &mut self.mode {
            SchemaSourceMode::Confluent { registry, memo } => {
                let (id, datum) = wire::parse_confluent(raw.bytes)?;
                match registry.cache.lookup(memo, id) {
                    Lookup::Ready(schema) => (schema, datum),
                    Lookup::Missing => {
                        registry.request(id);
                        return Err(DeserError::NotReady {
                            reason: format!("schema {id} is being fetched from the registry"),
                        });
                    }
                    Lookup::Failed(reason) => {
                        return Err(DeserError::SchemaUnavailable { reason });
                    }
                }
            }
            SchemaSourceMode::Raw { schema } => (Arc::clone(schema), raw.bytes),
            SchemaSourceMode::SingleObject {
                schema,
                fingerprint,
            } => {
                let (fp, datum) = wire::parse_single_object(raw.bytes)?;
                if fp != *fingerprint {
                    return Err(DeserError::SchemaUnavailable {
                        reason: format!(
                            "single-object fingerprint {fp:#018x} does not match the \
                             configured schema ({:#018x})",
                            fingerprint
                        ),
                    });
                }
                (Arc::clone(schema), datum)
            }
        };
        Ok(Some(resolved))
    }

    /// Decode one payload to an [`AvroValue`], or `None` for a tombstone.
    fn decode(&mut self, raw: &RawPayload<'_>) -> Result<Option<AvroValue>, DeserError> {
        let Some((writer, mut datum)) = self.resolve(raw)? else {
            return Ok(None);
        };
        let schema = writer
            .schema
            .as_ref()
            .map_err(|reason| DeserError::SchemaUnavailable {
                // Pre-rendered in `CompiledSchema::compile`: clone, never
                // format. A registry schema only the fast backend accepts
                // reaches here on the apache path.
                reason: reason.clone(),
            })?;
        let value =
            from_avro_datum(schema, &mut datum, self.reader_schema.as_deref()).map_err(|e| {
                DeserError::Malformed {
                    reason: format!("avro datum decode failed: {e}"),
                }
            })?;
        Ok(Some(value))
    }
}

/// Dynamically-typed deserializer: emits [`AvroValue`] records. Use when
/// the schema is only known at runtime, or as the lower-allocation path —
/// it decodes each datum exactly once. The typed [`AvroSerdeDeserializer`]
/// keeps `apache-avro` types out of your pipeline, but it is **not** faster:
/// it decodes to an [`AvroValue`] and then re-decodes that into `T`.
#[derive(Clone, Debug)]
pub struct AvroValueDeserializer {
    core: DecoderCore,
}

impl AvroValueDeserializer {
    pub(crate) fn new(core: DecoderCore) -> Self {
        AvroValueDeserializer { core }
    }
}

impl Deserializer<Owned<AvroValue>> for AvroValueDeserializer {
    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, AvroValue>,
    ) -> Result<(), DeserError> {
        if let Some(value) = self.core.decode(raw)? {
            let _ = out.emit(Record {
                payload: value,
                meta: raw.meta(),
                ack: ack.clone(),
            });
        }
        Ok(())
    }
}

/// Typed deserializer: decodes each datum into `T` via serde. The record
/// type is plain Rust — no Avro types leak into the pipeline.
///
/// # Performance
///
/// This path is **not** faster than [`AvroValueDeserializer`]. `apache-avro`
/// 0.21 exposes no single-pass datum-to-`T` decode, so each record is decoded
/// twice — once into an intermediate [`AvroValue`] via `from_avro_datum`,
/// then again into `T` via `from_value` — roughly doubling the per-record
/// allocations and CPU of the dynamically-typed path. Choose it for the clean
/// typed API (no `apache-avro` types in your pipeline), not for throughput.
///
/// With the `fast` feature enabled, `AvroFastDeserializer` (built by
/// [`crate::AvroDeserializerBuilder::build_serde_fast`]) decodes straight
/// into `T` in a single pass — choose it when decode throughput matters and
/// its schema-evolution model fits (see its docs).
#[derive(Clone, Debug)]
pub struct AvroSerdeDeserializer<T> {
    core: DecoderCore,
    _t: PhantomData<fn() -> T>,
}

impl<T> AvroSerdeDeserializer<T> {
    pub(crate) fn new(core: DecoderCore) -> Self {
        AvroSerdeDeserializer {
            core,
            _t: PhantomData,
        }
    }
}

impl<T> Deserializer<Owned<T>> for AvroSerdeDeserializer<T>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, T>,
    ) -> Result<(), DeserError> {
        if let Some(value) = self.core.decode(raw)? {
            // Second decode pass: apache-avro 0.21 has no single-pass
            // datum→T path, so we re-decode the intermediate Value into T.
            // See the type-level `# Performance` note.
            let payload =
                apache_avro::from_value::<T>(&value).map_err(|e| DeserError::Malformed {
                    reason: format!("avro record does not match the target type: {e}"),
                })?;
            let _ = out.emit(Record {
                payload,
                meta: raw.meta(),
                ack: ack.clone(),
            });
        }
        Ok(())
    }
}

/// Fast typed deserializer (feature `fast`): decodes each datum **directly**
/// into the record type in a single pass via `serde_avro_fast` — no
/// intermediate [`AvroValue`] — several times faster than either apache-avro
/// path. It is also the only Avro deserializer that can emit **borrowed**
/// records: `&'buf str` / `&'buf [u8]` fields point straight into the payload
/// buffer (zero-copy).
///
/// Construct through [`crate::AvroDeserializerBuilder::build_serde_fast`]
/// (owned records — the simple default) or
/// [`crate::AvroDeserializerBuilder::build_fast`] (any record family,
/// including borrowed ones). No `serde_avro_fast` types appear in this API.
///
/// # Schema evolution
///
/// This backend has **no reader-schema resolution by design**: the serde
/// target type *is* the reader shape, and each datum is decoded against its
/// writer schema alone. Evolution is expressed with serde attributes —
/// `#[serde(default)]` for fields newer writers added, `#[serde(alias)]` for
/// renames — instead of Avro resolution rules (reader pinning, type
/// promotions). In `confluent` mode every registry writer schema decodes
/// straight into the target type, so the type must tolerate **every live
/// writer version** of the subject. Configuring a `reader_schema` together
/// with this backend is rejected at build time; the apache-avro
/// [`AvroSerdeDeserializer`] remains the right choice when you need real
/// Avro schema resolution.
///
/// # Borrowed (zero-copy) records
///
/// A borrowed record family is two lines — the family names the lifetime so
/// the record type can cross the chain's generic boundaries:
///
/// ```ignore
/// #[derive(serde::Deserialize)]
/// struct Order<'a> {
///     #[serde(borrow)] sku: &'a str,
///     quantity: i32,
/// }
/// struct OrderFam;
/// impl RecFamily for OrderFam {
///     type Rec<'buf> = Order<'buf>;
/// }
/// let deser = builder.build_fast::<OrderFam>()?;
/// ```
///
/// The flagship shape is a batch payload — one datum holding an array of
/// events — exploded with the chain's `flat_map`: each `Event<'buf>` moved
/// out of the decoded batch keeps borrowing the payload buffer, which
/// outlives the whole synchronous fan-out. Zero-copy covers string/bytes
/// *contents*; the batch's `Vec` containers still allocate once per payload,
/// amortized over its events.
#[cfg(feature = "fast")]
pub struct AvroFastDeserializer<F: RecFamily> {
    core: DecoderCore,
    _f: PhantomData<fn() -> F>,
}

#[cfg(feature = "fast")]
impl<F: RecFamily> AvroFastDeserializer<F> {
    pub(crate) fn new(core: DecoderCore) -> Self {
        AvroFastDeserializer {
            core,
            _f: PhantomData,
        }
    }
}

// Manual impls: the derived ones would demand `F: Clone`/`F: Debug`, and
// family tags are typically bound-free unit structs.
#[cfg(feature = "fast")]
impl<F: RecFamily> Clone for AvroFastDeserializer<F> {
    fn clone(&self) -> Self {
        AvroFastDeserializer {
            core: self.core.clone(),
            _f: PhantomData,
        }
    }
}

#[cfg(feature = "fast")]
impl<F: RecFamily> std::fmt::Debug for AvroFastDeserializer<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroFastDeserializer")
            .field("core", &self.core)
            .finish()
    }
}

#[cfg(feature = "fast")]
impl<F> Deserializer<F> for AvroFastDeserializer<F>
where
    F: RecFamily,
    for<'buf> F::Rec<'buf>: serde::Deserialize<'buf>,
{
    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, F::Rec<'buf>>,
    ) -> Result<(), DeserError> {
        let Some((writer, datum)) = self.core.resolve(raw)? else {
            return Ok(());
        };
        let fast = writer
            .fast
            .as_ref()
            .map_err(|reason| DeserError::SchemaUnavailable {
                // Pre-rendered in `CompiledSchema::compile`: clone, never format.
                reason: reason.clone(),
            })?;
        let payload: F::Rec<'buf> =
            serde_avro_fast::from_datum_slice(datum, fast).map_err(|e| DeserError::Malformed {
                reason: format!("avro datum decode failed: {e}"),
            })?;
        let _ = out.emit(Record {
            payload,
            meta: raw.meta(),
            ack: ack.clone(),
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::to_avro_datum;
    use apache_avro::types::Value;
    use etl_core::record::{Flow, PartitionId};

    const WRITER_V1: &str = r#"{"type":"record","name":"Event","fields":[
        {"name":"id","type":"int"},
        {"name":"name","type":"string"}]}"#;
    const READER_V2: &str = r#"{"type":"record","name":"Event","fields":[
        {"name":"id","type":"long"},
        {"name":"name","type":"string"},
        {"name":"region","type":"string","default":"emea"}]}"#;

    struct Collected<T>(Vec<Record<T>>);
    impl<'buf, T> EmitRecord<'buf, T> for Collected<T> {
        fn emit(&mut self, rec: Record<T>) -> Flow {
            self.0.push(rec);
            Flow::Continue
        }
    }

    fn raw_payload(bytes: &[u8]) -> RawPayload<'_> {
        RawPayload {
            bytes,
            key: Some(b"k"),
            partition: PartitionId(3),
            offset: 42,
            timestamp_ms: 1_000,
        }
    }

    fn writer_schema() -> Schema {
        Schema::parse_str(WRITER_V1).unwrap()
    }

    fn datum(id: i32, name: &str) -> Vec<u8> {
        let schema = writer_schema();
        let mut rec = apache_avro::types::Record::new(&schema).unwrap();
        rec.put("id", id);
        rec.put("name", name);
        to_avro_datum(&schema, rec).unwrap()
    }

    fn raw_core(reader: Option<&str>) -> DecoderCore {
        DecoderCore {
            mode: SchemaSourceMode::Raw {
                schema: Arc::new(crate::cache::CompiledSchema::compile(0, WRITER_V1)),
            },
            reader_schema: reader.map(|r| Arc::new(Schema::parse_str(r).unwrap())),
        }
    }

    fn test_ack() -> AckRef {
        AckRef::test_pair().0
    }

    #[test]
    fn value_round_trip_and_meta() {
        let payload = datum(7, "orders");
        let mut out = Collected(Vec::new());
        AvroValueDeserializer::new(raw_core(None))
            .deserialize(&raw_payload(&payload), &test_ack(), &mut out)
            .unwrap();
        assert_eq!(out.0.len(), 1);
        let rec = &out.0[0];
        assert_eq!(rec.meta.offset, 42);
        let Value::Record(fields) = &rec.payload else {
            panic!("expected a record value");
        };
        assert_eq!(fields[0], ("id".into(), Value::Int(7)));
        assert_eq!(fields[1], ("name".into(), Value::String("orders".into())));
    }

    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct EventV2 {
        id: i64,
        name: String,
        region: String,
    }

    #[test]
    fn serde_round_trip_with_reader_schema_evolution() {
        // Writer v1 (int id, no region) resolved into reader v2:
        // int→long promotion plus a defaulted field.
        let payload = datum(7, "orders");
        let mut out = Collected(Vec::new());
        AvroSerdeDeserializer::<EventV2>::new(raw_core(Some(READER_V2)))
            .deserialize(&raw_payload(&payload), &test_ack(), &mut out)
            .unwrap();
        assert_eq!(
            out.0[0].payload,
            EventV2 {
                id: 7,
                name: "orders".into(),
                region: "emea".into()
            }
        );
    }

    #[test]
    fn tombstones_emit_nothing() {
        let mut out = Collected::<AvroValue>(Vec::new());
        AvroValueDeserializer::new(raw_core(None))
            .deserialize(&raw_payload(b""), &test_ack(), &mut out)
            .unwrap();
        assert!(out.0.is_empty());
    }

    #[test]
    fn garbage_is_malformed() {
        let mut out = Collected::<AvroValue>(Vec::new());
        let err = AvroValueDeserializer::new(raw_core(None))
            .deserialize(&raw_payload(&[0xFF, 0xFF, 0xFF]), &test_ack(), &mut out)
            .unwrap_err();
        assert!(matches!(err, DeserError::Malformed { .. }), "{err}");
    }

    #[test]
    fn type_mismatch_is_malformed_not_panic() {
        #[derive(Debug, serde::Deserialize)]
        struct Wrong {
            #[expect(dead_code, reason = "shape only")]
            id: String,
        }
        let payload = datum(1, "x");
        let mut out = Collected::<Wrong>(Vec::new());
        let err = AvroSerdeDeserializer::<Wrong>::new(raw_core(None))
            .deserialize(&raw_payload(&payload), &test_ack(), &mut out)
            .unwrap_err();
        assert!(matches!(err, DeserError::Malformed { .. }), "{err}");
    }

    #[test]
    fn single_object_checks_fingerprint() {
        use apache_avro::rabin::Rabin;
        let schema = writer_schema();
        let fp = schema.fingerprint::<Rabin>();
        let fingerprint = u64::from_le_bytes(fp.bytes.as_slice().try_into().unwrap());
        let core = |expected: u64| DecoderCore {
            mode: SchemaSourceMode::SingleObject {
                schema: Arc::new(crate::cache::CompiledSchema::compile(0, WRITER_V1)),
                fingerprint: expected,
            },
            reader_schema: None,
        };

        let mut framed = vec![0xC3, 0x01];
        framed.extend_from_slice(&fingerprint.to_le_bytes());
        framed.extend_from_slice(&datum(9, "so"));

        let mut out = Collected::<AvroValue>(Vec::new());
        AvroValueDeserializer::new(core(fingerprint))
            .deserialize(&raw_payload(&framed), &test_ack(), &mut out)
            .unwrap();
        assert_eq!(out.0.len(), 1);

        let err = AvroValueDeserializer::new(core(fingerprint ^ 1))
            .deserialize(&raw_payload(&framed), &test_ack(), &mut out)
            .unwrap_err();
        assert!(matches!(err, DeserError::SchemaUnavailable { .. }), "{err}");
    }

    /// The fast backend's mirror of the table above: same framing, schema
    /// resolution, tombstone, and error-mapping semantics, single-pass decode.
    #[cfg(feature = "fast")]
    mod fast {
        use super::*;

        #[derive(Debug, serde::Deserialize, PartialEq)]
        struct EventOwned {
            id: i32,
            name: String,
        }

        #[derive(Debug, serde::Deserialize, PartialEq)]
        struct EventRef<'a> {
            id: i32,
            name: &'a str,
        }

        struct EventRefFam;
        impl RecFamily for EventRefFam {
            type Rec<'buf> = EventRef<'buf>;
        }

        #[test]
        fn owned_round_trip_and_meta() {
            let payload = datum(7, "orders");
            let mut out = Collected(Vec::new());
            AvroFastDeserializer::<Owned<EventOwned>>::new(raw_core(None))
                .deserialize(&raw_payload(&payload), &test_ack(), &mut out)
                .unwrap();
            assert_eq!(out.0.len(), 1);
            assert_eq!(out.0[0].meta.offset, 42);
            assert_eq!(
                out.0[0].payload,
                EventOwned {
                    id: 7,
                    name: "orders".into()
                }
            );
        }

        #[test]
        fn borrowed_round_trip_is_zero_copy_by_pointer_provenance() {
            let payload = datum(7, "orders");
            let mut out = Collected(Vec::new());
            AvroFastDeserializer::<EventRefFam>::new(raw_core(None))
                .deserialize(&raw_payload(&payload), &test_ack(), &mut out)
                .unwrap();
            assert_eq!(out.0.len(), 1);
            let rec = &out.0[0].payload;
            assert_eq!(
                *rec,
                EventRef {
                    id: 7,
                    name: "orders"
                }
            );
            // The borrowed string points into the payload buffer itself.
            assert!(
                payload.as_ptr_range().contains(&rec.name.as_ptr()),
                "borrowed field must point into the payload buffer"
            );
        }

        #[test]
        fn tombstones_emit_nothing() {
            let mut out = Collected::<EventOwned>(Vec::new());
            AvroFastDeserializer::<Owned<EventOwned>>::new(raw_core(None))
                .deserialize(&raw_payload(b""), &test_ack(), &mut out)
                .unwrap();
            assert!(out.0.is_empty());
        }

        #[test]
        fn garbage_is_malformed_not_panic() {
            let mut out = Collected::<EventOwned>(Vec::new());
            let err = AvroFastDeserializer::<Owned<EventOwned>>::new(raw_core(None))
                .deserialize(&raw_payload(&[0xFF, 0xFF, 0xFF]), &test_ack(), &mut out)
                .unwrap_err();
            assert!(matches!(err, DeserError::Malformed { .. }), "{err}");
        }

        #[test]
        fn type_mismatch_is_malformed_not_panic() {
            #[derive(Debug, serde::Deserialize)]
            struct Wrong {
                #[expect(dead_code, reason = "shape only")]
                id: String,
            }
            let payload = datum(1, "x");
            let mut out = Collected::<Wrong>(Vec::new());
            let err = AvroFastDeserializer::<Owned<Wrong>>::new(raw_core(None))
                .deserialize(&raw_payload(&payload), &test_ack(), &mut out)
                .unwrap_err();
            assert!(matches!(err, DeserError::Malformed { .. }), "{err}");
        }

        #[test]
        fn single_object_checks_fingerprint() {
            use apache_avro::rabin::Rabin;
            let schema = writer_schema();
            let fp = schema.fingerprint::<Rabin>();
            let fingerprint = u64::from_le_bytes(fp.bytes.as_slice().try_into().unwrap());
            let core = |expected: u64| DecoderCore {
                mode: SchemaSourceMode::SingleObject {
                    schema: Arc::new(crate::cache::CompiledSchema::compile(0, WRITER_V1)),
                    fingerprint: expected,
                },
                reader_schema: None,
            };

            let mut framed = vec![0xC3, 0x01];
            framed.extend_from_slice(&fingerprint.to_le_bytes());
            framed.extend_from_slice(&datum(9, "so"));

            let mut out = Collected::<EventOwned>(Vec::new());
            AvroFastDeserializer::<Owned<EventOwned>>::new(core(fingerprint))
                .deserialize(&raw_payload(&framed), &test_ack(), &mut out)
                .unwrap();
            assert_eq!(out.0.len(), 1);

            let err = AvroFastDeserializer::<Owned<EventOwned>>::new(core(fingerprint ^ 1))
                .deserialize(&raw_payload(&framed), &test_ack(), &mut out)
                .unwrap_err();
            assert!(matches!(err, DeserError::SchemaUnavailable { .. }), "{err}");
        }

        /// A `CompiledSchema` with the given backend's side replaced by a
        /// stored failure — the state a real one-backend rejection leaves.
        fn half_compiled(fail_apache: bool) -> crate::cache::CompiledSchema {
            let mut compiled = crate::cache::CompiledSchema::compile(0, WRITER_V1);
            if fail_apache {
                compiled.schema = Err("schema 0 is not usable by the apache backend: nope".into());
            } else {
                compiled.fast = Err("schema 0 is not usable by the fast backend: nope".into());
            }
            compiled
        }

        #[test]
        fn fast_unusable_schema_is_unavailable_for_fast_and_fine_for_apache() {
            // A schema whose fast form failed to compile: the fast pipeline
            // surfaces SchemaUnavailable per record (Skip/Fail policy fodder,
            // no panic), while the apache path on the very same core decodes.
            let core = DecoderCore {
                mode: SchemaSourceMode::Raw {
                    schema: Arc::new(half_compiled(false)),
                },
                reader_schema: None,
            };
            let payload = datum(3, "iso");

            let mut out = Collected::<EventOwned>(Vec::new());
            let err = AvroFastDeserializer::<Owned<EventOwned>>::new(core.clone())
                .deserialize(&raw_payload(&payload), &test_ack(), &mut out)
                .unwrap_err();
            assert!(matches!(err, DeserError::SchemaUnavailable { .. }), "{err}");
            assert!(out.0.is_empty());

            let mut value_out = Collected::<AvroValue>(Vec::new());
            AvroValueDeserializer::new(core)
                .deserialize(&raw_payload(&payload), &test_ack(), &mut value_out)
                .unwrap();
            assert_eq!(value_out.0.len(), 1);
        }

        #[test]
        fn apache_unusable_schema_is_unavailable_for_apache_and_fine_for_fast() {
            // The mirror image: a schema only the fast backend compiled. The
            // apache paths surface SchemaUnavailable per record with the
            // stored reason; the fast path on the very same core decodes.
            let core = DecoderCore {
                mode: SchemaSourceMode::Raw {
                    schema: Arc::new(half_compiled(true)),
                },
                reader_schema: None,
            };
            let payload = datum(4, "mirror");

            let mut value_out = Collected::<AvroValue>(Vec::new());
            let err = AvroValueDeserializer::new(core.clone())
                .deserialize(&raw_payload(&payload), &test_ack(), &mut value_out)
                .unwrap_err();
            assert!(matches!(err, DeserError::SchemaUnavailable { .. }), "{err}");
            assert!(value_out.0.is_empty());

            let mut out = Collected::<EventOwned>(Vec::new());
            AvroFastDeserializer::<Owned<EventOwned>>::new(core)
                .deserialize(&raw_payload(&payload), &test_ack(), &mut out)
                .unwrap();
            assert_eq!(out.0.len(), 1);
        }
    }
}
