//! The Avro deserializers.
//!
//! Every deserializer shares [`DecoderCore`]: framing → schema resolution →
//! datum decode, with the backend-specific datum decode swapped on top of
//! [`DecoderCore::resolve`]. Schema resolution never blocks a pipeline
//! thread: on a registry cache miss the core triggers an asynchronous fetch
//! and returns [`DeserError::NotReady`], which the chain converts into a
//! retriable `Blocked` (see `spate-core`'s deserializer contract).
//!
//! Empty payloads (Kafka tombstones) decode to **zero records** in every
//! mode.

use crate::cache::{CacheSnapshot, CompiledSchema, Lookup};
use crate::registry::RegistryHandle;
use crate::wire;
use apache_avro::Schema;
use apache_avro::from_avro_datum;
use spate_core::checkpoint::AckRef;
use spate_core::deser::{Deserializer, EmitRecord, Owned};
use spate_core::error::DeserError;
use spate_core::record::{RawPayload, Record};
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
    pub(crate) fn resolve<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
    ) -> Result<Resolved<'buf>, DeserError> {
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
                // format.
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
/// the schema is only known at runtime, in pipelines that inspect or route
/// on structure they cannot name at compile time. When the record type
/// *is* known, [`crate::AvroDatumDeserializer`] decodes it in a single
/// pass without materialising the [`AvroValue`] tree at all, and is the
/// faster choice.
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
/// type is plain Rust; no Avro types leak into the pipeline.
///
/// # Performance
///
/// This path decodes each record **twice**, once into an intermediate
/// [`AvroValue`] via `from_avro_datum` and then again into `T` via
/// `from_value`, roughly doubling the per-record allocations and CPU of
/// the dynamically-typed path. Choose it when you need Avro's full
/// schema-resolution rules (a configured `reader_schema`: field
/// reordering, type promotions, defaults, aliases). When you don't,
/// [`crate::AvroDatumDeserializer`] decodes the same `T` in a single pass
/// and is substantially cheaper.
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

// Manual impls: the derived ones would demand `T: Clone`/`T: Debug`, which
// the record type has no reason to satisfy: `PhantomData<fn() -> T>` holds
// no `T`, and every chain lane clones its deserializer.
impl<T> Clone for AvroSerdeDeserializer<T> {
    fn clone(&self) -> Self {
        AvroSerdeDeserializer {
            core: self.core.clone(),
            _t: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for AvroSerdeDeserializer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroSerdeDeserializer")
            .field("core", &self.core)
            .finish()
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

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::to_avro_datum;
    use apache_avro::types::Value;
    use spate_core::record::{Flow, PartitionId};

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

    #[test]
    fn an_uncompilable_schema_is_unavailable_per_record_not_a_panic() {
        // `CompiledSchema::compile` stores a parser rejection rather than
        // failing hard, so the entry can be negatively cached. Each record
        // that lands on it must then surface `SchemaUnavailable`, as Skip/Fail
        // policy fodder, instead of unwinding the pipeline thread.
        let mut compiled = crate::cache::CompiledSchema::compile(0, WRITER_V1);
        compiled.schema = Err("schema 0 is not usable: nope".into());
        let core = DecoderCore {
            mode: SchemaSourceMode::Raw {
                schema: Arc::new(compiled),
            },
            reader_schema: None,
        };

        let mut out = Collected::<AvroValue>(Vec::new());
        let err = AvroValueDeserializer::new(core)
            .deserialize(&raw_payload(&datum(4, "poison")), &test_ack(), &mut out)
            .unwrap_err();
        assert!(
            matches!(&err, DeserError::SchemaUnavailable { reason } if reason.contains("nope")),
            "{err}"
        );
        assert!(out.0.is_empty());
    }
}
