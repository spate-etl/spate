//! The Avro deserializers.
//!
//! Every deserializer shares [`DecoderCore`]: framing → schema resolution →
//! datum decode, with the backend-specific datum decode swapped on top of
//! [`DecoderCore::resolve`]. Schema resolution never blocks a pipeline
//! thread: on a registry cache miss the core triggers an asynchronous fetch
//! and returns [`DeserError::NotReady`], which the chain converts into a
//! retriable `Blocked` (see `spate-core`'s deserializer contract). Once the
//! registry has rejected the client, a miss returns [`DeserError::Fatal`].
//!
//! Empty payloads (Kafka tombstones) decode to **zero records** in every
//! mode.

use crate::cache::{CacheSnapshot, CompiledSchema, Lookup};
use crate::de::{Names, ReaderSide, check_datum, compile_names};
use crate::registry::RegistryHandle;
use crate::resolve::{REFUSED, check_resolution};
use crate::wire;
use apache_avro::Schema;
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::schema::ResolvedSchema;
use spate_core::checkpoint::AckRef;
use spate_core::deser::{Deserializer, EmitRecord, Owned};
use spate_core::error::DeserError;
use spate_core::hash::QualityMap;
use spate_core::record::{RawPayload, Record};
use std::borrow::Cow;
use std::io::{self, Read};
use std::marker::PhantomData;
use std::sync::Arc;

/// How many readers one deserializer caches.
///
/// Each chain lane clones its own deserializer and holds its own map, so a
/// resident reader costs its resolved name maps once per lane.
const MAX_HELD_READERS: usize = 64;

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
        /// costs no shared-lock acquisition; refreshed on a miss, and after
        /// the cache has evicted a compiled schema.
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

/// A [`GenericDatumReader`] beside the schemas it borrows.
///
/// Each schema's named types are resolved once at construction. Both schemas
/// outlive it here: the writer's is owned by the `CompiledSchema` the cache
/// hands out, the reader's by the deserializer. The reader decodes in the
/// writer's shape, and [`read_datum`] resolves the value against the reader
/// schema itself, so that it can weigh the resolution first. `names` indexes
/// the writer schema's named types for the check that runs before each
/// decode: the datum path's index when the schema has one, else its own.
/// `reader_side` is the reader schema as that check charges it.
#[ouroboros::self_referencing]
struct HeldReader {
    writer: Arc<CompiledSchema>,
    reader_schema: Option<Arc<Schema>>,
    reader_side: Option<Arc<ReaderSide>>,
    #[borrows(writer)]
    #[covariant]
    names: Cow<'this, Names>,
    #[borrows(writer)]
    schema: &'this Schema,
    #[borrows(writer)]
    #[covariant]
    reader: GenericDatumReader<'this>,
    #[borrows(reader_schema)]
    #[covariant]
    resolution: Option<(&'this Schema, ResolvedSchema<'this>)>,
}

/// The parsed writer schema, or the pre-rendered reason it did not parse.
fn writer_schema(writer: &CompiledSchema) -> Result<&Schema, DeserError> {
    writer
        .schema
        .as_ref()
        .map_err(|reason| DeserError::SchemaUnavailable {
            // Pre-rendered in `CompiledSchema::compile`: clone, never format.
            reason: reason.clone(),
        })
}

impl HeldReader {
    /// Build a reader over a compiled schema, or the pre-rendered reason that
    /// schema is unusable.
    fn compile(
        writer: Arc<CompiledSchema>,
        reader_schema: Option<Arc<Schema>>,
        reader_side: Option<Arc<ReaderSide>>,
    ) -> Result<HeldReader, DeserError> {
        let id = writer.id;
        HeldReaderTryBuilder {
            writer,
            reader_schema,
            reader_side,
            names_builder: |writer| match &writer.datum {
                Ok(names) => Ok(Cow::Borrowed(names)),
                // The datum path refused the schema (a `duration` or
                // `big-decimal`), so its index was not kept.
                Err(_) => compile_names(writer_schema(writer)?)
                    .map(Cow::Owned)
                    .map_err(|reason| DeserError::SchemaUnavailable {
                        reason: format!("schema {}: {reason}", writer.id),
                    }),
            },
            schema_builder: |writer| writer_schema(writer),
            reader_builder: |writer| {
                GenericDatumReader::builder(writer_schema(writer)?)
                    .build()
                    .map_err(|e| DeserError::SchemaUnavailable {
                        reason: format!("schema {} cannot be resolved: {e}", writer.id),
                    })
            },
            resolution_builder: |reader_schema| {
                let Some(reader) = reader_schema.as_deref() else {
                    return Ok(None);
                };
                ResolvedSchema::new(reader)
                    .map(|names| Some((reader, names)))
                    .map_err(|e| DeserError::SchemaUnavailable {
                        reason: format!("schema {id} cannot be resolved: {e}"),
                    })
            },
        }
        .try_build()
    }
}

/// Shared framing + schema resolution + decode.
pub(crate) struct DecoderCore {
    pub(crate) mode: SchemaSourceMode,
    /// Optional reader schema: pins the shape records are resolved into
    /// (Avro schema-resolution rules: field reordering, defaults, type
    /// promotions).
    pub(crate) reader_schema: Option<Arc<Schema>>,
    /// The reader schema as [`check_datum`] charges it.
    reader_side: Option<Arc<ReaderSide>>,
    /// Readers by writer schema id, holding each id's resolved named types
    /// across payloads. At most [`MAX_HELD_READERS`] entries; at the ceiling an
    /// arriving id displaces an arbitrary resident one.
    ///
    /// The key is the writer id alone, which is valid only while
    /// `reader_schema` is fixed for this deserializer's life. Do not hoist this
    /// into `CompiledSchema`: that is keyed by writer id and carries no
    /// reader-schema identity, so a reader stored there is correct only while
    /// every consumer of one cache shares one reader schema.
    held: QualityMap<u32, HeldReader>,
    /// The cache's eviction count when `held` was last checked against the
    /// memo. A reader whose schema the cache has evicted is dropped, so a
    /// deserializer does not keep a schema the cache let go.
    pruned_at: u64,
}

/// A clone starts with no held readers and builds its own on first payload.
impl Clone for DecoderCore {
    fn clone(&self) -> Self {
        DecoderCore::new(self.mode.clone(), self.reader_side.clone())
    }
}

impl std::fmt::Debug for DecoderCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecoderCore")
            .field("mode", &self.mode)
            .field("reader_schema", &self.reader_schema)
            .field("held", &self.held.keys())
            .finish()
    }
}

/// Tracks failed exact reads throughout one datum decode.
struct DatumReader<'a, 'buf> {
    bytes: &'a mut &'buf [u8],
    truncated: bool,
}

impl Read for DatumReader<'_, '_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.bytes.read(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let result = self.bytes.read_exact(buf);
        if result
            .as_ref()
            .is_err_and(|e| e.kind() == io::ErrorKind::UnexpectedEof)
        {
            self.truncated = true;
        }
        result
    }
}

/// Decode one datum through a reader, once [`check_datum`] has bounded its
/// depth, its collection items and its lengths, and resolve it against the
/// reader schema once [`check_resolution`] has bounded what that costs.
fn read_datum(held: &HeldReader, datum: &mut &[u8]) -> Result<AvroValue, DeserError> {
    let side = held.borrow_reader_side().as_deref();
    check_datum(held.borrow_schema(), held.borrow_names(), side, datum).map_err(|e| {
        DeserError::Malformed {
            reason: format!("avro datum refused: {e}"),
        }
    })?;
    let payload = datum.len();
    let mut input = DatumReader {
        bytes: datum,
        truncated: false,
    };
    let result = held.borrow_reader().read_value(&mut input);
    // The bytes the datum took. Bytes after it are not part of it and earn
    // the resolution check nothing.
    let encoded = payload - input.bytes.len();
    if input.truncated {
        return Err(DeserError::Malformed {
            reason: "avro datum is truncated".into(),
        });
    }
    let failed = |e: apache_avro::Error| refusal("avro datum decode failed", &e);
    let Some((reader, names)) = held.borrow_resolution() else {
        return result.map_err(failed);
    };
    let value = result.map_err(failed)?;
    check_resolution(&value, encoded, reader, names.get_names()).map_err(|_| {
        DeserError::Malformed {
            reason: REFUSED.into(),
        }
    })?;
    value
        .resolve_with_names(reader, names.get_names())
        .map_err(failed)
}

/// The reason for a datum `apache-avro` refused: `lead`, and the reader
/// field the record lacks when that is the cause.
///
/// The rest of `apache-avro`'s text is left out, because most of it prints
/// the value it refused.
fn refusal(lead: &str, e: &apache_avro::Error) -> DeserError {
    let reason = match e.details() {
        // The name is the reader schema's.
        apache_avro::error::Details::GetField(field) => {
            format!("{lead}: reader field `{field}` is missing from the record")
        }
        _ => lead.to_owned(),
    };
    DeserError::Malformed { reason }
}

/// A resolved payload: the writer schema plus the datum slice, which borrows
/// the payload buffer. `None` is a tombstone (empty payload, zero records).
type Resolved<'buf> = Option<(Arc<CompiledSchema>, &'buf [u8])>;

impl DecoderCore {
    pub(crate) fn new(mode: SchemaSourceMode, reader_side: Option<Arc<ReaderSide>>) -> DecoderCore {
        DecoderCore {
            mode,
            reader_schema: reader_side.as_ref().map(|side| side.schema().clone()),
            reader_side,
            held: QualityMap::default(),
            pruned_at: 0,
        }
    }

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
                let found = registry.cache.lookup(memo, id);
                if memo.evictions() != self.pruned_at {
                    self.held.retain(|_, held| memo.holds(held.borrow_writer()));
                    self.pruned_at = memo.evictions();
                }
                match found {
                    Lookup::Ready(schema) => (schema, datum),
                    Lookup::Missing => {
                        if let Some(reason) = registry.rejection.get() {
                            return Err(DeserError::Fatal {
                                reason: reason.clone(),
                            });
                        }
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
        let id = writer.id;
        if let Some(held) = self.held.get(&id) {
            return Ok(Some(read_datum(held, &mut datum)?));
        }
        // Build before the eviction and cache before the read: a schema that
        // will not build must not displace a resident reader, and a payload
        // that will not decode must not discard a valid one.
        let built =
            HeldReader::compile(writer, self.reader_schema.clone(), self.reader_side.clone())?;
        if self.held.len() >= MAX_HELD_READERS {
            // An arbitrary resident id, neither the oldest nor a uniform draw.
            if let Some(&victim) = self.held.keys().next() {
                self.held.remove(&victim);
            }
        }
        let held = self.held.entry(id).or_insert(built);
        Ok(Some(read_datum(held, &mut datum)?))
    }
}

/// Dynamically-typed deserializer: emits [`AvroValue`] records. Use when
/// the schema is only known at runtime, in pipelines that inspect or route
/// on structure they cannot name at compile time. When the record type
/// *is* known, [`crate::AvroDatumDeserializer`] decodes it in a single
/// pass without materialising the [`AvroValue`] tree at all, and is the
/// faster choice.
///
/// # Limits
///
/// Each datum is checked before it is decoded, and refused as
/// [`DeserError::Malformed`] when it nests more than 32 levels deep, when its
/// arrays and maps claim more than `max(payload length, 65 536)` items, or
/// when its decoded [`AvroValue`] tree would take more than 1,024 bytes for
/// each of those. A level is a record field, a collection item, a union
/// branch, a named-type reference, or the bytes of a `decimal` or
/// `big-decimal`, so a linked list of a recursive record decodes up to 10
/// nodes deep. The size counts each value, the field names copied into every
/// record, enum symbols, and string, bytes and map key contents. With a
/// `reader_schema`, it also counts the fields, defaults and enum symbols that
/// resolution into the reader schema adds.
///
/// # Resolution limit
///
/// With a `reader_schema` that recurses through a union, each datum is
/// weighed before it is resolved. A value weighs one, plus one for each 64
/// bytes of content it holds, doubled for each time its path has already
/// passed through the union it is under. A datum is refused as
/// [`DeserError::Malformed`] when the doubling adds more than
/// `max(3 × min(its weight with no doubling, its encoded bytes), 1 024)`. A list whose `next` field is `["null", "Node"]`
/// resolves up to 9 nodes when its nodes have one other field, and up to 5
/// when they have 40. A reader schema that does not recurse refuses nothing.
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
    fn component_type(&self) -> &str {
        crate::config::TAG
    }

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
/// [`AvroValue`] and then again into `T` via `from_value`, roughly
/// doubling the per-record allocations and CPU of the dynamically-typed
/// path. Choose it when you need Avro's schema-resolution rules (a
/// configured `reader_schema`: field reordering, type promotions,
/// defaults). When you don't,
/// [`crate::AvroDatumDeserializer`] decodes the same `T` in a single pass
/// and is substantially cheaper.
///
/// # Limits
///
/// The intermediate decode checks each datum as
/// [`AvroValueDeserializer`] does, with the same limits.
///
/// # Resolution limit
///
/// A `reader_schema` that recurses through a union limits how deep a datum
/// may recurse, as on [`AvroValueDeserializer`].
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
    fn component_type(&self) -> &str {
        crate::config::TAG
    }

    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, T>,
    ) -> Result<(), DeserError> {
        if let Some(value) = self.core.decode(raw)? {
            // The second decode pass, which applies the reader schema's
            // resolution rules. See the type-level `# Performance` note.
            let payload = apache_avro::from_value::<T>(&value)
                .map_err(|e| refusal("avro record does not match the target type", &e))?;
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
#[expect(deprecated, reason = "fixtures call the datum free functions directly")]
mod tests {
    use super::*;
    use apache_avro::to_avro_datum;
    use apache_avro::types::Value;
    use spate_test::EmitCollector;

    const WRITER_V1: &str = r#"{"type":"record","name":"Event","fields":[
        {"name":"id","type":"int"},
        {"name":"name","type":"string"}]}"#;
    const READER_V2: &str = r#"{"type":"record","name":"Event","fields":[
        {"name":"id","type":"long"},
        {"name":"name","type":"string"},
        {"name":"region","type":"string","default":"emea"}]}"#;

    fn raw_payload(bytes: &[u8]) -> RawPayload<'_> {
        RawPayload {
            offset: 42,
            ..spate_test::raw_payload(bytes)
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

    /// A held reader borrows into an `Arc` a clone does not share, so a clone
    /// starts empty and each chain lane builds its own on first payload.
    #[test]
    fn a_clone_starts_with_no_held_readers() {
        let mut core = raw_core(None);
        let bytes = datum(1, "a");
        core.decode(&raw_payload(&bytes)).unwrap().unwrap();
        assert_eq!(core.held.len(), 1, "the original holds one after decoding");

        let mut lane = core.clone();
        assert!(lane.held.is_empty(), "a clone holds none");
        lane.decode(&raw_payload(&bytes)).unwrap().unwrap();
        assert_eq!(lane.held.len(), 1, "and builds its own on first payload");
    }

    /// A deserializer drops the reader of a schema the cache has evicted on
    /// its next payload, so the schema is freed, and keeps the reader of one
    /// the cache still holds.
    #[test]
    fn held_readers_leave_with_their_schema() {
        use crate::cache::{Lookup, SchemaCache};
        use spate_core::clock::SyncClock;

        let cache = Arc::new(SchemaCache::new(
            std::time::Duration::from_secs(30),
            SyncClock::system(),
        ));
        let mut core = DecoderCore::new(
            SchemaSourceMode::Confluent {
                registry: crate::registry::RegistryHandle::detached(Arc::clone(&cache)),
                memo: SchemaCache::empty_snapshot(),
            },
            None,
        );
        let body = datum(1, "a");
        let decode = |core: &mut DecoderCore, id: u32| {
            let frame = spate_test::confluent_frame(id, &body);
            core.decode(&raw_payload(&frame))
        };

        cache.insert_ready(crate::cache::CompiledSchema::compile(1, WRITER_V1));
        decode(&mut core, 1).unwrap().unwrap();
        let first = match cache.get(1) {
            Lookup::Ready(schema) => Arc::downgrade(&schema),
            other => panic!("id 1 is held, got {other:?}"),
        };
        assert!(
            first.upgrade().is_some(),
            "the cache and the reader hold it"
        );

        // Id 1 was looked up once, so it takes two passes of the eviction
        // queue to leave. Id 2 is decoded throughout and stays.
        let cap = u32::try_from(crate::cache::MAX_READY_ENTRIES).unwrap();
        cache.insert_ready(crate::cache::CompiledSchema::compile(2, WRITER_V1));
        for id in 3..3 * cap {
            decode(&mut core, 2).unwrap().unwrap();
            cache.insert_ready(crate::cache::CompiledSchema::compile(id, WRITER_V1));
        }
        assert!(matches!(cache.get(1), Lookup::Missing), "id 1 is evicted");

        decode(&mut core, 2).unwrap().unwrap();
        assert!(
            first.upgrade().is_none(),
            "nothing holds the evicted schema"
        );
        assert_eq!(
            core.held.keys().copied().collect::<Vec<_>>(),
            [2],
            "the reader of a held schema stays"
        );
        assert!(matches!(
            decode(&mut core, 1),
            Err(DeserError::NotReady { .. })
        ));
    }

    /// At the ceiling an arriving id takes a resident id's place, so the cache
    /// stays bounded and every id a stream carries can reach it.
    #[test]
    fn an_id_past_the_ceiling_replaces_a_resident_reader() {
        let mut core = raw_core(None);
        let bytes = datum(1, "a");

        // Every id maps to the same compiled schema here; only the key varies,
        // which is what fills the map.
        for id in 0..MAX_HELD_READERS as u32 {
            core.mode = SchemaSourceMode::Raw {
                schema: Arc::new(crate::cache::CompiledSchema::compile(id, WRITER_V1)),
            };
            core.decode(&raw_payload(&bytes)).unwrap().unwrap();
        }
        assert_eq!(core.held.len(), MAX_HELD_READERS);
        let before: Vec<u32> = core.held.keys().copied().collect();

        let arriving = MAX_HELD_READERS as u32;
        core.mode = SchemaSourceMode::Raw {
            schema: Arc::new(crate::cache::CompiledSchema::compile(arriving, WRITER_V1)),
        };
        let decoded = core.decode(&raw_payload(&bytes)).unwrap().unwrap();
        let Value::Record(fields) = &decoded else {
            panic!("expected a record, got {decoded:?}");
        };
        assert_eq!(fields[0].1, Value::Int(1));

        assert_eq!(core.held.len(), MAX_HELD_READERS, "the cache stays bounded");
        assert!(
            core.held.contains_key(&arriving),
            "the arriving id is resident"
        );
        let mut evicted = before.iter().filter(|id| !core.held.contains_key(id));
        let evicted = *evicted.next().expect("one resident id made way");
        assert!(
            before
                .iter()
                .filter(|id| !core.held.contains_key(id))
                .nth(1)
                .is_none(),
            "and only one"
        );

        // The evicted id is re-admitted on its next payload.
        core.mode = SchemaSourceMode::Raw {
            schema: Arc::new(crate::cache::CompiledSchema::compile(evicted, WRITER_V1)),
        };
        let again = core.decode(&raw_payload(&bytes)).unwrap().unwrap();
        assert_eq!(again, decoded);
        assert!(core.held.contains_key(&evicted), "and is resident again");
        assert_eq!(core.held.len(), MAX_HELD_READERS, "still bounded");
    }

    /// A schema that will not build displaces nothing. The build is fallible
    /// and runs before the eviction, so a stream of unusable ids arriving at a
    /// full cache cannot empty it of the readers its good ids are using.
    #[test]
    fn an_unusable_schema_at_the_ceiling_evicts_nothing() {
        let mut core = raw_core(None);
        let bytes = datum(1, "a");

        for id in 0..MAX_HELD_READERS as u32 {
            core.mode = SchemaSourceMode::Raw {
                schema: Arc::new(crate::cache::CompiledSchema::compile(id, WRITER_V1)),
            };
            core.decode(&raw_payload(&bytes)).unwrap().unwrap();
        }
        let before: Vec<u32> = core.held.keys().copied().collect();

        let mut poison = crate::cache::CompiledSchema::compile(MAX_HELD_READERS as u32, WRITER_V1);
        poison.schema = Err("schema 64 is not usable: nope".into());
        core.mode = SchemaSourceMode::Raw {
            schema: Arc::new(poison),
        };
        let err = core.decode(&raw_payload(&bytes)).unwrap_err();
        assert!(
            matches!(err, DeserError::SchemaUnavailable { .. }),
            "got {err:?}"
        );

        assert_eq!(core.held.len(), MAX_HELD_READERS);
        for id in &before {
            assert!(core.held.contains_key(id), "id {id} is still resident");
        }
    }

    /// Two ids carrying *different* schemas decode as their own schema says.
    #[test]
    fn two_ids_decode_under_their_own_schemas() {
        const OTHER: &str = r#"{"type":"record","name":"Event","fields":[
            {"name":"id","type":"string"}]}"#;

        let other_schema = Schema::parse_str(OTHER).unwrap();
        let mut other_rec = apache_avro::types::Record::new(&other_schema).unwrap();
        other_rec.put("id", "an-id");
        let other_bytes = to_avro_datum(&other_schema, other_rec).unwrap();
        let v1_bytes = datum(7, "seven");

        let mut core = raw_core(None);

        // Id 0 is the two-field WRITER_V1 record.
        let v1 = core.decode(&raw_payload(&v1_bytes)).unwrap().unwrap();
        let Value::Record(fields) = &v1 else {
            panic!("expected a record, got {v1:?}");
        };
        assert_eq!(fields[0].1, Value::Int(7));
        assert_eq!(fields.len(), 2);

        // Id 1 is a one-field record whose `id` is a string. Decoding it under
        // id 0's schema would read the string's length prefix as an int.
        core.mode = SchemaSourceMode::Raw {
            schema: Arc::new(crate::cache::CompiledSchema::compile(1, OTHER)),
        };
        let other = core.decode(&raw_payload(&other_bytes)).unwrap().unwrap();
        let Value::Record(fields) = &other else {
            panic!("expected a record, got {other:?}");
        };
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].1, Value::String("an-id".into()));

        // And id 0 still decodes as itself afterwards.
        core.mode = SchemaSourceMode::Raw {
            schema: Arc::new(crate::cache::CompiledSchema::compile(0, WRITER_V1)),
        };
        let again = core.decode(&raw_payload(&v1_bytes)).unwrap().unwrap();
        assert_eq!(again, v1);
        assert_eq!(core.held.len(), 2);
    }

    fn raw_core(reader: Option<&str>) -> DecoderCore {
        DecoderCore::new(
            SchemaSourceMode::Raw {
                schema: Arc::new(crate::cache::CompiledSchema::compile(0, WRITER_V1)),
            },
            reader.map(|r| {
                Arc::new(ReaderSide::compile(Arc::new(Schema::parse_str(r).unwrap())).unwrap())
            }),
        )
    }

    use spate_test::test_ack;

    #[test]
    fn value_round_trip_and_meta() {
        let payload = datum(7, "orders");
        let mut out = EmitCollector::new();
        AvroValueDeserializer::new(raw_core(None))
            .deserialize(&raw_payload(&payload), &test_ack(), &mut out)
            .unwrap();
        assert_eq!(out.records.len(), 1);
        let rec = &out.records[0];
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
        let mut out = EmitCollector::new();
        AvroSerdeDeserializer::<EventV2>::new(raw_core(Some(READER_V2)))
            .deserialize(&raw_payload(&payload), &test_ack(), &mut out)
            .unwrap();
        assert_eq!(
            out.records[0].payload,
            EventV2 {
                id: 7,
                name: "orders".into(),
                region: "emea".into()
            }
        );
    }

    #[test]
    fn tombstones_emit_nothing() {
        let mut out = EmitCollector::<AvroValue>::new();
        AvroValueDeserializer::new(raw_core(None))
            .deserialize(&raw_payload(b""), &test_ack(), &mut out)
            .unwrap();
        assert!(out.records.is_empty());
    }

    #[test]
    fn garbage_is_malformed() {
        let mut out = EmitCollector::<AvroValue>::new();
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
        let mut out = EmitCollector::<Wrong>::new();
        let err = AvroSerdeDeserializer::<Wrong>::new(raw_core(None))
            .deserialize(&raw_payload(&payload), &test_ack(), &mut out)
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "malformed payload: avro record does not match the target type"
        );
    }

    /// `apache-avro` words this refusal with the number it could not narrow.
    #[test]
    fn a_value_the_reader_schema_refuses_is_not_quoted() {
        const WIDE_WRITER: &str =
            r#"{"type":"record","name":"Count","fields":[{"name":"quantity","type":"long"}]}"#;
        const NARROW_READER: &str =
            r#"{"type":"record","name":"Count","fields":[{"name":"quantity","type":"int"}]}"#;
        let writer = Schema::parse_str(WIDE_WRITER).unwrap();
        let mut rec = apache_avro::types::Record::new(&writer).unwrap();
        rec.put("quantity", 5_000_000_000_i64);
        let payload = to_avro_datum(&writer, rec).unwrap();
        let core = DecoderCore::new(
            SchemaSourceMode::Raw {
                schema: Arc::new(crate::cache::CompiledSchema::compile(0, WIDE_WRITER)),
            },
            Some(Arc::new(
                ReaderSide::compile(Arc::new(Schema::parse_str(NARROW_READER).unwrap())).unwrap(),
            )),
        );
        let mut out = EmitCollector::<AvroValue>::new();
        let err = AvroValueDeserializer::new(core)
            .deserialize(&raw_payload(&payload), &test_ack(), &mut out)
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "malformed payload: avro datum decode failed"
        );
    }

    /// apache-avro's decode text prints the character it stopped at. The
    /// refusal leaves it out, with and without a reader schema.
    #[test]
    fn a_failed_decode_is_not_quoted() {
        const UUID: &str = r#"{"type":"string","logicalType":"uuid"}"#;
        let mut datum = vec![14];
        datum.extend(b"hunter2");
        for reader in [None, Some(UUID)] {
            let core = DecoderCore::new(
                SchemaSourceMode::Raw {
                    schema: Arc::new(crate::cache::CompiledSchema::compile(0, UUID)),
                },
                reader.map(|r| {
                    Arc::new(ReaderSide::compile(Arc::new(Schema::parse_str(r).unwrap())).unwrap())
                }),
            );
            let mut out = EmitCollector::<AvroValue>::new();
            let err = AvroValueDeserializer::new(core)
                .deserialize(&raw_payload(&datum), &test_ack(), &mut out)
                .unwrap_err();
            assert_eq!(
                err.to_string(),
                "malformed payload: avro datum decode failed",
                "reader {reader:?}"
            );
        }
    }

    /// The field's name comes from the reader schema, so it is kept.
    #[test]
    fn a_reader_field_the_record_lacks_is_named() {
        const READER: &str = r#"{"type":"record","name":"Event","fields":[
            {"name":"id","type":"int"},
            {"name":"name","type":"string"},
            {"name":"region","type":"string"}]}"#;
        let payload = datum(1, "hunter2");
        let mut out = EmitCollector::<AvroValue>::new();
        let err = AvroValueDeserializer::new(raw_core(Some(READER)))
            .deserialize(&raw_payload(&payload), &test_ack(), &mut out)
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "malformed payload: avro datum decode failed: reader field `region` is missing from \
             the record"
        );
    }

    #[test]
    fn single_object_checks_fingerprint() {
        use apache_avro::rabin::Rabin;
        let schema = writer_schema();
        let fp = schema.fingerprint::<Rabin>();
        let fingerprint = u64::from_le_bytes(fp.bytes.as_slice().try_into().unwrap());
        let core = |expected: u64| {
            DecoderCore::new(
                SchemaSourceMode::SingleObject {
                    schema: Arc::new(crate::cache::CompiledSchema::compile(0, WRITER_V1)),
                    fingerprint: expected,
                },
                None,
            )
        };

        let framed = crate::wire::single_object_frame(fingerprint, &datum(9, "so"));

        let mut out = EmitCollector::<AvroValue>::new();
        AvroValueDeserializer::new(core(fingerprint))
            .deserialize(&raw_payload(&framed), &test_ack(), &mut out)
            .unwrap();
        assert_eq!(out.records.len(), 1);

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
        let core = DecoderCore::new(
            SchemaSourceMode::Raw {
                schema: Arc::new(compiled),
            },
            None,
        );

        let mut out = EmitCollector::<AvroValue>::new();
        let err = AvroValueDeserializer::new(core)
            .deserialize(&raw_payload(&datum(4, "poison")), &test_ack(), &mut out)
            .unwrap_err();
        assert!(
            matches!(&err, DeserError::SchemaUnavailable { reason } if reason.contains("nope")),
            "{err}"
        );
        assert!(out.records.is_empty());
    }
}

#[cfg(test)]
mod datum_reader_tests {
    use super::*;

    /// EOF remains latched after successful reads; complete boundary reads leave it clear.
    #[test]
    fn datum_reader_keeps_eof_latched() {
        let mut bytes = &[1][..];
        let mut reader = DatumReader {
            bytes: &mut bytes,
            truncated: false,
        };
        reader.read_exact(&mut [0]).unwrap();
        assert!(!reader.truncated);
        assert_eq!(
            reader.read_exact(&mut [0]).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        reader.read_exact(&mut []).unwrap();
        assert!(reader.truncated);
        assert_eq!(reader.read(&mut []).unwrap(), 0);
        assert!(reader.truncated);
    }
}
