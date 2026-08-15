//! The single-pass datum deserializer: [`DecoderCore`]'s framing and
//! schema resolution with the in-tree schema-driven decoder
//! (`crate::de`) on top: one wire pass, with no intermediate
//! [`crate::AvroValue`].

use crate::de;
use crate::deser::DecoderCore;
use spate_core::checkpoint::AckRef;
use spate_core::deser::{Deserializer, EmitRecord, RecFamily};
use spate_core::error::DeserError;
use spate_core::record::{RawPayload, Record};
use std::marker::PhantomData;

/// Single-pass typed deserializer: decodes each datum **directly** into the
/// record family's type via serde, never materialising the intermediate
/// [`crate::AvroValue`] tree the other two deserializers build.
///
/// Built by [`crate::AvroDeserializerBuilder::build_datum`] (any record
/// family, including borrowed ones) or
/// [`crate::AvroDeserializerBuilder::build_serde_datum`] (the owned-`T`
/// convenience form).
///
/// # Performance
///
/// This is the throughput path. The dynamically-typed
/// [`crate::AvroValueDeserializer`] allocates a node per schema position
/// per record, including a heap `String` per field name, and the typed
/// [`crate::AvroSerdeDeserializer`] decodes that tree a second time. Here
/// the writer schema drives serde directly over the datum bytes: no tree,
/// no field-name allocations, and string/bytes contents borrow the payload
/// buffer until your record type copies them.
///
/// # Borrowed (zero-copy) records
///
/// With a borrowed record family (`F::Rec<'buf>` containing `&'buf str` /
/// `&'buf [u8]`), string and bytes fields point straight into the payload
/// buffer, with no per-record copies at all. Enum symbols and record field
/// names live in the schema, not the payload, so those cannot be borrowed
/// (`String` targets work; `&str` targets for *string contents* work).
///
/// # Schema evolution
///
/// No reader schema: records decode in the writer schema's shape, and
/// [`crate::AvroDeserializerBuilder::build_datum`] rejects a configured
/// `reader_schema` at build time. Additive evolution still works through
/// serde: `#[serde(default)]` covers fields newer readers expect and old
/// writers lack, `#[serde(alias)]` covers renames, and unknown fields are
/// skipped structurally. If you need Avro's full resolution rules (field
/// reordering by name, type promotions, defaults from the schema), use
/// [`crate::AvroDeserializerBuilder::build_serde`] instead.
///
/// # Parity and strictness
///
/// Decoded values are identical to running the two-pass path
/// (the `Value` decode plus `from_value`) for every well-formed payload both
/// accept; the differential suite in `tests/datum_parity.rs` pins this.
/// The deliberate differences are strictness on truncated payloads (this
/// path errors where apache-avro 0.21 silently yields `Null`), a per-datum
/// budget of `max(payload length, 65 536)` claimed collection items (a
/// hostile block count over zero-width items errors instead of walking),
/// skipped-field contents (structurally validated only, not UTF-8-checked,
/// and a skipped size-prefixed block is trusted at its declared byte
/// size, so a corrupt block whose size field lies can skip differently
/// than it decodes), and schemas using the `duration` or `big-decimal`
/// logical types, which are rejected up front (build error for fixed
/// schemas, per-record `SchemaUnavailable` for registry ids) instead of
/// failing on every record's type.
pub struct AvroDatumDeserializer<F: RecFamily> {
    core: DecoderCore,
    _f: PhantomData<fn() -> F>,
}

impl<F: RecFamily> AvroDatumDeserializer<F> {
    pub(crate) fn new(core: DecoderCore) -> Self {
        AvroDatumDeserializer {
            core,
            _f: PhantomData,
        }
    }
}

// Manual impls: the derived ones would demand bounds on `F` that a family
// tag has no reason to satisfy: `PhantomData<fn() -> F>` holds no `F`,
// and every chain lane clones its deserializer.
impl<F: RecFamily> Clone for AvroDatumDeserializer<F> {
    fn clone(&self) -> Self {
        AvroDatumDeserializer {
            core: self.core.clone(),
            _f: PhantomData,
        }
    }
}

impl<F: RecFamily> std::fmt::Debug for AvroDatumDeserializer<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroDatumDeserializer")
            .field("core", &self.core)
            .finish()
    }
}

impl<F> Deserializer<F> for AvroDatumDeserializer<F>
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
        // Pre-rendered in `CompiledSchema::compile`: clone, never format.
        let names = writer
            .datum
            .as_ref()
            .map_err(|reason| DeserError::SchemaUnavailable {
                reason: reason.clone(),
            })?;
        let schema = writer
            .schema
            .as_ref()
            .map_err(|reason| DeserError::SchemaUnavailable {
                reason: reason.clone(),
            })?;
        let payload: F::Rec<'buf> =
            de::decode_datum(schema, names, datum).map_err(|e| DeserError::Malformed {
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
#[expect(deprecated, reason = "fixtures call the datum free functions directly")]
mod tests {
    use super::*;
    use crate::deser::SchemaSourceMode;
    use apache_avro::Schema;
    use apache_avro::to_avro_datum;
    use spate_core::deser::Owned;
    use spate_core::record::{Flow, PartitionId};
    use std::sync::Arc;

    const WRITER: &str = r#"{"type":"record","name":"Event","fields":[
        {"name":"id","type":"int"},
        {"name":"name","type":"string"}]}"#;

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

    fn datum(id: i32, name: &str) -> Vec<u8> {
        let schema = Schema::parse_str(WRITER).unwrap();
        let mut rec = apache_avro::types::Record::new(&schema).unwrap();
        rec.put("id", id);
        rec.put("name", name);
        to_avro_datum(&schema, rec).unwrap()
    }

    fn raw_core() -> DecoderCore {
        DecoderCore::new(
            SchemaSourceMode::Raw {
                schema: Arc::new(crate::cache::CompiledSchema::compile(0, WRITER)),
            },
            None,
        )
    }

    fn test_ack() -> AckRef {
        AckRef::test_pair().0
    }

    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct Event {
        id: i64, // int → long widening through serde's visitor defaults
        name: String,
    }

    #[test]
    fn owned_round_trip_and_meta() {
        let payload = datum(7, "orders");
        let mut out = Collected(Vec::new());
        AvroDatumDeserializer::<Owned<Event>>::new(raw_core())
            .deserialize(&raw_payload(&payload), &test_ack(), &mut out)
            .unwrap();
        assert_eq!(out.0.len(), 1);
        assert_eq!(out.0[0].meta.offset, 42);
        assert_eq!(
            out.0[0].payload,
            Event {
                id: 7,
                name: "orders".into()
            }
        );
    }

    #[test]
    fn tombstones_emit_nothing() {
        let mut out = Collected::<Event>(Vec::new());
        AvroDatumDeserializer::<Owned<Event>>::new(raw_core())
            .deserialize(&raw_payload(b""), &test_ack(), &mut out)
            .unwrap();
        assert!(out.0.is_empty());
    }

    #[test]
    fn garbage_is_malformed() {
        let mut out = Collected::<Event>(Vec::new());
        let err = AvroDatumDeserializer::<Owned<Event>>::new(raw_core())
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
        let err = AvroDatumDeserializer::<Owned<Wrong>>::new(raw_core())
            .deserialize(&raw_payload(&payload), &test_ack(), &mut out)
            .unwrap_err();
        assert!(matches!(err, DeserError::Malformed { .. }), "{err}");
    }

    #[test]
    fn an_unusable_schema_is_unavailable_per_record_not_a_panic() {
        let mut compiled = crate::cache::CompiledSchema::compile(0, WRITER);
        compiled.datum = Err("schema 0 is not usable on the datum path: nope".into());
        let core = DecoderCore::new(
            SchemaSourceMode::Raw {
                schema: Arc::new(compiled),
            },
            None,
        );
        let mut out = Collected::<Event>(Vec::new());
        let err = AvroDatumDeserializer::<Owned<Event>>::new(core)
            .deserialize(&raw_payload(&datum(4, "poison")), &test_ack(), &mut out)
            .unwrap_err();
        assert!(
            matches!(&err, DeserError::SchemaUnavailable { reason } if reason.contains("nope")),
            "{err}"
        );
        assert!(out.0.is_empty());
    }

    #[test]
    fn single_object_framing_works_unchanged() {
        use apache_avro::rabin::Rabin;
        let schema = Schema::parse_str(WRITER).unwrap();
        let fp = schema.fingerprint::<Rabin>();
        let fingerprint = u64::from_le_bytes(fp.bytes.as_slice().try_into().unwrap());
        let core = DecoderCore::new(
            SchemaSourceMode::SingleObject {
                schema: Arc::new(crate::cache::CompiledSchema::compile(0, WRITER)),
                fingerprint,
            },
            None,
        );
        let mut framed = vec![0xC3, 0x01];
        framed.extend_from_slice(&fingerprint.to_le_bytes());
        framed.extend_from_slice(&datum(9, "so"));
        let mut out = Collected::<Event>(Vec::new());
        AvroDatumDeserializer::<Owned<Event>>::new(core)
            .deserialize(&raw_payload(&framed), &test_ack(), &mut out)
            .unwrap();
        assert_eq!(out.0[0].payload.id, 9);
    }
}
