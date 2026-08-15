//! Turning an event into the bytes a payload borrows.
//!
//! Both encoders append **into a buffer the caller owns** rather than returning
//! one, so a lane appends event after event to a single reused arena and hands
//! out subslices of it. Once the arena has grown to a batch, neither encoder
//! allocates it again: the JSON encoder allocates nothing at all, and the Avro
//! encoder allocates only inside `apache-avro`'s serializer.
//!
//! `encoding:` selects the format. The generator authors its own payloads, so
//! an encoder is chosen from that set rather than supplied by the assembly.

use crate::config::Encoding;
use crate::events::StorefrontEvent;
use spate_core::error::{ErrorClass, SourceError};

/// A resolved payload encoder. Built once at `open`, never on the record path.
#[derive(Debug)]
pub(crate) enum Encoder {
    /// One JSON document per payload.
    Json,
    /// One bare Avro datum per payload, against the parsed
    /// [`EVENT_SCHEMA_JSON`](crate::EVENT_SCHEMA_JSON).
    #[cfg(feature = "avro")]
    Avro(Box<apache_avro::Schema>),
}

impl Encoder {
    /// Resolve `encoding`. The Avro schema is parsed here, once, so the record
    /// path never touches it.
    pub(crate) fn new(encoding: Encoding) -> Result<Encoder, SourceError> {
        match encoding {
            Encoding::Json => Ok(Encoder::Json),
            #[cfg(feature = "avro")]
            Encoding::Avro => {
                let schema =
                    apache_avro::Schema::parse_str(crate::EVENT_SCHEMA_JSON).map_err(|e| {
                        SourceError::Client {
                            class: ErrorClass::Fatal,
                            reason: format!("parsing the built-in datagen Avro schema: {e}"),
                        }
                    })?;
                Ok(Encoder::Avro(Box::new(schema)))
            }
            // Unreachable through configuration, since `validate` rejects
            // this combination at load time, but a hand-built config reaches
            // `open` without passing through it.
            #[cfg(not(feature = "avro"))]
            Encoding::Avro => Err(SourceError::Client {
                class: ErrorClass::Fatal,
                reason: crate::config::AVRO_FEATURE_OFF.into(),
            }),
        }
    }

    /// Append `event`'s encoded bytes to `out`.
    ///
    /// On error `out` is left as it was found: both encoders write
    /// incrementally, so the partial bytes are rolled back before returning.
    pub(crate) fn encode(
        &self,
        event: &StorefrontEvent,
        out: &mut Vec<u8>,
    ) -> Result<(), SourceError> {
        let start = out.len();
        let result = match self {
            // Infallible in practice, since the event model has no map
            // keys, no non-finite floats and no custom `Serialize`, but the
            // writer API is fallible.
            Encoder::Json => serde_json::to_writer(&mut *out, event)
                .map_err(|e| format!("encoding a datagen event as JSON: {e}")),
            #[cfg(feature = "avro")]
            Encoder::Avro(schema) => {
                // The built-in schema defines its four records inline and
                // carries no `Schema::Ref`, so the resolution map is empty.
                // `HashMap::new` does not allocate.
                let names = apache_avro::schema::NamesRef::new();
                apache_avro::write_avro_datum_ref(schema, &names, &AvroDatum(event), out)
                    .map(|_bytes_written| ())
                    .map_err(|e| format!("encoding a datagen event as an Avro datum: {e}"))
            }
        };
        result.map_err(|reason| {
            out.truncate(start);
            SourceError::Client {
                class: ErrorClass::Fatal,
                reason,
            }
        })
    }
}

/// An event in the shape `EVENT_SCHEMA_JSON`'s top-level union declares,
/// serialized straight into the caller's buffer.
///
/// [`StorefrontEvent`]'s own `Serialize` is the JSON contract, internally
/// tagged by `type`, which is not a union. This view selects the branch by
/// **index** instead, and those indices are the contract this crate publishes:
/// they follow the union order in
/// [`EVENT_SCHEMA_JSON`](crate::EVENT_SCHEMA_JSON) and cannot be reordered
/// independently of it.
#[cfg(feature = "avro")]
struct AvroDatum<'a>(&'a StorefrontEvent);

#[cfg(feature = "avro")]
impl serde::Serialize for AvroDatum<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        const NAME: &str = "StorefrontEvent";
        match self.0 {
            StorefrontEvent::OrderPlaced(e) => {
                serializer.serialize_newtype_variant(NAME, 0, "OrderPlaced", &AvroRecord(e))
            }
            StorefrontEvent::PaymentCaptured(e) => {
                serializer.serialize_newtype_variant(NAME, 1, "PaymentCaptured", &AvroRecord(e))
            }
            StorefrontEvent::RefundIssued(e) => {
                serializer.serialize_newtype_variant(NAME, 2, "RefundIssued", &AvroRecord(e))
            }
        }
    }
}

/// A record in the numeric types `EVENT_SCHEMA_JSON` declares.
///
/// Avro has no unsigned types, so the schema declares `int` and `long` where
/// the event model carries `u32` and `u64`. This view narrows each field to
/// the declared type, and refuses a value the declared type cannot carry
/// rather than wrapping it. The field order and names are the record's own;
/// only the numeric types differ.
#[cfg(feature = "avro")]
struct AvroRecord<'a, T>(&'a T);

/// `v` as the Avro `int` the schema declares.
#[cfg(feature = "avro")]
fn avro_int<E: serde::ser::Error>(field: &str, v: u32) -> Result<i32, E> {
    i32::try_from(v)
        .map_err(|_| E::custom(format!("{field} exceeds the Avro int the schema declares")))
}

/// `v` as the Avro `long` the schema declares.
#[cfg(feature = "avro")]
fn avro_long<E: serde::ser::Error>(field: &str, v: u64) -> Result<i64, E> {
    i64::try_from(v)
        .map_err(|_| E::custom(format!("{field} exceeds the Avro long the schema declares")))
}

#[cfg(feature = "avro")]
impl serde::Serialize for AvroRecord<'_, crate::events::OrderPlaced> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let e = self.0;
        let mut s = serializer.serialize_struct("OrderPlaced", 5)?;
        s.serialize_field("order_id", &avro_long("order_id", e.order_id)?)?;
        s.serialize_field("customer_id", &avro_int("customer_id", e.customer_id)?)?;
        s.serialize_field("region", &e.region)?;
        s.serialize_field("placed_at", &e.placed_at)?;
        s.serialize_field("lines", &AvroLines(&e.lines))?;
        s.end()
    }
}

/// The `lines` array, each element in the declared numeric types.
#[cfg(feature = "avro")]
struct AvroLines<'a>(&'a [crate::events::OrderLine]);

#[cfg(feature = "avro")]
impl serde::Serialize for AvroLines<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut s = serializer.serialize_seq(Some(self.0.len()))?;
        for line in self.0 {
            s.serialize_element(&AvroRecord(line))?;
        }
        s.end()
    }
}

#[cfg(feature = "avro")]
impl serde::Serialize for AvroRecord<'_, crate::events::OrderLine> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let e = self.0;
        let mut s = serializer.serialize_struct("OrderLine", 3)?;
        s.serialize_field("sku", &e.sku)?;
        s.serialize_field("qty", &avro_int("qty", e.qty)?)?;
        s.serialize_field("unit_cents", &avro_int("unit_cents", e.unit_cents)?)?;
        s.end()
    }
}

#[cfg(feature = "avro")]
impl serde::Serialize for AvroRecord<'_, crate::events::PaymentCaptured> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let e = self.0;
        let mut s = serializer.serialize_struct("PaymentCaptured", 2)?;
        s.serialize_field("order_id", &avro_long("order_id", e.order_id)?)?;
        s.serialize_field("amount_cents", &avro_long("amount_cents", e.amount_cents)?)?;
        s.end()
    }
}

#[cfg(feature = "avro")]
impl serde::Serialize for AvroRecord<'_, crate::events::RefundIssued> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let e = self.0;
        let mut s = serializer.serialize_struct("RefundIssued", 3)?;
        s.serialize_field("order_id", &avro_long("order_id", e.order_id)?)?;
        s.serialize_field("amount_cents", &avro_long("amount_cents", e.amount_cents)?)?;
        s.serialize_field("reason", &e.reason)?;
        s.end()
    }
}

#[cfg(test)]
#[expect(deprecated, reason = "fixtures call the datum free functions directly")]
mod tests {
    use super::*;
    use crate::config::DatagenSourceConfig;
    use crate::plan::EventPlan;

    /// Every encoding this build can resolve, so a property asserted of "the
    /// encoder" is asserted of each of them.
    #[cfg(feature = "avro")]
    const ENCODINGS: [Encoding; 2] = [Encoding::Json, Encoding::Avro];
    #[cfg(not(feature = "avro"))]
    const ENCODINGS: [Encoding; 1] = [Encoding::Json];

    fn events(count: usize) -> Vec<StorefrontEvent> {
        plan().take(count).collect()
    }

    /// An endless lane, so a caller can draw *different* batches one after
    /// another the way a running lane does.
    fn plan() -> impl Iterator<Item = StorefrontEvent> {
        let cfg = DatagenSourceConfig {
            seed: 21,
            ..DatagenSourceConfig::default()
        };
        let mut plan = EventPlan::new(&cfg, 0);
        std::iter::from_fn(move || Some(plan.next().0))
    }

    /// The encoder appends, and the caller keeps the buffer. A lane depends on
    /// both halves of that: the spans it hands out are only correct if nothing
    /// before them moved, and the arena is only reusable if `encode` never
    /// clears it.
    #[test]
    fn encoding_appends_to_the_caller_s_buffer_and_leaves_earlier_bytes_alone() {
        let encoder = Encoder::new(Encoding::Json).unwrap();
        let mut arena = Vec::new();
        let mut spans = Vec::new();
        let generated = events(64);
        for event in &generated {
            let start = arena.len();
            encoder.encode(event, &mut arena).unwrap();
            spans.push(start..arena.len());
        }
        assert_eq!(spans.len(), generated.len());
        for (span, event) in spans.iter().zip(&generated) {
            let decoded: StorefrontEvent = serde_json::from_slice(&arena[span.clone()]).unwrap();
            assert_eq!(&decoded, event, "a span decodes to the event that wrote it");
        }
    }

    /// A warm arena is never reallocated, which keeps a steady-state poll
    /// allocation-free.
    ///
    /// Every batch here is *different*, because the lane runs on rather than
    /// restarting from its seed, so the assertion is about the encoder rather
    /// than about `Vec::clear` keeping a capacity that identical input refills.
    #[test]
    fn a_warm_arena_is_never_reallocated() {
        for encoding in ENCODINGS {
            let encoder = Encoder::new(encoding).unwrap();
            let mut lane = plan();
            let mut arena = Vec::new();

            // Comfortably past the largest a 256-event batch can reach: every
            // event a placement of five lines, each field the longest its
            // dimension table holds. The per-batch length assertion below is
            // what keeps that judgement honest.
            const BOUND: usize = 256 * 1024;
            arena.reserve(BOUND);
            let warm = arena.capacity();

            let mut batches = Vec::new();
            for _ in 0..50 {
                arena.clear();
                for event in lane.by_ref().take(256) {
                    encoder.encode(&event, &mut arena).unwrap();
                }
                assert!(
                    arena.len() < BOUND,
                    "{encoding:?}: a batch reached {} bytes, so BOUND no longer bounds it",
                    arena.len()
                );
                assert_eq!(
                    arena.capacity(),
                    warm,
                    "{encoding:?}: a warm arena reallocated"
                );
                batches.push(arena.clone());
            }
            assert!(
                batches[0] != batches[1],
                "{encoding:?}: the batches are identical, so nothing was exercised"
            );
        }
    }

    /// Every field of every datum, decoded under `EVENT_SCHEMA_JSON` by the
    /// reference implementation and compared against the event that wrote it.
    ///
    /// `apache-avro` catches a datum whose *shape* drifts from the published
    /// schema on its own (a misnamed field, a wrong type, a branch index past
    /// the union) and rejects it at encode time. It cannot catch a field
    /// carrying the wrong value, which is what the assertions here cover: two
    /// fields of the same Avro type transposed, or one zeroed, fails on this
    /// test rather than on a consumer.
    #[cfg(feature = "avro")]
    #[test]
    fn every_avro_datum_reads_back_field_for_field() {
        use apache_avro::types::Value;

        let schema = apache_avro::Schema::parse_str(crate::EVENT_SCHEMA_JSON).unwrap();
        let encoder = Encoder::new(Encoding::Avro).unwrap();
        let mut seen = [0usize; 3];

        for event in events(512) {
            let mut buf = Vec::new();
            encoder.encode(&event, &mut buf).unwrap();
            let decoded = apache_avro::from_avro_datum(&schema, &mut buf.as_slice(), None).unwrap();
            let Value::Union(branch, inner) = decoded else {
                panic!("the top-level schema is a union; got {decoded:?}");
            };
            let Value::Record(fields) = *inner else {
                panic!("every union branch is a record");
            };
            // By name rather than by position: the schema fixes the names, and
            // a lookup that followed the encoder's own order could not detect
            // two fields swapped.
            let field = |name: &str| {
                fields
                    .iter()
                    .find(|(f, _)| f == name)
                    .unwrap_or_else(|| panic!("branch {branch} has no field {name}"))
                    .1
                    .clone()
            };

            seen[branch as usize] += 1;
            match &event {
                StorefrontEvent::OrderPlaced(e) => {
                    assert_eq!(branch, 0);
                    assert_eq!(field("order_id"), Value::Long(e.order_id as i64));
                    assert_eq!(field("customer_id"), Value::Int(e.customer_id as i32));
                    assert_eq!(field("region"), Value::String(e.region.to_string()));
                    assert_eq!(field("placed_at"), Value::TimestampMillis(e.placed_at));
                    let Value::Array(lines) = field("lines") else {
                        panic!("lines is an array");
                    };
                    assert_eq!(lines.len(), e.lines.len());
                    for (decoded, line) in lines.iter().zip(&e.lines) {
                        let Value::Record(cells) = decoded else {
                            panic!("a line is a record");
                        };
                        let cell =
                            |name: &str| cells.iter().find(|(f, _)| f == name).unwrap().1.clone();
                        assert_eq!(cell("sku"), Value::String(line.sku.to_string()));
                        assert_eq!(cell("qty"), Value::Int(line.qty as i32));
                        assert_eq!(cell("unit_cents"), Value::Int(line.unit_cents as i32));
                    }
                }
                StorefrontEvent::PaymentCaptured(e) => {
                    assert_eq!(branch, 1);
                    assert_eq!(field("order_id"), Value::Long(e.order_id as i64));
                    assert_eq!(field("amount_cents"), Value::Long(e.amount_cents as i64));
                }
                StorefrontEvent::RefundIssued(e) => {
                    assert_eq!(branch, 2);
                    assert_eq!(field("order_id"), Value::Long(e.order_id as i64));
                    assert_eq!(field("amount_cents"), Value::Long(e.amount_cents as i64));
                    assert_eq!(field("reason"), Value::String(e.reason.to_string()));
                }
            }
        }
        assert!(
            seen.iter().all(|&n| n > 0),
            "some union branch went untested: {seen:?}"
        );
    }

    /// The datum bytes, pinned. Decoding proves the value survives a
    /// round trip through this build; this holds the wire format itself
    /// steady, including across a change in how `apache-avro` encodes.
    #[cfg(feature = "avro")]
    #[test]
    fn the_avro_wire_format_is_pinned_across_builds() {
        let encoder = Encoder::new(Encoding::Avro).unwrap();
        let mut arena = Vec::new();
        for event in events(500) {
            encoder.encode(&event, &mut arena).unwrap();
        }
        let digest = arena.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, &byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3)
        });
        assert_eq!(arena.len(), 17_343, "the encoded length moved");
        assert_eq!(
            digest, 9_404_063_270_987_324_100,
            "the encoded datums moved"
        );
    }

    #[cfg(not(feature = "avro"))]
    #[test]
    fn avro_without_the_feature_fails_at_open_rather_than_silently_emitting_json() {
        let err = Encoder::new(Encoding::Avro).unwrap_err().to_string();
        assert!(err.contains("avro"), "{err}");
    }
}
