//! Turning an event into the bytes a payload borrows.
//!
//! Both encoders write **into a buffer the caller owns** rather than returning
//! one, so a lane appends event after event to a single reused arena and
//! hands out subslices of it. That is what makes a poll allocation-free once
//! the arena has grown.
//!
//! There is deliberately **no `with_encoder` seam**. Every other connector has
//! one because it transports somebody else's bytes and cannot know their
//! format; a generator authors its own payloads, so the format is a property
//! of the generator rather than a decision left to the assembly. Choosing it
//! is what `encoding:` is for.

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
            // Unreachable through configuration — `validate` rejects this
            // combination at load time — but a hand-built config reaches
            // `open` without passing through it.
            #[cfg(not(feature = "avro"))]
            Encoding::Avro => Err(SourceError::Client {
                class: ErrorClass::Fatal,
                reason: "source.datagen.encoding: avro needs spate-datagen's `avro` feature; \
                         it is off in this build"
                    .into(),
            }),
        }
    }

    /// Append `event`'s encoded bytes to `out`.
    pub(crate) fn encode(
        &self,
        event: &StorefrontEvent,
        out: &mut Vec<u8>,
    ) -> Result<(), SourceError> {
        match self {
            // Infallible in practice — the event model has no map keys, no
            // non-finite floats and no custom `Serialize` — but the writer
            // API is fallible, and swallowing that would be the wrong shape.
            Encoder::Json => serde_json::to_writer(out, event).map_err(|e| SourceError::Client {
                class: ErrorClass::Fatal,
                reason: format!("encoding a datagen event as JSON: {e}"),
            }),
            #[cfg(feature = "avro")]
            Encoder::Avro(schema) => {
                let datum = apache_avro::to_avro_datum(schema, avro_value(event)).map_err(|e| {
                    SourceError::Client {
                        class: ErrorClass::Fatal,
                        reason: format!("encoding a datagen event as an Avro datum: {e}"),
                    }
                })?;
                out.extend_from_slice(&datum);
                Ok(())
            }
        }
    }
}

/// The Avro value for `event`, as the union branch its schema declares.
///
/// Built by hand rather than through `apache_avro::to_value`: the serde bridge
/// would have to reconcile an internally-tagged Rust enum with a union of
/// records, which is a different shape, and the branch indices below are the
/// contract this crate actually publishes.
#[cfg(feature = "avro")]
fn avro_value(event: &StorefrontEvent) -> apache_avro::types::Value {
    use apache_avro::types::Value;

    let field = |name: &str, value: Value| (name.to_owned(), value);
    let (branch, record) = match event {
        StorefrontEvent::OrderPlaced(e) => (
            0,
            vec![
                field("order_id", Value::Long(e.order_id as i64)),
                field("customer_id", Value::Int(e.customer_id as i32)),
                field("region", Value::String(e.region.clone().into_owned())),
                field("placed_at", Value::TimestampMillis(e.placed_at)),
                field(
                    "lines",
                    Value::Array(
                        e.lines
                            .iter()
                            .map(|l| {
                                Value::Record(vec![
                                    field("sku", Value::String(l.sku.clone().into_owned())),
                                    field("qty", Value::Int(l.qty as i32)),
                                    field("unit_cents", Value::Int(l.unit_cents as i32)),
                                ])
                            })
                            .collect(),
                    ),
                ),
            ],
        ),
        StorefrontEvent::PaymentCaptured(e) => (
            1,
            vec![
                field("order_id", Value::Long(e.order_id as i64)),
                field("amount_cents", Value::Long(e.amount_cents as i64)),
            ],
        ),
        StorefrontEvent::RefundIssued(e) => (
            2,
            vec![
                field("order_id", Value::Long(e.order_id as i64)),
                field("amount_cents", Value::Long(e.amount_cents as i64)),
                field("reason", Value::String(e.reason.clone().into_owned())),
            ],
        ),
    };
    Value::Union(branch, Box::new(Value::Record(record)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DatagenSourceConfig;
    use crate::plan::EventPlan;

    fn events(count: usize) -> Vec<StorefrontEvent> {
        let cfg = DatagenSourceConfig {
            seed: 21,
            ..DatagenSourceConfig::default()
        };
        let mut plan = EventPlan::new(&cfg, 0);
        (0..count).map(|_| plan.next().0).collect()
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

    /// The arena stops growing its allocation once it has seen a full batch —
    /// the property that makes a steady-state poll allocation-free.
    #[test]
    fn a_reused_arena_stops_reallocating() {
        let encoder = Encoder::new(Encoding::Json).unwrap();
        let mut arena = Vec::new();
        for _ in 0..50 {
            arena.clear();
            for event in &events(256) {
                encoder.encode(event, &mut arena).unwrap();
            }
        }
        let settled = arena.capacity();
        arena.clear();
        for event in &events(256) {
            encoder.encode(event, &mut arena).unwrap();
        }
        assert_eq!(arena.capacity(), settled, "a warm arena reallocated");
    }

    /// The Avro branch is checked against `EVENT_SCHEMA_JSON` by the reference
    /// implementation, so a hand-built value that drifts from the published
    /// schema fails here rather than downstream.
    #[cfg(feature = "avro")]
    #[test]
    fn every_avro_datum_reads_back_under_the_published_schema() {
        let schema = apache_avro::Schema::parse_str(crate::EVENT_SCHEMA_JSON).unwrap();
        let encoder = Encoder::new(Encoding::Avro).unwrap();
        let mut seen = [false; 3];
        for event in events(512) {
            let mut buf = Vec::new();
            encoder.encode(&event, &mut buf).unwrap();
            let value = apache_avro::from_avro_datum(&schema, &mut buf.as_slice(), None).unwrap();
            let apache_avro::types::Value::Union(branch, inner) = value else {
                panic!("the top-level schema is a union; got {value:?}");
            };
            let apache_avro::types::Value::Record(fields) = *inner else {
                panic!("every union branch is a record");
            };
            let order_id = fields
                .iter()
                .find(|(name, _)| name == "order_id")
                .map(|(_, v)| v.clone());
            assert_eq!(
                order_id,
                Some(apache_avro::types::Value::Long(event.order_id() as i64)),
                "the decoded datum lost its order id"
            );
            seen[branch as usize] = true;
        }
        assert!(seen.iter().all(|&s| s), "some union branch went untested");
    }

    #[cfg(not(feature = "avro"))]
    #[test]
    fn avro_without_the_feature_fails_at_open_rather_than_silently_emitting_json() {
        let err = Encoder::new(Encoding::Avro).unwrap_err().to_string();
        assert!(err.contains("avro"), "{err}");
    }
}
