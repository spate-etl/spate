//! The storefront event model: what a generated record *is*.
//!
//! Three events over one entity. An order is placed, its payment is captured,
//! and some of those payments are refunded, so a pipeline built on this
//! stream has a join to make, a sum to check, and a late-arriving reference to
//! handle.
//!
//! # Why the string fields are `Cow<'static, str>`
//!
//! Generation borrows: a region or a SKU is an entry in [`crate::dims`], so
//! producing an event copies no bytes. Consumption owns: an example decodes
//! these types back out of JSON with `build_serde::<StorefrontEvent>()`, and
//! `&'static str` has no `Deserialize` impl to decode *into*. `Cow` is the one
//! spelling that serves both, with `Cow::Borrowed` on the way out and
//! `Cow::Owned` on the way back in. `PartialEq` compares the strings either way, so a
//! round-trip test can assert equality against the value that was generated.

use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// The Avro schema of a [`StorefrontEvent`], as JSON.
///
/// A top-level union of the three record types, the idiomatic Avro spelling
/// of a sum type, and what `encoding: avro` writes a bare datum against. The
/// JSON encoding tags the same three shapes with a `type` field instead;
/// neither derives from the other, so this constant is what a downstream
/// reader is pinned to.
pub const EVENT_SCHEMA_JSON: &str = r#"[
  {
    "type": "record",
    "name": "OrderPlaced",
    "namespace": "spate.datagen",
    "fields": [
      {"name": "order_id", "type": "long"},
      {"name": "customer_id", "type": "int"},
      {"name": "region", "type": "string"},
      {"name": "placed_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
      {"name": "lines", "type": {"type": "array", "items": {
        "type": "record",
        "name": "OrderLine",
        "fields": [
          {"name": "sku", "type": "string"},
          {"name": "qty", "type": "int"},
          {"name": "unit_cents", "type": "int"}
        ]
      }}}
    ]
  },
  {
    "type": "record",
    "name": "PaymentCaptured",
    "namespace": "spate.datagen",
    "fields": [
      {"name": "order_id", "type": "long"},
      {"name": "amount_cents", "type": "long"}
    ]
  },
  {
    "type": "record",
    "name": "RefundIssued",
    "namespace": "spate.datagen",
    "fields": [
      {"name": "order_id", "type": "long"},
      {"name": "amount_cents", "type": "long"},
      {"name": "reason", "type": "string"}
    ]
  }
]"#;

/// One event in the storefront stream, tagged by `type` in the encoded
/// payload (`order_placed`, `payment_captured`, `refund_issued`).
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum StorefrontEvent {
    /// A customer placed an order.
    OrderPlaced(OrderPlaced),
    /// The payment for an order that was already placed came through.
    PaymentCaptured(PaymentCaptured),
    /// Part or all of a captured payment was refunded.
    RefundIssued(RefundIssued),
}

impl StorefrontEvent {
    /// The order this event is about. Every event in the stream carries one,
    /// which is what makes the whole stream partitionable by order.
    #[must_use]
    pub fn order_id(&self) -> u64 {
        match self {
            StorefrontEvent::OrderPlaced(e) => e.order_id,
            StorefrontEvent::PaymentCaptured(e) => e.order_id,
            StorefrontEvent::RefundIssued(e) => e.order_id,
        }
    }
}

/// A new order, with between one and five lines.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct OrderPlaced {
    /// Unique within the whole stream: lanes own disjoint id slices, so no
    /// two lanes can ever mint the same order.
    pub order_id: u64,
    /// Which customer placed it, in `0..`[`CUSTOMERS`](crate::CUSTOMERS).
    pub customer_id: u32,
    /// One of [`REGIONS`](crate::REGIONS).
    pub region: Cow<'static, str>,
    /// Event time, milliseconds since the Unix epoch.
    pub placed_at: i64,
    /// One to five lines; the order's total is their `qty × unit_cents`.
    pub lines: Vec<OrderLine>,
}

/// One line of an order.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct OrderLine {
    /// One of [`SKUS`](crate::SKUS).
    pub sku: Cow<'static, str>,
    /// Units ordered, at least one.
    pub qty: u32,
    /// List price of a single unit, in cents.
    pub unit_cents: u32,
}

/// The payment for an order, always preceded in the same partition by the
/// [`OrderPlaced`] it names.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct PaymentCaptured {
    /// The order being paid for.
    pub order_id: u64,
    /// The order's line total. A downstream sum over [`OrderPlaced::lines`]
    /// must reproduce it.
    pub amount_cents: u64,
}

/// A refund against a payment that was already captured.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct RefundIssued {
    /// The order being refunded.
    pub order_id: u64,
    /// Never more than the captured amount; often a partial refund.
    pub amount_cents: u64,
    /// A short, bounded-cardinality reason string.
    pub reason: Cow<'static, str>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn placed() -> StorefrontEvent {
        StorefrontEvent::OrderPlaced(OrderPlaced {
            order_id: 12,
            customer_id: 7,
            region: Cow::Borrowed("eu-west"),
            placed_at: 1_767_225_600_000,
            lines: vec![OrderLine {
                sku: Cow::Borrowed("KBD-01"),
                qty: 2,
                unit_cents: 7_900,
            }],
        })
    }

    /// The encoded shape is a contract: a demo pipeline's YAML, a ClickHouse
    /// column list and a docs page all name these keys.
    #[test]
    fn the_json_encoding_is_internally_tagged_snake_case() {
        let json = serde_json::to_string(&placed()).unwrap();
        assert!(
            json.starts_with(r#"{"type":"order_placed","order_id":12"#),
            "{json}"
        );

        let refund = StorefrontEvent::RefundIssued(RefundIssued {
            order_id: 12,
            amount_cents: 500,
            reason: Cow::Borrowed("damaged"),
        });
        assert_eq!(
            serde_json::to_string(&refund).unwrap(),
            r#"{"type":"refund_issued","order_id":12,"amount_cents":500,"reason":"damaged"}"#
        );
    }

    /// What an example does: decode the generated bytes back into these types.
    /// Borrowed-out, owned-back-in has to compare equal, or the `Cow` choice
    /// documented above would be buying nothing.
    #[test]
    fn every_variant_round_trips_through_json() {
        for event in [
            placed(),
            StorefrontEvent::PaymentCaptured(PaymentCaptured {
                order_id: 12,
                amount_cents: 15_800,
            }),
            StorefrontEvent::RefundIssued(RefundIssued {
                order_id: 12,
                amount_cents: 500,
                reason: Cow::Borrowed("damaged"),
            }),
        ] {
            let bytes = serde_json::to_vec(&event).unwrap();
            let back: StorefrontEvent = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(back, event, "round trip changed the value");
            assert_eq!(back.order_id(), 12);
        }
    }

    #[test]
    fn an_unknown_tag_is_a_decode_error_rather_than_a_silent_drop() {
        let err = serde_json::from_str::<StorefrontEvent>(r#"{"type":"order_shipped"}"#)
            .expect_err("an unmodelled event type must not decode");
        assert!(err.to_string().contains("order_shipped"), "{err}");
    }
}
