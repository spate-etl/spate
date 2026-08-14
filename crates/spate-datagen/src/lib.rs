//! Synthetic commerce-event source for Spate, giving a pipeline you can run
//! with nothing installed.
//!
//! Every other source in this workspace needs infrastructure before it says
//! anything: a broker, a bucket, a coordination store. That prerequisite is
//! the first thing between a reader and a running pipeline, and it is the only
//! thing this crate removes. Point a pipeline at a [`DatagenSource`] and it
//! produces a stream of storefront events (orders, their payments, and
//! refunds against those payments) on as many partitions as you ask for, at a
//! rate you set, for as long as you want.
//!
//! ```yaml
//! source:
//!   datagen:
//!     partitions: 4
//!     events_per_tick: 10
//!     tick_interval: 100ms   # 400 events/s in total
//!     count: 10000           # omit for an unbounded stream
//! ```
//!
//! # A dataset, not a schema language
//!
//! `datagen` generates **one built-in, named dataset**. There is no `fields:`
//! map, and adding one is out of scope rather than unimplemented.
//!
//! The reason is the thing that makes the stream worth generating. A payment
//! must name an order that was really placed, for an amount that matches its
//! lines, on the same partition, at a later offset. That is a property
//! of the whole dataset, not of any field in it. A field-wise schema can say
//! "a `u64` here"; it cannot say "this `u64`, drawn from the ids the same lane
//! minted earlier and not yet drawn". A named dataset gets the property for
//! free and stays forty lines of configuration.
//!
//! Datasets are enumerated by [`Dataset`]; the storefront model lives in
//! [`storefront`].
//!
//! # Referential integrity without coordination
//!
//! Each lane owns a **disjoint** slice of the order-id space
//! (`order_id = n × partitions + lane_index`) and keeps its own bounded ring
//! of the orders it has placed and captured. A payment or a refund is drawn
//! from that ring, so it always references an order:
//!
//! - the same lane minted, and therefore the **same partition**;
//! - at a **strictly greater offset**, because the lane released the order
//!   first.
//!
//! A payment settles the order's line total exactly. A refund is that total,
//! or its half, third or quarter rounded down, never more than was captured,
//! which is what a balance check downstream rests on.
//!
//! The mix places faster than it captures, so orders placed and never paid
//! accumulate for as long as the pipeline runs; `open_orders` reports how
//! many. A check reconciles the orders that were captured, not all of them.
//!
//! No lane reads another lane's state, so nothing is shared on the record
//! path and the whole property survives the CPU-pinned fan-out. The payload
//! key carries the order id, so
//! [`KeyHashRouter`](spate_core::sink::KeyHashRouter) colocates an order and
//! its payment in one sink shard.
//!
//! # Delivery, stated plainly
//!
//! **This is a demo and test source. Do not build a production pipeline on
//! it.**
//!
//! - `commit()` stores watermarks **in memory and nowhere else**. They are
//!   readable through [`DatagenSource::committed`], published as
//!   `committed_offset` when `metrics.per_partition_detail` is on, and gone
//!   when the process exits.
//! - The source claims **no resumability**. A restart begins every lane at
//!   offset 0, so with a fixed seed the entire stream is regenerated from the
//!   beginning, which is strictly *more* duplication than a real
//!   at-least-once source, which would replay only from its last committed
//!   position.
//! - A `resume_from:` file is **declined**. A demo source that appears to
//!   resume durably gets built on, and the failure surfaces as silent data
//!   loss in a deployment that never meant to take one.
//!
//! Opening the source logs a `WARN` saying so, once, on the same principle.
//!
//! # Metrics
//!
//! Families under `spate_datagen_source_*`: `events_generated_total{event}`,
//! `ticks_total` and `tick_overrun_total` are counted by the lanes;
//! `events_remaining`, `open_orders` and (with `metrics.per_partition_detail`)
//! `committed_offset{partition}` are published by the control plane.
//!
//! There is deliberately no `spate_source_lag_records`. For an unbounded
//! generator the lag is infinite, so the series would exist or not depending
//! on whether `count` was set.

mod config;
mod dims;
mod encode;
mod events;
mod lane;
mod metrics;
mod plan;
mod rng;
mod source;

pub use config::{Clock, DatagenSourceConfig, Dataset, Encoding};
pub use dims::{CUSTOMERS, REGIONS, SKUS};
pub use events::{
    EVENT_SCHEMA_JSON, OrderLine, OrderPlaced, PaymentCaptured, RefundIssued, StorefrontEvent,
};
pub use lane::{DatagenBatch, DatagenLane};
pub use source::DatagenSource;

/// The storefront dataset's event model, under the name a pipeline assembly
/// reads best:
///
/// ```
/// use spate_datagen::storefront::{OrderLine, OrderPlaced, PaymentCaptured, RefundIssued};
/// ```
///
/// The same types are re-exported at the crate root; this module is the
/// spelling to prefer when a file also imports the source and its
/// configuration, because it says which dataset the events belong to.
pub mod storefront {
    pub use crate::events::{
        OrderLine, OrderPlaced, PaymentCaptured, RefundIssued, StorefrontEvent,
    };
}
