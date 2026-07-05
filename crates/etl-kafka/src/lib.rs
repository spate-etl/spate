//! Kafka source for the `etl-rs` framework.
//!
//! Built on `rdkafka` with a **single consumer per process**: partitions
//! are split into per-partition queues (`split_partition_queue`) and fanned
//! across pipeline threads as [`SourceLane`](etl_core::source::SourceLane)s,
//! keeping payload borrows local to the polling thread with zero copies.
//! Offsets are stored when checkpoint watermarks advance and committed on
//! an interval; rebalances and shutdown share the framework's drain
//! choreography, with completion deferred until draining and a synchronous
//! commit have finished (see [`KafkaSource`] docs).
//!
//! One topic per pipeline: the framework's `PartitionId` is the Kafka
//! partition number. Kafka tombstones (null payloads) surface as empty
//! payload slices.
//!
//! Configuration is read from the pipeline's opaque `source: { kafka: ... }`
//! section — see [`KafkaSourceConfig`] for the schema and the raw
//! `rdkafka:` passthrough (framework-owned properties are validated and
//! rejected with explanations).
//!
//! ```yaml
//! source:
//!   kafka:
//!     brokers: ${KAFKA_BROKERS:-localhost:9092}
//!     topic: orders
//!     group_id: orders-etl
//!     commit_interval: 5s
//!     rdkafka:
//!       fetch.message.max.bytes: "1048576"
//! ```
//!
//! This crate deliberately re-exports nothing from `rdkafka`: its types
//! stay out of public signatures so `rdkafka` major bumps are not breaking
//! changes here (see `docs/DESIGN.md` § Dependency policy).

mod config;
mod context;
mod lane;
mod source;

pub use config::KafkaSourceConfig;
pub use lane::{KafkaBatch, KafkaLane};
pub use source::KafkaSource;
