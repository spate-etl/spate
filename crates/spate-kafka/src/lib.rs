//! Kafka source and producer sink for the Spate framework.
//!
//! # Source
//!
//! Built on `rdkafka` with a **single consumer per process**: partitions
//! are split into per-partition queues (`split_partition_queue`) and fanned
//! across pipeline threads as [`SourceLane`](spate_core::source::SourceLane)s,
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
//! Observability: besides the framework's `spate_source_*` stage metrics, the
//! source translates librdkafka's statistics snapshot (emitted every
//! `statistics_interval`, default 5s) into connector-owned
//! `spate_kafka_source_*` families — transport totals, per-broker health and
//! round-trip time, client-side queue saturation, and consumer-group
//! stability. [The metrics reference] carries the full table under Kafka
//! source; setting `statistics_interval: 0s` disables the families.
//!
//! # Sink
//!
//! The [`sink`] module is the producer half: pipelines terminate in a
//! Kafka topic through the framework's sink seam, with a batch
//! acknowledged only once **every** message's delivery report confirmed —
//! see the module docs for topology, delivery semantics, and the
//! `sink: { kafka: ... }` configuration schema.
//!
//! This crate deliberately re-exports nothing from `rdkafka`: its types
//! stay out of public signatures so `rdkafka` major bumps are not breaking
//! changes here (INV-6; ADR-0011).
//!
//! [the metrics reference]: https://spate.kainth.dev/docs/METRICS

mod config;
mod context;
mod error;
mod lane;
mod metrics;
mod security;
pub mod sink;
mod source;

pub use config::KafkaSourceConfig;
pub use lane::{KafkaBatch, KafkaLane};
pub use sink::{
    KafkaBytesEncoder, KafkaEncoder, KafkaJsonEncoder, KafkaMessage, KafkaSink, KafkaSinkConfig,
    KafkaWriter, MessageEncoder,
};
pub use source::KafkaSource;
