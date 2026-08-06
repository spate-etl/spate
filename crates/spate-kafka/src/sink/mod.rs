//! Kafka producer sink: pipelines terminate in a Kafka topic.
//!
//! The framework owns batching, retry, backpressure, and drain (see
//! `docs/adr/0006-per-shard-sink-workers.md`); this connector supplies the two halves of the
//! seam:
//!
//! - **CPU half** ([`KafkaEncoder`] over a [`MessageEncoder`]): on pipeline
//!   threads, each record becomes one Kafka message — key, headers,
//!   payload — appended to the chunk frame in a connector-internal
//!   length-delimited format.
//! - **I/O half** (the sink writer): a shard worker hands the writer a
//!   sealed batch; the writer parses the messages back out, produces them,
//!   and returns only after **every** delivery report confirmed — which is
//!   what lets the framework treat `write_batch → Ok` as the durable-ack
//!   point and advance source watermarks safely.
//!
//! One topic per sink instance: fan-out to multiple topics is the chain's
//! `split` terminal with one Kafka sink per topic. Delivery is
//! **at-least-once, honestly**: `enable.idempotence` (forced on) dedupes
//! only librdkafka's own in-session retries; a framework batch retry after
//! partial delivery re-produces the already-delivered prefix, and crash
//! replay re-produces re-batched data. Nothing here is exactly-once.

mod config;
mod context;
mod encoder;
pub(crate) mod frame;
mod metrics;
mod writer;

pub use config::{Compression, KafkaSink, KafkaSinkConfig, build, from_component_config};
pub use encoder::{
    BytesKeyFn, KafkaBytesEncoder, KafkaEncoder, KafkaJsonEncoder, KafkaMessage, KeyFn,
    MessageEncoder,
};
pub use writer::{KafkaEndpoint, KafkaWriter};
