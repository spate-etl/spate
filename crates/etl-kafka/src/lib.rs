//! Kafka source for the `etl-rs` framework.
//!
//! Built on `rdkafka` with a single consumer per process: partitions are
//! split into per-partition queues (`split_partition_queue`) and fanned
//! across pipeline threads, keeping payload borrows local to the polling
//! thread. Offsets are stored on checkpoint watermark advance and committed
//! on an interval; rebalances and shutdown share one drain choreography.
