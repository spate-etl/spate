# etl-rs

A high-performance, at-least-once ETL pipeline framework for Rust.

`etl-rs` provides the abstractions for building streaming Extract-Transform-Load
pipelines with a chaining operator API (in the spirit of Flink or Java Streams),
CPU-pinned processing threads, zero-copy deserialization, checkpoint-driven
source commits, sharded and replicated asynchronous sinks, built-in
backpressure, and first-class Prometheus metrics.

## Crates

| Crate | Description |
|---|---|
| `etl` | The facade crate — the only crate applications depend on. Enables connectors via features (`kafka`, `clickhouse`, `avro`, `full`). |
| `etl-core` | The engine: records, operator chains, source/sink abstractions, checkpointing, backpressure, config, metrics, pipeline runtime. |
| `etl-kafka` | Kafka source built on `rdkafka` (single consumer per process, partition queues fanned across pipeline threads). |
| `etl-clickhouse` | ClickHouse sink built on the official `clickhouse` crate (direct-to-shard writes, replica rotation, deduplication tokens). |
| `etl-avro` | Avro deserialization (Confluent wire format, schema registry integration). |
| `etl-test` | Public testing utilities: in-memory sources/sinks with scripting handles for testing your pipelines. |

## Status

Under active initial development — APIs are not yet stable.

## Delivery semantics

At-least-once. Sinks acknowledge batches only after durable writes; sources
commit the contiguous acknowledged watermark on a configurable interval.
After a crash, uncommitted records are replayed — downstream tables should be
designed to tolerate duplicates (e.g. ClickHouse `ReplacingMergeTree`).

## License

MIT OR Apache-2.0
