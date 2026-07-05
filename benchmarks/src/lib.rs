//! Benchmark harnesses for the `etl-rs` framework.
//!
//! System-level benchmarks are release binaries (not `cargo bench` targets):
//! `kafka_topology` (raw rdkafka consumer-topology A/B), `pipeline_synthetic`
//! (framework overhead with no broker in the loop), `e2e_kafka_clickhouse`
//! (full pipeline against local containers or external clusters), and
//! `loadgen` (Confluent-framed Avro producer). Results are recorded in
//! `docs/BENCHMARKS.md`.
