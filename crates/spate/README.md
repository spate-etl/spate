# spate

The front door to the [Spate](https://github.com/spate-etl/spate) framework:
a high-performance, at-least-once ETL pipeline library for Rust. You write the
operator graph in Rust and it chains into a single monomorphized loop over
zero-copy borrowed records on pinned processing threads, with checkpoint-driven
source commits, sharded asynchronous sinks, backpressure, and first-class
Prometheus metrics.

```toml
[dependencies]
spate = { version = "0.1", features = ["full"] }
```

This crate re-exports the engine and every connector behind a feature flag, so
an application depends on one crate rather than eight. Nothing is on by
default — a pipeline that only writes to ClickHouse never compiles the Kafka
tree, and never resolves `rdkafka` into its lockfile.

| Feature | Brings in |
| --- | --- |
| `kafka` | Kafka source and sink ([`spate-kafka`](https://crates.io/crates/spate-kafka)) |
| `clickhouse` | ClickHouse sink ([`spate-clickhouse`](https://crates.io/crates/spate-clickhouse)) |
| `s3` | Coordinated object-storage backfill source ([`spate-s3`](https://crates.io/crates/spate-s3)) |
| `avro` | Avro deserialization ([`spate-avro`](https://crates.io/crates/spate-avro)) |
| `json` | JSON deserialization ([`spate-json`](https://crates.io/crates/spate-json)) |
| `coordination` | Multi-instance work assignment, in-memory store |
| `coordination-nats` | The same over NATS JetStream KV |
| `datagen` | Synthetic storefront source, no infrastructure ([`spate-datagen`](https://crates.io/crates/spate-datagen)) — keeps no durable progress |
| `datagen-avro` | Avro payloads from that generator instead of JSON (implies `datagen`) |
| `full` | Every connector above except `datagen`, which a production build should not enable |

`examples/README.md` indexes every example by what it shows, in five tiers,
with what each one needs in order to run. Start with
`examples/memory_pipeline.rs`, which runs without any infrastructure;
`examples/kafka_avro_to_clickhouse.rs` is the flagship assembly and ships with
its YAML alongside it.

## Delivery semantics

At-least-once, and the framework is built around not quietly weakening that: a
source watermark is never committed past unacknowledged data, including across
rebalances and shutdown. Records that fail are either skipped or fail the
pipeline — there is no third policy that silently drops them — and both
outcomes are counted in metrics rather than only logged.

Duplicates are therefore possible after a crash. Sinks that need effectively
once should be idempotent or deduplicating; the ClickHouse guide covers how to
get there with `ReplacingMergeTree` and with insert deduplication.

Full documentation is at [spate.kainth.dev](https://spate.kainth.dev/).
