# etl-rs

[![Documentation](https://img.shields.io/badge/docs-etl--rs.pages.kainth.net-e8590c)](https://etl-rs.pages.kainth.net/)

A high-performance, at-least-once ETL pipeline framework for Rust.

`etl-rs` provides the abstractions for streaming Extract-Transform-Load
pipelines with a chaining operator API in the spirit of Flink or Java
Streams: CPU-pinned processing threads over zero-copy borrowed records,
checkpoint-driven source commits, sharded and replicated asynchronous
sinks, built-in backpressure, and first-class Prometheus metrics —
measured at **~9 ns/record with zero per-record allocations** through a
realistic operator chain (see [docs/benchmarks/](docs/benchmarks/)).

```toml
[dependencies]
etl = { version = "0.1", features = ["kafka", "clickhouse", "avro"] }
```

## A taste

Operators are stateful closures composed into one monomorphized loop; YAML
carries the tuning and connector configuration:

```rust,ignore
let chains = move |_thread| {
    chain_owned::<Order, _>(avro.clone())
        .with_metrics("orders", "main")
        .try_map(validate, ErrorPolicy::Skip)
        .map(enrich)
        .sink(ClickHouseEncoder::new(), KeyHashRouter,
              ChunkConfig::default(), queues.clone(), budget.clone())
        .build()
};
PipelineRuntime::new(config, kafka_source, chains, sink, budget).run()?;
```

Start at [`crates/etl/examples`](crates/etl/examples): `memory_pipeline`
runs with zero infrastructure (`cargo run -p etl --example
memory_pipeline`); `kafka_avro_to_clickhouse` is the fully-commented
production assembly; `custom_source_sink` is the connector-author
tutorial. [`examples/docker`](examples/docker) covers containers and
Kubernetes (probes, drain timeouts, sizing).

## Crates

| Crate | Description |
|---|---|
| `etl` | The facade — the only crate applications depend on. Features: `kafka`, `clickhouse`, `avro`, `full`. |
| `etl-core` | The engine: records and acknowledgements, operator chains, source/sink abstractions, checkpointing, backpressure, config, metrics, the pipeline runtime. |
| `etl-kafka` | Kafka source on `rdkafka`: one consumer per process, partitions fanned across pipeline threads as zero-copy lanes. |
| `etl-clickhouse` | ClickHouse sink: RowBinary encoded on pipeline threads, one deduplication-tokened `INSERT` per batch, replica rotation. |
| `etl-avro` | Avro deserialization: Confluent wire format, async schema-registry fetching that never blocks a pipeline thread. |
| `etl-test` | In-memory sources/sinks with scripting handles — test your pipelines without infrastructure. |

## Delivery semantics, honestly

At-least-once. A batch's offsets commit only after every record derived
from it is durably written (or intentionally dropped by `filter`/`Skip`
policies) — enforced across rebalances, shutdown, and failure, where the
watermark stalls rather than ever committing past unacknowledged data.
Duplicates remain possible: in-session retries are idempotent where sinks
support it (ClickHouse deduplication tokens), but **crash replay re-batches
with new boundaries and will land rows twice** — design target tables to
tolerate that (`ReplacingMergeTree` with a version column is the sanctioned
ClickHouse pattern).

## Documentation

The full documentation site — the user guide plus the generated API reference —
is published at **<https://etl-rs.pages.kainth.net/>** (source in
[`website/`](website), content in [`docs/`](docs)).

- [docs/DESIGN.md](docs/DESIGN.md) — architecture and the decision log.
- [docs/METRICS.md](docs/METRICS.md) — every metric, its labels, and
  alerting starting points.
- [docs/benchmarks/](docs/benchmarks/) — methodology and measured
  results, including the consumer-topology A/B that shaped the Kafka
  connector.
- [examples/docker](examples/docker) — containers and Kubernetes.

## Status

Under active initial development — APIs are not yet stable (0.x).

## License

MIT OR Apache-2.0.
