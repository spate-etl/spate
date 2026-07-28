# Spate

[![crates.io](https://img.shields.io/crates/v/spate.svg)](https://crates.io/crates/spate)
[![docs.rs](https://img.shields.io/docsrs/spate)](https://docs.rs/spate)
[![CI](https://github.com/spate-etl/spate/actions/workflows/ci.yml/badge.svg)](https://github.com/spate-etl/spate/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/spate-etl/spate/branch/main/graph/badge.svg)](https://codecov.io/gh/spate-etl/spate)
[![Documentation](https://img.shields.io/badge/docs-spate.kainth.dev-e8590c)](https://spate.kainth.dev/)
[![MSRV](https://img.shields.io/badge/MSRV-1.94-blue.svg)](https://blog.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/spate-etl/spate/badge)](https://scorecard.dev/viewer/?uri=github.com/spate-etl/spate)

A high-performance, at-least-once ETL pipeline framework for Rust.

Spate provides the abstractions for streaming Extract-Transform-Load pipelines:
an operator graph you write in Rust and chain into a single monomorphized loop,
CPU-pinned processing threads over zero-copy borrowed records, checkpoint-driven
source commits, sharded and replicated asynchronous sinks, built-in
backpressure, and first-class Prometheus metrics — measured at **~9 ns/record
with zero per-record allocations** through a realistic operator chain (see
[docs/benchmarks/](docs/benchmarks/)).

```toml
[dependencies]
spate = { version = "0.1", features = ["kafka", "clickhouse", "avro"] }
```

Nothing is enabled by default. A pipeline that only writes to ClickHouse never
compiles the Kafka tree and never resolves `rdkafka` into its lockfile.

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

Start at [`crates/spate/examples`](crates/spate/examples): `memory_pipeline`
runs with zero infrastructure (`cargo run -p spate --example
memory_pipeline`); `kafka_avro_to_clickhouse` is the fully-commented
production assembly; `custom_source_sink` is the connector-author
tutorial. [`examples/docker`](examples/docker) covers containers and
Kubernetes (probes, drain timeouts, sizing).

## Crates

| Crate | Feature | Description |
|---|---|---|
| [`spate`](https://crates.io/crates/spate) | — | The facade — the only crate applications depend on. |
| [`spate-core`](https://crates.io/crates/spate-core) | — | The engine: records and acknowledgements, operator chains, source/sink abstractions, checkpointing, backpressure, config, metrics, the pipeline runtime. |
| [`spate-kafka`](https://crates.io/crates/spate-kafka) | `kafka` | Kafka source and sink on `rdkafka`: one consumer per process, partitions fanned across pipeline threads as zero-copy lanes. |
| [`spate-clickhouse`](https://crates.io/crates/spate-clickhouse) | `clickhouse` | ClickHouse sink: Native or RowBinary encoded on pipeline threads, one deduplication-tokened `INSERT` per batch, replica rotation. |
| [`spate-s3`](https://crates.io/crates/spate-s3) | `s3` | Coordinated object-storage backfill source: a leader plans a prefix into splits, workers lease them with fenced progress. |
| [`spate-avro`](https://crates.io/crates/spate-avro) | `avro` | Avro deserialization: Confluent wire format, async schema-registry fetching that never blocks a pipeline thread. |
| [`spate-json`](https://crates.io/crates/spate-json) | `json` | JSON deserialization: single, NDJSON and array framings, with an optional SIMD backend. |
| [`spate-coordination`](https://crates.io/crates/spate-coordination) | `coordination`, `coordination-nats` | Multi-instance work assignment: leader-computed sticky assignment over a pluggable store. |
| [`spate-test`](https://crates.io/crates/spate-test) | — | In-memory sources and sinks with scripting handles — test your pipelines without infrastructure. |

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
is published at **<https://spate.kainth.dev/>** (source in
[`website/`](website), content in [`docs/`](docs)).

- [docs/DESIGN.md](docs/DESIGN.md) — architecture and the decision log.
- [docs/METRICS.md](docs/METRICS.md) — every metric, its labels, and
  alerting starting points.
- [docs/benchmarks/](docs/benchmarks/) — methodology and measured
  results, including the consumer-topology A/B that shaped the Kafka
  connector.
- [examples/docker](examples/docker) — containers and Kubernetes.

## Status

Under active initial development — APIs are not yet stable (0.x). Breaking
changes ship in a minor bump and are called out in
[CHANGELOG.md](CHANGELOG.md). The newest `0.x` minor is the supported one.

## Contributing

The most useful contribution is one that proves a delivery guarantee wrong.
[CONTRIBUTING.md](CONTRIBUTING.md) has the invariants, the gates, and how
changes land; the [Code of Conduct](CODE_OF_CONDUCT.md) applies throughout.

Vulnerabilities go through
[GitHub's private advisory flow](https://github.com/spate-etl/spate/security/advisories/new),
never a public issue — see [SECURITY.md](SECURITY.md).

## License

Copyright 2026 Marcus Kainth.

Licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE).

Dependency licenses are inventoried in [THIRD-PARTY.md](THIRD-PARTY.md); the full
texts are published at
[spate.kainth.dev/licenses](https://spate.kainth.dev/licenses/).

Contributions are accepted under the same terms, per Apache-2.0 §5 — there is no
CLA to sign.
