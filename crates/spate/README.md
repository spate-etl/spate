# spate

The front door to the [Spate](https://spate.kainth.dev/) framework: a
high-performance, at-least-once ETL pipeline library for Rust.

## Overview

You write the operator graph in Rust. YAML configures the connectors and the
tuning.

- **At-least-once delivery.** A source commits its position only after every
  record derived from it is durably written to the sink or deliberately dropped.
- **Duplicates are possible.** A crash replays records, so target tables have to
  tolerate duplicates.
- **Sources never block.** Backpressure pauses lanes and the source keeps
  polling, so a slow sink does not cost the source its assignment.
- **Errors are Skip or Fail.** A record that fails is skipped or stops the
  pipeline. A chain built with `with_metrics` counts both outcomes.
- **No hidden costs on the hot path.** Operator chains are monomorphized, and
  metric handles are registered when the pipeline is built.

## Install

```toml
[dependencies]
spate = { version = "0.4", features = ["kafka", "clickhouse", "avro"] }
```

An application depends on this one crate. It re-exports the engine
(`spate-core`) at the root and each connector behind a feature. Nothing is on by
default. A pipeline that only writes to ClickHouse never compiles the Kafka
connector and never resolves `rdkafka` into its lockfile.

## Connectors by role

| Role | Connector | Feature | Crate |
| --- | --- | --- | --- |
| Source | [Kafka](https://spate.kainth.dev/docs/user-guide/connectors/sources/kafka) | `kafka` | [`spate-kafka`](https://crates.io/crates/spate-kafka) |
| Source | [S3 (object storage)](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3) | `s3` | [`spate-s3`](https://crates.io/crates/spate-s3) |
| Source | [Datagen](https://spate.kainth.dev/docs/user-guide/connectors/sources/datagen) | `datagen` | [`spate-datagen`](https://crates.io/crates/spate-datagen) |
| Sink | [ClickHouse](https://spate.kainth.dev/docs/user-guide/connectors/sinks/clickhouse) | `clickhouse` | [`spate-clickhouse`](https://crates.io/crates/spate-clickhouse) |
| Sink | [Kafka](https://spate.kainth.dev/docs/user-guide/connectors/sinks/kafka) | `kafka` | [`spate-kafka`](https://crates.io/crates/spate-kafka) |
| Format | [Avro](https://spate.kainth.dev/docs/user-guide/connectors/formats/avro) | `avro` | [`spate-avro`](https://crates.io/crates/spate-avro) |
| Format | [JSON](https://spate.kainth.dev/docs/user-guide/connectors/formats/json) | `json` | [`spate-json`](https://crates.io/crates/spate-json) |
| Feed | [Amazon SQS](https://spate.kainth.dev/docs/user-guide/connectors/feeds/sqs) | `sqs` | [`spate-sqs`](https://crates.io/crates/spate-sqs) |
| Feed | [Kafka](https://spate.kainth.dev/docs/user-guide/connectors/feeds/kafka) | `kafka` | [`spate-kafka`](https://crates.io/crates/spate-kafka) |

A feed carries work units to a source. The S3 source reads its work from a feed,
and a pipeline with an S3 source and no `feed:` section fails to start. The
datagen source generates synthetic storefront events with no infrastructure
behind it, and keeps no durable progress. `datagen-avro` lets the source emit
Avro payloads, and implies `datagen`.

Other features refine one connector, such as `kafka-tls` for TLS and SASL to
Kafka. The [API reference on docs.rs](https://docs.rs/spate) lists every feature
with what it enables.

## Quick start

[`memory_pipeline`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/memory_pipeline.rs)
builds a whole pipeline in memory, drives records through it and checks what the
sink wrote. It needs no infrastructure and no features. Run it from a clone of
the repository:

```sh
git clone https://github.com/spate-etl/spate
cd spate
cargo run -p spate --example memory_pipeline
```

It prints the pipeline's exit state, the final watermarks and the rows the sink
wrote. The
[quickstart](https://spate.kainth.dev/docs/user-guide/getting-started/quickstart)
walks through the same program step by step.

## Delivery semantics

Delivery is at-least-once. A source commits its position only after every record
derived from it is durably written to the sink or deliberately dropped. This
holds across rebalances, graceful shutdown and crashes. [INV-1 in Spate
invariants](https://spate.kainth.dev/docs/INVARIANTS#inv-1) states the property
and its exceptions.

Records can arrive more than once after a crash. Each sealed batch carries a
dedup token that stays the same across retries of that batch, so a destination
with insert deduplication can drop a retried batch. A restart replays records in
new batches with new tokens, and those rows land twice. Design target tables to
tolerate duplicates. The
[ClickHouse sink page](https://spate.kainth.dev/docs/user-guide/connectors/sinks/clickhouse#deduplication-tokens-and-the-window-warning)
covers the deduplication window and `ReplacingMergeTree`.

A record that fails is skipped or stops the pipeline. On a chain built with
`with_metrics`, no policy drops a record without counting it. [Error
handling](https://spate.kainth.dev/docs/user-guide/concepts/error-handling)
lists the metrics that count each outcome.

[Delivery guarantees](https://spate.kainth.dev/docs/user-guide/concepts/delivery-guarantees)
explains the mechanism, and
[Guarantees at a glance](https://spate.kainth.dev/docs/user-guide/#guarantees-at-a-glance)
lists the framework's guarantees in one table.

## Minimum supported Rust version

Rust 1.96.
[Installation](https://spate.kainth.dev/docs/user-guide/getting-started/installation#toolchain-requirements)
states the policy and what the `kafka` and `kafka-tls` features need on the
build host.

## Documentation and examples

- [User guide](https://spate.kainth.dev/docs/user-guide/) — concepts, how-to
  guides and a page for each connector.
- [API reference on docs.rs](https://docs.rs/spate) — every type, built with all
  features on.
- [Examples index](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/README.md)
  — every example by what it shows, with the features and infrastructure each
  one needs.
- [`spate-test`](https://crates.io/crates/spate-test) — the in-memory source and
  sink for testing a pipeline with no infrastructure.
- [Changelog](https://github.com/spate-etl/spate/blob/main/CHANGELOG.md) — what
  changed in each release.

## License

Apache-2.0. The [license
text](https://github.com/spate-etl/spate/blob/main/LICENSE) is in the
repository.
