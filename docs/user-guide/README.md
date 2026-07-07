# etl-rs User Guide

etl-rs is a high-performance, at-least-once ETL pipeline framework in Rust.
Its flagship shape is Kafka in, Avro decoding, a chain of operators you write
in Rust, and sharded ClickHouse out — but every piece behind that shape is a
small, stable trait, so you can swap in your own sources, sinks, and
deserializers. You define the operator graph in code (Flink / Java Streams
style); YAML configures connectors and tuning, never the topology.

One process runs one pipeline. CPU work (polling, deserialization, the
operator chain) runs on pinned threads with zero per-record allocations on
the hot path; I/O (sink writes, schema fetches, the admin server) runs on a
small shared tokio runtime. The design is Kubernetes-native: scrape-based
Prometheus metrics, `/healthz` and `/readyz` probes, and drain-on-SIGTERM.

## Guarantees at a glance

| Guarantee | What it means |
|---|---|
| At-least-once delivery | A source offset commits only after every record derived from it was durably written to the sink (or intentionally dropped). This holds across consumer rebalances, graceful shutdown, and crashes. See [Delivery guarantees](02-concepts/02-delivery-guarantees.md). |
| Duplicates are possible | At-least-once means replays happen. Dedup tokens make same-batch retries idempotent, but crash replay re-batches with new boundaries — design target tables to tolerate duplicates. |
| Sources never block | Backpressure pauses lanes and keeps polling; a source thread never parks in a channel send, so sink slowness never triggers a consumer-group eviction. See [Backpressure](02-concepts/03-backpressure.md). |
| Errors are Skip or Fail | Record-level error policies are Skip (count and continue) or Fail (stop the pipeline) — never silent, always surfaced through metrics. See [Error handling](02-concepts/04-error-handling.md). |
| No hidden costs on the hot path | Operator chains are fully monomorphized (one virtual call per batch, not per record); metric handles are pre-registered at build time. |

Not in scope for v1: exactly-once sinks, general DAG topologies, windowing,
dead-letter queues, and config hot-reload. The rationale lives in
[docs/DESIGN.md](../DESIGN.md) — the source of truth when code and intuition
disagree.

## Where to start

- Never used etl-rs? [Getting started](01-getting-started/README.md) has a
  five-minute in-memory pipeline and a full Kafka-to-ClickHouse tutorial.
- Want the mental model first? Read [Concepts](02-concepts/README.md).
- Building or operating a real pipeline? Jump to the
  [Guides](03-guides/README.md) and [Deployment](05-deployment/README.md).

## Guide map

### [1. Getting started](01-getting-started/README.md)

- [Installation](01-getting-started/01-installation.md) — the `etl` facade
  crate, feature flags, MSRV.
- [Quickstart](01-getting-started/02-quickstart.md) — a zero-infrastructure
  pipeline in five minutes.
- [Your first pipeline](01-getting-started/03-first-pipeline.md) — Kafka →
  Avro → operator chain → ClickHouse, end to end.

### [2. Concepts](02-concepts/README.md)

- [Architecture](02-concepts/01-architecture.md) — the anatomy of a running
  pipeline process.
- [Delivery guarantees](02-concepts/02-delivery-guarantees.md) — how
  at-least-once actually works, and its honest limits.
- [Backpressure](02-concepts/03-backpressure.md) — flow control without
  blocking the source.
- [Error handling](02-concepts/04-error-handling.md) — Skip, Fail, and the
  metrics that surface both.

### [3. Guides](03-guides/README.md)

- [Assembling a pipeline](03-guides/assembling-a-pipeline.md)
- [Configuring pipelines](03-guides/configuring-pipelines.md)
- [Testing pipelines](03-guides/testing-pipelines.md)
- [Schema validation](03-guides/schema-validation.md)
- [Graceful shutdown](03-guides/graceful-shutdown.md)
- [Manual assembly](03-guides/manual-assembly.md)

### [4. Connectors](04-connectors/README.md)

- [Kafka](04-connectors/kafka.md) · [ClickHouse](04-connectors/clickhouse.md)
  · [Avro](04-connectors/avro.md) · [Memory (testing)](04-connectors/memory.md)

### [5. Deployment](05-deployment/README.md)

- [Docker](05-deployment/docker.md) · [Monitoring](05-deployment/monitoring.md)
  · [Tuning](05-deployment/tuning.md)

### [6. Extending](06-extending/README.md)

- [Custom sources](06-extending/custom-source.md) ·
  [Custom sinks](06-extending/custom-sink.md) ·
  [Custom operators](06-extending/custom-operators.md)

### [7. Reference](07-reference/README.md)

- [Configuration](07-reference/configuration.md) ·
  [Glossary](07-reference/glossary.md)

## Related documentation

- [docs/DESIGN.md](../DESIGN.md) — the architecture and every decision's
  rationale (canonical; this guide links to it rather than restating it).
- [docs/METRICS.md](../METRICS.md) — the full metric taxonomy and alerting
  starting points.
- API reference on docs.rs — this guide teaches concepts and contracts;
  signatures and per-item docs live in the rustdoc for the `etl` crate.
