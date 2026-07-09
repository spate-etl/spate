# Reference

Where to look things up. This guide teaches concepts and contracts; the
sources of truth it defers to are listed here.

## In this section

- [Configuration](configuration.md) — the full YAML schema: every key,
  type, and default in the framework sections, plus pointers to each
  connector's own section.
- [Glossary](glossary.md) — precise definitions of the terms used
  throughout the guide (lane, epoch, watermark, sealed batch, ...).

## API reference (docs.rs)

Signatures, trait contracts, and per-item docs live in the rustdoc:

| Crate | Role |
|---|---|
| [etl](https://docs.rs/etl) | The facade — the only crate applications depend on. Feature-forwards the connectors (`kafka`, `clickhouse`, `avro`, `full`). |
| [etl-core](https://docs.rs/etl-core) | The engine: record/ack types, source/sink traits, operator chain, checkpointer, backpressure, pipeline builder and runtime, config, metrics, admin server. |
| [etl-kafka](https://docs.rs/etl-kafka) | Kafka source (single consumer, split partition queues). |
| [etl-clickhouse](https://docs.rs/etl-clickhouse) | ClickHouse `ShardWriter` and RowBinary encoder. |
| [etl-avro](https://docs.rs/etl-avro) | Avro deserializer: Confluent wire format, schema-registry client. |
| [etl-test](https://docs.rs/etl-test) | In-memory source/sink mocks with scripting handles, for testing pipelines. |

Applications should import through `etl` (which re-exports the connector
crates behind features); the per-crate docs matter mostly to connector
authors — see [Extending](../06-extending/README.md).

## Canonical documents

- [docs/METRICS.md](../../METRICS.md) — the full metric taxonomy: every
  metric name, type, label set, histogram bucketing, and alerting starting
  points. The [Monitoring](../05-deployment/monitoring.md) page explains
  the model; this file owns the list.
- [docs/DESIGN.md](../../DESIGN.md) — the architecture and every
  decision's rationale, including the frozen v1 trait contracts and the
  decision log. The source of truth when code and intuition disagree.

## Related pages elsewhere in the guide

- Connector configuration bodies: [Kafka](../04-connectors/kafka.md),
  [ClickHouse](../04-connectors/clickhouse/README.md),
  [Avro](../04-connectors/avro.md), [Memory](../04-connectors/memory.md).
- Operational defaults in context: [Tuning](../05-deployment/tuning.md).
