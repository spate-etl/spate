# Connectors

Connectors are the technology-specific edges of a pipeline: a **source**
feeds it, a **format** (deserializer) decodes payloads into records, and a
**sink** writes them out. Everything between — batching, routing, retries,
acknowledgements, backpressure — is framework-owned, so each connector is a
thin implementation of a small trait. Configuration reaches every connector
the same way: an opaque single-key section in the pipeline YAML, handed to
the connector's factory (see
[Configuring pipelines](../03-guides/configuring-pipelines.md)).

## The matrix

| Connector | Role | Crate | Feature on `etl` | Config tag |
|---|---|---|---|---|
| [Kafka](kafka.md) | Source | `etl-kafka` | `kafka` | `source: { kafka: ... }` |
| [ClickHouse](clickhouse.md) | Sink | `etl-clickhouse` | `clickhouse` | `sink: { clickhouse: ... }` |
| [Avro](avro.md) | Format (deserializer) | `etl-avro` | `avro` | `deserializer: { avro: ... }` |
| [Memory / Capture](memory.md) | Test source + sink | `etl-test` | — (separate dev-dependency) | `source: { memory: {} }` / `sink: { capture: {} }` (informational) |

`features = ["full"]` on the `etl` facade enables all production
connectors.

## Guarantee notes per connector

- **Kafka (source)** — at-least-once anchored here: offsets are *stored*
  only when checkpoint watermarks advance (auto-offset-store is disabled)
  and committed on an interval; rebalances and shutdown drain and commit
  synchronously before partitions are released. Backpressure pauses
  partitions without ever blocking the poll loop, so sink slowness cannot
  cause a consumer-group eviction. One topic per pipeline.
- **ClickHouse (sink)** — a batch acknowledges only after the server
  confirms the insert end-to-end (`wait_end_of_query=1`, materialized views
  included). Deterministic dedup tokens make same-batch retries idempotent
  — **only if the table keeps a deduplication window**; crash replay is not
  covered, so target tables must tolerate duplicates. See the
  [warning](clickhouse.md) — it is the sharpest edge in the framework.
- **Avro (format)** — schema fetches never block a pipeline thread: a
  cache miss holds the batch and replays it when the schema lands — records
  are never dropped or duplicated by the mechanism. Undecodable payloads
  follow the deserializer's Skip/Fail policy like any poison payload.
- **Memory / Capture (test)** — deterministic, in-process, and honest about
  the same contracts: commits only advance past durably "written" data, so
  tests can assert the at-least-once behavior itself. Not for production.

For the semantics behind these notes, read
[Delivery guarantees](../02-concepts/02-delivery-guarantees.md) and
[docs/DESIGN.md](../../DESIGN.md).

## Writing your own

The traits are public and small: a source is `Source` + `SourceLane`, a
sink is `RowEncoder` + `ShardWriter`. Start with
[Extending](../06-extending/README.md); the dependency policy that keeps
third-party connector crates viable (no 0.x types in public trait bounds)
is in [docs/DESIGN.md](../../DESIGN.md) § Dependency policy.
