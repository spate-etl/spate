---
description: "The framework's sink keys sit beside the component key in a sink section, and a connector's base pool is overlaid by YAML or replaced from code."
---

# ADR-0109 — The framework's sink keys sit beside the component key, over a connector's base pool

- **Status:** accepted
- **Date:** 2026-11-23
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A sink section names one component, such as `clickhouse:` or `kafka:`. The
framework reads six settings for every sink: `queue`, `chunk`, `batch`,
`inflight`, `retry` and `breaker`. `chunk` sat inside each component body,
where core peeled it off. The ClickHouse and Kafka sinks each declared the four
pool blocks in their own config. The shard queue's size could be set only in
code.

The question is where these keys live in the file, how they combine with a
connector's own defaults, and how code sets them.

## Considered options

Where the keys sit:

- Beside the component key in the sink section, as `units_held` sits beside the
  backend key in `feed:`
  ([ADR-0079](0079-the-feed-is-configured-in-a-top-level-section-and-provisioned-by-the-operator.md))
- Inside each component body, with core peeling them off, as `chunk` was
- Inside each component body, with each connector declaring them

How a connector's defaults combine with the file:

- The connector supplies a base pool; a block in the file overlays it field by
  field
- The connector supplies a base pool; a block in the file replaces it whole
- No connector base: core's defaults for every sink

How a stale key inside a body is refused:

- By the connector's own `deny_unknown_fields`
- By one core check that reserves the names in every sink body

## Decision outcome

Chosen options: "beside the component key", "a base overlaid field by field",
and "the connector's own `deny_unknown_fields`".

The keys sit beside the component key because core then parses each one once,
and every sink, a custom one included, accepts the same keys. A body holds only
what its connector reads, so the body is passed through to the connector
untouched ([ADR-0009](0009-yaml-configuration-with-opaque-passthrough.md)).
Any other key beside the component key is refused as a second component, so a
misspelled framework key fails at load.

A connector passes its base pool in `SinkParts`. A YAML block overlays that
base per field, so `batch: { linger: 2s }` keeps the base's row and byte
limits. A block set in code with `SinkOptions::with_batch` (and the other three)
replaces the base block whole. A key set both in YAML and in code on one sink is
refused at install, so each key is declared in exactly one place. The pool's
bounds run on the resolved pool when the pipeline installs the sink.
`SinkOptions::resolve` returns the values the builder installs, so a pipeline
assembled from the primitives reads the same keys.

`queue` takes a byte size, `queue: { max_bytes }`, converted to chunk slots once
at startup against the sink's chunk size. Unset, each queue holds 8 chunks.

Core reserves only `chunk` inside component bodies, since core peeled it from
every body. Each connector's `deny_unknown_fields` refuses a pool block left in
its body, and the changelog shows where the block went. The other names stay
free for a connector's own body: a broker sink in
[#2092](https://github.com/spate-etl/spate/issues/2092), such as one for
RabbitMQ, would name its destination `queue`.

### Consequences

- Good, because every sink accepts the same six keys in the same place, and no
  connector declares a framework key.
- Good, because the shard queue is sized from the file, per sink, without a
  rebuild.
- Good, because a connector can carry defaults of its own, and the file still
  changes one field without restating the rest.
- Neutral, because the overlay round-trips each pool block through its serde
  form, so `batch.max_bytes` serializes as a plain integer to stay exact.
- Bad, because a pool block left inside the body of a sink that decodes no body,
  such as the capture sink or a custom one, is ignored silently. Only `chunk`
  gets a refusal there.
- Bad, because every existing file that set these keys inside a body had to
  move them, at one minor release.

### Confirmation

`crates/spate-core/src/config/mod.rs`: `framework_keys_sit_beside_one_backend`,
`a_misspelled_framework_key_is_refused_as_a_second_backend` and
`chunk_inside_a_body_is_rejected`. `crates/spate-core/src/config/sink.rs`:
`an_empty_overlay_keeps_the_base_exactly`.
`crates/spate-core/src/pipeline/builder.rs`:
`a_yaml_pool_block_overlays_the_base_and_code_replaces_it` and
`each_key_set_in_yaml_and_in_code_collides`.
`crates/spate-test/tests/sink_pool_keys.rs` pins that the resolved pool reaches
the spawned pool. The ClickHouse and Kafka configs each have
`a_pool_block_in_the_body_is_refused`.

## Evidence

Queued bytes per shard set throughput more than the chunk count. Against a
ClickHouse sink, 1 MiB chunks × 8 slots and 64 KiB × 128 slots, both 8 MiB per
shard, gave 11.24M and 10.84M rows/s. 64 KiB × 8 slots, 512 KiB per shard, gave
4.81M. The figures are from the message of commit `89f6481e`.

## More information

- Decided on [#2325](https://github.com/spate-etl/spate/issues/2325), and landed
  in [#2356](https://github.com/spate-etl/spate/pull/2356) (core, closing
  [#2351](https://github.com/spate-etl/spate/issues/2351)),
  [#2365](https://github.com/spate-etl/spate/pull/2365) (Kafka) and the
  pull request that closed #2325 (ClickHouse).
- Deriving a queue size when `queue` is unset is
  [#2354](https://github.com/spate-etl/spate/issues/2354).
- [The sink section](../user-guide/07-reference/configuration.mdx#sink-section)
  in the configuration reference.
