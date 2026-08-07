# ADR-0017 — One producer per Kafka sink; shards are workers over clones

- **Status:** accepted
- **Date:** 2026-07-13 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The framework's sink model is shards, each with replicas, each shard owned by a
worker ([ADR-0006](0006-per-shard-sink-workers.md)). Kafka does not fit that
shape. A broker cluster does its own partitioning and its own batching inside
the client, and the client is designed to be one long-lived instance per
process, not one per shard.

Mapping shards onto producers would mean several clients against one cluster,
each batching independently — and librdkafka's statistics counters are
absolute, so several clients reporting into one metric family would produce
sums that mean nothing.

## Considered options

- One producer per shard, each with its own broker connections
- One producer per sink instance; framework shards become worker parallelism
  over clones of it, one replica each
- One shard only, bypassing the pool

## Decision outcome

Chosen option: "One producer per sink instance, with shards as workers over
clones", because librdkafka already owns broker routing and batching, and
because the absolute-mapped statistics counters are only sound with a single
client.

Framework shards remain as worker parallelism. Each has one replica, so replica
rotation degenerates to a no-op — but the rest of the pool machinery keeps
working and keeps earning its place: the circuit breaker still quarantines a
failing destination, and `spate_sink_shard_healthy` still provides the
backpressure signal that stalls intake.

Collapsing to a single shard was rejected because it would remove that
machinery entirely, along with the concurrency the workers provide.

### Consequences

- Good, because there is one client, so its statistics are meaningful and its
  connection pool is shared as librdkafka intends.
- Good, because the framework's breaker and health signaling work unchanged,
  with no Kafka-specific backpressure path.
- Bad, because replica rotation is dead code in this connector — present,
  exercised by no Kafka deployment, and potentially confusing to read.
- Bad, because shard count means something different here than for a sharded
  database sink: it is write concurrency, not placement. Raising it past a small
  number collapses throughput, because the clones contend on one client.

### Confirmation

Nothing automated. The single-client property is a construction detail of the
sink bundle rather than an enforced invariant, which is worth knowing if the
connector is ever refactored.

## More information

- Landed in `6af6861` (#24).
- [ADR-0006](0006-per-shard-sink-workers.md) — the shard model this adapts to.
- [Kafka sink](../user-guide/04-connectors/sinks/kafka/README.mdx) — including
  the guidance not to raise shard count.
