# ADR-0006 — One sink worker per shard, rotating across replicas

- **Status:** accepted
- **Date:** 2026-07-05 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The sink has to turn a stream of records into writes against a destination that
has several shards, each with several replicas. Two things pull against each
other. Analytical destinations want **few large inserts**. A small insert
becomes a small part, and small parts become merge pressure that outlives the
ingest. But a single writer per shard leaves replica capacity idle and makes one
slow response stall the shard entirely.

The framework also needs a readiness signal: a process that has not yet
connected to its destination must not report itself ready, and nothing except
the sink knows whether it has.

## Considered options

- One worker per replica, each accumulating its own batches
- One worker per shard, sealing whole batches and dispatching several
  concurrently across healthy replicas
- One worker per shard, strictly one write in flight

## Decision outcome

Chosen option: "One worker per shard, dispatching several concurrently across
healthy replicas", because it is the only arrangement that keeps batches
full-sized *and* uses more than one replica at a time.

A per-shard worker accumulates to `max_rows`, `max_bytes` or `linger`, seals the
batch, and then dispatches up to `max_inflight` concurrent flushes, rotating
round-robin across replicas whose circuit breaker is closed. A failure retries
**the same sealed batch** on the next healthy replica with capped exponential
backoff, which is what makes a deduplication token meaningful: the retry carries
identical bytes and an identical token.

Per-replica workers were rejected because they divide the record stream by
replica count, so each batch is a fraction of the size, which is the merge
pressure the design is avoiding. Strictly-one-in-flight was rejected
because it makes shard throughput a function of round-trip latency.

Records are never rerouted to another shard when one is unhealthy. Rerouting
would break both placement parity and the determinism the deduplication token
depends on, so an unhealthy shard back-pressures the source instead.

### Consequences

- Good, because batches stay full-sized regardless of replica count, so the
  destination sees few large inserts.
- Good, because replica parallelism is a knob (`max_inflight`) rather than a
  consequence of topology.
- Bad, because a shard whose replicas are all unhealthy stalls rather than
  shedding load, surfaced as `spate_sink_shard_healthy == 0`, and the watermark
  stalls behind it.
- Bad, because parked write attempts hold their in-flight permits, so intake
  stalls too. That stall is deliberate, being how the shard back-pressures the
  source, but it means one bad shard slows the whole pipeline.

### Confirmation

INV-5 — the worker's intake path never awaits outside its `select!`, which is
what keeps the drain deadline pollable while the worker is blocked.
`ShardWorker::dispatch` is deliberately not `async`: it parks a sealed batch for
a permit instead of awaiting one, so the await cannot be reintroduced without
changing the signature.

Readiness is confirmed by the `SinkRuntime.probe` hook, polled by the runtime:
`/readyz` needs live destination connectivity, and nothing but the sink could
ever set it.

## More information

- Landed in `c8973e6`; the readiness hook in `e062465`.
- [ADR-0007](0007-clickhouse-insert-path.md) — what a sealed batch becomes on
  the wire for the first destination this served.
- [Sink sharding](../user-guide/02-concepts/05-sink-sharding.mdx) — the routing
  and worker model as it behaves in operation.
