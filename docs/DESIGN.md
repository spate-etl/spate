# etl-rs Design

This document records the architecture of `etl-rs` and the reasoning behind
its decisions. It is the source of truth when code and intuition disagree.

## Goals

- At-least-once delivery, end to end, including across consumer rebalances,
  crashes, and graceful shutdown.
- Extract maximum single-node performance: borrowed data and zero per-record
  allocations on the hot path, CPU work pinned to pipeline threads, I/O
  concurrency at the edges.
- Small, stable abstractions so technology-specific connectors (Kafka,
  ClickHouse, ...) are thin and third parties can implement their own.
- A chaining pipeline API in the spirit of Flink / Java Streams, defined in
  Rust code; YAML configures connectors and tuning, never the operator graph.
- Kubernetes-native operations: one pipeline per process, scrape-based
  metrics, health probes, drain-on-SIGTERM.

## Non-goals (v1)

- Exactly-once / transactional sinks. Replays happen; sinks must tolerate
  duplicates (see *Delivery semantics*).
- General DAG topologies, stateful operators, windowing, watermark-based
  event-time processing. Pipelines are linear with multi-output routing at
  the sink end.
- Dead-letter queues. Record-level error policies are **Skip** (count and
  continue) or **Fail** (stop the pipeline) — always surfaced via metrics.
- Config hot-reload. Reconfigure via rolling restart (checksum-annotation
  pattern).

## Process anatomy

One process runs one pipeline.

```
                    ┌────────────────────────────────────────────────┐
 pinned std thread  │ lane.poll → deserialize (borrowed) → operator  │──try_send──▶ per-shard
 (× N, cgroup-aware)│ chain (map/filter/flat_map, monomorphized)     │              bounded queues
                    └────────────────────────────────────────────────┘                   │
                         ▲            full? pause lanes, keep polling                     ▼
                         │                                              ┌──────────────────────────────┐
                    ┌──────────┐   acks (unbounded, never block)        │ tokio runtime (small):       │
                    │ source   │◀───────────────────────────────────────│ shard workers: batch, seal,  │
                    │ control  │   watermarks → store/commit            │ rotate replicas, retry;      │
                    │ plane    │                                        │ checkpointer; admin server   │
                    └──────────┘                                        └──────────────────────────────┘
```

- **Pipeline threads** (plain `std::thread`, optionally core-pinned,
  count derived from `available_parallelism` minus an I/O reserve, always
  overridable in YAML): each owns a set of source *lanes* and runs
  poll → deserialize → operator chain → route-to-shard, single-threaded.
  Deserialization stays here — payload borrows from the source's buffers
  cannot cross threads, and this keeps all CPU work on pinned cores.
- **I/O runtime** (shared multi-thread tokio, default 2 workers): sink shard
  workers, flush timers, the checkpointer, the admin HTTP server, and any
  async edge work (e.g. schema-registry fetches).
- **Control plane**: a controller services source events (rebalances,
  statistics), owns shutdown, and runs commit ticks.

## Source abstraction

Split into a control plane and a data plane:

- `Source` (control plane): lifecycle, lane assignment/revocation events,
  watermark commits, pause/resume. Poll-based — no `futures::Stream`. For
  Kafka, librdkafka already owns network I/O on its own threads; a Stream
  wrapper only adds waker overhead and `'static` bounds that break borrowing.
- `SourceLane` (data plane): a pollable unit pinned to one pipeline thread,
  yielding borrowed payloads. For Kafka a lane is a partition queue; for the
  in-memory test source, a single lane.

The Kafka source uses a **single consumer per process** with
`split_partition_queue`: one consumer-group member per pod (small groups,
fast rebalances), partitions mapped m:n onto pipeline threads under local
control, borrows kept thread-local. Consumption parallelism is still bounded
by partition count — identical to per-thread consumers — the win is group
scale and unified drain choreography. A per-thread-consumer fallback remains
possible behind the same traits if split-queue semantics ever bite
(benchmarked and validated in `docs/BENCHMARKS.md`).

*Exact trait signatures are frozen after the zero-copy seam spike; see
`crates/etl-core/src/source/`.*

## Records and checkpointing

`Record<T>` carries the payload, a `Copy` metadata struct (partition, offset,
event time, key hash), and an `AckRef` — a clone of an `Arc<AckState>` created
**per source poll batch** (Vector-finalizer style). Dropping a record resolves
its share: filter drops count as success; `flat_map` children clone the same
Arc; multi-sink routing clones it per output with worst-status merge. When the
last clone drops, `(partition, seq, status)` flows over an unbounded channel
to the checkpointer.

The checkpointer is a synchronous, tokio-free module (loom-tested). Per
partition (and per *epoch* — bumped on every rebalance so stale acks from
revoked assignments are discarded) it keeps a ring of outstanding batch seqs,
pops the contiguous acknowledged prefix, and advances the committable
watermark. Watermarks are stored on advance and committed on an interval
(default 5s); revocation and shutdown drain then commit synchronously.

At-least-once invariant: **never commit past unacknowledged data.** A failed
batch stalls its partition's watermark (alert on
`etl_checkpoint_watermark_age_seconds`), it never silently advances.

## Operator chain

Push/collector model (Neumann-style): each operator implements a push
interface and calls its downstream inline; `flat_map` emits through a
stack-borrowed emitter, so fan-out allocates nothing. Within a chain,
composition is fully static and monomorphized — the whole chain compiles to
one loop. Type erasure happens exactly once, at the chain boundary, with one
virtual call **per batch** (the Arroyo/DataFusion pattern) — per-record dyn
dispatch would defeat cross-operator inlining and vectorization.

Partial-push handling: if the terminal router's `try_send` fails mid-batch,
the boundary records the exact resume index; already-pushed records are never
re-processed.

## Backpressure

Two layers, one invariant.

1. **Passive**: bounded per-shard queues plus a global in-flight byte budget.
2. **Active**: when a queue rejects or the budget crosses the high watermark,
   the source loop pauses the offending lanes and *keeps polling* (serving
   rebalance callbacks and liveness); it resumes under hysteresis (low
   watermark + minimum pause).

**Invariant: a source thread never blocks on a channel send.** A poll loop
parked in `send()` stops calling `poll()`, trips `max.poll.interval.ms`, gets
evicted from the consumer group, and turns sink slowness into a rebalance
storm. Acks are likewise never allowed to block: the ack path is
unbounded/atomic by construction.

librdkafka's own prefetch caps (`queued.min.messages`,
`queued.max.messages.kbytes`) are set as a hard memory backstop independent
of pipeline state.

## Sink

The framework owns everything generic; a connector implements only a small
`ShardWriter` ("write this sealed batch to this replica endpoint").

Per **shard**: one worker task accumulates rows to `max_rows` / `max_bytes` /
`linger`, seals the batch, then dispatches up to `max_inflight` (default 2)
concurrent flushes rotating across healthy replicas (round-robin skipping
open circuit breakers). Failures retry the **same sealed batch** on the next
healthy replica with capped exponential backoff. Per-shard-worker (rather
than per-replica) keeps batches full-sized — ClickHouse wants few big inserts
— while `max_inflight` still provides replica parallelism.

ClickHouse specifics: direct-to-shard writes against local tables
(`internal_replication=true`) rather than Distributed-table inserts (bigger
blocks, less merge pressure, and a synchronous server ack that checkpointing
requires); one `INSERT` per sealed batch (`Insert`, not `Inserter`, whose
soft thresholds hide insert boundaries) carrying a deterministic
`insert_deduplication_token` so in-session retries are idempotent.

## Delivery semantics — honest version

At-least-once means duplicates happen. Dedup tokens make **same-boundary
retries** idempotent (a batch retried on another replica after a timeout).
They do **not** cover crash replay: after restart, data re-batches with
different boundaries and different tokens, and those rows will land twice.
Design target tables accordingly — `ReplacingMergeTree` with a version
column is the sanctioned pattern. Documentation must never imply
exactly-once.

## Errors, panics, shutdown, health

- **Error taxonomy**: retryable (transient I/O — retried by the sink layer),
  record-level (bad payload, failed map — subject to per-stage `Skip`/`Fail`
  policy; deser defaults to Skip, operators to Fail), fatal (invariant
  violations — pipeline fails).
- **Panic policy**: user-code panics are caught per batch, the batch resolves
  as Failed (watermark stalls), the pipeline transitions to Failed and the
  process exits non-zero. Kubernetes restarts it. No thread resurrection.
- **Shutdown**: SIGTERM trips the same drain barrier as a full revocation:
  stop lanes → flush chain → sinks force-seal and drain in-flight under
  `drain_timeout` (default 25s, must be < `terminationGracePeriodSeconds`) →
  final synchronous commit → join. If a sink is down at the deadline,
  unflushed batches are abandoned loudly (metric + log) and replay on
  restart — at-least-once holds either way.
- **Probes**: `/readyz` = assignment received and sinks connected;
  `/healthz` = poll-loop heartbeat fresh AND no watermark stuck while data
  flows. Both on the admin server next to `/metrics`.

## Configuration

YAML, loaded once at startup: `${VAR:-default}` interpolation (Kubernetes
Secrets/ConfigMaps as env or mounted files), a typed top-level section
(threads, checkpointing, backpressure, metrics — `deny_unknown_fields`,
humantime durations, byte sizes), and **opaque per-component sections**
passed through to connector factories, which deserialize their own config
(`serde_path_to_error` wrapping for actionable messages). The Kafka section
additionally passes a raw librdkafka property map, with a validation denylist
for properties that would break the framework's guarantees
(e.g. `enable.auto.offset.store`).

## Metrics

See `docs/METRICS.md` for the full taxonomy. Implementation rules: the
`metrics` facade is the registry abstraction (implementors use it directly
for custom metrics); the Prometheus exporter mounts on the admin server; all
framework handles are pre-registered at build time; hot-path counting happens
at batch boundaries.

## Dependency policy

No rdkafka / clickhouse / apache-avro types appear in public trait bounds or
public structs of `etl-core` — all are 0.x crates whose breaking releases
must not become our breaking releases. Connector crates may re-export their
underlying crate for advanced use, clearly documented as exempt from our
stability promises. Avro's optional `fast` backend (`serde_avro_fast`) is
license-gated: crates.io metadata says LGPL-3.0-only while upstream shows
MPL-2.0 — it stays out of the dependency tree until verified (enforced by
`deny.toml`).

## Crate map

| Crate | Role |
|---|---|
| `etl` | Facade; feature-forwards connectors (`kafka`, `clickhouse`, `avro`, `avro-fast`, `full`). The only crate applications depend on. |
| `etl-core` | Engine: record/ack types, source/sink traits, operator chain, checkpointer, backpressure, pipeline runtime, config, metrics, admin server, telemetry. |
| `etl-kafka` | Kafka source (single consumer + partition queues). |
| `etl-clickhouse` | ClickHouse `ShardWriter`. |
| `etl-avro` | Avro deserializer: `apache-avro` backend (default), zero-copy backend (feature `fast`), Confluent wire format, registry client + per-thread schema cache. |
| `etl-test` | Public in-memory source/sink mocks with scripting handles. |
| `benchmarks` | Unpublished: topology A/B, synthetic framework-overhead, e2e harness, loadgen. |

## Decision log

| Decision | Choice | Why (short) |
|---|---|---|
| Delivery | at-least-once | Kafka→ClickHouse standard; exactly-once machinery not worth v1 cost |
| Source API | poll-based, control/data split | librdkafka owns I/O; Streams add overhead and `'static` bounds |
| Kafka topology | single consumer + split partition queues | group scale, m:n thread mapping, one drain choreography (validated by benchmark) |
| Deser placement | pipeline thread | borrow lifetimes force it; keeps CPU pinned |
| Operator dispatch | static in-chain, dyn per batch at boundary | preserves inlining; ~amortized-zero dispatch cost |
| Ack design | refcounted per-batch Arc + contiguity ring | supports filter/flat_map/multi-sink for free; ~2 atomics/record |
| Sink workers | per shard, replica rotation, max_inflight | full-size batches (ClickHouse merge pressure) + parallelism |
| ClickHouse insert | `Insert` per sealed batch + dedup token | deterministic batch boundaries for acks and idempotent retries |
| Metrics | `metrics` facade + prometheus exporter | facade *is* the MeterRegistry pattern; backend-pluggable |
| Config | YAML (`yaml_serde`), opaque passthrough | serde_yaml archived; serde-yml has RUSTSEC advisory |
| Error policy | Skip / Fail only, metrics-surfaced | no owned DLQ topic in target environments |
| MSRV | 1.94 (rolling N-2), edition 2024 | library-consumer reach; absorbs dep MSRV ratchets |
