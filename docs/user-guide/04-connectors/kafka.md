# Kafka source

The Kafka source (`etl-kafka`, feature `kafka` on the `etl` facade) is
built on `rdkafka` with a **single consumer per process**: one
consumer-group member per pod, whose assigned partitions are split into
per-partition queues (`split_partition_queue`) and fanned across pipeline
threads as lanes. Payload borrows stay local to the polling thread — zero
copies out of librdkafka's buffers. Rationale and the validated trade-offs
(group scale, unified drain choreography, ~3% vs per-thread consumers) are
in [docs/DESIGN.md](../../DESIGN.md) § Source abstraction.

Construct it from the pipeline's opaque section:

```rust
let source = KafkaSource::from_component_config(&pipeline.config().source)?;
```

## Configuration

The `source: { kafka: ... }` section deserializes into
`KafkaSourceConfig` (`crates/etl-kafka/src/config.rs`); unknown fields are
rejected with the offending key.

```yaml
source:
  kafka:
    brokers: ${KAFKA_BROKERS:-localhost:9092}
    topic: orders
    group_id: orders-etl
    commit_interval: 5s
    rdkafka:
      auto.offset.reset: earliest
      fetch.message.max.bytes: "1048576"
```

| Key | Type | Default | Meaning |
|---|---|---|---|
| `brokers` | string | required | Comma-separated bootstrap servers. |
| `topic` | string | required | The topic to consume. **One topic per pipeline**: the framework's `PartitionId` is the Kafka partition number, unique only within a topic. |
| `group_id` | string | required | Consumer group id. |
| `commit_interval` | duration | `5s` | How often stored offsets are auto-committed (maps to librdkafka `auto.commit.interval.ms`). |
| `startup_timeout` | duration | `30s` | How long to wait for the first partition assignment before the source reports a fatal startup error (fail fast on unreachable brokers or a wrong topic). |
| `statistics_interval` | duration | `5s` | Statistics emission interval feeding the lag metrics; `0s` disables statistics. |
| `rdkafka` | string map | `{}` | Raw librdkafka properties, applied verbatim after validation — see below. |

## The `rdkafka` passthrough and the denylist

The passthrough exists so you can tune anything librdkafka exposes
(security, fetch sizing, the prefetch backstops `queued.min.messages` /
`queued.max.messages.kbytes`) without waiting for a typed field. But
properties the framework owns for correctness are **rejected at load time
with an explanation** — overriding them silently breaks the delivery
guarantees or the threading model. Aliases librdkafka accepts for the same
setting are denied alongside the canonical names, because passthrough and
framework values apply in unspecified order.

| Denied property | Why |
|---|---|
| `enable.auto.offset.store` | The framework stores offsets itself when checkpoint watermarks advance; overriding breaks at-least-once. |
| `enable.auto.commit`, `auto.commit.enable` (alias) | Interval auto-commit of framework-stored offsets *is* the commit mechanism; disabling it means nothing is ever committed. |
| `auto.commit.interval.ms` | Owned by the typed `commit_interval` field. |
| `enable.partition.eof` | EOF events would pollute the partition queues the pipeline polls. |
| `bootstrap.servers`, `metadata.broker.list` (alias) | Owned by the typed `brokers` field. |
| `group.id` | Owned by the typed `group_id` field. |
| `statistics.interval.ms` | Owned by the typed `statistics_interval` field. |
| `group.protocol` | Only the classic consumer protocol is supported; eager assignment is a framework invariant. |

`partition.assignment.strategy` is additionally validated: any
`cooperative` strategy is rejected — the framework relies on eager (full)
rebalances.

## How offsets are committed

The framework never uses librdkafka's position-based auto-commit. Instead:

- `enable.auto.offset.store` is forced to `false` — librdkafka never stores
  the "consumed" position on its own.
- When the checkpointer advances a partition's watermark (every record up
  to it durably written or intentionally dropped), the source **stores**
  that offset explicitly.
- `enable.auto.commit` is forced to `true`: librdkafka commits the *stored*
  offsets every `commit_interval`.
- Rebalance revocations and shutdown drain first, then store and commit
  **synchronously** while the member still owns the partitions — so another
  member never starts from an offset covering unwritten data.

This is the mechanical heart of at-least-once; see
[Delivery guarantees](../02-concepts/02-delivery-guarantees.md).

## Lanes: partitions on pipeline threads

Each assigned partition becomes one lane, mapped m:n onto pipeline threads
under local control. The assignment choreography guarantees no message
leaks past the split: newly accepted partitions are paused immediately —
before any fetch can complete — split into queues, then resumed. Kafka
tombstones
(null payloads) surface as empty payload slices. Splitting means group
liveness never depends on the controller: every lane poll resets
`max.poll.interval.ms`.

## Backpressure: pause and resume

When the in-flight budget crosses its high watermark or a shard queue
rejects, the runtime pauses the affected lanes' partitions at the consumer
— **and keeps polling**. A paused partition fetches nothing but the poll
loop stays live for rebalance callbacks and liveness. Resume happens under
hysteresis (low watermark + `backpressure.min_pause`), because pausing a
Kafka partition purges its prefetch buffer — resume is not free, and
flapping would thrash the fetch pipeline.

> [!IMPORTANT]
> A source thread never blocks on a channel send. A poll loop parked in
> `send()` would stop polling, trip `max.poll.interval.ms`, get the member
> evicted, and turn sink slowness into a rebalance storm. This is a
> framework invariant, not a tuning choice — see
> [Backpressure](../02-concepts/03-backpressure.md).

## Dependency note

`etl-kafka` deliberately re-exports nothing from `rdkafka`: its types stay
out of public signatures so `rdkafka` major bumps are not breaking changes
here ([docs/DESIGN.md](../../DESIGN.md) § Dependency policy).

## Related

- [Configuring pipelines](../03-guides/configuring-pipelines.md) — how the
  opaque section reaches this connector.
- [Graceful shutdown](../03-guides/graceful-shutdown.md) — the drain
  choreography rebalances share.
- [Monitoring](../05-deployment/monitoring.md) — consumer lag and
  backpressure metrics.
