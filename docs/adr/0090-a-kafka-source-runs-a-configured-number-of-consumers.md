---
description: "The Kafka source runs a configured number of consumers per process, default one, each a group member with split partition queues. Supersedes ADR-0012."
---

# ADR-0090 — A configured number of Kafka consumers per process, to lift a fetch-bound source

- **Status:** accepted
- **Date:** 2026-11-02
- **Supersedes:**
  [ADR-0012](0012-single-kafka-consumer-with-split-partition-queues.md)
- **Superseded by:** —

## Context and problem statement

ADR-0012 gave the Kafka source one consumer per process, its partitions split
into queues that any pipeline thread drains, so group size follows pod count.
librdkafka keeps at most one Fetch in flight per broker connection per client,
so that one consumer fetches the whole assignment from each broker one request
at a time. On a benchmark rig reading a 32-partition topic from one broker, the
source became the bound: pipeline threads spent about a third of their time in
the poll, the client's fetch queue sat empty and the sink was far from its
in-flight cap ([#1651](https://github.com/spate-etl/spate/issues/1651)).

## Considered options

- One consumer per process, as ADR-0012 decided
- A configured number of consumers per process, default one, each a group
  member whose partitions are split into queues that any pipeline thread drains
- One consumer per pipeline thread
- A count derived from the thread count or the cores

## Decision outcome

Chosen option: "A configured number of consumers per process", because it puts
one more Fetch in flight per leader broker for each consumer added, keeps the
split queues and their m:n mapping onto pipeline threads, and leaves the default
deployment exactly as ADR-0012 built it.

The source holds each consumer's assignment until every consumer of the
rebalance has one, then hands the runtime one lane set. Eager assignment revokes
every member in every rebalance, so the first revocation on any consumer drains
every consumer's lanes, and each consumer commits through its own client while
it still owns its partitions. A count derived from threads or cores was rejected
because the throughput model has no core term: what bounds a fetch-bound source
is bytes per fetch, fetches in flight and the round trip. One consumer per
pipeline thread was rejected because it couples group size to thread count,
which ADR-0012 set out to avoid, and a per-thread rig measured it 56% slower
than one client once records carried work.

ADR-0012 said a per-thread-consumer fallback stayed available behind the same
traits. None existed in the tree. This key is the per-deployment reversal that
record anticipated, in a different shape.

### Consequences

- Good, because a fetch-bound source gains fetch concurrency without a new pod.
- Good, because the default of one leaves group size, `client.id`, metric series
  and the rebalance path as they were.
- Bad, because group size becomes `pods × consumers`, and each extra member
  joins every eager rebalance.
- Bad, because each consumer adds its own librdkafka threads, connections and
  in-flight fetch responses, a fixed cost per consumer.
- Bad, because the source now merges several members' rebalance events into the
  runtime's one, with a hold of up to 3 seconds for consumers still joining and
  a fresh epoch for an assignment that arrives while lanes are live.
- Bad, because the benefit is not monotonic: on the measured rig four consumers
  gained less than two.

### Confirmation

INV-1: `commit` stores each watermark through the consumer that owns its
partition and checks every element, pinned by the MockCluster test
`two_consumers_poll_ack_and_commit_every_partition`, the real-broker test
`real_broker_revocation_commits_every_consumers_offsets`, and the end-to-end
restarts `fewer_consumers_after_a_restart_lose_nothing` and
`more_consumers_after_a_restart_lose_nothing`. One lane set per rebalance is
pinned by `consumers_share_one_epoch`, whose driver refuses an assignment that
arrives with lanes live. Static membership with more than one consumer is
refused at load.

## Evidence

On the benchmark's `c8gd.metal-24xl` profile (one broker at 3 cores, ClickHouse
at 48 cores, a 32-partition topic, the arm at 32 pipeline threads), two
interleaved reps per cell with an A/A control, records under `--trigger tuning`
and not published:

| Consumers | Rows/s (mean) | vs 1 | Poll share | Fetch queue per consumer |
|---|---|---|---|---|
| 1 | 34.88M | | 31–32% | about 0 |
| 2 | 42.54M | +22.0% | 8–9% | 11k–31k |
| 4 | 38.64M | +10.8% | 22–23% | 40k–58k |

The A/A control agreed within 0.6% per core. CPU per row was 0.60–0.61 µs in
every cell. At two consumers the sink became the bound, delaying 54–67% of
inserts on its part count. Four consumers loaded the sink less than two and
spent more of their time in the poll with full fetch queues, a cost on the
source side this run does not explain. Raising one consumer's fetch sizes
instead cost 21.6%. Lifting the sink's part limits pinned its CPU on merges
rather than lifting it, so this rig cannot show scaling past two consumers.

Measured on the benchmark repository's tuning branch
`bootstrap/spate-1651-consumers` (not for merge), arm pinned to spate `4e161d49`,
an earlier head of the pull request that landed this decision, launch
41423442032 of 2026-11-02. Later commits on that pull request changed commit
routing, drop and revocation, none of them on the per-record path.

## More information

- Landed with the pull request that closes #1651.
- [Kafka source](../user-guide/04-connectors/sources/kafka/README.mdx) — the
  `consumers` key, `client.id` suffixes and changing the count.
- [More consumers in one process](../user-guide/04-connectors/sources/kafka/performance-tuning.mdx#more-consumers-in-one-process)
  — when to raise it and what each consumer costs.
- ADR-0003 (the poll-based source API, whose one `poll_events` call now serves
  every consumer), ADR-0017 (the Kafka sink keeps one producer) and ADR-0088
  (one constructor builds every consumer) still hold.
