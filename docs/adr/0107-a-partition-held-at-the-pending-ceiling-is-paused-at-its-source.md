---
description: "A partition that stays at checkpoint.max_pending_batches for backpressure.min_pause is paused at its source, so the client stops buffering for it."
---

# ADR-0107 — A partition held at the pending ceiling is paused at its source

- **Status:** accepted
- **Date:** 2026-11-19
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

At `checkpoint.max_pending_batches` a pipeline thread skips the partition's
lanes and asks nothing of the source. The source client keeps fetching for a
partition nobody polls. The Kafka client bounds that queue by the payload bytes
and the count of what it will deliver, and it keeps each fetch response until
the last record or transaction marker in it is polled. A response that holds an
aborted transaction is therefore kept at its full size and counted at close to
nothing, so a partition held at the ceiling could grow the process's memory
without a bound.

A Kafka lane also kept the messages of its last batch until its next poll,
which a skipped lane never makes.

## Considered options

What stops the growth:

- Pause the partition's lanes at the source while the partition is at the
  ceiling
- Set a lower prefetch cap on the Kafka client from the framework
- Keep polling a lane at the ceiling and discard what it returns

When the pause is requested:

- As soon as the partition reaches the ceiling
- Once it has stayed there for `backpressure.min_pause`
- Once it has stayed there for a fixed time
- Once it has stayed there for a new configuration key

Who retries a pause or resume the source refuses:

- The pipeline thread, by sending its request again
- The controller, on its commit tick

## Decision outcome

Chosen option: "Pause the partition's lanes at the source", requested "once it
has stayed there for `backpressure.min_pause`" and retried by "the controller,
on its commit tick", because a pause is the mechanism a source already has for
stopping its client, and the three choices reuse what backpressure built.

The pipeline thread sends the controller one pause request when a partition has
stayed at the ceiling for `min_pause`, and one resume request when the gate
reopens. The skip stays the bound on pending batches. The controller keeps, per
lane, which of the two causes holds: backpressure and the ceiling. It pauses the
lane at the source on the first cause and resumes it when the last one clears.
A ceiling request carries its assignment epoch, and the controller drops one
from an earlier epoch.

A prefetch cap set by the framework was rejected because the client's
thresholds do not count the bytes in question, and because a low cap costs
throughput on every pipeline. Polling a lane at the ceiling would create the
batches the ceiling exists to refuse.

The wait before the pause is `min_pause` because a pause is not free. Pausing a
Kafka partition purges its prefetch, and a partition that reaches the ceiling
and leaves it within a control cycle is working as designed. `min_pause` is
already the time the runtime treats as too short to pause for. A value of zero
pauses a partition as soon as it reaches the ceiling.

`SourceLane::release_batch` is the second half. The pipeline thread calls it
once it has finished a batch, and the Kafka lane frees the batch's messages
there. Freeing them when the batch object drops was rejected because a payload
borrows the lane and can outlive the batch object.

### Consequences

- Good, because a partition the pipeline has stopped polling holds no prefetch
  after `min_pause`, whatever the topic carries.
- Good, because a pause or resume the source refuses is made again on each
  commit tick with what is wanted at that moment. Before this decision a
  refused pause was dropped.
- Bad, because `min_pause` has two meanings, and no validation bounds it from
  above. A long value leaves the source fetching for that long.
- Bad, because a sink that acknowledges more slowly than `min_pause`, with a
  ceiling its partitions reach, pauses and refetches on every cycle. In the
  container test below, with a ceiling of two and a one-second sink linger, a
  partition delivered two batches every two seconds.
- Bad, because the source keeps fetching until the pause lands. That takes
  `min_pause`, one park of the pipeline thread and the rest of the controller's
  pass, which a waiting commit or a revoke drain makes longer.
- Bad, because a thread blocked on a batch the chain is not ready for reads no
  gate, so its lanes are unpolled and unpaused for as long as the block lasts.
- Neutral, because INV-1 holds: a resumed Kafka partition is fetched from the
  offset after the last record its lane took, and the watermark is unchanged.
- Neutral, because INV-2 holds: the requests travel on the unbounded event
  channel, and the pipeline thread never waits on the source.

### Confirmation

`real_broker_a_partition_at_the_ceiling_stops_fetching` in
`crates/spate-kafka/tests/kafka_broker.rs` runs a pipeline against a broker and
fails when the source is not paused. The request logic is tested in
`crates/spate-core/src/pipeline/driver.rs`
(`a_gate_asks_for_a_pause_once_it_has_stayed_closed_for_the_wait` and the tests
beside it), and the causes and retries in
`crates/spate-core/src/pipeline/controller.rs`
(`a_ceiling_pause_outlasts_a_backpressure_resume` and the tests beside it).

## Evidence

- With the pause, resident memory grew by under 1 MiB while 360 transactions of
  900 kB each were aborted behind a partition at the ceiling. With the call to
  the source's `pause` removed, it grew by 321,440 KiB. Measured on the Apache
  Kafka lane, one run each.
- Over those 360 aborted transactions, the client received 364 bytes with the
  pause and 324,172,024 bytes with the call to the source's `pause` removed.
  `real_broker_a_partition_at_the_ceiling_stops_fetching` reads them from
  `spate_kafka_source_rx_bytes_total` on the Apache Kafka lane, one run each,
  and fails above 64 MiB.
- The two-batches-every-two-seconds figure was hand-recorded from one run of
  that test before its sink linger was shortened. No committed test measures
  it.

## More information

- Landed in the pull request that added `SourceLane::release_batch`.
- The behaviour is described in
  [Backpressure](../user-guide/02-concepts/03-backpressure.mdx#the-checkpoint-ceiling)
  and, for the Kafka source, in
  [Pause and resume](../user-guide/04-connectors/sources/kafka/README.mdx#backpressure-pause-and-resume).
