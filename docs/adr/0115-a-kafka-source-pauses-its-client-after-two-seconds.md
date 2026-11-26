---
description: "The Kafka source pauses a partition at the consumer once its lane has stayed paused for 2 s, so a short pause keeps the prefetch and fetches nothing again."
---

# ADR-0115 — A Kafka source pauses its client only after a lane has stayed paused for 2 s

- **Status:** accepted
- **Date:** 2026-11-26
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

`KafkaSource::pause` called `consumer.pause` at once. librdkafka 2.16.0 then
outdates the partition's fetch queue and purges it, and on resume fetches it
again from the position the pipeline last took. Since ADR-0112 a pause raised
while a shard waits for a write permit ends when a permit frees, so behind a
sink with a slow-write tail the runtime pauses and resumes a lane several times
a second, and each cycle fetched the prefetch again. The client pause itself
has to stay: ADR-0107 records that the client's queue thresholds do not count
aborted-transaction records, so an unpolled partition on such a topic grows
memory without a bound. The question was when `KafkaSource` pauses the
partition at the consumer after the runtime pauses its lane.

## Considered options

- Pause at the consumer once the lane has stayed paused for a fixed 2 s
- The same, for `backpressure.min_pause`, passed to the source by the runtime
- Have the runtime hold back its backpressure pause request for a dwell, as it
  already does for the pending-batch ceiling
- Tell the source why a lane is paused, and pause at the consumer only at the
  ceiling
- A configuration key for the wait

## Decision outcome

Chosen option: "pause at the consumer once the lane has stayed paused for a
fixed 2 s", because the refetch is a cost of the Kafka client alone and this
removes it for every pause the runtime lifts within the wait, in one crate and
with no change to the `Source` contract.

`KafkaSource::pause` records when each partition's client pause falls due.
`poll_events` pauses the due partitions at the consumer, one call per consumer,
and drops the record of a partition the consumer no longer holds. `resume`
drops a record that has not reached the consumer with no client call, and
resumes the others at the consumer as before. A partition whose seek is
accepted while its pause has not reached the consumer resumes, and its pause
reaches the consumer when it falls due.

At `min_pause` 50 ms most measured pauses would outlast `min_pause` and still
purge. A dwell in the runtime would delay the pause of every source, where the
other sources pause by setting a flag. A pause cause in the contract would leave
a long backpressure pause unbounded on a topic with aborted transactions, which
ADR-0107 rules out. No evidence showed anyone needs to set the wait.

### Consequences

- Good, because a pause lifted within 2 s keeps the prefetch, so behind a sink
  that cycles pauses with its write permits the client receives each message
  once.
- Good, because a pause that lasts longer purges and stops fetching as before,
  so a long backpressure pause and the pending-ceiling pause keep the bound
  ADR-0107 set, delayed by the wait.
- Bad, because for 2 s after the runtime pauses a lane the partition keeps
  fetching up to the client's thresholds, and on a topic with aborted
  transactions it takes in what the producer writes in that time. The bound is
  in time only.
- Bad, because the pending-ceiling pause reaches the consumer at least
  `min_pause` plus 2 s after the partition reaches the ceiling, plus up to one
  controller pass. The source cannot tell that pause from a backpressure one,
  so the two waits run in series.
- Bad, because `spate_checkpoint_ceiling_pauses_total` counts a ceiling pause
  that the runtime lifts within 2 s, and such a pause never reaches the
  consumer. A debug line records each client pause.
- Neutral, because INV-1 holds: a partition never paused at the consumer keeps
  its fetch position, and a paused one resumes from the position the pipeline
  last took, as before.
- Neutral, because INV-2 holds: the pipeline thread stops polling a paused lane
  at once, and the client call moves from `pause` to `poll_events`, both on the
  controller thread.

### Confirmation

`crates/spate-kafka/tests/mock_cluster.rs` tests the rule
(`a_short_pause_keeps_the_prefetch` counts the messages the client receives
across a pause lifted before the wait, and
`pause_stops_delivery_and_resume_recovers_gapless` shows a lane fetched just
short of the wait and not fetched at it). The in-file tests in
`crates/spate-kafka/src/source.rs` cover the record of each pause.
`real_broker_a_partition_at_the_ceiling_stops_fetching` in
`crates/spate-kafka/tests/kafka_broker.rs` checks that a partition at the
ceiling still stops fetching aborted transactions once the wait has passed.

## Evidence

- Mock cluster, one partition of 1,000 records, one read, then a pause and an
  immediate resume: the client received 1,936 messages at 87f649c0 and 1,000
  with this change (`a_short_pause_keeps_the_prefetch`).
- A queue benchmark rig this repository does not carry, with a Kafka source
  over Apache Kafka 4.4.0 on one host, 2 pipeline threads, a 256 ms sink with
  every twentieth write four times slower, three reps per arm at `min_pause`
  500 ms and 50 ms. At 87f649c0 pauses averaged 0.21 to 0.24 s, 60 to 62 per
  30 s window, and the client received 1.78 to 2.02 messages per settled row.
  With this change it received 0.98 to 1.08. The rig records only the mean
  pause, so it does not show what share of pauses end within 2 s.
- Hand-recorded, one run each, Apache Kafka 4.4.0 in Docker, the ceiling test's
  rig on the real clock with a producer writing about 4.2 MB/s of aborted
  transactions: from the ceiling to the client pause the client took in
  1.8 MB at 87f649c0 and 9.9 MB with this change.

## More information

- Landed in the pull request that defers the client pause in `KafkaSource`.
- [ADR-0107](0107-a-partition-held-at-the-pending-ceiling-is-paused-at-its-source.md),
  [ADR-0111](0111-a-queue-rejection-pauses-only-under-sink-pressure.md) and
  [ADR-0112](0112-a-pause-under-sink-pressure-ends-when-a-permit-frees.md) state
  Kafka consequences of their decisions that this record changes: a paused
  Kafka partition holds no prefetch after `min_pause`, and each pause purges.
  Their decisions about when the runtime pauses and resumes stand.
- The behavior is described in the Kafka source's
  [Backpressure: pause and resume](../user-guide/04-connectors/sources/kafka/README.mdx#backpressure-pause-and-resume).
