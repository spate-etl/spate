---
description: "The Kafka feed records each unit's attempt on a compacted topic before the unit is read, so a unit that kills its worker reaches the dead-letter topic."
---

# ADR-0070 — On Kafka, a unit's attempt is recorded before the unit is read

- **Status:** accepted
- **Date:** 2026-10-18
- **Supersedes:** [ADR-0027](0027-split-delivery-attempts-and-quarantine.md)
  (which tenancy ends count)
- **Superseded by:** —

## Context and problem statement

A work feed moves a unit to its dead-letter destination at `attempt_cap`
deliveries, so it needs a count of them. SQS counts receives. A Kafka classic
consumer group carries no delivery count, and the Kafka feed's retry topic
counted only the failures a live worker reported. A unit that crashed its
worker, through an OOM kill, an abort or a SIGKILL, left its partition's
committed offset at the unit. Every later owner of the partition read it and
died, and the unit never reached the dead-letter topic.
[ADR-0027](0027-split-delivery-attempts-and-quarantine.md) names that case as
the one to count.

The decision covers the Kafka feed in `spate-kafka/src/feed/`: where a unit's
count lives, when it is written, and how a worker reads it.

## Considered options

- Write the unit's attempt count to a compacted attempt topic before the
  unit is read, confirmed by its delivery report, and read the topic with a
  follower on each worker that keeps every count in memory
- The same attempt record, read by a scan of the unit's attempt partition
  before each unit
- Count only reported failures, so a unit that kills its worker replays with
  every later unit of its partition until an operator removes it
- Kafka share groups (KIP-932), which count deliveries, and defer the backend
  until librdkafka's share consumer is production-ready

## Decision outcome

Chosen option: "Write the unit's attempt count to a compacted attempt topic
before the unit is read, … with a follower on each worker", because one poison
unit otherwise stops a partition indefinitely, and share groups put the
backend's ship date in librdkafka's hands. The follower reads each record
once per worker and answers each unit from memory, where a scan reads the
unit's attempt partition again for every unit.

The count is keyed by the publish identity of the unit's message,
`<topic>/<partition>/<offset>` of the work-topic message a publish wrote,
where #1091 keyed it by the unit's id. The work-feed contract has each publish
start its own count whatever the unit's id, and a count keyed by id would
carry one publish's attempts into the next. A copy on the retry or dead-letter
topic carries its identity in a `spate.origin` header.

A confirmed dead-letter write is followed by a tombstone of the unit's
record, and the unit's offset finishes once the tombstone is confirmed. #1091
left the record in place after a dead-letter write. The maintainer chose the
tombstone, so the attempt topic holds records for units in flight or waiting
to be retried, and accepted the window it opens, stated below. A unit that
retention deletes after a read keeps its record, because only an
acknowledgment or a confirmed dead-letter write tombstones it.

The follower carries the feed's `group.id` and assigns the attempt topic's
partitions itself, because librdkafka refuses `assign` on a consumer with no
group. It never subscribes, so it never takes part in the feed's rebalances.

### Consequences

- Good, because a unit that kills its worker on every read reaches the
  dead-letter topic at the cap, and its partition moves past it.
- Good, because every read counts on both backends, as every receive counts on
  SQS, so the cap means the same on each. The one exception is on Kafka, where
  two copies of one publish read at the same time, as after a rebalance, can
  count once between them.
- Bad, because every unit costs, before its read starts, a watermark read of
  its attempt partition, the follower's catch-up to that watermark, and one
  confirmed produce.
- Bad, because every worker holds in memory the count of each identity with a
  live record, and reads the attempt topic from its start when it connects.
  That includes the record of a unit lost to retention after a read, which
  nothing tombstones.
- Bad, because a graceful stop or a rebalance uses an attempt, so the cap must
  exceed the restarts and rebalances a unit can see. Under eager assignment a
  rolling restart of N members can cost a long unit up to 2N attempts, where
  SQS charges one receive to the units the restarted pod held.
- Bad, because ADR-0027's rule that only non-graceful ends count holds on
  neither backend.
- Bad, because each bounded job needs four fresh topics, the work, attempt,
  retry and dead-letter topics, and a continuous job needs attempt, retry and
  dead-letter topics of its own.
- Bad, because the backend keeps the consumer group's ordering. A slow unit
  holds back its partition's commit point, and each rebalance reads again every
  unit after it and cuts short every member's units in flight.
- Bad, because a Kafka feed spreads units across at most as many workers as the
  work topic has partitions.
- Bad, because the units of an attempt partition that cannot be read wait at
  the gate and count against the 128 units a worker buffers. The units of the
  other attempt partitions get through until those held units fill the
  buffer; then every work partition pauses until that partition can be read.
  Releasing held units and reading their offsets again later is #1258.
- Bad, because the attempt topic's partition count cannot change. Workers
  started before and after a change count a unit separately, and nothing
  detects the change.
- Bad, because the dead-letter tombstone opens a window. A crash within the
  commit interval after the tombstone is confirmed, or a revocation while it
  is in flight, lets the next owner read the unit again from a count of zero
  and dead-letter a second copy, after at most `attempt_cap` more reads. INV-1
  holds, because each copy's offset still finishes only once the write that
  settles it is confirmed.
- Neutral, because the follower is in the feed's group by `group.id` alone. It
  needs no group ACL beyond the feed's own, and a test against a real broker
  shows the group with one member.
- Neutral, because the feed still has no lease and no watchdog. A worker whose
  control plane wedges keeps its partitions and their units until the process
  exits, and the attempts those units used stay recorded.

### Confirmation

- `a_unit_that_kills_its_worker_at_each_read_reaches_the_dead_letter_topic`,
  in `crates/spate-kafka/tests/feed_kafka.rs`, on a real broker.
- `an_acknowledged_unit_finishes_only_once_its_tombstone_is_confirmed`, in
  `crates/spate-kafka/src/feed/tests.rs`.
- `startup_refuses_a_missing_or_uncompacted_attempt_topic`, in
  `crates/spate-kafka/tests/feed_kafka.rs`, on a real broker.
- `the_attempt_follower_leaves_the_feed_members_assignment_alone`, in
  `crates/spate-kafka/tests/feed_follower_membership.rs`, on a real broker.

## Evidence

- Gate latency, from the consumer handing a unit to the gate to the
  confirmation of its attempt write, over 500 units read and acknowledged one
  at a time: p50 7.1 ms, p90 7.6 ms, p99 9.7 ms, max 38.9 ms. Rig: the
  `ci/kafka` image under Docker Desktop on an Apple M5 Max, one broker,
  replication factor 1, a two-partition attempt topic, and librdkafka's default
  `linger.ms` of 5 ms. Spike-measured, hand-recorded; no committed rig.
  Single-broker latency does not carry over to a replicated cluster, where
  `acks=all` waits on the in-sync replicas.
- Follower memory, about 140 bytes per live record: resident memory grew by
  138 bytes per entry when 1,000,000 identities of 22 bytes went into one
  follower's ledger, under the macOS allocator. The figure includes the hash
  table's spare capacity after its last growth. Spike-measured, hand-recorded;
  no committed rig.

## More information

- Landed in the pull request that closed #1091.
- [ADR-0016](0016-kafka-sink-ack-wiring.md) holds the delivery-report
  contract the attempt and tombstone writes follow, and
  [ADR-0018](0018-kafka-sink-forced-durability.md) the forced `acks=all` and
  idempotence they are written under.
- The
  [work-feed record](0080-a-work-feed-replaces-leased-fenced-split-coordination.md)
  and
  [the work-unit record](0071-a-work-unit-is-one-object-or-one-byte-range-of-one-object.md)
  describe the feed and the unit this record counts. ADR-0027's `Superseded by`
  line is set with them.
- The backend can move to share groups once librdkafka's share consumer is
  production-ready. Cooperative assignment is the change to revisit when
  rebalance replay costs a deployment too much.
- [Kafka feed](../user-guide/04-connectors/feeds/kafka/README.mdx) — the
  topics to create, the attempt record's format, and what counting costs.
