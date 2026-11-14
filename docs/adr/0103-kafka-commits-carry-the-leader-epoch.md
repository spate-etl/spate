---
description: "Kafka feed and source commits carry the leader epoch of the record before them, and an assignment seeks to the commit so a truncated partition is read from the point of truncation."
---

# ADR-0103 — Kafka commits carry the leader epoch, and an assignment validates it

- **Status:** accepted
- **Date:** 2026-11-14
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

An unclean leader election can truncate a Kafka partition below the group's
commit. Units written afterwards take the reused offsets below that commit.
[ADR-0100](0100-a-kafka-commit-above-a-partitions-end-holds-a-bounded-job.md)
holds a bounded job while the commit stands above the partition's end. Once the
end reaches the commit, a member assigned the partition starts at the commit and
never reads the units below it
([#2104](https://github.com/spate-etl/spate/issues/2104)). The Kafka source has
the same gap.

librdkafka 2.12.1 checks a start against the partition's leader only when the
start carries a leader epoch and arrives through a seek. The feed and the source
both read the committed offsets themselves and hand concrete starts to `assign`
([#703](https://github.com/spate-etl/spate/issues/703)), and librdkafka fetches
from such a start without a check.

## Considered options

- Commit the epoch only
- Commit the epoch, seek to each commit that carries one and is at or below the
  partition's end, and start a commit above the end from the log start
- Commit the epoch, and seek to every commit that carries one
- Describe the gap and change nothing

## Decision outcome

Chosen option: "Commit the epoch, seek to each commit that carries one and is at
or below the partition's end, and start a commit above the end from the log
start", because it is the option that both records the epoch and checks it on
assignment.

A committed epoch alone changes nothing on the path both components take: a
start handed to `assign` with epoch 7 began at the commit on both brokers. A
seek to a commit above the end moves the member to the end of the epoch's
records. With nothing written after that, the member reads nothing and commits
nothing, so ADR-0100's hold never releases. Starting from the log start reads
the same units and lets the hold end. Under the source's `error` policy every
commit that carries an epoch is sought, so the policy decides what a detected
truncation does.

### Consequences

- Good, because a member assigned a truncated partition reads it from the point
  of truncation or earlier, in the feed and in the source.
- Good, because a commit above the end still ends a bounded job once a member
  reads the partition.
- Bad, because a refused seek keeps the partition paused until a seek is
  accepted. If librdkafka loses the deferred start, as in the class #703 works
  around, the partition reads nothing until the next rebalance, and a warning
  names it.
- Bad, because #703 can no longer remove the committed read: the seek needs the
  committed epoch.
- Bad, because an assignment reads the end offsets of the partitions whose
  commit carries an epoch, one ListOffsets request per leader inside the bound
  the committed read already has.
- Bad, because under the source's `error` policy a detected truncation, or an
  epoch the leader does not know, fails the source.
- Bad, because a failed end read leaves a commit above the end sought. When
  the leader answers that the epoch's records end at the log's end, the member
  starts there and reads only later writes. With nothing written after it, a
  feed worker commits nothing on the partition, and a bounded job waits until
  the next rebalance.
- Neutral, because a commit with no epoch starts unchecked, as before, and
  INV-1 names it with a failed committed read and a failed seek.

### Confirmation

INV-1 names the unchecked starts.
`crates/spate-kafka/tests/feed_kafka.rs`:
`a_feed_commit_carries_the_epoch_of_the_record_before_it`,
`a_member_reads_the_units_below_a_commit_the_leader_answers_lower` and
`a_member_reads_a_partition_whose_epoch_carrying_commit_is_above_its_end`.
`crates/spate-kafka/tests/kafka_broker.rs`:
`real_broker_a_source_commit_carries_the_epoch_of_the_record_before_it` and
`real_broker_a_source_reads_below_a_commit_the_leader_answers_lower`. Each runs
on Apache Kafka and Redpanda. No committed test runs an unclean election. The
tests commit an epoch the leader never had, which the leader answers with an
offset below the commit.

## Evidence

Probe runs against the Apache Kafka 4.4.0 and Redpanda v26.2.6 lanes,
hand-recorded; no committed rig. A start handed to `assign` with a committed
epoch of 7 began at the commit on both brokers, and a seek to the same commit
moved the member below it. They do not establish what either broker answers
after a real unclean election.

## More information

Landed in [#2117](https://github.com/spate-etl/spate/pull/2117), which fixes
[#2104](https://github.com/spate-etl/spate/issues/2104).
[ADR-0100](0100-a-kafka-commit-above-a-partitions-end-holds-a-bounded-job.md)
still applies to a commit above the end. The behavior is described on the
[Kafka feed page](../user-guide/04-connectors/feeds/kafka/README.mdx#transaction-markers)
and the
[Kafka source page](../user-guide/04-connectors/sources/kafka/README.mdx#how-offsets-are-committed).
