---
description: "The Kafka feed reads each partition's position before a commit and raises its commit point past offsets with no record, so a bounded job ends after a transaction."
---

# ADR-0093 — A Kafka feed commits past offsets with no record from the consumer's position

- **Status:** accepted
- **Date:** 2026-11-06
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A transaction marker takes an offset in a Kafka log and is never delivered, and
under `read_committed` an aborted transaction's records are skipped the same
way. The Kafka feed commits only past offsets it delivered, so its committed
offset stops short of a partition's end. A bounded job ends once the feed's
depth, the end offset less the committed offset, stays at zero over its settling
window, so a marker after the last unit holds the job open with every unit read
([#1728](https://github.com/spate-etl/spate/issues/1728)). Any commit past those
offsets touches INV-1, which [ADR-0002](0002-at-least-once-delivery.md) rests
on.
[ADR-0092](0092-a-kafka-lane-commits-past-offsets-with-no-record.md) settled the
same question for the Kafka source, on a lane's thread and through the
checkpoint tracker. The feed has one consumer thread and its own tracker.

## Considered options

- The consumer thread reads every owned partition's position before a commit
  and raises the partition's next offset to it in the tracker
- A depth that subtracts markers from the end offset
- A commit taken from the end offset the depth thread reads

## Decision outcome

Chosen option: "The consumer thread reads every owned partition's position
before a commit", because a position comes from the group member's own
`read_committed` fetch, and the tracker's existing cap at the first unfinished
offset holds the commit behind any unit below it.

The consumer thread records a delivery before it reads a position, and only that
thread delivers, so a position above the tracker's next offset covers offsets
with no record. The commit path reads positions after its latch check, since
after a `BadMessage` latch librdkafka may have skipped an offset.

A depth that subtracts markers needs a count no read gives, and the gap a
transaction leaves differs by broker. A commit taken from an end offset can pass
records behind an open transaction, which breaks INV-1.

The lane's guards do not carry over. A lane stops reading after a poll error,
and the feed's one queue also carries client errors that name no partition, so a
bar there would hold a job with no later unit from ending. The fetch position
stays at the failed offset after a retryable error, so a marker above it arrives
only after that offset is delivered. A raised next offset never lowers a
delivery: a delivery below it takes the commit point back down as it does today.

### Consequences

- Good, because a bounded job ends after a marker or an aborted transaction, and
  a partition no unit was read from commits past its markers too.
- Good, because a restart starts past the marker, and markers add no commit: the
  raised points join the one commit each pass already sends.
- Bad, because each commit pass takes the client, topic and partition locks once
  per owned partition on the consumer thread.
- Bad, because a group that passes an aborted transaction under `read_committed`
  does not deliver those records after a later switch of level.
- Neutral, because a marker not yet served at a revocation or a stop falls to
  the partition's next owner, and a revocation's commit reads no position.

### Confirmation

INV-1. `the_commit_point_never_passes_an_unfinished_offset` is a property test
of the rule, with position moves in its operations.
`a_marker_on_the_work_topic_ends_a_bounded_job` and
`a_position_from_before_a_rebalance_commits_nothing` pin it on the mock, and
`an_aborted_transaction_at_the_end_of_the_work_topic_ends_a_bounded_job` pins
the aborted range and a restart past it on a real broker.

## More information

Landed with the pull request that closes #1728.
[Kafka feed: Transaction markers](../user-guide/04-connectors/feeds/kafka/README.mdx#transaction-markers)
states the behaviour.
