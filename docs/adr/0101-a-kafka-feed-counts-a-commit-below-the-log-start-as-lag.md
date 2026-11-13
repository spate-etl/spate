---
description: "A Kafka feed counts lag from a commit below the log start, commits past the deleted offsets, and reports what a new assignment could not read."
---

# ADR-0101 — A Kafka feed counts a commit below the log start as lag and reports the offsets it skips

- **Status:** accepted
- **Date:** 2026-11-13
- **Supersedes:**
  [ADR-0076](0076-a-bounded-job-on-a-feed-ends-on-an-empty-feed-and-a-checked-dead-letter-destination.md)
  (in part: on Kafka, a commit below its log start counts from the commit)
- **Superseded by:** —

## Context and problem statement

[ADR-0076](0076-a-bounded-job-on-a-feed-ends-on-an-empty-feed-and-a-checked-dead-letter-destination.md)
counts a Kafka partition's lag from its log start when the group's commit is
below it. A worker that holds a unit caps the group's commit at that unit's
offset. When retention deletes the unit's message, the commit falls below the
log start and every peer reads the partition as drained. A peer then ends its
job and leaves the group, and the rebalance its exit causes takes the unit from
the holder ([#1758](https://github.com/spate-etl/spate/issues/1758)).

The feed runs eager assignment, so a rebalance revokes every partition, and a
held receipt then answers `Gone`. The next owner starts at the group's commit,
which the broker no longer holds, and librdkafka resets the partition to its log
start under `auto.offset.reset: earliest`. Nothing in Spate sees the reset. When
no record follows the gap, the commit never passes it, because librdkafka sets
the position a commit reads only from a consumed message. The job ends
`Completed` with the unit unread, which touches INV-1.

## Considered options

- Count lag from the commit, move each owner's commit past offsets below the
  log start from its own floor, and report the range each assignment could not
  read
- Count lag from the commit and nothing more
- Stall the job on a reported range, through a marker on the dead-letter topic
- Detect the reset through `auto.offset.reset: error`
- Keep counting from the log start, and say so on the feed's page

## Decision outcome

Chosen option: "Count lag from the commit, move each owner's commit past
offsets below the log start, and report the range", because a holder that keeps
its partition finishes its unit, a gap at a partition's end still lets a job
end, and a unit lost to a rebalance shows on the worker that lost it.

The depth thread stores each partition's log start after a read of the feed's
depth. The consumer thread uses those log starts before each commit, beside the
position read of
[ADR-0093](0093-a-kafka-feed-commits-past-offsets-with-no-record.md), so one
thread still delivers and raises a partition's next offset. The floor it raises
from is the tracker's next offset, or the committed offset the assignment
started at when nothing has been delivered. A floor that is unknown raises
nothing, because the tracker reads an unknown floor as 0, and a raise from 0
could move a group's commit back. The commit point stays capped at the first
unfinished offset, so a held unit still holds the commit and its peers still
see lag.

The range an assignment reports runs from the committed offset it started at
up to the log start, or up to the first offset it delivered where that is
lower. It reports once per assignment, with a `warn` line and
`spate_kafka_feed_offsets_skipped_total{topic}`.

Counting from the commit alone holds a bounded job open for ever over a gap at a
partition's end, since the position never passes it. A stall needs a marker
design, and the reported range cannot tell a unit lost to a rebalance from units
retention deleted before anyone read them, so a stall would also end a job
`Stalled` on the second case.
[#2107](https://github.com/spate-etl/spate/issues/2107) holds that design.
rdkafka 0.39.0 returns a consumer error event without its topic or partition,
so an `error` reset does not tell the feed which partition reset. Counting
from the log start leaves the loss silent.

### Consequences

- Good, because a held unit whose message retention deletes keeps the job open
  on every worker until its holder finishes it.
- Good, because a partition whose last messages retention deleted commits up to
  its log start, and the job ends.
- Good, because a unit lost to a rebalance after its message is deleted shows as
  a `warn` line and a counter on the next owner.
- Bad, because a rebalance during the hold still loses the unit, and the job can
  end `Completed` without it. INV-1 states it as an exception.
- Bad, because the report and the commit past the log start run only while a
  bounded job reads its depth, over the settling window.
- Bad, because a worker that could not read the group's committed offsets at
  assignment raises nothing on a partition until it reads a record there, so a
  gap at the end of a partition it has read nothing from holds the job open
  until the next assignment.
- Bad, because a partition with no commit yet still counts from its log start,
  so for one commit interval after a first delivery a deleted unit reads as
  drained.
- Neutral, because a later assignment that starts at the same commit counts the
  range again, and a transaction marker in the range counts as one offset.
- Neutral, because a record fetched before the deletion can wait in librdkafka's
  fetch queue past the raise. Its delivery takes the commit point back down.

### Confirmation

INV-1. `the_commit_point_never_passes_an_unfinished_offset` is a property test
of the cap with log-start raises in its operations.
`a_unit_whose_record_is_deleted_while_held_is_read_to_its_end`,
`a_gap_at_a_partitions_end_does_not_hold_a_job_open` and
`a_unit_lost_to_a_rebalance_after_its_record_is_deleted_is_reported` pin the
three cases on a real broker.

## More information

Landed with the pull request that closes #1758.
[ADR-0093](0093-a-kafka-feed-commits-past-offsets-with-no-record.md) is the
rule for raising a partition's next offset on the consumer thread.
[Kafka feed: When retention deletes a unit's message](../user-guide/04-connectors/feeds/kafka/README.mdx#when-retention-deletes-a-units-message)
states the behavior.
