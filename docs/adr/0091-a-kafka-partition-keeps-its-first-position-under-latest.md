---
description: "Under auto.offset.reset latest, the Kafka source commits a partition's end before it first reads it, so only a first start skips records."
---

# ADR-0091 — A Kafka partition with no commit keeps its first position under `latest`

- **Status:** accepted
- **Date:** 2026-11-04
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The Kafka source commits only offsets a sink has acknowledged, and it reads
`auto.offset.reset` at every assignment for a partition with no committed
offset. Under `latest`, the default, that rule starts the partition at its end
each time. The next commit then moves past everything written before that
assignment. A restart of a quiet partition, a restart with the sink down, a
process joining the group, a member the group removes and a crash before a first
commit all reach it, so INV-1 did not hold for any partition before its first
commit.

## Considered options

- The source reads the partition's end and commits it before any read.
- Default `auto.offset.reset` to `earliest`.
- The source remembers in process where it read a partition with no commit.
- Record the exception on INV-1 and change nothing else.

## Decision outcome

Chosen option: "The source reads the partition's end and commits it before any
read", because it keeps what `latest` means on a group's first start and gives
every later start a position the group holds, in any process. The commit
acknowledges nothing, and it passes only records no member was given.

One consumer reads every offer's committed offsets and end offsets with one
lookup and one listing, since committed offsets belong to the group. Each
consumer then sends its own commit, for the partitions with no commit alone,
ahead of `assign`. The commits are in flight together under one 10 s bound, so
an accept waits for the slowest answer. A failed read or commit holds the
assignment and retries it at a pace, until `startup_timeout` or
`assignment_timeout` ends the run. A permanent refusal stops the run. A refusal
because the group has moved past the member discards its offer.

Defaulting to `earliest` was rejected because a new group would read its topic's
history, and an explicit `latest` would stay exposed for good. Process memory
was rejected because it closes only the routes that stay in one process, and the
position has to reach another process.

### Consequences

- Good, because a restart, a process joining and a removal resume from the first
  position under `latest`, so INV-1 holds for them.
- Good, because a partition has a lag series from its first statistics tick.
- Bad, because a partition added to the topic still starts at its end, past
  records written before the group first assigned it.
- Bad, because a group left empty past its offset retention starts at the end
  again.
- Bad, because a group's first start still skips what came before it, which is
  what `latest` asks for.
- Bad, because an assignment waits on a commit outside the committed-offset
  lookup bound, and a commit that keeps failing can stop a pipeline at the
  assignment deadline.
- Bad, because a record a chain fails on stops the source at every restart, as
  on a partition with a commit.

### Confirmation

A Kafka source partition under `latest` has a committed offset before any lane
reads it, and no later start of that partition skips a record written after that
offset. Each route is pinned under `latest` in the `latest_start` modules of
`tests/mock_cluster.rs` and `tests/kafka_broker.rs`:
`a_quiet_partition_reads_what_arrived_across_a_clean_restart`,
`a_new_group_with_its_sink_down_reads_from_its_start_after_a_restart`,
`a_second_process_joining_a_group_with_no_commit_loses_no_record` and
`a_removed_single_consumer_loses_no_record`.
`a_partition_added_to_a_running_group_starts_at_its_end` pins the exception.

## Evidence

Before this change, a quiet restart, a restart with the sink down, a removed
member and an added partition lost records with one consumer on a real broker at
`v0.1.0` and `v0.4.0`, and a process joining the group did at `v0.4.0`. Each
passed under `earliest`
([#1704](https://github.com/spate-etl/spate/issues/1704)).

## More information

- Landed in [#1723](https://github.com/spate-etl/spate/pull/1723). The Kafka
  source page states the rule, under
  [How offsets are committed](../user-guide/04-connectors/sources/kafka/README.mdx#how-offsets-are-committed).
- [ADR-0002](0002-at-least-once-delivery.md) — the guarantee this restores.
- [ADR-0090](0090-a-kafka-source-runs-a-configured-number-of-consumers.md) — the
  consumers whose starts settle together.
