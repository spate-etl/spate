---
description: "A Kafka feed partition whose group commit is above its end, as after an unclean leader election, counts from its log start and holds a bounded job with no bound of its own."
---

# ADR-0100 — A Kafka commit above a partition's end holds a bounded job

- **Status:** accepted
- **Date:** 2026-11-13
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

[ADR-0076](0076-a-bounded-job-on-a-feed-ends-on-an-empty-feed-and-a-checked-dead-letter-destination.md)
ends a bounded job once the feed's depth stays at zero over a settling window.
On Kafka the depth is each work and retry partition's end offset less the
group's commit, and a negative difference read as zero.

An unclean leader election can truncate a partition below the group's commit.
Units written after the truncation then sit below the commit. While the
partition's end stays below the commit, the depth read the partition as
drained, and a worker kept out of the group for its whole window ended the job
`Completed`, exit 0, over those units
([#1759](https://github.com/spate-etl/spate/issues/1759)).

Under the feed's `auto.offset.reset` of `earliest`, librdkafka 2.12.1 resolves
such a partition in two of three cases. A member assigned it starts at the
commit, which carries no leader epoch, so its fetch is out of range and resets
to the log start. A member that held it through the truncation moves to the
point of truncation and reads what is written after it, which brings its commit
down. In the third case, a member that held it with nothing written after the
truncation commits nothing, and the commit stays above the end until the group
rebalances. Nothing is unread in that case.

## Considered options

- Count the partition from its log start, and hold the job with no bound
- Count it from its log start, and fail the depth read once the commit has
  stood above the end for 20 reads with the group's commits unchanged
- Fail the depth read as soon as it finds a commit above the end
- Have the member's commit follow its position down to the point of truncation

## Decision outcome

Chosen option: "Count the partition from its log start, and hold the job with
no bound", because it never fails a job that can finish. It follows
[ADR-0094](0094-a-kafka-feed-reads-its-end-at-the-high-watermark.md), which
holds a job with no bound for another writer's open transaction.

Failing at once fails jobs that a member about to join would finish within
seconds, as in the run behind #1759, where the worker could join once a dead
member's session ended. Failing after 20 reads fails the third case, where
nothing is unread, and adds a bound with no measured basis. A commit that
follows the position down never sees the truncation: librdkafka moves its app
position only on a consumed message or a control message, not on the seek its
offset validation makes.

### Consequences

- Good, because a bounded job no longer ends `Completed` while a work or retry
  partition's commit stands above its end.
- Good, because the change is one comparison in the depth read, and the
  tracker, the consumer and the end check keep their code.
- Bad, because in the third case the job waits until the group rebalances. The
  worker's held line names the partition after 20 reads, about five minutes, and
  the operator starts or stops any worker in the group.
- Bad, because a stop during the hold exits `Completed`, exit 0, with the units
  unread, as a stop during ADR-0094's hold does. A later worker on the group
  reads them.
- Bad, because once units written after a truncation carry the end to the
  commit or past it, a member assigned the partition starts at the commit and
  skips the reused offsets below it. The depth cannot see this. Committing with
  the delivered record's leader epoch would let librdkafka detect the
  truncation, and rdkafka 0.39 exposes no setter for it
  ([#2104](https://github.com/spate-etl/spate/issues/2104)).
- Neutral, because a partition whose log holds nothing reads as drained, the
  same rule that applies after retention.

### Confirmation

`crates/spate-kafka/src/feed/depth.rs`:
`a_commit_above_the_end_offset_reads_from_the_log_start` and
`a_held_job_names_a_commit_above_the_end`.
`crates/spate-kafka/tests/feed_mock_cluster.rs`:
`a_commit_above_the_end_holds_a_bounded_job_until_a_member_reads`.
`crates/spate-kafka/tests/feed_kafka.rs`:
`a_member_reads_a_partition_whose_commit_is_above_its_end`, on Apache Kafka and
Redpanda. No committed test runs an unclean election, and the member that holds
a partition through one rests on librdkafka's source.

## More information

Landed in [#2103](https://github.com/spate-etl/spate/pull/2103), which fixes
[#1759](https://github.com/spate-etl/spate/issues/1759). The behavior is
described on the
[Kafka feed page](../user-guide/04-connectors/feeds/kafka/README.mdx#transaction-markers).
