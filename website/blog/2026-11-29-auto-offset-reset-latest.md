---
title: "How auto.offset.reset: latest skipped Kafka records"
description: "librdkafka's auto.offset.reset picks a new partition's start and moves an out-of-range fetch. Under latest, Spate's Kafka source skipped 135 retained records."
slug: auto-offset-reset-latest
authors: [marcus]
tags: [engineering]
draft: true
---

In 0.1.0 and in 0.4.0, Spate's Kafka source could commit a partition past
records no sink had written, while the log still held them. On 0.4.0, under the
default `auto.offset.reset: latest`, a quiet partition restarted with 40 of its
records unwritten, and a group whose commit had fallen below the log start
committed past 135 retained records. This happened because librdkafka's reset
policy does two jobs: it picks where a partition with no committed offset
starts, and it moves a fetch the broker answers as out of range. Each job worked
as librdkafka's documentation says, and Spate's at-least-once guarantee
accounted for neither. 0.5.0 commits a position at each partition's first start
and hands librdkafka `earliest` for a fetch out of range. Each commit now
carries the leader epoch, so a commit that is inside the log but stale is caught
as well. Under `latest`, four starts still skip records, and the end of this
post lists them.

<!-- truncate -->

## One setting picks a start and moves a fetch

Spate's Kafka source runs on librdkafka 2.12.1, through the `rdkafka` crate.
The librdkafka [configuration
reference](https://github.com/confluentinc/librdkafka/blob/v2.12.1/CONFIGURATION.md)
defines `auto.offset.reset` as the "action to take when there is no initial
offset in offset store or the desired offset is out of range". The first case is
a partition the consumer group has never committed. The second is a committed
offset the broker no longer holds, because retention deleted the records at it,
or because the log now ends below it. The default in librdkafka and in Spate,
`latest`, answers both cases by moving to the end of the partition.

Apache Kafka's [consumer
configuration](https://kafka.apache.org/41/configuration/consumer-configs/)
defines the setting the same way, with `latest` as the default. Its one warning
about lost messages covers a partition added to a topic while consumers run
under `latest`. Spate promises at-least-once delivery, and under `latest` each
move to the end could pass records no sink had written.

## A partition with no commit restarted at its end

Spate's [delivery guarantee](/docs/INVARIANTS#inv-1) says a source never commits
past a record the sink has not acknowledged. The Kafka source kept it by
committing only offsets the sink had written. This meant that a partition with
no acknowledged record had no commit, and under `latest` librdkafka started it
at the end every time the partition was assigned. The group assigned the
partition again on every restart, when a second process joined, and when it
removed a member. Each time, the next commit moved past everything written since
the previous start.

I found the first route while testing a change to how several consumers in one
process accept their assignments
([#1696](https://github.com/spate-etl/spate/pull/1696)). A member the group
removed lost the records it had read and could not commit, and running the same
check with one consumer showed the route was older than that change
([#1704](https://github.com/spate-etl/spate/issues/1704)). Each route below ran
against a real broker with one consumer and a session timeout of 6 seconds:

| Route | 0.4.0 | 0.1.0 |
| --- | --- | --- |
| A quiet partition, restarted cleanly, with records written while it was down | committed 80, 40 unwritten | committed 80, 20 unwritten |
| A new group with its sink down, then a restart | committed 100, 60 unwritten | committed 80, 20 unwritten |
| A second process joins while the first has no commit | committed 140, 40 unwritten | not established |
| A member stalls past `max.poll.interval.ms` and the group removes it | the next member started at 20 and committed 40, and records 0–19 were never acknowledged | the same |

Each run produced records until a commit landed, so the figures differ between
releases. 0.4.0 ran against the pinned Apache Kafka image, and 0.1.0 against the
default image of that release's container crate. Under `auto.offset.reset:
earliest` every route passed on both releases, except the second-process route
on 0.1.0. That route reached no result under either policy. 0.2.0 and 0.3.0 were
not run. Up to 0.4.0, Spate's invariants stated no exception for any of these
routes.

## A commit below the log start jumped to the end

The reset policy also moves a fetch that the broker answers as out of range, and
that happens when a group falls behind. Kafka deletes a partition's oldest
segments once they pass the topic's retention, and the log start moves up. A
group that is stopped, or that reads slower than retention deletes, can be left
with a commit below the log start. Its next fetch at the commit was answered out
of range, and under `latest` librdkafka moved the partition to its end. The
source then committed from there, past every record the log still held between
the log start and the end
([#1713](https://github.com/spate-etl/spate/issues/1713)).

The test that showed it ran a group under `earliest` until it committed offset
20, then stopped it. A producer then wrote to a topic with `retention.bytes` and
`segment.bytes` each set to 1 MiB until the log start passed offset 20, and a
second run started under `latest`. On 0.4.0 the second run committed offset
16,408. That commit passed 135 records the log still held, offsets 16,269 to
16,403, and no sink had received any of them. The same run under `earliest` left
none unwritten. A group that keeps running takes the same path when its fetch
position falls below the log start, and a regression test on librdkafka's mock
cluster covers that case as well.

## Why the source commits the end before it reads

I decided to commit each new partition's start before any of its records is
read, because a later start can only begin from a position the group holds. A
restart, a second process, and a member the group removes all start from the
group's committed offsets, and none of them can read another process's memory.
Under `latest` the source now reads each uncommitted partition's end offset,
commits it, and assigns that exact offset
([ADR-0091](/docs/adr/a-kafka-partition-keeps-its-first-position-under-latest)).
That commit acknowledges nothing, since it passes only records no member was
given. A group's first start under `latest` still begins at the end, as `latest`
asks, and every later start resumes from a committed offset.

Before a partition's first message, librdkafka has no position to commit, so the
source lists the ends itself. One consumer reads the committed offsets and end
offsets of a whole assignment with one lookup and one listing, and each consumer
then commits its own partitions that have no commit.

I weighed two other options and rejected both. Defaulting `auto.offset.reset` to
`earliest` was the smaller change, but a new group would then read its topic's
whole history, and anyone who set `latest` explicitly would stay exposed.
Remembering in process where a partition was first read would cover only the
routes that stay inside one process, and a restart or a second process starts
with nothing.

## Under `latest`, librdkafka now gets `earliest`

Once the source settles every start under `latest` itself, librdkafka's reset
policy runs in one case only: the broker no longer holds the fetch position. In
that case `earliest` resumes at the oldest record the broker still holds, so no
retained record is skipped. So when a pipeline is configured with `latest`, the
source now hands librdkafka `earliest` under the same key the configuration
used, `auto.offset.reset` or `topic.auto.offset.reset`
([#1880](https://github.com/spate-etl/spate/pull/1880)).

The regression test commits offset 20, deletes the records below 40, and
restarts the group under `latest`. In 0.5.0 librdkafka logs the reset, and the
source reads every retained record from 40:

```text
WARN librdkafka: [thrd:main]: orders [0]: offset reset (at offset 20 (leader epoch 0), broker 1) to offset BEGINNING (leader epoch -1): fetch failed due to requested offset not available on the broker: Broker: Offset out of range fac="OFFSET"
```

The line names `BEGINNING` under a `latest` configuration, because `earliest` is
what the source passed to librdkafka.

The librdkafka
[fetcher](https://github.com/confluentinc/librdkafka/blob/v2.12.1/src/rdkafka_fetcher.c#L258-L292)
handles every out-of-range answer from a partition's leader in one branch, so it
cannot tell a commit below the log start from a commit past the log end. A
commit past the end, which a truncated log can leave, now also resumes at the
log start, and the partition's retained records are read again. Keeping the end
in that case would mean configuring `error` and seeking each partition from the
source, and that needs a new path from the lane back to the source's controller.

## A commit inside the log can still be stale

An unclean leader election makes a replica that was behind the leader into the
new leader, and the partition's log is cut back to that replica's end. When the
cut lands below the group's commit, the records written afterwards take the
offsets below the commit again. Once the new end passes the commit, the commit
is inside the log, so no fetch is out of range and no reset policy runs. A
member assigned the partition starts at the commit and never reads the records
written to the reused offsets below it
([#2104](https://github.com/spate-etl/spate/issues/2104)).

Kafka detects this case through leader epochs. Each leader of a partition has an
epoch number, and a consumer whose position carries one can ask the leader where
that epoch ended, then move back to the point of truncation. For a position
with no epoch, librdkafka skips that check, and every commit Spate made carried
none, because the `rdkafka` crate 0.39 has no way to set an epoch on a committed
offset. The source now sets the epoch of the record before each committed offset
through `rd_kafka_topic_partition_set_leader_epoch`, from the C bindings that
`rdkafka` re-exports
([ADR-0103](/docs/adr/kafka-commits-carry-the-leader-epoch)).

Committing the epoch was not enough on its own. On Apache Kafka 4.4.0 and on
Redpanda v26.2.6, librdkafka fetched a start handed to `assign` with no epoch
check. So the source now seeks each commit with its epoch after `assign`, and
the leader's answer moves the lane to the point of truncation. A commit above
the partition's end starts from the log start. The Kafka work feed commits
epochs and seeks the same way.

No test runs a real unclean election. The regression tests commit an epoch the
leader never had, and the leader answers it with an offset below the commit.
They run on Apache Kafka and Redpanda, and fail without the change. What each
broker answers after a real unclean election is not established.

## What the fixes cost

Each assignment now does more before a lane reads. The source reads the group's
committed offsets and the partitions' end offsets, and commits the end of every
partition with no commit. The lookup and the commit each wait at most 10
seconds. A partition whose commit carries an epoch adds one `ListOffsets`
request per leader, within the lookup's bound, and one seek after `assign`. I
have not timed any of these steps. They run once per assignment, so their cost
grows with rebalances and restarts. A record's path also gains one leader-epoch
read per batch, and I have not measured that either.

A start that cannot be settled holds the assignment and retries it once a
second, until `startup_timeout` or `assignment_timeout` ends the run. A
permanent refusal, such as a failed group authorization, stops the run at once.
If the chain fails on one of a partition's first records, that record now stops
the source again after every restart, and you have to move the partition's
committed offset past it to go on. Under `latest`, 0.4.0 started past such a
record. A commit past the log end re-reads the partition's retained records.
Under `error`, a detected truncation fails the source.

## What `latest` still skips

Under `latest`, four starts still skip records, and
[INV-1](/docs/INVARIANTS#inv-1) states each one. A group's first start begins at
the end, as `latest` asks. A partition added to the topic starts at its end,
past the records written before the group first assigned it. New partitions
appear when librdkafka refreshes metadata, by default every five minutes. A
group left empty past its offset retention starts at the end again. That
retention is `offsets.retention.minutes` on Apache Kafka and
`group_offset_retention_sec` on Redpanda, and each defaults to seven days. A
commit with no epoch starts without the truncation check, and every commit from
an earlier Spate release has none. A partition whose seek fails also starts
without it.

Setting `earliest` closes the first three. If you need every record of a
partition added to a running topic, or of a group that sits idle for more than a
week, set `auto.offset.reset: earliest` in the `rdkafka` passthrough:

```yaml
source:
  kafka:
    brokers: localhost:9092
    topic: orders
    group_id: orders-etl
    rdkafka:
      auto.offset.reset: earliest
```

A new group then reads the topic's whole history before it reaches new records.

[How offsets are
committed](/docs/user-guide/connectors/sources/kafka#how-offsets-are-committed),
on the Kafka source page, has every start rule, including the `error` policy.

