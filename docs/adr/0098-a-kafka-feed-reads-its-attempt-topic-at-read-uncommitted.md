---
description: "The Kafka feed's attempt follower and its gate read the attempt topic at read_uncommitted, so another writer's open transaction there does not suspend attempt_cap."
---

# ADR-0098 — A Kafka feed reads its attempt topic at read_uncommitted, so an open transaction keeps the attempt cap

- **Status:** accepted
- **Date:** 2026-11-11
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The Kafka feed's attempt follower reads the attempt topic into the ledger, and
the gate admits a unit once the follower has read up to the attempt topic's
end. [ADR-0094](0094-a-kafka-feed-reads-its-end-at-the-high-watermark.md) gave
only the depth client `read_uncommitted`, so the follower read at librdkafka's
default `read_committed` and the gate listed the end through the producer at
the same level. Another writer's open transaction on the attempt topic then
hides the feed's own attempt writes behind it from the follower. Each read of a
failing unit counts as its first, and `attempt_cap` stops working until the
transaction resolves
([#1765](https://github.com/spate-etl/spate/issues/1765)). Spate itself writes
no transactions, so this needs a foreign transactional writer on the feed's
own attempt topic.

## Considered options

- The follower and the gate read at `read_uncommitted`
- The gate lists the log end and the follower stays at `read_committed`, so
  the gate holds until the transaction resolves
- The gate compares the last stable offset with the log end per partition, and
  puts back only that partition's units
- The feed page states the gap, and the code stays as it is

## Decision outcome

Chosen option: "The follower and the gate read at `read_uncommitted`",
because it is the only option that keeps both the cap and the feed's progress
while a transaction is open. The follower's role owns `isolation.level`, and
the gate lists the attempt topic's ends through a consumer of its own, built
with the follower's settings and never assigned. The producer keeps
librdkafka's default and logs no warning for a consumer property. Listing
through the follower's own consumer would queue each ListOffsets behind the
follower's Fetch, which the broker holds for up to `fetch.wait.max.ms` once the
follower is caught up: one pass over 80 attempt partitions then took 31 s and
put back 20 units, where a consumer of its own took 22 ms. Both halves are needed: with only the follower changed, a gate that
lists the last stable offset admits a unit before the follower has read the
feed's own write.

The group member keeps `read_committed`, so
[ADR-0093](0093-a-kafka-feed-commits-past-offsets-with-no-record.md)'s commit
from its position is unchanged. The follower commits nothing.

The gate at the log end with the follower unchanged holds every unit of the
pass for `delivery_timeout` and then puts them back, for as long as the
transaction lasts. A per-partition comparison needs a client at each level or
the admin ListOffsets through the C API, an extra listing per partition per
pass, and still holds that partition. Stating the gap leaves a failing unit
read at loop speed against the object store.

### Consequences

- Good, because a unit on a feed with another writer's open transaction on its
  attempt topic is dead-lettered after `attempt_cap` reads, and the feed keeps
  reading.
- Bad, because the follower applies foreign records whether their transaction
  commits or aborts. A foreign record that is not an attempt record stops the
  feed, aborted or not, and a well-formed one keyed by a real unit changes that
  unit's count.
- Neutral, because a committed foreign record that is not an attempt record
  already stopped the feed, and the page asks for an attempt topic per group.

### Confirmation

`an_open_transaction_on_the_attempt_topic_keeps_the_attempt_cap` pins the cap
on a real broker. `only_the_depth_client_and_the_follower_read_uncommitted`
pins the level on each of the six roles.

## Evidence

On Apache Kafka and on Redpanda, with one valid foreign attempt record held in
an open transaction on each attempt partition and `attempt_cap: 2`, the feed
wrote 267 and 374 attempt records for a unit whose object was gone, and no
dead-letter record. With both changes it wrote two and then the dead-letter
record, on both brokers. With only the follower changed, the result turned on
timing: Apache Kafka passed in each of six runs, and Redpanda wrote 24 to 26 in
three of five.

## More information

- Landed in the pull request that closes #1765.
- [Kafka feed](../user-guide/04-connectors/feeds/kafka/README.mdx) states the
  isolation level of each client.
