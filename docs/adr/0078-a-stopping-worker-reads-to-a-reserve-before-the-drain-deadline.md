---
description: "A stopping worker reads until 5 s before the drain deadline, then hands back its unfinished units and drains the sink in the order its backend needs."
---

# ADR-0078 — A stopping worker reads to a reserve before the drain deadline, then hands back what it holds

- **Status:** accepted
- **Date:** 2026-10-24
- **Supersedes:** [ADR-0055](0055-graceful-departure-not-withheld.md)
- **Superseded by:** —

## Context and problem statement

[ADR-0055](0055-graceful-departure-not-withheld.md) hands a gracefully departing
instance's splits back so they move at once. The
[work-feed record](0080-a-work-feed-replaces-leased-fenced-split-coordination.md)
removes the store that hand-back ran through. On SQS a unit a stopped worker
held returns after one visibility period unless it is handed back, and each
hand-back still costs one receive. On Kafka a member that leaves causes a
rebalance. A unit stopped mid-read replays whole (the
[work-feed record](0080-a-work-feed-replaces-leased-fenced-split-coordination.md)).

## Considered options

- Hand back every held unit at once on stop
- No hand-back; units return after one visibility period
- Read up to the drain deadline, then hand back the rest
- Read up to the drain deadline less a fixed reserve, then hand back the rest
  and drain the sink in the order the backend allows, all inside
  `checkpoint.drain_timeout`
- A `feed:` key for the read period, checked at startup to fall below
  `checkpoint.drain_timeout`
- On Kafka, the same order as on SQS, leaving the group before the sink drain

## Decision outcome

Chosen option: "Read up to the drain deadline less a fixed reserve", because
a unit that finishes before then does not replay, a unit that does not is free
for a peer before the pod's grace period ends, and the stop stays inside the
one key operators size against that grace period. Reading to the full deadline
leaves the hand-back and the sink drain outside it, racing the kill. A key of
its own adds a setting and a startup check.

On a graceful stop a worker stops receiving and lets the units it holds
finish reading until the drain deadline (`checkpoint.drain_timeout`) less a
fixed reserve of 5 s. On SQS it then sets every unit it has not finished
visible at once, the sink drains, and the finished units are acknowledged. On
Kafka the sink drains, the worker commits each partition up to its first
unfinished unit, and then it leaves the group, since a member that has left
cannot commit. Each sequence falls inside `checkpoint.drain_timeout`. Renewal
continues until each unit is acknowledged or handed back.

### Consequences

- Good, because on SQS a rolling restart replays only the units still
  unfinished 5 s before the deadline.
- Good, because on SQS a handed-back unit moves to a peer at once, unless SQS
  has received it three or more times and moves it to the back of the queue.
- Bad, because on Kafka a stop cuts every member's in-flight units under the
  eager assignment the
  [work-feed record](0080-a-work-feed-replaces-leased-fenced-split-coordination.md)
  fixes, so the drain saves only the stopping member's units. Each of those
  reads has used an attempt
  ([ADR-0070](0070-on-kafka-a-units-attempt-is-recorded-before-the-unit-is-read.md)).
- Bad, because a stop takes up to the drain deadline, and a unit that needs
  longer than the deadline less 5 s replays whole.
- Bad, because the hand-back runs inside the same budget as the sink drain,
  and a pod killed before it lands returns its units only after one visibility
  period, on Kafka after the session timeout.
- Bad, because the sink drain has the 5 s reserve less 1 s for the feed's close,
  and a longer drain is cut. With a feed on SQS or Kafka, startup refuses a
  `checkpoint.drain_timeout` of 5 s or less (the
  [feed-section record](0079-the-feed-is-configured-in-a-top-level-section-and-provisioned-by-the-operator.md)).
- Bad, because on Kafka a commit waiting on a slow coordinator, and a leave the
  broker does not answer for up to 5 s, finish after the deadline, on the
  consumer thread.
- Bad, because a unit handed back on SQS has used one receive toward
  `maxReceiveCount`.
- Neutral, because SQS refuses a visibility change for a receipt past its
  12-hour hold, by when the message is visible again, and the feed sends none
  past a receipt's local deadline.

### Confirmation

INV-1: a partition commits only up to its first unfinished unit, so a
handed-back unit, and one whose partition a rebalance revokes during the read
period, is read again by the partition's next owner.

`crates/spate-sqs/tests/feed_sqs.rs`:
`a_stopping_worker_finishes_units_inside_the_drain_deadline`,
`a_stopping_worker_hands_back_what_it_still_holds_at_once` and
`the_hand_back_and_sink_drain_finish_inside_the_drain_timeout`.
`crates/spate-kafka/tests/feed_kafka.rs`'s
`a_stopping_worker_commits_finished_units_before_leaving_the_group`. The
[feed-section record](0079-the-feed-is-configured-in-a-top-level-section-and-provisioned-by-the-operator.md)'s
`a_feed_refuses_a_drain_timeout_within_the_read_reserve` pins the reserve's
lower bound.

## More information

- Landed in the pull request that closed #1098.
- [ADR-0059](0059-final-coordinated-commit-runs-under-one-op-timeout-budget.md)
  — the bounded stop under coordination, superseded by
  [ADR-0066](0066-a-feed-trait-replaces-the-split-coordination-traits.md).
- [Graceful shutdown](../user-guide/03-guides/graceful-shutdown.mdx#the-drain-choreography),
  and how each feed stops:
  [SQS](../user-guide/04-connectors/feeds/sqs/README.mdx#stop) ·
  [Kafka](../user-guide/04-connectors/feeds/kafka/README.mdx#stop).
