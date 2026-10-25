---
description: "A work feed replaces leased, fenced split coordination: a queue of units fed by a lister and storage notifications, read directly by every instance."
---

# ADR-0080 — A work feed replaces leased, fenced split coordination

- **Status:** accepted
- **Date:** 2026-10-25
- **Supersedes:** [ADR-0024](0024-coordination-store-external-kv.md),
  [ADR-0025](0025-embedded-consensus-rejected.md),
  [ADR-0026](0026-coordination-fencing.md),
  [ADR-0030](0030-split-record-layout.md),
  [ADR-0032](0032-s3-always-coordinated.md),
  [ADR-0038](0038-leader-computed-sticky-assignment.md),
  [ADR-0039](0039-split-revocation-cooperative-then-forced.md),
  [ADR-0041](0041-cancel-a-withdrawn-revocation.md),
  [ADR-0050](0050-coordination-stores-may-declare-a-polled-watch.md),
  [ADR-0060](0060-the-plan-record-names-the-worker-whose-election-wrote-its-generation.md),
  [ADR-0061](0061-a-watch-delete-orders-above-what-its-handle-has-seen.md) (its
  delete-revision rule;
  [ADR-0065](0065-the-dynamodb-coordination-store-is-removed.md) took the
  DynamoDB exception)
- **Superseded by:** —

## Context and problem statement

A source with no broker of its own, such as an object-storage prefix, needs
something to divide its work between instances. `spate-coordination` did it
with a leader-run planner, leases, an epoch-fenced progress record in an
external store and cooperative revocation. Object stores publish "object
created" notifications to queues, and those queues provide at-least-once
delivery, a visibility lease, redelivery and dead-lettering. The question is
whether Spate keeps building the division of work itself or reads it from a
queue.

## Considered options

- Keep `spate-coordination`, on NATS JetStream KV or etcd.
- A work feed: a queue of work units that a lister and storage notifications
  fill, read directly by every instance.
- Static sharding, each of N instances taking `hash(key) mod N`.

## Decision outcome

Chosen option: "the work feed", because the queue provides the delivery, lease
and redelivery that the coordination protocol rebuilt, and continuous ingestion
from a store that publishes notifications needs a queue in any case. Three
backends ship: in-process, SQS standard queues and Kafka. NATS JetStream is a
candidate fourth that this project has not tested; a work-queue stream has
acknowledgement, redelivery, a delivery cap and progress extension, and it is
added when a user asks. Static sharding is rejected for the reason of
[ADR-0023](0023-dynamic-work-stealing-coordination.md): it cannot rescale
mid-job and bounds skew by luck.

The ack rule holds on every backend. A source acknowledges a unit only after
the sink has acknowledged every record read from it, and a unit whose read stops
before its end leaves the feed through redelivery. A held unit is renewed until
it is acknowledged or given up. On SQS a failed unit is made visible at once and
a unit past `maxReceiveCount` moves to the dead-letter queue. On Kafka a failed
unit is written to a retry topic and, past the attempt cap, to a dead-letter
topic, and the work topic's offset completes only after the retry write is
confirmed. Kafka assigns partitions eagerly, so every rebalance cuts every
in-flight unit of its consumer-group generation. The in-process feed serves one
instance, keeps nothing across a restart and lists its source's prefix itself.
v0.3.0 deprecated the coordination crate, and this record is its removal.

### Consequences

- Good, because the object-storage source loses its leader, leases, fence and
  store, and one instance needs no store.
- Good, because one feed type serves a backfill and continuous ingestion, each
  on its own queue.
- Bad, because on AWS a notification queue must sit in its bucket's region.
- Bad, because the crate, its stores and its features are removed, and a
  coordinated job started on v0.3.0 cannot resume across the upgrade. It
  finishes on v0.3.0 or runs again through the feed, which lists the prefix
  again.
- Bad, because a deployment that runs NATS and no other queue has no feed
  backend for a fleet.
- Bad, because the AWS SDK returns for SQS users.
- Bad, because `refresh_listing` is removed, so a store with no S3-format bucket
  notifications, such as Garage, has no continuous ingestion, and a backfill on
  such a store can miss keys. Ceph RGW reaches continuous ingestion through a
  Kafka feed.
- Bad, because a single instance that must resume runs as a fleet of one with a
  durable feed, a dead-letter destination and a separate lister run.
- Bad, because a unit replays whole whenever its worker stops before the sink
  acknowledges all of it, where a coordinated graceful stop drained to a fenced
  commit. No in-tree final commit blocks the controller thread, and a deferred
  acknowledgement at the final pass costs a redelivery.
- Bad, because SQS counts every receive toward `maxReceiveCount`, a graceful
  hand-back's included.
- Bad, because nothing fences a stopped worker, so two workers can emit the same
  unit.
- Bad, because SQS holds a unit for at most 12 hours from its first receive, so
  unit size has to leave room for the slowest sink.
- Bad, because a queue deletes units nobody read at the end of its retention.
  SQS retains for 4 days by default and at most 14, counted from first enqueue.
  Kafka deletes at the topic's `retention.ms`, and a consumer group with no
  members loses its offsets after `offsets.retention.minutes`. Nothing enforces
  the replay bound.
- Bad, because Kafka commits in order per work-topic partition within one group
  generation, and under eager assignment every rebalance cuts every in-flight
  unit.
- Bad, because a Kafka feed spreads units across at most as many instances as
  its topic has partitions.
- Bad, because SQS charges per request.
- Bad, because the quarantine gauge `spate_coordination_splits_quarantined`
  leaves with the crate and nothing in the framework replaces it. A bounded job
  with a dead-lettered unit ends stalled, and a continuous feed's dead letters
  have no framework signal.

### Confirmation

The ack rule:
`a_unit_is_acknowledged_only_after_its_last_record_is_acknowledged` in
`crates/spate-s3/tests/feed_driver.rs` and
`a_unit_is_deleted_only_after_its_last_record_is_acknowledged` in
`crates/spate-sqs/tests/feed_sqs.rs`, which runs in the Docker job.

Renewal: `held_units_are_renewed_while_the_sink_stalls` in
`crates/spate-sqs/tests/feed_sqs.rs` and `only_marked_receipts_are_extended` in
`crates/spate-sqs/src/feed/tests.rs`. `an_extension_is_clamped_to_the_time_left`
in the same file pins the clamp.

Failure: `a_failed_unit_is_set_visible_with_a_zero_visibility` in
`crates/spate-sqs/src/feed/tests.rs`.

The Kafka overrides: `an_eager_assignment_strategy_override_is_refused` in
`crates/spate-kafka/src/feed/config.rs` and
`the_offset_completes_only_after_the_retry_write_is_confirmed` in
`crates/spate-kafka/tests/feed_mock_cluster.rs`, which runs against
librdkafka's mock cluster.

A ranged unit: `a_whole_unit_over_a_large_object_is_read_in_ranges` in
`crates/spate-s3/tests/feed_driver.rs` and
`a_unit_read_in_ranges_is_deleted_after_its_last_range_is_acknowledged` in
`crates/spate-sqs/tests/feed_sqs.rs`.

The in-process cap:
`in_process_units_are_acknowledged_or_dead_lettered_at_the_cap` in
`crates/spate-s3/tests/feed_driver.rs`, and
`a_unit_at_the_cap_moves_to_the_in_memory_dead_letter_list` and
`the_attempt_cap_sets_the_delivery_that_dead_letters` in
`crates/spate-core/src/feed/memory.rs`.

The solo run: `a_solo_bounded_job_ends_with_no_settling_window`,
`a_solo_job_with_a_dead_lettered_unit_ends_stalled` and
`a_solo_run_given_a_feed_in_code_keeps_running` in
`crates/spate-s3/tests/job_end.rs`, and
`a_solo_run_publishes_the_objects_the_s3_client_cannot_address` in
`crates/spate-s3/src/lister.rs`.

The reader's wake rule: `every_lane_wakes_the_readers_park` in
`crates/spate-s3/src/unit_reader.rs`, and `end_of_input_wakes_the_control_plane`
and `poison_wakes_the_control_plane` in `crates/spate-s3/src/lane.rs`.
`the_runtime_closes_its_feed_after_the_final_commit` in
`crates/spate-core/src/pipeline/tests.rs` pins the order of the final commit and
the feed's close.

SQS accepts a stale receipt handle, so the backend treats a successful
`DeleteMessage` as no evidence of a held lease, and no test depends on
ElasticMQ's rejection of one. Nothing enforces the replay bound.

## Evidence

The AWS benchmark of the queue arms against the static split and the
NATS-coordinated fleet, run in one session (`s4-20261011`). Measured by
spate-etl/ingestbench `queue-arms` at 7475b28 against spate b7a9a9ce; results at
fc958df. Each figure is a mean of `decoded_bytes_per_s` over the arm's reps, in
GB/s, with its spread (maximum less minimum), and criterion 1 holds an arm's
mean at or above the NATS-coordinated (C-N) mean less C-N's spread.

| N | Arm | Reps | Mean | Spread | Tail (s) |
|---|---|---|---|---|---|
| 4 | Static split | 1 | 45.32 | | 244.14 |
| 4 | NATS-coordinated | 3 | 41.98 | 0.449 | 19.44 |
| 4 | SQS queue | 3 | 46.15 | 0.099 | 20.04 |
| 4 | Kafka queue | 3 | 46.12 | 0.054 | 20.74 |
| 8 | Static split | 1 | 90.64 | | 282.49 |
| 8 | NATS-coordinated | 3 | 59.81 | 1.911 | 31.14 |
| 8 | SQS queue | 3 | 92.23 | 0.202 | 20.22 |
| 8 | Kafka queue | 1 | 92.24 | | 20.89 |

Criterion 1 passed at both sizes. The threshold is 41.53 GB/s at N=4 and 57.90
GB/s at N=8. The SQS mean is above the NATS-coordinated mean, at 109.9% of it at
N=4 and 154.2% at N=8, and at 101.8% of the static split at both. The
NATS-coordinated fleet reached 92.6% of the static split at N=4 and 66.0% at
N=8. The Kafka arms are reported and not gated.

The tail is the time from 95% of the expected rows to the last row (criterion
3). The SQS tail sits within the NATS-coordinated mean plus its spread (20.12 s)
at N=4 by 0.08 s, which is not a clear margin, and by 12.95 s at N=8.

The SQS kill arm at N=4 ran three reps. Each killed host held 96 units, each
unit was gained again exactly once and none that the host did not hold was
regained. The Kafka kill arm at N=4 held 103, 103 and 97 units at the kill, with
288, 289 and 288 units gained elsewhere and between 48.9 s and 192.4 s to
resume all of them. The small-object arms are in
[ADR-0071](0071-a-work-unit-is-one-object-or-one-byte-range-of-one-object.md),
and the notification arm, in which one rep lost rows, in
[ADR-0072](0072-missed-storage-notifications-are-not-reconciled-by-re-listing.md).

The arms implemented the coordination traits at b7a9a9ce with units enqueued
before the fleet started, so the figures do not cover the feed trait, a lister
running beside consumption, a worker paused past its visibility timeout, units
from notifications, the Kafka attempt record and its retry and dead-letter
topics, ranged units, dead-lettering or the startup checks. The SQS arm kept the
newer receipt of a second delivery and did not read the unit again, where the
feed trait reads it again. SQS ran over TLS through a VPC interface endpoint,
and NATS ran without TLS inside the VPC. The Kafka arm at N=8 ran once.

## More information

- Landed in the pull request that closed #1100.
- [ADR-0023](0023-dynamic-work-stealing-coordination.md) — the reason static
  sharding is not an option.
- [ADR-0025](0025-embedded-consensus-rejected.md) — the embedded consensus the
  coordination protocol rejected.
- [ADR-0028](0028-deterministic-split-ids.md) — its derivation still names a
  unit, and its create-if-absent property does not hold on a queue.
- [ADR-0065](0065-the-dynamodb-coordination-store-is-removed.md) — the DynamoDB
  exception to the delete-revision rule.
- [ADR-0066](0066-a-feed-trait-replaces-the-split-coordination-traits.md) — the
  feed trait.
- [Work feed](../user-guide/02-concepts/09-work-feed.mdx) — the ack rule and
  redelivery as the user guide states them.
