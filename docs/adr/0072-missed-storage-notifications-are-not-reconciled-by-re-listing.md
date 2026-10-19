---
description: "Spate does not reconcile missed storage notifications by re-listing; delivery rests on store settings that each store page states."
---

# ADR-0072 — Missed storage notifications are not reconciled by re-listing

- **Status:** accepted
- **Date:** 2026-10-19
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Continuous ingestion learns of new objects from storage notifications. AWS
designs S3 notifications for at-least-once delivery. Ceph RGW retries only on
a persistent topic. RustFS replays only with a `queue_dir`. A periodic
re-listing would catch a lost event, but it also re-announces every object
already ingested, so it needs a record of what was seen. The work-feed record
removes `refresh_listing`, the re-listing mode that
[ADR-0028](0028-deterministic-split-ids.md)'s create-if-absent made cheap.

## Considered options

- No reconciliation: rely on the store's delivery and document its settings
- Re-list the whole prefix each interval and accept the duplicates
- Re-list against a durable seen-set whose loss costs only duplicates

## Decision outcome

Chosen option: "No reconciliation", because it keeps continuous ingestion
free of durable state, and the stores Spate supports deliver when configured
as their pages state. Re-listing the whole prefix re-reads a growing prefix
at every interval. A seen-set brings back durable state and a deduplication
cost per listing, and is added only when a deployment needs it.

### Consequences

- Good, because a fleet ingesting continuously keeps no state beyond its feed.
- Bad, because a notification the store drops is an object never ingested,
  and nothing in Spate reports it.
- Bad, because correctness depends on the store settings the
  [S3 source](../user-guide/04-connectors/sources/s3/README.mdx#storage-notifications)
  and [SQS feed](../user-guide/04-connectors/feeds/sqs/README.mdx) pages
  state, and Spate observes only one of them. RustFS writes each Kafka
  event with timestamp 0, and time-based retention deletes such records at the
  broker's next retention check. The Kafka feed refuses at connect a work topic
  whose reported `message.timestamp.type` is not `LogAppendTime`, and warns
  about and skips one that reports none. It checks every Kafka work topic,
  since no Kafka job names a per-job location (the job-location record) and
  every one is continuous.
- Neutral, because a seen-set can be added later behind its own trait without
  changing the feed.

### Confirmation

Nothing checks a store's notification settings except the Kafka feed's
timestamp check: `crates/spate-kafka/src/feed/startup.rs`'s
`a_work_topic_not_stamped_on_append_is_refused` pins the check, and
`crates/spate-kafka/tests/feed_kafka.rs`'s
`startup_refuses_a_notification_topic_without_log_append_time` pins that a
topic created with the broker's default is refused.

## Evidence

E-SQS: S3 notifications into SQS while objects are copied in, at N=4, three
reps, reporting event latency from each record's `eventTime` to its receive, S3
duplicate notifications (distinct message ids for one object) and objects never
notified. Informative, with no pass criterion. Measured by
spate-etl/ingestbench `queue-arms` at 7475b28 against spate b7a9a9ce; results
at fc958df. One rep of three completed: 27,144 objects, each notified once and
read once, with event latency 0.83 s at p50, 1.16 s at p90, 3.25 s at p99 and
5.69 s at most. In the second rep S3 sent no notification for the 16,000
objects copied in the first 160 s of the copy, which began at least 21 s after
the bucket's notification configuration was written, so for at least 181 s
after the write; neither the queue nor its dead-letter queue held them
afterwards. The bound on the write comes from the operator machine's clock. The third rep was not run. Objects written before a
notification configuration takes effect reach the feed only through a lister
run. It does not establish delivery for any other store, nor loss under fault.

## More information

- Landed in the pull request that closed #1092, with
  [ADR-0071](0071-a-work-unit-is-one-object-or-one-byte-range-of-one-object.md).
- [S3 source](../user-guide/04-connectors/sources/s3/README.mdx#storage-notifications)
  — each store's notification settings.
