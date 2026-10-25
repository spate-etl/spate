---
description: "The feed is configured in a top-level feed: section and provisioned by the operator, who creates every queue, topic and notification rule; Spate reads their settings at startup."
---

# ADR-0079 — The feed is configured in a top-level section and provisioned by the operator

- **Status:** accepted
- **Date:** 2026-10-25
- **Supersedes:**
  [ADR-0049](0049-coordination-section-in-the-pipeline-config.md),
  [ADR-0056](0056-in-process-store-selectable-in-coordination-section.md)
- **Superseded by:** —

## Context and problem statement

A source that reads its units through a feed needs a backend, a queue or topics,
a read parallelism, and for a bounded job a location. The `coordination:`
section of [ADR-0049](0049-coordination-section-in-the-pipeline-config.md) held
the equivalent settings for source coordination, and
[ADR-0056](0056-in-process-store-selectable-in-coordination-section.md) let it
select the in-process store.
[ADR-0066](0066-a-feed-trait-replaces-the-split-coordination-traits.md) replaces
coordination with a feed. The question is where the feed's settings live in the
pipeline file, and who creates the queues, dead-letter destinations and
notification rules the feed reads.

## Considered options

- A top-level `feed:` section beside `source:`, with the operator provisioning
  each queue, topic and dead-letter destination, and Spate reading their
  settings at startup
- Keys on the source
- Spate creates the queues, topics and notification rules

## Decision outcome

Chosen option: "a top-level `feed:` section, with the operator provisioning",
because it keeps deployment settings in one section of the file as ADR-0049 did,
and keeps read parallelism a single knob as ADR-0056 did. Source keys would
split one budget across two knobs, the reason ADR-0056 rejected them. Creating
resources needs `sqs:CreateQueue` and `s3:PutBucketNotification`, and S3
replaces a bucket's whole notification configuration on each PUT, so Spate would
own that hazard.

The section names the backend as a single key (`memory`, `sqs` or `kafka`),
`units_held`, `attempt_cap`, `location` and `settling_window`. A backend's own
keys sit in its body: `visibility` is a key of `sqs:`, which renews every third
of it, and `sqs` refuses `attempt_cap`. A source with no `feed:` section runs on
the in-process feed. `units_held` unset leaves the source's own default, so a
derived default is a separate change. The defaults the section carries are a
settling window of 5 minutes, an attempt cap of 10, and a visibility period of
60 s renewed every 20 s.

### Consequences

- Good, because a deployment changes its backend, queue and read parallelism in
  the file.
- Good, because a queue that would cycle a poison unit until retention deletes
  it, or a dead-letter queue that would expire first, is refused at startup. On
  SQS the check refuses a queue with no dead-letter queue, a dead-letter queue
  it cannot read, and one whose `MessageRetentionPeriod` is not longer than the
  source queue's. The check also refuses a FIFO queue and a queue that does not
  report its three message counts as numbers. When either queue reports no
  retention, the comparison is skipped with a WARN.
- Good, because on Kafka startup refuses a missing work, retry, dead-letter or
  attempt topic, an attempt topic with no partitions, a work, retry or
  dead-letter topic that is compacted, an attempt topic that is not, and a work
  topic whose `message.timestamp.type` is not `LogAppendTime`. A broker that
  reports no entry skips that check with a WARN. These checks run against Apache
  Kafka in CI.
- Good, because a `memory` feed that names a location is refused, and a feed
  that hands back refuses a `checkpoint.drain_timeout` of 5 s or less.
- Neutral, because the operator provisions everything and Spate checks only the
  feed's own settings. The startup check needs permission to read both queues'
  attributes or the topics' configuration.
- Bad, because a file with no `feed:`, or naming `memory`, deployed to several
  instances runs the whole job on each with its own lister, and only the startup
  WARN guards against it.
- Bad, because the SQS retention check accepts any margin, and the Kafka feed
  compares no retention: the dead-letter topic's `retention.ms` is the
  operator's to set.
- Bad, because a store-written Kafka work topic left at the broker's default
  `CreateTime` is refused, even for a store whose timestamps are correct.
- Bad, because the default of 96 units held costs up to 2 × 96 × 8 MiB = 1,536
  MiB of read-ahead on large objects, by arithmetic and not measurement, and
  completes at most about `units_held / batch.linger` small units a second per
  worker, 96 at the defaults.

### Confirmation

`crates/spate-core/src/pipeline/tests.rs`: `the_feed_section_defaults` pins the
defaults core owns. `a_source_without_a_feed_rejects_the_feed_section`,
`a_memory_feed_with_a_location_is_refused` and
`a_feed_refuses_a_drain_timeout_within_the_read_reserve` pin the refusals.

`crates/spate-sqs/tests/feed_sqs.rs`:
`startup_refuses_a_queue_with_no_redrive_policy` and
`startup_refuses_an_unreadable_dead_letter_queue`.
`crates/spate-sqs/src/startup.rs`:
`startup_refuses_a_dead_letter_queue_that_does_not_outlive_the_source`, scripted
because ElasticMQ returns no `MessageRetentionPeriod`.
`the_body_is_read_with_its_defaults` in `crates/spate-sqs/src/config.rs` pins
the visibility period and its renewal.

`crates/spate-kafka/tests/feed_kafka.rs`:
`startup_refuses_a_missing_retry_or_dead_letter_topic`,
`startup_refuses_a_compacted_work_retry_or_dead_letter_topic`,
`startup_refuses_a_missing_or_uncompacted_attempt_topic` and
`startup_refuses_a_notification_topic_without_log_append_time`.
`the_body_is_read_with_its_defaults` in `crates/spate-kafka/src/feed/config.rs`
pins the attempt cap.

`crates/spate-s3/tests/request_shape.rs`:
`the_feed_section_sets_read_parallelism`. `the_reader_holds_at_most_96_units` in
`crates/spate-s3/src/unit_reader.rs` pins the default units held.

## Evidence

With 96 units held, 4,608 local objects of 64 KB and 20 ms of added GET latency,
the feed path read 93.4 objects a second at the sink's default 1 s linger and
3,124 at 5 ms. The figures come from a local run on one machine. They show the
linger ceiling and say nothing about throughput on other hardware.

## More information

- Landed in the pull request that closed #1099.
- [ADR-0069](0069-feed-backends-live-in-their-own-crates.md), the backend
  crates.
- [ADR-0010](0010-skip-or-fail-record-error-policies.md) still holds for
  records. A unit's dead-letter destination is the feed's own queue or topic.
- [ADR-0009](0009-yaml-configuration-with-opaque-passthrough.md), for why a
  deployment setting belongs in the file.
- [The `feed:` section](../user-guide/07-reference/configuration.mdx#feed) and
  [Who provisions the feed](../user-guide/02-concepts/09-work-feed.mdx#who-provisions-the-feed).
