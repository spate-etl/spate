---
description: "A Kafka feed message with no key is a unit named by its position, topic/partition/offset, so producers that send no key, such as Ceph RGW, are read."
---

# ADR-0083 — A keyless Kafka message names its unit by its position

- **Status:** accepted
- **Date:** 2026-10-27
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The Kafka feed takes a message's key as its unit's id, and sends a message
with no key to the dead-letter topic unread. Ceph RGW produces every Kafka
notification with no key (`publish_internal` in Ceph v20.2.4
`src/rgw/rgw_kafka.cc` passes `nullptr, 0`), and has no SQS target, so no RGW
notification reaches a reader. The
[work-feed record](0080-a-work-feed-replaces-leased-fenced-split-coordination.md)
states that RGW reaches continuous ingestion through a Kafka feed.
[ADR-0066](0066-a-feed-trait-replaces-the-split-coordination-traits.md) lets
two units share an id, and no backend deduplicates by it.

## Considered options

- Name a keyless message's unit by its position, `<topic>/<partition>/<offset>`
- Name it by a digest of its body
- Add the key in a relay outside Spate
- Choose the fallback with a configuration key
- Keep dead-lettering keyless messages

## Decision outcome

Chosen option: "Name a keyless message's unit by its position", because the
position names a message an operator can fetch, and needs neither a setting
nor a process outside Spate. The position is the message's own, on whichever
topic it was read from. Its attempts count under its publish identity as any
unit's do
([ADR-0070](0070-on-kafka-a-units-attempt-is-recorded-before-the-unit-is-read.md)).
A message with an empty key keeps the empty key as its id, and a message whose
key is not UTF-8 still goes to the dead-letter topic at once.

A keyless unit's retry and dead-letter copies carry its id as their key, as
every copy does, so a retry copy is read back under the same id.

### Consequences

- Good, because RGW's notifications are read, as the work-feed record states.
- Good, because the id of a keyless unit names one message, and two positions
  never format to the same text, since a topic name cannot hold `/`.
- Bad, because a keyless message from any producer is read and counted, and
  reaches the dead-letter topic only at `attempt_cap`.
- Bad, because a keyless unit's copies carry a key the original lacked, and
  the producer places them on the retry and dead-letter topics by it.
- Neutral, because a producer can write a key that reads as a position, and
  the two units then share an id, which ADR-0066 allows.
- Neutral, because a unit re-enqueued or redriven from the dead-letter topic
  keeps the position of the message that first carried it.

### Confirmation

`crates/spate-kafka/src/feed/tests.rs`: `a_message_names_a_unit_by_its_key`,
`a_full_gate_queue_of_keyless_units_pauses_the_partitions` and
`a_keyless_message_logs_no_dead_letter_warning`.
`crates/spate-kafka/tests/feed_mock_cluster.rs`:
`a_message_without_a_key_takes_its_position_as_its_id` and
`a_keyless_unit_keeps_its_position_through_retries`.
`crates/spate-kafka/tests/feed_kafka.rs`'s
`a_keyless_message_is_read_as_a_unit` pins the null key against Apache Kafka in
the Docker job.

## More information

- Landed in the pull request that partly addressed #1281.
- [ADR-0066](0066-a-feed-trait-replaces-the-split-coordination-traits.md) —
  unit ids may be shared.
- [ADR-0070](0070-on-kafka-a-units-attempt-is-recorded-before-the-unit-is-read.md)
  — the publish identity attempts count under.
- [ADR-0082](0082-a-notification-key-is-read-as-written-when-its-record-names-ceph-rgw.md)
  — how the S3 source reads an RGW notification's key.
- [Kafka feed](../user-guide/04-connectors/feeds/kafka/README.mdx#message-mapping)
  — the message mapping.
