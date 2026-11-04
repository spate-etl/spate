---
description: "A Kafka lane reads the position on an empty poll and retires offsets with no record through the checkpoint tracker, so lag reaches zero past a transaction marker."
---

# ADR-0091 — A Kafka lane retires offsets with no record through the checkpoint tracker

- **Status:** accepted
- **Date:** 2026-11-04
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A transaction marker takes an offset in a Kafka log and is never delivered, and
under `read_committed` an aborted transaction's records are skipped the same way.
The Kafka source stores offsets only from acknowledged batches, with
`enable.auto.offset.store` forced off by
[ADR-0009](0009-yaml-configuration-with-opaque-passthrough.md), so its committed
offset stops after the last delivered record. `spate_source_lag_records` and the
broker's group lag then stay above zero on a caught-up partition, and an alert on
lag not reaching zero fires on a healthy pipeline
([#1709](https://github.com/spate-etl/spate/issues/1709)). Any commit past those
offsets touches INV-1, which [ADR-0002](0002-at-least-once-delivery.md) rests on.

## Considered options

- A lane reads the position when a poll comes back empty, and returns a batch with
  no payloads whose ack covers the offsets past what it issued
- The controller reads the position and stores it beside the watermarks
- Correct the lag figure and leave the committed offset alone
- Document the reading and change nothing

## Decision outcome

Chosen option: "A lane reads the position when a poll comes back empty", because
the ack takes its place in the tracker behind every batch the lane issued, so a
pending or failed batch holds the commit back with no new check, and the empty
return is the one point where nothing popped is unissued.

A record moves librdkafka's position when it is popped, before its batch is
issued, so a read from the controller thread can pass a record the tracker has not
heard of. That rules out the second option, and with it the statistics snapshot's
`app_offset`, which is a stale copy of the same figure. Correcting the figure alone
leaves the broker's group lag reporting the same offsets, which
`docs/METRICS.md` calls comparable with the framework's, and a restart reading high
until the lane is polled. Documenting the reading leaves the alert firing.

### Consequences

- Good, because the framework's lag, the broker's group lag and the stored-offset
  lag agree on a caught-up partition, and a restart starts past the marker.
- Good, because a batch with no payloads counts against the pending ceiling, so a
  stream of aborted transactions behind a stalled head cannot grow the tracker.
- Bad, because an empty return now takes the client, topic and partition locks. A
  counter bounds the rate on a thread polling with no timeout, with no measurement
  behind its value.
- Bad, because a group that passes an aborted transaction under `read_committed`
  does not deliver those records after a later switch to `read_uncommitted`.
- Neutral, because a poll error or a main-queue rewind stops the read, leaving the
  committed offset where it stood and the lag reading above zero.

### Confirmation

INV-1. `an_acknowledgement_held_past_its_batch_holds_the_marker_back`,
`a_failed_batch_before_a_marker_holds_the_commit` and
`real_broker_lag_reaches_zero_after_a_committed_transaction` pin the property on the
mock and on a real broker. `a_commit_never_passes_an_unacknowledged_record` is a
property test of the rule.

## More information

[#1709](https://github.com/spate-etl/spate/issues/1709) reports the defect.
[ADR-0090](0090-a-kafka-source-runs-a-configured-number-of-consumers.md) holds the
consumer model the lanes run under.
[Kafka source: Transaction markers](../user-guide/04-connectors/sources/kafka/README.mdx#transaction-markers)
states the behaviour and its readings.
