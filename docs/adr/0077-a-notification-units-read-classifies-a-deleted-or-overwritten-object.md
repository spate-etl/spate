---
description: "On a notification unit, a 404 poisons, a 403 fails the pipeline, a 412 re-enqueues the key at its current ETag, and a named version is read."
---

# ADR-0077 — A notification unit's read classifies a deleted or overwritten object

- **Status:** accepted
- **Date:** 2026-10-24
- **Supersedes:** [ADR-0035](0035-s3-poison-policy.md) (in part: a
  precondition failure on a notification unit)
- **Superseded by:** —

## Context and problem statement

[ADR-0035](0035-s3-poison-policy.md) classifies object-storage failures by
scope. A permission denial fails the pipeline, and a failed precondition
poisons the split. A unit from a storage notification
([ADR-0071](0071-a-work-unit-is-one-object-or-one-byte-range-of-one-object.md))
names an object that may change after the event was sent. It may be deleted
(403 to a role without `s3:ListBucket`, 404 to one with it), encrypted under a
key the worker lacks, or overwritten before or during the read. AWS once
documented that two concurrent writes to an object without versioning may send
one notification (`awsdocs/amazon-s3-userguide@3e8e42ad`, 2021), so
acknowledging an overwritten unit can drop the only unit for the key's current
content.

## Considered options

- 403 and 404 poison a notification unit, and a 412 on a key without
  versioning acknowledges it
- Keep ADR-0035 as it stands and require `s3:ListBucket` on worker roles
- 404 poisons and 403 stays fatal; a 412 acknowledges the unit and counts it
- 404 poisons and 403 stays fatal; a 412 re-reads the key without the
  precondition
- 404 poisons and 403 stays fatal; a 412 re-enqueues a unit for the key at
  its current ETag, then acknowledges

## Decision outcome

Chosen option: "404 poisons and 403 stays fatal; a 412 re-enqueues a unit for
the key at its current ETag, then acknowledges", because it covers deletes and
overwrites without losing an overwrite that sent no notification, and keeps
ADR-0035's fast failure for a fleet that lost its read permission. A poisoned
403 would dead-letter units one by one after a GetObject grant is revoked.
Acknowledging a 412 alone drops the only unit for a key whose overwrite sent no
notification. An unpinned re-read can splice two versions under a further
overwrite.

For a unit from a notification, a 404 on the object read poisons the unit, and a
403 fails the pipeline. A precondition failure on a key without versioning, or a
416 a store sends for a range past the end of a smaller new object before it
checks the precondition, stops the read, reads the key's current ETag with a
HEAD, and publishes a unit for the key at that ETag through the feed trait
([ADR-0066](0066-a-feed-trait-replaces-the-split-coordination-traits.md)).
Once the feed confirms the new unit and the sink has acknowledged every record
already emitted from the old one, the old unit is acknowledged without
dead-lettering. The unit it publishes is a notification unit with the old
unit's id. On SQS it goes to the queue the old unit came from, and on Kafka to
the work topic, where it starts its own attempt count
([ADR-0070](0070-on-kafka-a-units-attempt-is-recorded-before-the-unit-is-read.md)).
It carries a re-enqueue count one higher than the old unit's. A unit whose
re-enqueue count has reached the cap is, on a 412, acknowledged without
re-enqueueing, and the worker counts its object in a metric; the key's later
versions arrive through their own notifications. The cap is the `feed:`
section's `attempt_cap`, which the
[feed-section record](0079-the-feed-is-configured-in-a-top-level-section-and-provisioned-by-the-operator.md)
settles, and 10 when the section sets none or the backend refuses the key. The
worker also counts in a metric each object whose read a 412 cut short, once per
delivery. A HEAD that answers 404 is handled as a 404 on the read, and one that
answers 403 as a 403. A HEAD that reports the ETag the read was pinned to
poisons the unit, since a unit at that ETag fails the same way. A publish the
feed refuses or cannot confirm fails the old unit back. Units from the lister
keep ADR-0035's classification.

A notification unit carries the object's ETag. The worker sends that ETag in
`If-Match` in its quoted form, adding the quotes when the event leaves them
out. A notification unit names a version when its record's `versionId` is a
string other than `""` and `"null"`; the worker reads that version, and a
unit that names none is a key without versioning. A precondition failure on a
read that names a version poisons the unit.

### Consequences

- Good, because a deleted object costs one unit, an overwritten key is read at
  its current content, and a fleet without read permission still fails fast.
- Bad, because an object overwritten while its unit is read delivers the
  records read before the overwrite, and the rest of that version is never
  ingested.
- Bad, because each range read under the old ETag costs one HEAD, and each
  overwritten key one more queue message per delivery. A key
  overwritten faster than its read is re-enqueued on every overwrite up to
  the cap, and its final version is read once for each unit still in the
  chain. Two notifications for one overwritten key start two chains.
- Bad, because an overwrite that sent no notification is lost once its key
  has been re-enqueued as many times as the cap. The metric counts each such
  object. A message forged with a count at the cap drops only its own key's
  re-enqueue, which a writer to the queue could do as well by sending nothing.
- Bad, because a KMS denial on one object fails each worker that receives its
  unit, until `maxReceiveCount` dead-letters it.
- Bad, because a worker role without `s3:ListBucket` reads a deleted object as
  403 and fails.
- Bad, because a worker role without `s3:GetObjectVersion` fails on every
  notification unit from a versioned bucket, and a role with it lets anyone
  who can write to the feed have a past version read.
- Bad, because a worker without permission to send to the feed, or to use an
  encrypted queue's key, fails the pipeline at its first re-enqueue.
- Neutral, because the order of a unit's acknowledgement after its
  re-enqueue holds per delivery only: a redelivered old unit reads the key
  again and may re-enqueue it again.

### Confirmation

INV-1 holds for the object version a unit names: a 412 acknowledges a unit
only after the sink has acknowledged every record emitted from that version,
and the version's unread records no longer exist in the store. INV-1's
statement places data the store deletes, by an overwrite included, outside the
property, and names a notification unit acknowledged at its re-enqueue cap as
the one exception, counted in a metric.

`crates/spate-s3/src/error.rs`: `a_404_on_a_notification_unit_poisons_it` and
`a_403_on_a_notification_unit_fails_the_pipeline`.
`crates/spate-s3/tests/feed_driver.rs`, over the scripted feed:
`a_412_mid_read_acknowledges_only_after_emitted_records_are_acknowledged`,
`a_412_re_enqueues_the_key_at_its_current_etag_before_acknowledging`,
`a_412_notification_unit_is_acknowledged_without_dead_lettering`,
`a_unit_at_the_re_enqueue_cap_412s_and_is_acknowledged` and
`a_head_after_a_412_classifies_404_and_403_as_the_read_does`.
`crates/spate-s3/src/unit_reader.rs`: `a_unit_at_the_re_enqueue_cap_is_counted`
and `a_412_truncation_is_counted_once_per_key`.
`crates/spate-s3/src/fetch.rs`:
`a_notification_unit_with_a_version_id_reads_that_version` and
`a_null_or_absent_version_id_reads_by_key`. `crates/spate-s3/src/unit.rs`:
`an_unquoted_event_etag_is_sent_quoted`.

## More information

- Landed in the pull request that closed #1097.
- [ADR-0035](0035-s3-poison-policy.md) — the scope classification this
  record narrows, and its consequence on ambiguous scope.
- [ADR-0072](0072-missed-storage-notifications-are-not-reconciled-by-re-listing.md)
  — why a notification the store never sent is not recovered by re-listing.
- [Reading a notification's object](../user-guide/04-connectors/sources/s3/README.mdx#reading-a-notifications-object)
  — the behavior on the S3 source page.
