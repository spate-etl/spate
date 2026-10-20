---
description: "A work unit is one object or one byte range of one object, from the lister and from notifications, so small objects are unpacked."
---

# ADR-0071 — A work unit is one object or one byte range of one object

- **Status:** accepted
- **Date:** 2026-10-19
- **Supersedes:** [ADR-0054](0054-s3-byte-range-subdivision.md) (packing)
- **Superseded by:** —

## Context and problem statement

[ADR-0054](0054-s3-byte-range-subdivision.md) has the object-storage planner
pack small objects first-fit into up to ten open bins, about sixteen objects to
a split, and cut a qualifying large object into byte ranges. A storage
notification names one object. The unit's size sets the replay cost in the
work-feed record, the queue requests per object, and what the lister's
checkpoint waits on in [the lister-resume record](0074-a-lister-resumes-after-the-last-key-it-checkpointed-once-the-feed-confirmed-it.md).

## Considered options

- One object or one byte range per unit; a large object from a notification
  is one unit its worker reads in ranges
- One object or one byte range per unit; a large object from a notification
  is re-enqueued as range units
- The lister packs small objects as ADR-0054 does; a notification carries one
  object

## Decision outcome

Chosen option: "One object or one byte range per unit, read in ranges by its
worker", because both producers then emit one shape, and the id of a unit the
lister publishes depends only on its object's key and ETag, its range, and the
configuration that cuts ranges. A packing lister mints ids that depend on
store order and page size, so a resumed lister mints different ids from the
run it replaces. Re-enqueueing ranges needs every notification consumer to cut
ranges exactly as the lister does.

The lister cuts a qualifying object into byte ranges as ADR-0054 states, one
unit each, and packs nothing. A unit carries the key as `object_store` reports
it, which for a lister unit is the store's raw key unless the key begins or
ends with `/`, and a worker addresses it with `Path::parse`
([#1081](https://github.com/spate-etl/spate/issues/1081)). A listed key that
`Path::parse` rejects or rewrites cannot be read under its own name, so its
object is one whole unit, which poisons on its first read without a request.

A notification's key is decoded as `application/x-www-form-urlencoded` (`+` is
a space, then percent-decoding as UTF-8). A notification becomes a unit only
for a record whose `eventName` is `ObjectCreated:*`, with or without an `s3:`
prefix. A record with no `eventName`, or one that is not a string, is
dead-lettered. A record whose `eventName` is not `ObjectCreated:*` is
acknowledged and dropped, whatever its bucket. A record for a bucket other than the source's, or for a
key outside the source's prefix, taken with the `/` the listing appends, is
dead-lettered. A record whose object size is 0 is acknowledged without a unit,
whatever its key's `Path` form. A notification is an S3 `Records[]` message,
the S3 test message (`"Event": "s3:TestEvent"`), or the same records inside the
envelope RustFS sends. The test message is acknowledged without a unit. A
message that is neither a notification nor a unit Spate published is
dead-lettered, and so are a created record that lacks a record's fields or
carries one of the wrong type, a created record with no size, or a size that is
not a non-negative integer, a key that does not decode, and, for an object of
one byte or more, a key that `Path::parse` rejects or whose `Path` form differs
from the decoded key, or an ETag holding a control byte other than a tab, which
`If-Match` cannot carry. A unit Spate published with such an ETag is
dead-lettered the same way. A message is one unit whose created records of non-empty
objects give one part each, acknowledged once after every part has retired
under the work-feed record's ack rule; a worker reads a part's ranges as
ADR-0054 cuts them, and each range in progress counts toward the units it holds
(the feed-section record).

### Consequences

- Good, because the lister and a resumed lister with the same configuration
  mint the same ids for unchanged objects.
- Good, because a large listed object is read by as many workers as it has
  ranges, as under ADR-0054.
- Bad, because small objects are unpacked. A prefix of N small objects is N
  units and N queue round trips, where ADR-0054's packing gave about N / 16
  splits and kept a prefix's objects together.
- Bad, because a large object from a notification is one unit on one worker,
  and on SQS its read must finish inside the 12-hour hold.
- Bad, because a large object from a notification takes up to `units_held`
  lanes on its worker, and later deliveries on that worker wait until all of
  its ranges have opened.
- Bad, because a queue accepts the same unit twice, so a duplicate
  notification or a unit the lister re-publishes is read twice. A notification
  unit's id is the feed message's, and an object both a listing and a
  notification announce during a backfill is read twice.
- Bad, because on a store that puts several records in one message, a poison
  record dead-letters the message with its other records.

### Confirmation

`crates/spate-s3/src/unit.rs`:
`prop_units_tile_the_listing_one_object_or_range_each`,
`a_notification_key_is_form_decoded`,
`an_event_name_with_or_without_the_s3_prefix_is_accepted`,
`a_record_for_another_bucket_or_prefix_is_dead_lettered`,
`a_size_zero_record_is_acknowledged_without_a_unit`,
`a_test_event_and_a_removal_are_acknowledged_without_a_unit`,
`a_key_path_parse_rejects_or_rewrites_is_dead_lettered`,
`an_unknown_notification_shape_is_dead_lettered`,
`an_unquoted_event_etag_is_sent_quoted`,
`an_etag_that_cannot_be_a_header_value_is_dead_lettered`,
`an_empty_object_with_an_unsendable_etag_is_dropped`,
`a_ranged_descriptor_with_an_unsendable_etag_fails`,
`a_record_without_an_event_name_fails_its_message` and
`a_message_with_several_records_gives_one_part_each`.
`crates/spate-s3/src/unit_reader.rs`:
`a_unit_with_nothing_to_read_is_acknowledged_by_the_sweep`.
`crates/spate-s3/tests/feed_driver.rs`:
`a_message_with_two_records_is_acknowledged_after_both_units` and
`a_removal_and_a_test_event_are_acknowledged_by_the_reader`.
`crates/spate-kafka/tests/feed_notifications.rs`'s
`a_key_with_reserved_and_non_ascii_bytes_reads_its_object` pins the decoded
key's read against RustFS through the Kafka feed in the Docker job.

## Evidence

Q-SQS-sm (one message per object) against C-N-sm (packed splits under NATS
coordination) on the `sm64` corpus at N=4, three reps each, judged by
pass criterion 2: Q-SQS-sm's mean at or above C-N-sm's mean less C-N-sm's
spread. Measured by spate-etl/ingestbench `queue-arms` at 7475b28 against spate
b7a9a9ce; results at fc958df. Criterion 2 passed: Q-SQS-sm ran at 30.76 GB/s
(spread 0.45) against C-N-sm's 13.39 GB/s (spread 0.25), and
30.76 ≥ 13.39 − 0.25 = 13.14. Q-SQS-sm had exact rows and all 868,608 units
complete in each rep. Every C-N-sm rep reached the harness's 15-minute cap
first, rep 1 having completed about 724,000 of the 868,608 objects, so its
figure is the rate over the capped window and it has no tail or rows verdict.
The arms differ in unit shape and in backend, so the result measures the pair.
Both arms ran on the coordination traits at b7a9a9ce, before the feed trait,
so the figures do not measure the feed trait's own cost. The harness writes a
per-unit completion log before each acknowledgement, and the cost it adds was
not measured.

## More information

- Landed in the pull request that closed #1092.
- [ADR-0028](0028-deterministic-split-ids.md) — its derivation still names a
  lister unit: the same key, ETag and range give the same id. Its
  create-if-absent property does not hold on a queue.
- [ADR-0034](0034-s3-split-identity.md) — the digest a lister unit's id uses,
  over member keys, ETags and the packing version.
- [ADR-0072](0072-missed-storage-notifications-are-not-reconciled-by-re-listing.md)
  — what Spate does about a notification the store never sends.
- [S3 source](../user-guide/04-connectors/sources/s3/README.mdx#storage-notifications)
  — the notification shapes read and the store settings each needs.
