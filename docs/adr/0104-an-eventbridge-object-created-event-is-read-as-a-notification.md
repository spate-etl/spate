---
description: "The S3 source reads an EventBridge Object Created event as a notification, its key as written, and fails event types it does not list."
---

# ADR-0104 — An EventBridge Object Created event is read as a notification

- **Status:** accepted
- **Date:** 2026-11-15
- **Supersedes:**
  [ADR-0071](0071-a-work-unit-is-one-object-or-one-byte-range-of-one-object.md),
  for the notification shapes the source reads;
  [ADR-0082](0082-a-notification-key-is-read-as-written-when-its-record-names-ceph-rgw.md),
  for form-decoding every key other than a Ceph RGW record's
- **Superseded by:** —

## Context and problem statement

[ADR-0071](0071-a-work-unit-is-one-object-or-one-byte-range-of-one-object.md)
has the S3 source read an S3 `Records[]` message, the RustFS envelope and the
S3 test event, and dead-letter any other body. AWS S3 can also send its events
to EventBridge, where a rule forwards them to an SQS queue. Several consumers
can then filter one bucket's events. Such an event carries one object under
`detail`, with no `Records` array, so the source dead-letters every one.

AWS does not document whether an EventBridge event's key is encoded. A
recording in eu-west-2 on 2026-10-27 showed EventBridge sending each key as
written, where the same bucket's direct notification sent it form-encoded.
[ADR-0082](0082-a-notification-key-is-read-as-written-when-its-record-names-ceph-rgw.md)
form-decodes every key other than a `ceph:s3` record's, and form-decoding
reads `a+b.txt` as `a b.txt` and refuses `100%.txt`.

The bucket's EventBridge setting sends every event type. An S3 API call that
CloudTrail records reaches EventBridge as `AWS API Call via CloudTrail`, and
AWS's example of that type carries the service's own `source`, so an S3 one
may carry `aws.s3` as well.

## Considered options

- Read an `Object Created` event in the source with its key as written,
  acknowledge the other S3 event types AWS lists, and fail any other type
- The same, acknowledging every other `aws.s3` type
- An EventBridge input transformer that rewrites the event into a `Records[]`
  message, with no change to the source
- A source setting that names the shape the queue carries

## Decision outcome

Chosen option: "Read an `Object Created` event in the source, acknowledge the
listed types, and fail any other", because the event names its producer in
`source`, as a record names it in `eventSource`. A listed type names no new
object content, and a type off the list fails where it can be seen, so a
CloudTrail record of a write is never acknowledged unread.

An `Object Created` event is read only under a `detail.event-version` of `1.`
followed by digits. AWS changes the major version for a change that is not
backward compatible, and the event's ETag, version and time are optional
fields that a renamed field would leave absent without an error. A body with a
`Records` array is read as records whatever else it carries, so every body the
source read before reads the same way.

The event's object meets the bucket, prefix, size, `Path` and ETag checks a
record's object meets, through one function. Its key is read as written, its
version comes from `version-id`, its event time from the top-level `time`, and
its re-enqueue count is 0. A unit re-enqueued after an overwrite writes its key
form-encoded under no `eventSource`, so it decodes to the key the event
announced.

### Consequences

- Good, because a queue an EventBridge rule fills feeds the source, and one
  bucket's events can be filtered for several consumers.
- Good, because one set of checks covers both shapes, so a key read as written
  meets the same scope rules a decoded key does.
- Bad, because a new S3 event type, or a new major event version, dead-letters
  every such event until the source lists it.
- Bad, because the key rule rests on one recording of seven keys, with no
  documentation behind it, and AWS does not run in CI.
- Bad, because whoever can publish to the queue can choose a shape whose key
  is read as written. Such a publisher already chooses the key, and the scope
  and `Path` checks still apply.

### Confirmation

`crates/spate-s3/src/unit.rs`:
`an_eventbridge_object_created_event_reads_its_key_as_written` (the recorded
keys), `an_eventbridge_event_meets_the_notification_checks`,
`an_eventbridge_event_version_other_than_major_1_fails`,
`an_eventbridge_event_detail_type_is_acknowledged_only_from_the_list`,
`an_event_source_beside_another_shape_keeps_that_shape` and
`a_re_enqueued_eventbridge_unit_reads_the_key_the_event_announced`.
`fuzz/fuzz_targets/s3_notification_decode.rs` builds events from arbitrary
records and predicts each outcome.

## More information

- Landed in the pull request that closed #1473.
- [ADR-0071](0071-a-work-unit-is-one-object-or-one-byte-range-of-one-object.md)
  — the notification shapes this extends.
- [ADR-0082](0082-a-notification-key-is-read-as-written-when-its-record-names-ceph-rgw.md)
  — the key rule for records, which this keeps.
- [S3 source](../user-guide/04-connectors/sources/s3/README.mdx#storage-notifications)
  — the shapes and rules the source applies.
- [SQS feed](../user-guide/04-connectors/feeds/sqs/README.mdx#storage-notifications)
  — the EventBridge rule and queue settings.
