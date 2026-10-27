---
description: "A notification record whose eventSource is ceph:s3 is read at its key as written; every other key is form-decoded. Supersedes ADR-0071's key decoding."
---

# ADR-0082 — A notification key is read as written when its record names Ceph RGW

- **Status:** accepted
- **Date:** 2026-10-27
- **Supersedes:**
  [ADR-0071](0071-a-work-unit-is-one-object-or-one-byte-range-of-one-object.md),
  for notification key decoding
- **Superseded by:** —

## Context and problem statement

[ADR-0071](0071-a-work-unit-is-one-object-or-one-byte-range-of-one-object.md)
has the S3 source decode every storage notification's key as
`application/x-www-form-urlencoded`, which is the encoding AWS documents and
RustFS sends. Ceph RGW writes the object's name unencoded
(`populate_event` in Ceph v20.2.4 `src/rgw/driver/rados/rgw_notify.cc`), so a
key holding `+` or a `%` followed by two hex digits decodes to another key, and
one holding any other `%` fails its message. A unit that reads the other key
404s and is dead-lettered. If an object has that other name, the read can
re-enqueue it under
[the notification-read record](0077-a-notification-units-read-classifies-a-deleted-or-overwritten-object.md)
and acknowledge the announced object unread. Each record names its producer in
`eventSource`: `aws:s3`, `rustfs:s3` or `ceph:s3`.

## Considered options

- Read the key as written when the record's `eventSource` is exactly
  `ceph:s3`, and form-decode every other key
- A source setting that names the key encoding of the store it reads
- Form-decode every key, and state RGW as unsupported

## Decision outcome

Chosen option: "Read the key as written when the record's `eventSource` is
exactly `ceph:s3`", because the record already says which producer wrote it,
so no operator setting can disagree with the store. The match is exact and
case-sensitive. A record whose `eventSource` is absent, JSON null or any other
string is form-decoded, and a record whose `eventSource` is any other JSON
value fails its message as a record with a field of the wrong type does.

A unit the source re-enqueues after an overwrite writes its key form-encoded
and carries no `eventSource`, so it decodes to the key the store announced
whichever store announced it. The prefix, size, `Path` and ETag checks apply
to the key the rule gives.

### Consequences

- Good, because one source reads a topic or queue that carries notifications
  from several stores.
- Good, because a re-enqueued unit needs no record of which store announced
  its key.
- Bad, because a store that writes keys unencoded under an `eventSource` other
  than `ceph:s3` has its keys form-decoded, and supporting it means another
  rule.
- Bad, because whoever can publish to the feed picks the rule for each record
  it writes. Such a publisher already chooses the key itself, and the scope
  and `Path` checks still apply.

### Confirmation

`crates/spate-s3/src/unit.rs`:
`a_notification_key_is_form_decoded`,
`a_ceph_notification_key_is_read_as_written`,
`a_ceph_key_meets_the_checks_that_follow_decoding` and
`a_re_enqueued_ceph_unit_reads_the_key_rgw_announced`.
`fuzz/fuzz_targets/s3_notification_decode.rs` predicts each record's key from
its `eventSource` and how the key was built.

## More information

- Landed in the pull request that partly addressed #1281.
- [ADR-0071](0071-a-work-unit-is-one-object-or-one-byte-range-of-one-object.md)
  — the notification decoding this narrows.
- [ADR-0077](0077-a-notification-units-read-classifies-a-deleted-or-overwritten-object.md)
  — the overwrite re-enqueue whose body this rule reads back.
- [ADR-0079](0079-the-feed-is-configured-in-a-top-level-section-and-provisioned-by-the-operator.md)
  — the operator provisions the notification target, so Spate reads the store
  only through what it sends.
- [S3 source](../user-guide/04-connectors/sources/s3/README.mdx#storage-notifications)
  — the key rule and each store's settings.
