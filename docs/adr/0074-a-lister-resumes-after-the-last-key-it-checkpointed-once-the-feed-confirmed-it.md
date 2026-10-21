---
description: "The S3 lister checkpoints a key once the feed confirmed every unit up to it, then resumes after it with StartAfter; SeaweedFS is unsupported for resume."
---

# ADR-0074 — A lister resumes after the last key it checkpointed, once the feed confirmed it

- **Status:** accepted
- **Date:** 2026-10-20
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The S3 source's lister ([the lister-run record](0075-the-lister-runs-once-per-job-apart-from-the-worker-replicas.md)) walks a prefix and publishes
units to the feed. A lister that stops has to resume without losing a key.
`ListObjectsV2` takes `StartAfter`, and `object_store` sends it on every page
beside the continuation token. Stores differ in list order: SeaweedFS walks its
directory tree, and resuming there after a key that is also a directory prefix
(`q/r` beside `q/r/s`) skips that directory. The lister reads through
`object_store`, whose `Path` strips a trailing `/` and rejects some keys. On
the S3 path the source's connector takes each object `Path` cannot address out
of the listing page before `object_store` parses it, so every key the listing
yields is addressable and its `Path` string is the raw key; a key ending in
`/`, a folder marker included, reaches the lister only through that record.
`object_store` sends no `StartAfter` to an S3 directory bucket and compares
keys itself there, in its trait default and on `LocalFileSystem`.

## Considered options

- Resume from a byte-order position the lister computes
- Issue `ListObjectsV2` directly, keep each raw key, and send it back as
  `StartAfter`
- Checkpoint the store's continuation token
- Stay on `object_store`, resume after the last key the store returned, and
  skip zero-byte objects; SeaweedFS is unsupported for resume

## Decision outcome

Chosen option: "Stay on `object_store`", because it needs no second S3 client,
and AWS documents the continuation token as opaque and states no lifetime for
it. A byte-order position loses keys on a store that lists in another order.
The connector already reads each raw `ListObjectsV2` page, so a client of the
lister's own would hold no key the lister lacks.

The lister checkpoints a key only once every unit it published before the
write has been confirmed, so the checkpoint names a key whose units, every
range included, and those of every key listed before it, in the store's
listing order for one lister, are confirmed. Confirmation is a successful
entry in an SQS `SendMessageBatch` response or a successful Kafka delivery
report, reported through the feed trait's publish
([ADR-0066](0066-a-feed-trait-replaces-the-split-coordination-traits.md)). The
rule holds under a later retry that re-publishes a unit in place. The Kafka
feed's producer forces `acks=all` and idempotence and refuses an override
(`crates/spate-kafka/src/feed/config.rs`). The lister rewrites the checkpoint
at most once each 1,000 listed keys, the largest page AWS returns, and once
when its listing ends. It resumes with `StartAfter` set to the checkpointed
key, and never compares two keys to decide order.

The checkpoint is one key, in the `checkpoint` object at the job's location
([ADR-0073](0073-a-per-job-location-holds-the-listers-checkpoint-end-marker-and-fingerprint.md)).
A run that finds no job fingerprint deletes a checkpoint it finds before it
creates the fingerprint, so a run that stops between the two leaves no
checkpoint beside a fingerprint. A run whose fingerprint matches the one it
reads then reads the checkpoint, and refuses one that is not a key under the
listing prefix. The read decides which, since Garage applies a conditional
create over an existing object and answers 200.
The in-process feed refuses a location. The lister skips every zero-byte
object, which holds no records, and never checkpoints at one.
It publishes each object the connector took out of a page whole, as
[ADR-0071](0071-a-work-unit-is-one-object-or-one-byte-range-of-one-object.md)
decides for a key `Path::parse` rejects or rewrites, and never checkpoints at
one, so a page holding such a key yields every other key. Over an S3 directory
bucket or a `file://` source the lister keeps no checkpoint and lists the
prefix from the start every run. SeaweedFS is unsupported for resume.

### Consequences

- Good, because resume holds in whatever order a supported store lists.
- Good, because the checkpoint is one key. The newest message on a Kafka work
  topic cannot serve as the checkpoint, because a produce that times out
  leaves a gap before later messages, and a topic with several partitions has
  no single newest message.
- Bad, because a crash re-publishes every unit after the last checkpoint,
  including units the feed had already confirmed.
- Bad, because a re-run at a finished job's location publishes only the keys
  that list after the checkpoint, and again each object the connector took out
  of a page after it, so a key added since that lists before the checkpoint
  is never read. A job whose feed lost its units, such as a purged queue,
  never reads the keys up to the checkpoint again.
- Bad, because a store whose order changes between two listings (a SeaweedFS
  SQL filer upgraded across its byte-order fix) loses the keys that moved
  ahead of the checkpoint. An operator who upgrades the store during a job
  deletes the checkpoint, so the lister lists from the start and re-publishes
  every unit.
- Bad, because the lister cannot merge or split listings by key range, since
  that needs an order.
- Bad, because a lister over an S3 directory bucket or a `file://` source
  lists the whole prefix every run.
- Neutral, because SeaweedFS also ends a listing whose page ends on a folder
  marker, so one listing there can miss keys whether or not it resumes.

### Confirmation

`crates/spate-s3/src/lister.rs`, against `object_store`'s in-memory store:
`every_checkpoint_write_names_a_key_whose_listing_prefix_is_confirmed`,
`a_failed_batch_entry_holds_the_checkpoint_before_its_key`,
`a_timed_out_produce_holds_the_checkpoint`,
`every_range_of_a_key_is_confirmed_before_it_is_checkpointed`,
`a_zero_byte_object_is_skipped_and_never_checkpointed`,
`a_resume_lists_after_the_checkpoint_byte_for_byte`,
`a_lister_that_creates_the_fingerprint_lists_from_the_start`,
`a_lost_fingerprint_create_reply_leaves_no_foreign_checkpoint`,
`a_store_that_ignores_conditional_create_still_resumes`,
`a_lister_that_cannot_resume_reads_and_writes_no_checkpoint` and
`an_unaddressable_key_with_bytes_is_published_and_never_checkpointed`.
`crates/spate-s3/src/source.rs`:
`only_an_s3_client_without_s3_express_resumes`.
`crates/spate-s3/tests/lister.rs`: `a_lister_resumes_after_its_checkpoint`,
and `resume_after_each_returned_key_publishes_the_rest` against RustFS in the
Docker job, seeded with `a/b/y`, `a/c`, `a-c`, `q/r`, `q/r/s` and a zero-byte
`x/` at a page boundary. The Kafka producer settings are pinned by
`each_client_gets_the_settings_the_feed_owns` and
`every_owned_property_and_alias_is_refused` in
`crates/spate-kafka/src/feed/config.rs`. Nothing enforces SeaweedFS's
exclusion.

## Evidence

On `chrislusf/seaweedfs:3.97` with keys `a/b/y a/b/z a/c a-c a0`, the store
lists in tree order, and resuming after each key it returned lost nothing. On
`chrislusf/seaweedfs:4.48`, `start-after=q/r` lost `q/r/s`, and
`start-after=x`, `object_store`'s form of the key `x/`, lost `x/1` and `x/2`.
On 4.48, a bucket with 999 keys `a000…a998`, a zero-byte `x/`, then
`x/1.ndjson` and `x/2.ndjson` returned 1,000 keys ending `a998.ndjson, x/`
from `ListObjectsV2` with max-keys 1000, `IsTruncated: false` and no
continuation token; with max-keys 2 the same layout paged correctly.
Spike-measured, hand-recorded; no committed rig.

## More information

- Landed in the pull request that closed #1094.
- [ADR-0071](0071-a-work-unit-is-one-object-or-one-byte-range-of-one-object.md)
  — why a unit is one object or one range, which lets the checkpoint wait on
  confirmed units alone, and why its id is stable across a re-publish.
- [S3 source](../user-guide/04-connectors/sources/s3/README.mdx#bounded-jobs-and-the-job-location)
  — the checkpoint, resume and the operator's part.
