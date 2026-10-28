---
description: "The lister packs small objects into shared units of up to 16, so a lane fetches the next object as it decodes one. Supersedes ADR-0071 for packing."
---

# ADR-0085 — The lister packs small objects into shared work units

- **Status:** accepted
- **Date:** 2026-10-28
- **Supersedes:**
  [ADR-0071](0071-a-work-unit-is-one-object-or-one-byte-range-of-one-object.md),
  for lister packing
- **Superseded by:** —

## Context and problem statement

[ADR-0071](0071-a-work-unit-is-one-object-or-one-byte-range-of-one-object.md)
has the lister publish one unit per object or byte range and pack nothing. A
fetcher reads each GET window into memory before it hands the lane a chunk,
and starts the next window's GET once the lane's hand-off has room. With one
object per unit, a lane reading a small object waits on its whole GET and has
no next object to fetch while it decodes, so pipeline threads idle on small
objects ([#1483](https://github.com/spate-etl/spate/issues/1483)). The
coordinated planner of [ADR-0054](0054-s3-byte-range-subdivision.md) packed
about sixteen small objects to a split, and the same fetcher overlapped each
object's GET with the previous object's decode. The reader reads a
descriptor of several objects whole on one lane
([#1484](https://github.com/spate-etl/spate/pull/1484)). The question is what
the lister in `crates/spate-s3/src/lister.rs` publishes.

## Considered options

- One object or one byte range per unit, as ADR-0071 has it
- Pack on the in-process feed only, and keep one object per unit on durable
  feeds
- Pack first-fit into up to ten open bins, as ADR-0054's planner did
- Pack into one open unit of at most 16 objects and `split_target_bytes`
  bytes, in listing order, on every feed

## Decision outcome

Chosen option: "Pack into one open unit of at most 16 objects and
`split_target_bytes` bytes, in listing order, on every feed", because it gives
a lane the next object to fetch while it decodes, one unit shape from the
lister whatever the feed, and units whose members are consecutive listed keys,
which the checkpoint rule needs.

An object shares a unit when it is read whole, its key is addressable and at
most 1,024 bytes, and its ETag is absent or at most 256 bytes and can be sent
in `If-Match`. The open unit closes before a listed object whose bytes would
take it past `split_target_bytes`, once it holds 16 objects, before any other
listed object, before each checkpoint write and at the end of the listing. Any
other listed object is minted as ADR-0071 mints it: one whole unit, or byte
ranges as ADR-0054 cuts them. An object whose key the S3 client cannot address
is a unit of its own, published ahead of the open unit, which stays open.
A unit of one object keeps the id ADR-0071 gives it, and a unit of several
takes `split_id_for` over its members, so `PACKING_VERSION` stays 2: no feed
id has shipped, and nothing on the feed path keys progress by id
([ADR-0080](0080-a-work-feed-replaces-leased-fenced-split-coordination.md)).

The checkpoint rule of
[ADR-0074](0074-a-lister-resumes-after-the-last-key-it-checkpointed-once-the-feed-confirmed-it.md)
holds unchanged. The lister publishes the open unit before it settles its
publishes for a checkpoint write, so a checkpoint passes a key only once the
unit holding it, and every unit before it, is confirmed.

ADR-0071's id ground is answered by where a unit closes. A unit closes on the
listed keys' sizes, at the member cap, and at each checkpoint write, never at
a page boundary or on the feed's timing. On a store that lists keys in order,
where the lister keeps a checkpoint, a run resumed at a checkpoint starts at a
write point and mints the ids the earlier run minted after it. The exception is
a zero-byte or uncheckpointable key between a checkpoint key and the next
write point, which counts toward the cadence and moves where later units
close. Over a store that
does not list in order, an S3 directory bucket or `file://`, the lister keeps
no checkpoint, and two listings can pack differently. Different ids cost a
duplicate read, which INV-1 permits.

A unit of several objects fails back whole when one of them fails, and is
dead-lettered with all of them at the feed's cap. A prefix of a few hundred
small objects makes about one unit per 16 of them, so it can read on fewer
lanes than `units_held` allows.

### Consequences

- Good, because a lane reading small objects fetches the next one while it
  decodes, as ADR-0054's splits let it.
- Good, because a prefix of N small objects is about N / 16 units and queue
  messages on a durable feed.
- Good, because members are consecutive listed keys, so ADR-0074's
  checkpoint rule needs no change.
- Bad, because one deleted, overwritten or undecodable object dead-letters
  every object of its unit, and each redelivery reads the whole unit again.
- Bad, because a resumed run whose cadence moved, or a listing over a store
  that does not list in order, packs differently and reads some objects twice
  under new ids.
- Bad, because a small prefix reads on fewer lanes than `units_held` allows.
- Bad, because the overlap a unit buys is bounded by the lane's hand-off
  depth, which starts the next GET only once the current object's last chunks
  fit it ([#877](https://github.com/spate-etl/spate/issues/877)).
- Neutral, because a unit's body is at most about 43 KB by the member, key and
  ETag caps, under the SQS and Kafka feeds' message limits, so the lister adds
  no refusal.

### Confirmation

`crates/spate-s3/src/lister.rs`: `small_objects_share_a_unit`,
`a_checkpoint_never_passes_a_key_in_an_unconfirmed_bin`,
`a_resumed_run_mints_the_ids_the_first_run_minted_after_its_checkpoint` and
`a_bin_of_long_keys_stays_under_the_body_cap`.
`crates/spate-s3/src/unit.rs`: `prop_packed_units_tile_the_listing`.
`crates/spate-s3/tests/lister.rs`: `a_solo_run_reads_small_objects_as_one_unit`,
and `a_resumed_lister_run_publishes_the_ids_of_an_uninterrupted_one` against
RustFS, Ceph RGW and Garage in the Docker job.

## Evidence

Measured on one c8g.16xlarge in eu-west-2 by spate-etl/coordbench's `backfill/`
harness at 8c4ec84, reading 217,152 zstd objects of about 2.1 MB (16 MiB
decoded) through the in-process feed with 48 pipeline threads, `io_threads: 12`,
spate's default `prefetch_bytes` and `chunk_bytes`, a 3-minute cap and a 149 s
steady window per arm. The base build is spate 80232259, which reads a
descriptor of several objects but whose lister publishes one object per unit;
the head build is 83af0b5e, this record's pull request before the record. The
builds ran in ABBA order on one host, two passes each:

| `units_held` | Base decoded GB/s | Head decoded GB/s | Head over base | Busy pipeline threads, base / head |
|---|---|---|---|---|
| 48 | 5.37, 5.39 | 7.47, 7.57 | 1.40 | 22.2 / 31.3 |
| 96 | 8.43, 8.50 | 11.03, 11.01 | 1.30 | 35.4 / 46.5 |

Head at 192 units, on a second host of the same type, read 11.32 GB/s with
48.0 threads busy, 1.03 times head at 96. Head at 96 matches the 10.96 GB/s
at 46.7 busy threads that ADR-0054's 16-object splits gave on the same corpus
at spate 25786e86. The arms differ only in the lister's packing. No SQS or
Kafka feed point was measured, so ADR-0071's Q-SQS-sm figure stands
unanswered for a durable feed.

## More information

The pull request that lands this record, and
[#1484](https://github.com/spate-etl/spate/pull/1484), which made the reader
read a descriptor of several objects. Related:
[ADR-0028](0028-deterministic-split-ids.md) (a repacking is a new set of
splits), [ADR-0034](0034-s3-split-identity.md) (the packing version),
[ADR-0054](0054-s3-byte-range-subdivision.md),
[ADR-0071](0071-a-work-unit-is-one-object-or-one-byte-range-of-one-object.md),
[ADR-0074](0074-a-lister-resumes-after-the-last-key-it-checkpointed-once-the-feed-confirmed-it.md),
[ADR-0075](0075-the-lister-runs-once-per-job-apart-from-the-worker-replicas.md)
(a solo run holds one unit per listed part not yet read, which stays true with
fewer units) and
[ADR-0080](0080-a-work-feed-replaces-leased-fenced-split-coordination.md)
(replay cost grows with unit size). The
[S3 source page](../user-guide/04-connectors/sources/s3/README.mdx#work-units)
describes the resulting behavior.
