---
description: "The object-storage planner cuts a large uncompressed object into byte-range splits and packs every other object as before. Supersedes ADR-0033."
---

# ADR-0054 — A large uncompressed object is cut into byte-range splits, and the rest pack as before

- **Status:** accepted
- **Date:** 2026-09-30
- **Supersedes:** [ADR-0033](0033-s3-split-packing.md)
- **Superseded by:** —

## Context and problem statement

[ADR-0033](0033-s3-split-packing.md) gives an object at or above the split
target a split of its own. One lane then reads it serially, one bounded GET of
`prefetch_bytes` at a time, so the largest object in a prefix sets the longest
split. That bounds how finely leases can be taken over and how long the job's
tail runs while other workers sit idle. The object-storage planner in
`spate-s3` decides the split boundaries, and the fetcher and lane in the same
crate read them.

A byte range can be read on its own only when a reader entering mid-object can
find the next record boundary. That holds for an uncompressed object whose
framer names a delimiter byte a record starts after. It does not hold for a
whole-stream gzip or zstd object, or for a format with a header, a byte-order
mark or quoted delimiters.

## Considered options

- Do nothing, as the Flink and Kafka Connect file sources do
- A second GET window in flight per lane
- Cut a qualifying object into byte ranges, one split each, and pack every
  other object as ADR-0033 does

## Decision outcome

Chosen option: "Cut a qualifying object into byte ranges", because it spreads
one large object across workers without changing how any other object is
planned or read.

An object qualifies when it is above the target, at most 50,000 GiB (the
largest object S3 can hold), uncompressed under the configured compression,
has an ETag, and the framer declares a resync delimiter. It becomes `n = ceil(size / target)` ranges with boundaries
at `floor(i * size / n)`, so each range is between half the target and the
whole target. A range `[s, e)` owns the records whose first byte lies in it,
where a record starts at byte 0 and after every delimiter, following Hadoop's
`LineRecordReader`. The reader starts at `s - 1` and drops bytes through the
first delimiter, and reads from `e - 1` on through the next delimiter. The
descriptor carries the range and the delimiter, and the id digests both.

The rest of ADR-0033 still holds. Packing is listing-order first-fit over a
lookback of ten open bins, each object costs at least `target / 16`, and an
object that does not qualify but sits at or above the target gets a split of
its own. A cut object evicts an open bin exactly as an oversized one does, and
its ranges are emitted at its listing position, so every other split is the
same whether or not the object is cut.

Doing nothing was rejected because the tail it leaves grows with the largest
object, which the operator does not control. A second window in flight raises
one lane's throughput without moving the object to other workers. It also
keeps a connection open across the hand-off that the bounded GET releases, and
doubles per-lane read-ahead memory.

### Consequences

- Good, because a large delimited object is read by as many workers as it has
  ranges, so the longest split is about one target's worth of bytes.
- Good, because compressed objects and small objects are planned and read
  exactly as before.
- Bad, because the packing version changes, so every split id changes. A job
  planned by an earlier release cannot resume and has to be finished on that
  release or run again from the start. This is the full re-run cost
  [ADR-0034](0034-s3-split-identity.md) accepts for a packing change.
- Bad, because the bytes around each range boundary are read twice, once by
  each neighbor.
- Bad, because records of one object no longer reach the pipeline in object
  order.
- Bad, because each range is a split, and each split adds seeding round trips
  to the coordination store (#639).
- Bad, because a cut object costs one split per range, so planner memory, the
  coordination view and the coordination store's records grow with
  `size / target` as well as with object count. At the default 64 MiB target,
  one 50,000 GiB object plans 800,000 splits. Planning them takes about 362 MB
  of leader memory, and seeding them takes 1.6 million store creates. They
  then hold about 758 MB of coordination view for the job's lifetime, in the
  leader's view on every store and in every worker's view on a store whose
  watch is pushed, and each reconcile listing reads all of them again. No cap
  bounds the ranges per object.
- Bad, because a large object the planner cannot cut, such as a whole-stream
  compressed one, is still read in full by one lane.
- Neutral, because the framer's delimiter becomes part of the job
  fingerprint, so workers with different framers are refused at startup.

### Confirmation

`prop_packing_partitions_the_listing_exactly` and
`prop_small_object_bins_do_not_depend_on_cutting` in
`crates/spate-s3/src/split.rs` pin the tiling and that other splits do not
move. `whole_object_bins_are_pinned` and
`prop_whole_object_packing_matches_the_reference` in the same file hold
whole-object packing to the packing ADR-0033 describes. `ranges_tiling_an_object_emit_each_record_once` in
`crates/spate-s3/src/fetch.rs` pins that the ranges of an object deliver each
record once. `a_large_plain_object_is_read_as_three_byte_ranges` in
`crates/spate-s3/tests/backfill_pipeline.rs` runs the whole pipeline over a cut
object.

## Evidence

- One listing entry planned through `S3Planner::plan` in a release build, with
  a store whose listing reports the object's size: 50,000 GiB at a 64 MiB
  target gives 800,000 splits in 0.25 s with a 362 MB peak footprint, and the
  same entry with no delimiter gives one split and 2.5 MB. Spike-measured during
  review of #862, hand-recorded; no committed rig.
- Seeding writes two records per split, a spec and a progress record, so
  800,000 splits are 1.6 million creates, counted in a scratch run against an
  in-process store. Spike-measured, hand-recorded; no committed rig.
- 800,000 split states built from 222-byte descriptors, the size one range of
  a 50,000 GiB object encodes to at the 64 MiB target, and inserted into the
  coordinator's view map under a counting allocator hold 758,353,608 live
  bytes, 947 per split. Spike-measured during review of #862, hand-recorded; no
  committed rig.

## More information

- Landed in [#862](https://github.com/spate-etl/spate/pull/862), on top of
  the ranged reader in [#858](https://github.com/spate-etl/spate/pull/858).
- [ADR-0033](0033-s3-split-packing.md) — the packing this supersedes, whose
  lookback and open cost carry over.
- [ADR-0034](0034-s3-split-identity.md) — the identity digest the range and
  packing version feed.
- [S3 source](../user-guide/04-connectors/sources/s3/README.mdx#large-objects-and-byte-ranges)
  — which objects are cut, and what a reader of one sees.
