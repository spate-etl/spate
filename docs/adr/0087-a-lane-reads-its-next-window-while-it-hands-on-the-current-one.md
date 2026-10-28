---
description: "An S3 lane reads its next GET window while it hands on the current one, so its read-ahead no longer depends on chunk_bytes; a lane holds three windows."
---

# ADR-0087 — A lane reads its next window while it hands on the current one

- **Status:** accepted
- **Date:** 2026-10-28
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The S3 source reads an ETag-pinned object as bounded GET windows of
`prefetch_bytes`, drains each window into memory, and hands it to the lane in
`chunk_bytes` chunks through a channel four messages deep
(`crates/spate-s3/src/fetch.rs`, `LANE_HANDOFF_CHUNKS` in
`crates/spate-s3/src/lane_ctx.rs`). The fetcher started the next window's GET
only once the current window's last chunk was accepted. The lane then had at
most four chunks left to decode while that GET ran: 2 MiB at the default
`chunk_bytes` of 512 KiB. At a member boundary of a packed unit
([ADR-0085](0085-the-lister-packs-small-objects-into-shared-work-units.md)),
`ObjectEnd` and `ObjectStart` took two of the four slots. A GET slower than
decoding that queue left the lane's pipeline thread idle, so throughput
depended on `chunk_bytes`
([#877](https://github.com/spate-etl/spate/issues/877)).

[ADR-0054](0054-s3-byte-range-subdivision.md) rejected a second window in
flight on two grounds: it keeps a connection open across the hand-off, and it
doubles per-lane read-ahead memory. The question is when a lane starts its
next GET.

## Considered options

- Start the next GET once the current window's last chunk fits the hand-off
- Raise the default `chunk_bytes` to 2 MiB
- Deepen the hand-off channel, or bound it by bytes
- Start the next GET once the current window is buffered, and read it while
  the current one is handed on

## Decision outcome

Chosen option: "Start the next GET once the current window is buffered",
because the lead it gives the lane is a whole window at every boundary,
inside an object and between the objects of a unit, whatever `chunk_bytes`
is.

When a window is in memory, the fetcher starts the GET for the next window,
or for the first window of the unit's next member that has bytes and an ETag,
and drives it in its own task while it sends the current window's chunks.
That GET drains its body as it arrives, so a lane parked on backpressure holds
buffered bytes and no open request, which answers ADR-0054's connection
ground. A lane has one GET in flight. A failed read ahead reaches the lane
after the window before it, in the place a read started later would report
it. A member without an ETag is not read ahead, since its un-ranged body
would stay open.

Raising `chunk_bytes` keeps the lead counted in messages, leaves the member
boundary at two chunks, and rejects configurations that set `prefetch_bytes`
below the new default. A deeper or byte-bounded channel ties the lead to the
queue and changes what the channel depth means. Both trade memory for lead
without fixing the boundary case.

### Consequences

- Good, because a lane with no sibling on its thread keeps its thread busy
  while its next GET runs, at the default `chunk_bytes`.
- Good, because a packed unit of small objects reads its next object while
  the current one is handed on.
- Bad, because a lane holds up to three windows: the one queued chunks
  retain, the one being handed on and the one read ahead. Read-ahead memory
  is about `3 × units_held × prefetch_bytes`, 48 MiB per pipeline thread at
  the defaults against 32 MiB, by arithmetic and not measurement. This is
  ADR-0054's memory ground, accepted.
- Neutral, because the second lane per thread that
  [ADR-0086](0086-an-unset-units-held-holds-twice-the-pipeline-thread-count.md)
  defaults to covers less of the wait it was chosen to cover; its measured
  knee is unchanged by this record.

### Confirmation

`crates/spate-s3/src/fetch.rs`:
`a_blocked_lane_holds_two_windows_and_fetches_no_further`,
`the_next_members_get_starts_while_the_last_window_is_queued`,
`a_lane_holds_at_most_three_windows`,
`a_failed_read_ahead_reaches_the_lane_after_the_earlier_window`,
`a_member_without_an_etag_is_not_read_ahead` and
`a_unit_streams_its_members_in_order_under_any_windows`.
`crates/spate-s3/tests/request_shape.rs`:
`concurrent_reads_reach_the_in_flight_budget` pins one GET in flight per lane.

## Evidence

Measured on c8g.16xlarge in eu-west-2 by spate-etl/coordbench's `backfill/`
harness, reading through the in-process feed with 48 pipeline threads,
`io_threads: 12`, spate's default `prefetch_bytes` and `chunk_bytes`, a
3-minute cap and a 149 s steady window per arm. Spate b38d6f4b against the
change, two passes each in ABBA order. `mm` is 3,344 zstd shards of 1 GiB
decoded, each read whole in one unit; `sm` is 217,152 zstd objects of about
2.1 MB, packed into units. Decoded GB/s, busy pipeline threads in brackets:

| Corpus, `units_held` | Before | After | After / before |
|---|---|---|---|
| `mm`, 48 | 10.49, 10.47 (44) | 11.21, 11.17 (47) | 1.07 |
| `mm`, 96 | 11.38, 11.32 (48) | 11.38, 11.35 (48) | 1.00 |
| `sm`, 48 | 6.91, 6.87 (28.5) | 9.16, 8.91 (37.6) | 1.31 |
| `sm`, 96 | 10.84, 10.73 (45.4) | 11.26, 11.23 (47.5) | 1.04 |

One lane per thread gains the most, on small objects most of all. At the
default of two per thread, large objects were already bound by the pipeline
threads and do not move. Before the change, the same 48-lane `mm` arm at
`chunk_bytes` 2 MiB read 11.50 GB/s; after it, the default reads 0.97 of
that. The runs are coordbench 47e1c26; the second before-pass ran on a second
host of the same type after the first host's shutdown timer fired. One host
shape and one decode were measured; resident memory was not.

## More information

[#877](https://github.com/spate-etl/spate/issues/877) reports the wait.
Related: [ADR-0054](0054-s3-byte-range-subdivision.md), whose decision stands,
[ADR-0085](0085-the-lister-packs-small-objects-into-shared-work-units.md),
whose hand-off-bound consequence this removes, and
[ADR-0086](0086-an-unset-units-held-holds-twice-the-pipeline-thread-count.md).
The
[S3 source page](../user-guide/04-connectors/sources/s3/README.mdx#read-parallelism)
states the memory figure.
