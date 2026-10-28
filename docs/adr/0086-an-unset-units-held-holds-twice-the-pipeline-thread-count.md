---
description: "An unset units_held holds twice the pipeline thread count, measured at the knee on packed units. Supersedes ADR-0079's fixed default of 96."
---

# ADR-0086 — An unset `units_held` holds twice the pipeline thread count

- **Status:** accepted
- **Date:** 2026-10-28
- **Supersedes:**
  [ADR-0079](0079-the-feed-is-configured-in-a-top-level-section-and-provisioned-by-the-operator.md),
  for the default units held
- **Superseded by:** —

## Context and problem statement

[ADR-0079](0079-the-feed-is-configured-in-a-top-level-section-and-provisioned-by-the-operator.md)
leaves `units_held` unset to the source's own default, a fixed 96, and names a
derived default as a separate change. The S3 source in
`crates/spate-s3/src/source.rs` reads one lane per unit or range it holds, and
pins each lane to one pipeline thread. A fixed count fits one host shape: on a
host with more pipeline threads the extra threads do no source work, and on a
smaller one each extra lane costs read-ahead memory and feed traffic. The
question is what an unset `units_held` holds
([#867](https://github.com/spate-etl/spate/issues/867)).

## Considered options

- A fixed 96, as ADR-0079 has it
- One unit per pipeline thread
- Two units per pipeline thread
- Four units per pipeline thread

## Decision outcome

Chosen option: "Two units per pipeline thread", because a lane pinned to a
thread leaves that thread idle while it waits on its next GET, and a second
lane per thread covers that wait. On packed units the measured gain stops at
two: four reads 3% more than two with every thread already busy.

The source resolves `units_held` from the `feed:` section when set, else as
twice the pipeline thread count the runtime hands it, saturating, else 96 for
a source opened outside a pipeline with no thread count. It logs the value and
its origin (`explicit`, `derived` or `fallback`) at `INFO` when it opens.

### Consequences

- Good, because the default follows the host: every pipeline thread has
  source work, and a small host holds fewer lanes.
- Bad, because read-ahead memory follows the default, about
  `2 × units_held × prefetch_bytes`, or 32 MiB per pipeline thread at the
  defaults, by arithmetic and not measurement.
- Bad, because a worker completes at most about `units_held / batch.linger`
  small units a second while held units total fewer rows than a batch seals
  on, twice the pipeline thread count a second at the defaults.
- Bad, because a source opened outside a pipeline keeps the fixed 96, which
  fits none of these hosts in particular.
- Neutral, because a host whose source work is not CPU-bound, such as one
  reading through a slow store, may read faster above two per thread, and an
  explicit `units_held` remains the way to set that.

### Confirmation

`crates/spate-s3/src/source.rs`:
`units_held_resolves_the_section_then_the_thread_count_then_the_fallback`.
`crates/spate-s3/tests/feed_driver.rs`:
`an_unset_units_held_holds_twice_the_pipeline_threads` and
`an_explicit_units_held_wins_over_the_thread_count`.
`crates/spate-s3/src/unit_reader.rs`: `the_reader_holds_at_most_96_units` pins
the fallback.

## Evidence

Measured on one c8g.16xlarge in eu-west-2 by spate-etl/coordbench's
`backfill/` harness, reading 217,152 zstd objects of about 2.1 MB (16 MiB
decoded) through the in-process feed with 48 pipeline threads,
`io_threads: 12`, spate's default `prefetch_bytes` and `chunk_bytes`, a
3-minute cap and a 149 s steady window per arm. Decoded GB/s, busy pipeline
threads in brackets:

| `units_held` | One object per unit, spate 846bd7a3 | Packed units, spate 83af0b5e |
|---|---|---|
| 48 | 5.30, 5.32 (22) | 7.47, 7.57 (31) |
| 96 | 8.29, 8.33 (35) | 11.03, 11.01 (46.5) |
| 144 | 9.18 (39) | |
| 192 | 9.62 (41) | 11.32 (48) |

On packed units, which the lister publishes since
[ADR-0085](0085-the-lister-packs-small-objects-into-shared-work-units.md),
one per thread reads 68% of two per thread, and four per thread reads 1.03
times two with all 48 threads busy. The unpacked column, coordbench 55b2cae,
measured the lister ADR-0085 replaced: there, four per thread read 1.16 times
two with threads still idle, the gap ADR-0085 closed. The packed column is
coordbench 8c4ec84, its 192-unit arm on a second host of the same type. Both
are one host shape and one corpus of small objects; a host with fewer or more
cores, or large objects read in ranges, was not measured.

## More information

[#1477](https://github.com/spate-etl/spate/pull/1477) landed the derived
default, and [#867](https://github.com/spate-etl/spate/issues/867) holds the
runs. Related:
[ADR-0079](0079-the-feed-is-configured-in-a-top-level-section-and-provisioned-by-the-operator.md)
and [ADR-0085](0085-the-lister-packs-small-objects-into-shared-work-units.md).
The
[S3 source page](../user-guide/04-connectors/sources/s3/README.mdx#read-parallelism)
describes the default.
