---
description: "On a seeded S3 run, a unit whose batch the sink abandons is failed back to its feed, so the run ends by itself and reports that unit alone."
---

# ADR-0096 — A seeded run fails back a unit the sink abandoned

- **Status:** accepted
- **Date:** 2026-11-10
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A sink batch abandoned after a write error the sink does not retry fails the
acknowledgements of every source batch in it, and each partition's watermark
stalls behind that failure for good.
[ADR-0002](0002-at-least-once-delivery.md) accepts the stall as the failure
mode, and `checkpoint.stalled_fail_after` turns it into a `Failed` exit and a
restart that replays the data.

A run on a seeded feed has no restart. The S3 source holds a unit whose batch
was abandoned, so the run cannot end by itself until a stop or
`stalled_fail_after`. A `Failed` exit tells the code that seeded the feed
nothing about which units the sink wrote, so it reports every unit as failed.
[ADR-0095](0095-a-seeded-run-ends-on-an-empty-feed-and-completes-over-its-dead-letters.md)
rejected that outcome for dead letters.

## Considered options

- The controller tells the source which partitions are stalled, and the S3
  source fails a seeded run's units on them back to the feed
- A seeded run fails as soon as a partition stalls, ignoring
  `stalled_fail_after`
- The caller sets a short `stalled_fail_after`

## Decision outcome

Chosen option: "The controller tells the source which partitions are stalled,
and the S3 source fails a seeded run's units on them back to the feed",
because the unit then fails alone. The feed delivers it again or dead-letters
it at its cap. The run goes on with the other units and exits `Completed`, and
the seeding code reads the dead letter as it reads any other.

`Source::stalled` names every stalled partition on each commit tick, and its
default does nothing. The S3 source marks the unit failing and revokes its
lanes through the path an object-level failure takes. The revoke drops the
partition's tracker, which ends the stall. On any other feed the S3 source
ignores the call, so the unit stays held and `stalled_fail_after` fails the
run as before.

Failing the run at once and a short `stalled_fail_after` were rejected because
each ends `Failed`, which reports every unit, including those the sink wrote.

### Consequences

- Good, because one unit the sink refused fails only itself on a seeded run,
  and the run ends without waiting for its stop.
- Good, because a source that does nothing with the hook keeps
  ADR-0002's restart-and-replay.
- Bad, because the same event has two outcomes in the S3 source, depending on
  whether code seeded the feed.
- Bad, because a sink that refuses every write fails each unit of a seeded run
  in turn. A feed with a cap above 1 reads each unit again into the same
  refusal before it dead-letters it.

### Confirmation

INV-1: a unit failed back is never acknowledged, since its watermark cannot
pass the failed batch, and a dead letter counts as unacknowledged.
`crates/spate-core/src/pipeline/tests.rs`:
`each_tick_names_the_stalled_partition_to_the_source` and
`revoking_a_stalled_partition_ends_its_stall`.
`crates/spate-s3/src/unit_reader.rs`:
`a_seeded_run_fails_back_a_unit_on_a_stalled_partition` and
`a_stall_off_a_seeded_run_leaves_the_unit_held`.
`crates/spate-s3/tests/job_end.rs`:
`a_seeded_run_fails_back_a_unit_whose_batch_the_sink_abandoned`.

## More information

- Landed in the pull requests that closed #1969.
- [ADR-0002](0002-at-least-once-delivery.md) — the stalled watermark as the
  failure mode this record narrows for seeded runs.
- [ADR-0095](0095-a-seeded-run-ends-on-an-empty-feed-and-completes-over-its-dead-letters.md)
  — how a seeded run ends and reports its dead letters.
- [S3 source](../user-guide/04-connectors/sources/s3/README.mdx#a-run-on-a-seeded-feed)
  — how a seeded run behaves.
