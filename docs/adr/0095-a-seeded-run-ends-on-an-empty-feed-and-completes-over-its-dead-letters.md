---
description: "An S3 run on a seeded in-process feed lists nothing, ends once the feed reads empty, and exits Completed with dead letters, which the seeding code reads."
---

# ADR-0095 — A seeded run ends on an empty feed and completes over its dead letters

- **Status:** accepted
- **Date:** 2026-11-09
- **Supersedes:** [ADR-0076](0076-a-bounded-job-on-a-feed-ends-on-an-empty-feed-and-a-checked-dead-letter-destination.md)
  (in part: only a solo run on the feed the runtime built ends with no window,
  and a non-empty dead-letter destination always ends a job `Stalled`)
- **Superseded by:** —

## Context and problem statement

Code can fill a `MemoryFeed` with a fixed set of units, pass it with
`Pipeline::seeded_feed`, and read the feed again after the run. The S3 source
receives that feed as `FeedOrigin::Seeded`. One caller is a function that runs
once per batch of storage notifications and reports each notification back to
its queue as done or failed.

[ADR-0076](0076-a-bounded-job-on-a-feed-ends-on-an-empty-feed-and-a-checked-dead-letter-destination.md)
lets a run on the in-process feed end by itself only when the runtime built
the feed, and ends every job `Stalled` when its dead-letter destination holds
a unit. Under those rules a seeded run lists the whole prefix, never ends, and
would fail the whole batch over one unreadable notification. A failed batch
makes the caller's queue redeliver units the sink has already written.

## Considered options

- A seeded run ends on an exact empty feed, and exits `Completed` with dead
  letters
- A seeded run ends on an exact empty feed, and ends `Stalled` with dead
  letters
- The caller stops the run itself once `MemoryFeed::unsettled` reads empty

## Decision outcome

Chosen option: "A seeded run ends on an exact empty feed, and exits
`Completed` with dead letters", because the code that seeded the feed holds
it and reads each dead letter from it, so no unit is lost with the process.
ADR-0076 kept `Stalled` because a dead letter on a queue leaves with the job
and `Completed` would present loss as success. A seeded feed's dead letters
stay in the hands of the caller, which reports each one to its own queue.
Ending `Stalled` exits `Failed`, and the caller cannot tell that exit from a
sink failure, so it would report every unit as failed.

On a seeded in-process feed the S3 source runs no lister. The run ends through
`EndCheck::exact`: one read of the feed that finds no visible or held unit
while the reader holds none, then one receive that takes nothing, with no
settling window. The source then reads the feed's dead-letter list. When the
list holds units the source logs their count at WARN and ends the run, which
exits `Completed`.

The exit state of a seeded run is not a per-unit status. A unit is done only
when neither `MemoryFeed::dead_letters` nor `MemoryFeed::unsettled` names it.

The S3 source refuses a seeded feed on any other backend when the pipeline
starts. The exact check needs exact counts, and ADR-0076 records that SQS
counts are estimates. A feed passed with `Pipeline::feed` keeps ADR-0076's
rule: the source lists into an in-process one, and the run ends at a shutdown.

Having the caller stop the run was rejected because the caller would poll the
feed from outside the pipeline, and a stop cannot tell a held unit whose
records are in the sink from one that is unread.

### Consequences

- Good, because one unreadable unit fails only itself, and the caller's queue
  redelivers that unit alone.
- Good, because the end rule stays in `spate_core::feed::EndCheck`, which is
  unchanged.
- Bad, because `Completed` no longer means every unit was read on a seeded
  run. A caller that reads only the exit state reports a dead-lettered unit as
  done, and that unit's records never reach the sink.
- Bad, because code that publishes into a seeded feed after the run starts can
  leave that unit unread, and nothing checks for it.
- Bad, because a seeded durable feed is refused, so a fixed batch on a queue
  still needs a job location to end.

### Confirmation

INV-1: the source reports its input drained only while the reader holds no
unit and the feed reads no visible or held unit, and a dead-lettered unit is
never acknowledged. `crates/spate-s3/tests/job_end.rs`:
`a_seeded_run_lists_nothing_and_ends_once_its_units_are_read`,
`a_seeded_run_with_a_dead_letter_ends_completed`,
`an_empty_seeded_feed_ends_the_run_at_once` and
`a_seeded_feed_on_a_durable_backend_is_refused`.
`crates/spate-s3/src/job_end.rs`:
`a_seeded_check_completes_over_dead_letters_and_logs_their_count`. Nothing
checks that a caller reads the dead-letter list.

## More information

- Landed in the pull request that closed #1472.
- [ADR-0076](0076-a-bounded-job-on-a-feed-ends-on-an-empty-feed-and-a-checked-dead-letter-destination.md)
  — the end rule and the `Stalled` rule this record narrows.
- [S3 source](../user-guide/04-connectors/sources/s3/README.mdx#a-run-on-a-seeded-feed)
  — how a seeded run behaves.
