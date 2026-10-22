---
description: "A fleet's S3 lister is a one-shot run of the workers' own binary that publishes a job's units and writes its end marker; worker replicas never list."
---

# ADR-0075 — The lister runs once per job, apart from the worker replicas

- **Status:** accepted
- **Date:** 2026-10-22
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Under source coordination the S3 source's planner runs on the elected leader,
so a fleet makes one listing per plan. The work feed has no leader
([ADR-0066](0066-a-feed-trait-replaces-the-split-coordination-traits.md)).
The scaling-out guide gives workers one binary, one pipeline file and no
persistent volume. Something still has to list a prefix once per job and
publish its units to the feed. The lister's state sits at the job location
([ADR-0073](0073-a-per-job-location-holds-the-listers-checkpoint-end-marker-and-fingerprint.md)),
and it resumes after its checkpoint
([ADR-0074](0074-a-lister-resumes-after-the-last-key-it-checkpointed-once-the-feed-confirmed-it.md)).
A solo instance on the in-process feed has no other process that can reach its
queue.

## Considered options

- A separate one-shot lister run of the workers' binary, started once per
  job, with worker replicas never listing
- A lister elected among the worker replicas
- Every replica lists a disjoint part of the prefix

## Decision outcome

Chosen option: "A separate one-shot lister run", because an election needs a
lease, which brings back a store or the queue's own primitive, and disjoint
listing is static sharding, with the fixed replica count and lack of failover
the work-feed record rejects.

The lister run's entry point is `S3Source::list_once`, an API in `spate-s3`
that the user's `main()` calls, for instance when the binary's first argument
asks for it. Spate ships no binary, and `main()` already builds the durable
feed from the `feed:` section
([ADR-0069](0069-feed-backends-live-in-their-own-crates.md)). The run reads
the source and `feed:` sections of the workers' own pipeline file and builds
no sink. It refuses the in-process feed, a durable feed with no location, a
feed other than the one the section names, a source given a coordinator, and
a call from a thread in a Tokio runtime context, the rule
`Pipeline::from_config` applies.

The run writes the end marker, `run=` and a uuid v4 fresh to the run, after
the feed has confirmed every unit and the last checkpoint is written. The
marker is an overwrite, since a conditional create applied before a lost
reply fails its own retry. A unit the feed rejects as retryable is published
again in its place, up to five attempts, after a wait from 200 ms that doubles
each attempt; ADR-0074's rule holds under a retry in place. A unit the feed
refuses, or still rejects after the fifth attempt, fails the run, which then
writes no marker.

A source whose `feed:` section names the in-process feed runs the lister
itself, as a task on the pipeline's I/O runtime, with no location and no
checkpoint. A restarted solo run lists and reads the prefix from the
beginning, as
[ADR-0032](0032-s3-always-coordinated.md) states for a solo run, and a
startup WARN says so.

### Consequences

- Good, because worker replicas stay identical, with one binary, one pipeline
  file and no election.
- Good, because the lister's checkpoint sits at the job location, outside
  every worker, so no process needs the volume a file would.
- Bad, because a fleet's backfill runs two kinds of process.
- Bad, because nothing fences a second lister run, which doubles the job's
  reads. Two runs at two locations publish every unit twice, and nothing
  detects it. At one location with one fingerprint the later run resumes
  after the earlier's checkpoint, or lists the whole prefix when both start
  before either writes one, and the first to finish writes the end marker
  while the other may still hold units the feed has not confirmed. At one
  location with two fingerprints the later run is refused when it reads the
  earlier's fingerprint, on any store; when both read before either wrote
  one, the conditional create refuses the later run only where the store
  honors `If-None-Match`.
- Bad, because a solo run on the in-process feed holds one unit for each
  listed part not yet read, in memory and outside `units_held`, and lists from
  the beginning after a restart.
- Neutral, because the end marker's run id is what the bounded-job record
  reads, and the deployment shape of a bounded job's workers is that record's,
  since a feed worker never ends by itself.
- Neutral, because `a_timed_out_produce_holds_the_checkpoint`, which ADR-0074's
  Confirmation names, rejects the held unit on every attempt until the retry
  budget is spent.
- Neutral, because on the Kafka feed the lister run publishes through
  `KafkaFeedPublisher`, which joins no consumer group, so it takes no
  partition from the workers and charges no unit an attempt.
- Neutral, because the work-feed record removes coordination, after which a
  solo run on the in-process feed is the only way the S3 source lists for
  itself.

### Confirmation

`crates/spate-s3/src/lister.rs`:
`the_end_marker_follows_every_confirmation_and_the_last_checkpoint`,
`a_rerun_after_a_finished_run_replaces_its_marker`,
`a_lister_run_writes_its_marker_whatever_it_publishes`,
`the_end_marker_is_written_without_a_conditional_create`,
`a_failed_end_marker_write_fails_the_run`,
`an_end_marker_stored_with_its_reply_lost_fails_the_run`,
`a_retryable_rejection_is_published_again_in_place` and
`a_unit_rejected_past_the_retry_budget_fails_the_run`.
`crates/spate-s3/tests/lister.rs`:
`a_lister_run_publishes_every_unit_and_an_end_marker`,
`a_refused_unit_withholds_the_end_marker`,
`a_lister_run_refuses_the_in_process_feed`,
`a_lister_run_refuses_a_feed_the_section_does_not_name`,
`a_restarted_solo_run_lists_from_the_beginning`,
`a_solo_run_warns_that_it_lists_from_the_beginning`,
`a_solo_run_reads_while_it_lists` and
`a_failed_solo_listing_fails_the_run`.
`crates/spate-s3/tests/request_shape.rs`:
`a_worker_on_a_durable_feed_issues_no_listing`. Nothing fences a second
lister run beyond the fingerprint's read and its conditional create.

## More information

- Landed in the pull request that closed #1095.
- [ADR-0073](0073-a-per-job-location-holds-the-listers-checkpoint-end-marker-and-fingerprint.md)
  — the job location, the end marker and the fingerprint.
- [ADR-0074](0074-a-lister-resumes-after-the-last-key-it-checkpointed-once-the-feed-confirmed-it.md)
  — the checkpoint and resume.
- [Scaling out](../user-guide/05-deployment/scaling-out.mdx#reading-from-a-feed)
  — the lister run and worker shapes.
- [S3 source](../user-guide/04-connectors/sources/s3/README.mdx#the-lister-run)
  — running the lister.
