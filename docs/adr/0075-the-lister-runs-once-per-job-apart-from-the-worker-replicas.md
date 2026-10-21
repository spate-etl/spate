---
description: "A fleet's lister is one run of the workers' binary per job, and worker replicas never list; a solo instance on the in-process feed lists for itself."
---

# ADR-0075 — The lister runs once per job, apart from the worker replicas

- **Status:** accepted
- **Date:** 2026-10-21
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Under coordination the planner runs on the elected leader, so a fleet lists
once per plan. On the feed path no instance is elected, and every worker reads
the feed directly. Something still has to list a prefix once and publish its
units, and the scaling-out guide promises workers one binary, one
configuration and no persistent volumes. The decision covers the S3 source's
lister
([the lister-resume record](0074-a-lister-resumes-after-the-last-key-it-checkpointed-once-the-feed-confirmed-it.md)),
the entry point that runs it, and the in-process feed.

## Considered options

- A separate one-shot lister run of the workers' binary, started once per job,
  with worker replicas never listing
- A lister elected among the worker replicas
- Every replica lists a disjoint part of the prefix

## Decision outcome

Chosen option: "A separate one-shot lister run", because an election needs a
lease, which brings back a store or the queue's own primitive, and disjoint
listing is static sharding, with the fixed N and lack of failover the work
feed rejects.

The entry point is `S3Source::list`, an API in `spate-s3`. The binary already
builds a durable feed in code
([ADR-0069](0069-feed-backends-live-in-their-own-crates.md)), and `spate`
parses no command line. A lister run builds no sink. It writes its end marker
at the job location
([ADR-0073](0073-a-per-job-location-holds-the-listers-checkpoint-end-marker-and-fingerprint.md))
only after the feed has confirmed every unit it published, and the marker
holds an id fresh to the run. A unit the feed rejects as retryable is
published again, up to five publishes in all, and a unit still rejected fails
the run, which then writes no end marker. A solo instance on the in-process
feed runs its own lister, since no other process can reach that feed, and
`S3Source::list` refuses the in-process feed.

### Consequences

- Good, because worker replicas stay identical, with one configuration and no
  election.
- Good, because the lister's checkpoint lives outside every worker, at the
  job's location. A file would need a volume the documented deployment does
  not have.
- Bad, because a fleet's backfill runs two kinds of process, and a bounded
  job's workers run as a Kubernetes `Job`, since a `Deployment` restarts each
  worker that exits.
- Bad, because nothing fences a second lister run over the same prefix. It
  publishes every unit again and doubles the job's reads. INV-1 allows the
  duplicates.
- Bad, because on Kafka the lister needs a feed that joins no group,
  `KafkaFeed::publisher`. A lister built with `KafkaFeed::connect` takes
  partitions from the workers.
- Bad, because a solo instance on the in-process feed lists and reads its
  prefix from the beginning after every restart, and holds every listed unit
  in memory until it is read.
- Neutral, because a bounded job cannot end ahead of a unit that never reached
  the feed, as INV-1 requires: the end marker follows the last confirmation.

### Confirmation

`crates/spate-s3/tests/lister.rs`:
`a_lister_run_publishes_every_unit_and_an_end_marker`,
`a_refused_unit_withholds_the_end_marker`,
`a_lister_run_on_the_in_process_feed_is_refused`,
`a_restarted_solo_run_lists_from_the_beginning` and
`a_solo_run_warns_that_it_lists_from_the_beginning`.
`crates/spate-s3/tests/request_shape.rs`:
`a_worker_on_a_durable_feed_issues_no_listing` and
`the_feed_section_sets_read_parallelism`.
`crates/spate-s3/src/lister.rs`:
`the_end_marker_follows_the_last_confirmation_and_names_its_run` and
`a_unit_rejected_at_every_attempt_withholds_the_end_marker`.
`crates/spate-kafka/tests/feed_kafka.rs`: `a_publishing_feed_joins_no_group`,
in the Docker job. Nothing enforces one lister run per job.

## More information

- Landed in the pull request that closed #1095.
- [ADR-0073](0073-a-per-job-location-holds-the-listers-checkpoint-end-marker-and-fingerprint.md)
  and
  [ADR-0074](0074-a-lister-resumes-after-the-last-key-it-checkpointed-once-the-feed-confirmed-it.md)
  hold the location and the resume rule the lister run uses.
- [Scaling out](../user-guide/05-deployment/scaling-out.mdx#deploying-a-fed-fleet)
- [S3 source](../user-guide/04-connectors/sources/s3/README.mdx#the-lister-run)
