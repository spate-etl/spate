---
description: "A bounded feed job ends after the lister's end marker and an empty feed over a settling window, and ends Stalled if a unit was dead-lettered."
---

# ADR-0076 — A bounded job on a feed ends on an empty feed and a checked dead-letter destination

- **Status:** accepted
- **Date:** 2026-10-23
- **Supersedes:** [ADR-0027](0027-split-delivery-attempts-and-quarantine.md)
  (in part: a quarantined split blocks completion)
- **Superseded by:** —

## Context and problem statement

Under coordination an instance with no splits stands by until every split is
committed complete, and a quarantined split ends the job `Stalled`, fatal by
default ([ADR-0027](0027-split-delivery-attempts-and-quarantine.md),
[ADR-0035](0035-s3-poison-policy.md)). On a queue, an instance that sees no
visible units cannot tell whether a peer holds one that will be redelivered,
and SQS's counts are estimates. SQS moves a unit to the dead-letter queue on
the server with no signal to any consumer, and the source queue then reads
empty.

## Considered options

- Keep `Stalled`: end on the lister's end marker and an empty feed over a
  settling window, then read the dead-letter destination
- End when the feed looks empty; a dead-lettered unit does not block
  completion, and operators alert on the dead-letter depth
- A bounded job on a feed never ends itself; the operator scales to zero

## Decision outcome

Chosen option: "Keep `Stalled`", because a bounded job that reports
`Completed` over a unit it never read presents loss as success, which
ADR-0027 and ADR-0035 refused. Ending on an empty feed reports success when
restarts dead-letter a healthy unit. Scaling to zero by hand leaves `Stalled`
nowhere to surface.

A bounded job is one
[ADR-0073](0073-a-per-job-location-holds-the-listers-checkpoint-end-marker-and-fingerprint.md)
defines as bounded. It ends when the end marker of one lister run is present
at the start and at the end of a settling window over which every read of the
feed reports no visible, delayed or in-flight units and the worker holds none.
The window spans the feed's reads from the first. The feed is read every
15 s, each read taken within 20 s of the one before and answered within 20 s
of its request; a non-empty read, a failed read, a late read or a unit the
worker takes starts the window again from a new read of the marker. One
receive that takes nothing follows the window. The settling window is the
`feed:` section's `settling_window`, five minutes by default, from 20 s to
24 h.

SQS needs the window, since its counts are approximate, and AWS asks for
several minutes of zero reads before a queue counts as empty ("Confirming that
an Amazon SQS queue is empty"); the SQS feed refuses a window under one minute.
On Kafka the feed reads the group's committed offset on every partition of the
work and retry topics, then each partition's log start and end offset, and
reports the lag; a partition with no commit, or a commit below its log start,
counts from its log start. That too must hold over the settling window. On
the in-process feed the end marker is the solo lister's completion, the counts
are exact, and the check needs no window; only a solo run on the feed the
runtime built ends this way, since code holding a feed it passed in may
publish into it after the listing.

The job then reads its own dead-letter destination, and a non-empty one ends
the job `Stalled`, which exits non-zero. On SQS that is the sum of the
dead-letter queue's visible, in-flight and delayed counts. On Kafka it is the
job's dead-letter topic, which Spate writes
([ADR-0070](0070-on-kafka-a-units-attempt-is-recorded-before-the-unit-is-read.md)).
Each job uses fresh topics (the feed-section record), so an end offset above
zero on any of its partitions ends the job `Stalled`, whether or not retention
has since deleted the record. On the in-process feed it is the in-memory
dead-letter list. A depth read the feed refuses for good, such as for a
missing permission, fails the job.

A `Stalled` job's re-run is a new job on fresh topics or queues at the earlier
job's location. Its lister resumes after the checkpoint and publishes only
what was listed after it, and the operator copies the dead-letter records into
its work topic or queue by hand. The operator copies them before starting the
re-run's lister. A re-run whose fingerprint differs
([ADR-0073](0073-a-per-job-location-holds-the-listers-checkpoint-end-marker-and-fingerprint.md))
uses a new location, re-reads the whole prefix, and takes no copied records.

A bounded job cannot share its feed with storage notifications. Nothing
enforces this. A bounded job whose queue also receives notifications ends
only after notifications stop for a settling window, and reads each object
both announce twice.

### Consequences

- Good, because a dead-lettered unit ends a bounded job `Stalled`, as a
  quarantined split did.
- Good, because an idle worker stands by to the end, so a unit redelivered
  after its owner dies still has a reader.
- Good, because the end rule lives in one state machine in `spate-core`, so
  every feed and source ends a job the same way, and its timing is tested on
  instants alone.
- Bad, because every bounded job on SQS or Kafka spends the settling window at
  its end.
- Bad, because the end check rests on approximate counts on SQS. A unit
  redelivered after the last instance exits stays in the queue until its
  retention expires.
- Bad, because a unit the queue deletes at the end of its retention leaves no
  trace. On SQS this covers the source queue and the dead-letter queue, whose
  retention counts from the first enqueue, so a job longer than the
  dead-letter queue's retention can end `Completed` over a unit dead-lettered
  early in it. On Kafka it covers work-topic segments deleted before they are
  read. The job then ends `Completed` over units it never read, and only the
  operator's retention settings prevent it.
- Bad, because on SQS Spate does not observe a move to the dead-letter queue
  while a job runs. A continuous job reports no dead-lettered unit, and the
  operator alerts on the dead-letter queue's depth in the queue's own
  monitoring.
- Bad, because an autoscaler that removes workers when the queue reads empty
  ends a bounded job with no exit status. A bounded job's workers run with a
  fixed count, or with a scaler whose minimum is one.
- Bad, because each bounded job needs a queue and a dead-letter destination of
  its own (the feed-section record), so the operator provisions a pair per job.
  A dead-letter destination an earlier job wrote ends a clean job `Stalled`.
- Bad, because a backfill and continuous ingestion need separate feeds.
- Bad, because records copied after a re-run has seen its end marker and an
  empty feed for one settling window are left unread.
- Bad, because a worker can end a job while a second lister run at the same
  location still publishes, as
  [ADR-0073](0073-a-per-job-location-holds-the-listers-checkpoint-end-marker-and-fingerprint.md)
  and
  [ADR-0075](0075-the-lister-runs-once-per-job-apart-from-the-worker-replicas.md)
  allow; that run's later units stay unread in the feed.
- Bad, because a feed that cannot report its depth serves only continuous
  jobs: a bounded job on it fails at startup.

### Confirmation

INV-1: a worker reports its input drained only while it holds no unit, so the
final commit covers every acknowledged unit. `spate_core::feed::EndCheck` is
tested on instants in `crates/spate-core/src/feed/end_check.rs`. The S3
source's end, with `a_worker_waits_out_a_marker_older_than_its_start`,
`a_marker_replaced_during_the_window_does_not_end_the_job`,
`exactly_one_dead_letter_ends_the_job_stalled`,
`a_solo_bounded_job_ends_with_no_settling_window` and
`a_continuous_job_does_not_end_on_an_empty_feed`, is tested in
`crates/spate-s3/tests/job_end.rs`.
`crates/spate-sqs/tests/feed_sqs.rs`: `a_bounded_job_waits_for_the_end_marker`,
`a_dead_lettered_unit_ends_the_job_stalled` and
`an_idle_worker_reads_a_unit_redelivered_after_its_owner_dies`.
`crates/spate-kafka/tests/feed_kafka.rs`:
`a_dead_letter_topic_entry_ends_the_job_stalled`;
`crates/spate-kafka/src/feed/depth.rs`:
`the_end_check_reads_committed_offsets_before_end_offsets`. ElasticMQ reports
exact counts, so nothing tests the window against SQS's approximate ones.

## Evidence

ingestbench records, for every queue arm in the work-feed record's session, the
time of the last unit's acknowledgement and of the last host's exit. Its exit
rule is the benchmark's own: the lister's done marker exists and the queue
reports zero visible, delayed and in-flight messages (Kafka: zero work-topic
lag) on three reads 20 s apart. Each read is logged. Measured by
spate-etl/ingestbench `queue-arms` at 7475b28 against spate b7a9a9ce; results
at fc958df. The last host exited 81.3–120.0 s after the last acknowledgement
on Q-SQS (six reps at N=4 and N=8) and 60.2–60.3 s after it on Q-KAFKA (four
reps). Less the rule's 40 s window, SQS's approximate counts first read zero
about 41–80 s after the last acknowledgement, and Kafka's lag about 20 s after
it. The rule reads no dead-letter destination, and its 40 s window is shorter
than the five-minute default and the one-minute floor the SQS feed sets.

## More information

- Landed in the pull request that closed #1096.
- [ADR-0035](0035-s3-poison-policy.md) — the `Stalled` rule this record
  carries onto a feed.
- The feed-section record — the finished `feed:` section, `settling_window`
  among its keys.
- [Work feed](../user-guide/02-concepts/09-work-feed.mdx#how-a-bounded-job-ends)
  and [S3 source](../user-guide/04-connectors/sources/s3/README.mdx#how-a-bounded-job-ends)
  — the end check and re-running a stalled job.
