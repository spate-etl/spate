---
description: "A bounded feed job keeps its lister's checkpoint, end marker and fingerprint at a per-job object-store location every worker reads; a mismatched worker refuses to start."
---

# ADR-0073 — A per-job location holds the lister's checkpoint, end marker and fingerprint

- **Status:** accepted
- **Date:** 2026-10-20
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

On the feed path a fleet's lister runs apart from its workers
([the lister-run record](0075-the-lister-runs-once-per-job-apart-from-the-worker-replicas.md)),
and a bounded job ends on the lister's end marker
([the bounded-job record](0076-a-bounded-job-on-a-feed-ends-on-an-empty-feed-and-a-checked-dead-letter-destination.md)).
A queued message reaches one SQS worker or one Kafka partition owner, so the
feed cannot carry a marker every worker sees. An object outlives its run, so a
re-run can find the previous run's marker. Under coordination a job name kept
two jobs apart on one store, and workers compared a job fingerprint through the
store. On the feed nothing compares a lister's settings with the workers'. The
decision covers the S3 source's feed path and the `feed:` section's
`FeedSection`.

## Considered options

- A per-job location in `feed:`, an object-store prefix holding the
  checkpoint, the end marker and the lister's fingerprint, read by every worker
- The same location as a compacted topic on a Kafka feed
- The same, with the end marker also written to the feed, one record per Kafka
  partition
- A job name in `feed:` from which a location under the source prefix derives
- A fingerprint carried on every lister unit and checked per unit
- No fingerprint check, with the loss stated

## Decision outcome

Chosen option: "A per-job location in `feed:`", because every worker,
including one started after the lister finished, reads the same record, and
SQS has no message every worker sees. A derived path under the source prefix
would have to be excluded from every listing and notification filter. A
fingerprint on each unit is checked only once units arrive, and notification
units carry none.

The location is a prefix holding three objects, `fingerprint`, `end` and
`checkpoint`, because the lister rewrites its checkpoint once per listing page
and the end marker is deleted and written on its own. It is a common `feed:` key
beside `units_held`, because the S3 source receives only the section and each
durable backend refuses a key it does not know. A worker compares no store
timestamp. It takes a marker as the end only as
[the bounded-job record](0076-a-bounded-job-on-a-feed-ends-on-an-empty-feed-and-a-checked-dead-letter-destination.md)
states, on one run's marker present at the start and the end of a settling
window, so a marker from an earlier run waits one window like any other.

### Consequences

- Good, because a worker configured for another bucket, prefix, compression,
  range target or framer delimiter refuses to start before it receives a
  unit, and the error prints both fingerprints.
- Good, because a bounded job on SQS and one on Kafka keep their state the
  same way.
- Bad, because the lister needs write and delete access to the location, and
  every worker read access, possibly in a bucket other than the source's. On
  AWS S3 that read access includes `s3:ListBucket`, without which an absent
  fingerprint reads as 403 and a worker started before its lister fails at
  once.
- Bad, because the location shares the source's `store` options, so a source
  whose `store` sends virtual-hosted requests to a set endpoint cannot name an
  S3 location, and the source refuses one.
- Bad, because a marker from an earlier run still ends a worker whose new
  lister has not deleted it within one settling window. INV-1 holds, since the
  late lister's units stay unacknowledged in the feed.
- Bad, because a new job pointed at an earlier job's location by mistake
  publishes only the keys listed after that job's checkpoint once the
  [lister-resume record](0074-a-lister-resumes-after-the-last-key-it-checkpointed-once-the-feed-confirmed-it.md)
  lands. Until then such a lister lists the whole prefix.
- Bad, because a lister upgraded across a unit format change cannot resume a
  job, which restarts at a new location, and a `Stalled` job whose fix changed
  the compression, range target, framer or unit format re-reads the whole
  prefix.
- Bad, because a bounded job's workers exit non-zero when the lister has not
  written the fingerprint within one settling window of their start. The window
  is 5 minutes until
  [the bounded-job record](0076-a-bounded-job-on-a-feed-ends-on-an-empty-feed-and-a-checked-dead-letter-destination.md)
  adds its key.
- Bad, because on the Kafka feed a waiting worker's units are charged an
  attempt as the feed readies them, and each rebalance of the group during the
  wait charges them again, so a fleet started well before its lister, or a
  worker restarting into a refusal, can move the head of each partition to the
  dead-letter topic unread.
- Bad, because the location needs read-after-write and read-after-delete
  consistency on one store: Garage with `consistency_mode = "consistent"`, and
  one Ceph RGW zone.
- Bad, because one lister per location is an operator rule. The lister
  creates the fingerprint with `PutMode::Create`, which refuses a second
  lister with another fingerprint where the store honors `If-None-Match`. CI
  checks that on RustFS, and it is verified on Ceph RGW and Garage. Two
  listers with one fingerprint both publish.
- Neutral, because the fingerprint includes the range target, though a worker
  given another target only cuts a whole-object unit into other ranges.
- Neutral, because a bounded Kafka job's work topic is still held to
  `LogAppendTime`, so the check
  [ADR-0072](0072-missed-storage-notifications-are-not-reconciled-by-re-listing.md)
  describes stands for every Kafka work topic.
- Neutral, because each lister run's marker names a run fresh to that run, so
  a replaced marker never compares equal to the one it replaced.

### Confirmation

`crates/spate-s3/tests/lister.rs`:
`a_worker_with_another_fingerprint_refuses_to_start`,
`a_worker_exits_when_no_fingerprint_appears_within_a_settling_window`,
`a_lister_with_another_fingerprint_refuses_to_start` and
`a_lister_run_deletes_an_earlier_marker_before_its_first_publish`.
`crates/spate-s3/src/location.rs`:
`the_feed_fingerprint_changes_with_each_field` and
`a_location_at_or_under_the_source_prefix_is_refused`.
`crates/spate-core/src/pipeline/tests.rs`:
`a_memory_feed_with_a_location_is_refused`. The one-lister rule has no check
beyond the conditional create.
`crates/spate-s3/tests/lister.rs`'s
`a_second_conditional_create_at_the_location_ends_as_the_store_answers` checks
the conditional create on RustFS, Ceph RGW and Garage in the Docker job, and
`a_lister_with_another_fingerprint_is_refused_by_its_read_on_the_store` checks
the read on each.

## More information

- Landed in the pull request that closed #1093.
- [ADR-0054](0054-s3-byte-range-subdivision.md) — the framer's resync
  delimiter, which the fingerprint covers.
- [S3 source](../user-guide/04-connectors/sources/s3/README.mdx#bounded-jobs-and-the-job-location)
  — the location, its permissions and the startup checks.
