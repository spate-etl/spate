---
description: "The Kafka feed's depth client owns isolation.level at read_uncommitted, so a bounded job counts writes behind an open transaction and waits for it with no bound of its own."
---

# ADR-0094 — A Kafka feed reads its end at the high watermark

- **Status:** accepted
- **Date:** 2026-11-06
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

[ADR-0076](0076-a-bounded-job-on-a-feed-ends-on-an-empty-feed-and-a-checked-dead-letter-destination.md)
ends a bounded job once the feed's depth stays at zero over a settling window
and the dead-letter read finds nothing. The Kafka feed's depth client declared
no `isolation.level`, so it stayed at librdkafka's default `read_committed` and
listed each end at the last stable offset. Another writer's open transaction
then hides every offset from its first record on. A job ended `Completed`, exit
0, over units on the work and retry topics, and over a dead-lettered unit that
should have stalled it
([#1763](https://github.com/spate-etl/spate/issues/1763)). ADR-0076 gives such
units no later run, since each job has fresh topics.

## Considered options

- The depth client owns `isolation.level` at `read_uncommitted`, and the key
  leaves the passthrough for every client
- The depth client sets the level after the passthrough, and the key stays open
- A second listing at `read_committed`, to name the transaction in the job's
  errors
- A typed limit on how long a transaction may hold a job

## Decision outcome

Chosen option: "The depth client owns `isolation.level` at `read_uncommitted`",
because a high watermark covers a confirmed write behind an open transaction,
and `Keys::own_if` is the mechanism a client's owned property already goes
through.

The one passthrough map feeds all six clients, so owning the key refuses it for
every role. That also keeps the group member at `read_committed`, where its
position cannot pass the last stable offset.
[ADR-0093](0093-a-kafka-feed-commits-past-offsets-with-no-record.md) relies on
that for its commit. Leaving the key open needs a second kind of key beside
`own`, and lets a consumer at `read_uncommitted` commit past a position above
the last stable offset.

A second listing means another client or a method on each `Offsets`
implementation, for a figure the job does not act on. A job held by a
transaction loses nothing while it waits, and the operator can stop it, so the
feed sets no limit: a typed limit adds public config and a state in `EndCheck`,
and a job it fails needs a re-run on fresh topics. The worker logs one `info`
line after 20 depth reads with the group's commits unchanged.

### Consequences

- Good, because a bounded job waits for a unit behind an open transaction, and a
  unit dead-lettered behind one stalls the job.
- Bad, because a hung transaction holds a job with no bound from the feed, and
  on Redpanda a live writer can too. The operator resolves it with the broker's
  tools or stops the job.
- Bad, because an end counts offsets. A marker, an aborted record or a record
  behind an open transaction alone on the dead-letter topic stalls a job.
- Bad, because `isolation.level` leaves the passthrough: a config that set it
  fails at load, and an operator loses `read_uncommitted` as a way past a hung
  transaction.
- Bad, because a stop during a hold exits `Completed`, exit 0, with units
  unread, and only a later worker on the group reads them.
- Neutral, because the figure 20 has no measured basis.

### Confirmation

`a_unit_behind_an_open_transaction_holds_a_bounded_job_open`,
`a_retry_write_behind_an_open_transaction_holds_a_bounded_job_open` and
`a_dead_letter_write_behind_an_open_transaction_stalls_a_bounded_job` pin the
job on a real broker. `only_the_depth_client_lists_ends_at_the_high_watermark`
pins the level on each of the six clients.

## More information

Landed with the pull request that closes #1763.
[Kafka feed: Ending a bounded job](../user-guide/04-connectors/feeds/kafka/README.mdx#ending-a-bounded-job)
states the behaviour and its limits.
