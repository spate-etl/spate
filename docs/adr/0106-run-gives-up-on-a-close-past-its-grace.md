---
description: "Pipeline::run waits for the source and feed to close until 3 s past the drain deadline, then returns with the teardown left on a thread of its own."
---

# ADR-0106 — `run` gives up on a close 3 s past the drain deadline

- **Status:** accepted
- **Date:** 2026-11-15
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

After the final commit, `Pipeline::run` joined the controller thread with no
deadline, and the controller drops the source as it ends. A source whose `Drop`
never returned kept `run` from returning, so the caller never got its
`ExitReport` ([#1702](https://github.com/spate-etl/spate/issues/1702)). The
feed's `Drop`, which ran as `run` returned, had the same shape.

A signal does not end such a process. Spate registers its SIGTERM and SIGINT
handlers through tokio, which keeps a registered handler for the life of the
process, so a second signal does nothing. An orchestrator sends SIGKILL at its
grace period; on a bare host nothing does.

Delivery is settled before this point. The controller runs the final commit and
reports it before the source drops, so what a close that never ends holds up is
the group leave and the rest of the teardown.

## Considered options

What the bound covers:

- The controller join alone
- The whole teardown after the final commit: the source's `Drop`, the feed's
  `Drop` and the I/O runtime's shutdown
- Everything after the stop, including the wait for the final commit and the
  driver joins

When it gives up:

- 2 s after the final commit
- 3 s past the later of the drain deadline and the teardown's start
- A configuration key

## Decision outcome

Chosen option: "The whole teardown after the final commit", given up on "3 s
past the later of the drain deadline and the teardown's start", because it ends
every close that can hold `run` today without touching the drain or the final
commit.

The teardown runs on its own thread in this order: join the controller (the
source's `Drop`), drop the feed, shut the I/O runtime down. The source and the
feed both drop while the runtime is up. The I/O tasks (the signal handler, the
sink probe, the admin server and upkeep) are aborted before the teardown starts.
Past the bound, `run` logs a warning naming the source and the feed, and returns
the report it already had. The exit state does not change.

The bound counts from the drain deadline, so a drain that ends early leaves its
unused budget to the close. A final commit that ends after the deadline still
leaves the close 3 s. At the defaults, `checkpoint.drain_timeout` of 25 s leaves
5 s before Kubernetes' 30 s grace; the pool's backstop takes 1 s of it, and 3 s
leaves 1 s of the rest to the final commit. A key was left out because the
bound only fires when a close does not end.

The other two scopes leave the final commit alone. The commit is bounded by the
source's own contract, and giving up on it would need its own exit state. The
driver joins wait on operator code, and an abandoned driver keeps shard queues
open.

### Consequences

- Good, because `run` returns once the final commit is in, whatever a source or
  feed does in its `Drop`.
- Good, because a `Drop` left running keeps the I/O runtime until it returns, so
  work it spawns there still runs.
- Bad, because a thread can outlive `run`. In an application that embeds Spate
  it holds the source, the feed and the I/O runtime, a caller-owned one
  included, until the teardown ends.
- Bad, because a process that exits after `run` ends the teardown. A group leave
  in progress is lost, and the group waits out its session timeout before it
  reassigns the member's partitions.
- Bad, because while the teardown holds the feed, its gauge claim stays taken,
  and so does the source's when the source kept its stage metrics handle. A
  second `run` in the same process with the same pipeline name then fails with
  `StartError::Metrics` until the teardown ends, as INV-10 requires.
- Bad, because the Kafka source and feed bound their own close by up to 10 s.
  When a silent coordinator holds the final commit until near the drain
  deadline, `run` returns before Kafka's own bound. A process that then exits
  stops a librdkafka client mid-close.
- Neutral, because INV-1 holds: the final commit runs before the teardown.
  After a controller panic the feed drops in the teardown without a close, and
  a feed that commits in its `Drop` commits only what its own contract allows.
  The Kafka feed's commit stops at each partition's first unfinished unit.

### Confirmation

The tests in `crates/spate-core/src/pipeline/tests.rs`:
`run_gives_up_on_a_source_drop_at_the_drain_deadline_and_its_grace`,
`the_grace_counts_from_a_final_commit_that_ends_after_the_drain_deadline`,
`run_gives_up_on_a_feed_drop_at_the_same_bound`,
`an_abandoned_source_drop_keeps_the_io_runtime`,
`a_controller_panic_gives_a_blocked_source_drop_the_grace`,
`the_feed_drops_before_the_io_runtime_stops`,
`the_io_tasks_stop_before_a_teardown_that_outlasts_run` and
`an_abandoned_feed_drop_keeps_its_series_claimed`. The wait itself is
`ThreadHandle::join_by`, tested in `crates/spate-core/src/task.rs`.

## More information

- Landed in the pull request that closed #1702.
- The behaviour is described in
  [Graceful shutdown](../user-guide/03-guides/graceful-shutdown.mdx#the-drain-choreography).
