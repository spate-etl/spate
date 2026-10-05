---
description: "A coordinated source's final commit at shutdown runs under one op_timeout budget for all its splits, and the splits it does not reach may replay."
---

# ADR-0059 — The final coordinated commit runs under one op_timeout budget, and the splits it does not reach may replay

- **Status:** accepted
- **Date:** 2026-10-04
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

At shutdown the pipeline controller makes one final commit after the drain. A
coordinated source hands it to `CoordinationDriver`, which commits each held
split in turn through `SplitCoordinator::commit`, and `StoreCoordinator` waits
up to three times `coordination.op_timeout` for each of those commits. Over a
store that stops answering after shutdown starts, the stop grows with the
number of held splits and can outlast the gap between
`checkpoint.drain_timeout` and the orchestrator's SIGKILL. A split whose final
commit does not land keeps its older durable record, and its next owner
replays from that record.

## Considered options

- One `op_timeout` budget for the final commit as a whole; splits it does not
  reach replay
- A new `final_commit_timeout` key
- The part of `drain_timeout` the drain left unspent
- A deadline on each commit command inside the coordination task, or
  cancelling a store write mid-flight
- Committing the splits in parallel
- A budget that scales with the number of held splits
- A stop that ends every handle command waiting on the store, through a
  `StopSignal` the runtime hands the source
- A stop check in the coordinator's commit only
- The stop passed with each call, through new `Source` and
  `SplitCoordinator` methods
- A new `Cancelled` coordination error kind for a command the stop cut short
- Waking a waiting command from the shutdown trigger
- A required `SplitCoordinator::set_stop`

## Decision outcome

Chosen option: "One `op_timeout` budget for the final commit as a whole",
because `op_timeout` already bounds the departure as a whole, and a separate
key would have no case to differ from it. The drain may spend all of
`drain_timeout`, which would leave the commit nothing. A deadline inside the
task changes the task's command handling, and a cancelled write can leave the
task's view of what it holds out of step with the store. Parallel commits and
a budget that scales with the split count both still grow with the number of
splits over a store that does not answer.

`Source` and `SplitCoordinator` each gain a `commit_final` method with a
default body, so a source that does not forward it keeps the per-split commit.
`Source::commit_final` returns the partitions the source still holds and did
not store, so the exit report and the drained-exit check count a partial final
commit per partition. A split the commit does not reach is safe to replay
(INV-1): it keeps its older durable record, and a command abandoned at the
deadline that lands later carries an acknowledged watermark and is fenced by
revision ([ADR-0026](0026-coordination-fencing.md)) if a peer has claimed the
split.

A stop also ends every handle command waiting on the store: a tick or
revocation commit, a failure report, a drain release and a revocation decline.
The runtime hands the source a `StopSignal` in `SourceCtx`, and
`CoordinationDriver::set_stop` passes it to the coordinator. While the signal
is set, `StoreCoordinator` sends no command and ends a wait already under way
within 10 ms. The driver sends no further split of the commit and returns a
retryable error, so that commit's positions stay pending and go out in the
final commit. The runtime clears the signal before the final commit. The
coordination task still runs a command whose wait ended, as it runs one whose
wait timed out, so its view stays in step with the store.

A check in the coordinator's commit only would leave failure reports, drain
releases and declines waiting up to three `op_timeout`s each. Passing the stop
with each call needs two new trait methods, and the signal fits the
`SourceCtx` and `set_waker` pattern that carries the other per-run handles. A
cut-short command answers `Retryable`, which every caller already reads as a
command that may or may not land with the previous state authoritative; a
`Cancelled` kind would reach every hand-rolled caller of `SplitCoordinator`.
A waiting command polls an atomic every 10 ms, so no notifier is threaded
through the shutdown handle and the signal handler. A required `set_stop`
would break every existing `SplitCoordinator`.

### Consequences

- Good, because over a store that stops answering once the drain begins, a
  coordinated stop ends within the drain plus about two `op_timeout`s, one
  for the final commit and one for the departure, whatever the number of
  held splits. The same bound holds when the store stopped answering before
  the stop, because the stop ends any command waiting on the store and the
  final commit carries the positions of a commit it cut short.
- Good, because a partial final commit is reported per partition.
  `ExitReport.final_watermarks` lists a position only once the store has
  stored it, apart from a partition the source no longer holds; a tick commit
  the store defers leaves its positions to a later commit. The checkpoint
  commit counter records a deferred pass as failed.
- Bad, because at the 10s default the final commit and the departure together
  can take 20s, more than the default 5s gap between `drain_timeout` and
  `terminationGracePeriodSeconds`. The graceful-shutdown guide's sizing rule
  covers the gap, and the defaults do not change.
- Bad, because a budget below about (2N + 3) times the store's write latency,
  for N held splits, leaves splits to replay even on a healthy store.
- Neutral, because a custom coordinated source that does not forward
  `commit_final` keeps the per-split commit, at up to three `op_timeout`s per
  split. A custom coordinated source whose `open` does not pass
  `SourceCtx::stop` to `CoordinationDriver::set_stop` keeps the wait of up to
  three `op_timeout`s per held split for a command under way when the stop
  arrives. A `SplitCoordinator` that does not implement `set_stop` waits out
  the one command under way, and a wrapping coordinator has to forward it.

### Confirmation

`a_final_commit_over_a_store_that_stops_answering_ends_within_its_budget`
(`crates/spate-s3/tests/coordinated_final_commit.rs`) and
`commit_final_over_a_wedged_store_sends_nothing_after_its_budget`
(`crates/spate-coordination/tests/departure.rs`) pin the bound.
`a_partial_final_commit_commits_the_stored_partitions`
(`crates/spate-core/src/pipeline/tests.rs`) pins the per-partition report.
`a_stop_while_a_tick_commit_waits_on_the_store_reaches_the_drain_promptly`
(`crates/spate-s3/tests/coordinated_final_commit.rs`) pins the bound when the
store stopped answering before the stop, and
`a_stop_cancels_a_commit_waiting_on_the_store`
(`crates/spate-coordination/tests/departure.rs`) pins the cut of a wait under
way.

## Evidence

The (2N + 3) figure: a final commit can wait behind one heartbeat pass already
under way. That pass renews presence, then leadership, then each of the N held
splits, then settles owed leases, about N + 3 store operations
(`crates/spate-coordination/src/task/renew.rs:12-25`). The commit then sends N
commands, one store operation each. Derived from reading the heartbeat pass
and the commit path; not measured. The `step()` the task runs after each
command batch (`crates/spate-coordination/src/task.rs:641`) can add claim
or publish operations between final commits and is not counted, so the figure
may be low. A command the stop cut short may still be running in the task when
the final commit starts, which adds one store operation over a wedged store
and up to three over a slow one; the figure does not count it.

With six held splits, a 1s `op_timeout` and a store that stopped answering at
shutdown, the stop took 13,186 to 17,236 ms in 11 of 11 runs before this
change. The same fixture over a healthy store stopped in 165 to 215 ms, and
every split's watermark was written by the final commit. Measured at
`a7d015ae` with the fixture of
`a_final_commit_over_a_store_that_stops_answering_ends_within_its_budget`.

With six held splits, a 1s `op_timeout`, a 1s checkpoint interval and a store
that stopped answering while a tick commit waited on it, the stop took 5.23 to
14.13 s in 20 of 20 runs at `98861f95`, and 2.11 to 2.25 s in 20 of 20 runs
after the change.

## More information

- Landed in the pull request closing #863.
- The stop's end of waiting commands landed in the pull request closing #962.
- [ADR-0026](0026-coordination-fencing.md) — the revision fence a late commit
  meets.
- [ADR-0029](0029-framework-owned-coordination-driver.md) — the driver that
  owns the per-split commits.
- [Graceful shutdown](../user-guide/03-guides/graceful-shutdown.mdx) — the
  shutdown steps and the sizing rule.
