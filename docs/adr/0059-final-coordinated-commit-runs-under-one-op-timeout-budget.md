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

### Consequences

- Good, because a coordinated stop over a store that does not answer ends
  within the drain plus about two `op_timeout`s, one for the final commit and
  one for the departure, whatever the number of held splits.
- Good, because a partial final commit is reported per partition:
  `ExitReport.final_watermarks` holds only the stored positions, and the
  checkpoint commit counter records the pass as failed.
- Bad, because at the 10s default the final commit and the departure together
  can take 20s, more than the default 5s gap between `drain_timeout` and
  `terminationGracePeriodSeconds`. The graceful-shutdown guide's sizing rule
  covers the gap, and the defaults do not change.
- Bad, because a budget below about (2N + 3) times the store's write latency,
  for N held splits, leaves splits to replay even on a healthy store.
- Neutral, because a custom coordinated source that does not forward
  `commit_final` keeps the per-split commit, at up to three `op_timeout`s per
  split.

### Confirmation

`a_final_commit_over_a_store_that_stops_answering_ends_within_its_budget`
(`crates/spate-s3/tests/coordinated_final_commit.rs`) and
`commit_final_over_a_wedged_store_sends_nothing_after_its_budget`
(`crates/spate-coordination/tests/departure.rs`) pin the bound.
`a_partial_final_commit_commits_the_stored_partitions`
(`crates/spate-core/src/pipeline/tests.rs`) pins the per-partition report.

## Evidence

The (2N + 3) figure: a final commit can wait behind one heartbeat pass already
under way. That pass renews presence, then leadership, then each of the N held
splits, then settles owed leases, about N + 3 store operations
(`crates/spate-coordination/src/task/renew.rs:12-25`). The commit then sends N
commands, one store operation each. Derived from reading the heartbeat pass
and the commit path; not measured. The `step()` the task runs after each
command batch (`crates/spate-coordination/src/task.rs:641`) can add claim
or publish operations between final commits and is not counted, so the figure
may be low.

With six held splits, a 1s `op_timeout` and a store that stopped answering at
shutdown, the stop took 13,186 to 17,236 ms in 11 of 11 runs before this
change. The same fixture over a healthy store stopped in 165 to 215 ms, and
every split's watermark was written by the final commit. Measured at
`a7d015ae` with the fixture of
`a_final_commit_over_a_store_that_stops_answering_ends_within_its_budget`.

## More information

- Landed in the pull request closing #863.
- [ADR-0026](0026-coordination-fencing.md) — the revision fence a late commit
  meets.
- [ADR-0029](0029-framework-owned-coordination-driver.md) — the driver that
  owns the per-split commits.
- [Graceful shutdown](../user-guide/03-guides/graceful-shutdown.mdx) — the
  shutdown steps and the sizing rule.
