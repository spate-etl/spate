---
description: "Pipeline::run waits for each pipeline thread until 500 ms past the drivers' deadline, then fails the run and returns while a thread that has not stopped runs on."
---

# ADR-0114 — `run` gives up on a pipeline thread 500 ms past the drivers' deadline

- **Status:** accepted
- **Date:** 2026-11-26
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

`Pipeline::run` joined each pipeline thread with no deadline, on the drain path,
after a controller panic and on a startup error. A thread that never took its
`Shutdown` message kept `run` from returning for as long as it ran. One run
logged both drain deadlines and then waited 67 minutes on one thread
([#2399](https://github.com/spate-etl/spate/issues/2399)). The cause of that
missed `Shutdown` is not known.

A pipeline thread takes `Shutdown` between lane polls, so a lane whose `poll`
blocks past its timeout is one way to reach this. The thread holds its lanes,
its operator chain and a clone of the shard queues. ADR-0106 bounded the
teardown after the final commit and set the driver joins aside, because an
abandoned driver keeps the shard queues open.

## Considered options

What `run` does with a thread that has not stopped:

- Give up on it after a grace, log it and fail the run
- Have the thread exit on the shutdown flag as well as the message
- Log it and keep waiting

How long the grace is:

- 1 s, with the controller's wait for the sink drain raised to keep the pool's
  backstop inside it
- 500 ms
- 100 ms

Where the grace counts from:

- The drivers' deadline, `lanes_by`
- The drain deadline

## Decision outcome

Chosen option: "Give up on it after a grace, log it and fail the run", with a
grace of "500 ms" counted from "the drivers' deadline", because it ends every
wait on a pipeline thread with the mechanism ADR-0106 uses, and keeps the order
of the waits after it.

All the threads share one deadline, so several stuck threads cost the grace
once. A thread still running at that deadline is logged at `ERROR` with how long
ago it last beat, and the run exits `Failed` with the component `driver-N` of
the first one. The sink drain then starts with what is left of its budget, which
is nothing, so every shard worker sweeps what it still holds, as at any drain
deadline.

The drivers' deadline depends on the branch:

- With no feed that hands back, it is the drain deadline. The pool's backstop
  fires by 1.5 s past it, inside the controller's 2 s wait for the drain.
- With a feed that hands back, it is 1 s before the drain deadline. `run` gives
  up 0.5 s before the drain deadline, and the feed's close gets what is left of
  its share, possibly nothing. A unit the feed cannot give up in time comes back
  after the feed's redelivery delay.
- After a controller panic there is no drain deadline. `run` sets the threads a
  deadline of `drain_timeout` from that point, and gives up 500 ms after it.
  The run stays `Failed` on the controller.

A second stop signal was left out because the thread already has one: a thread
that misses the message would miss the flag for the same reason. A grace of
1 s needed a longer controller wait, which moves the final commit later in the
5 s that the defaults leave before Kubernetes' kill. A grace of 100 ms would fail
a run whose thread is late only by a lane drop.

### Consequences

- Good, because `run` returns whatever a pipeline thread does, and the exit
  state says which thread did not stop.
- Good, because the log line tells a thread that loops without taking its
  message (a recent heartbeat) from one held in a call (an old heartbeat).
- Bad, because a thread can outlive `run`. It keeps its lanes, so a Kafka lane
  keeps its consumer alive through its shared handle. It keeps its chain, whose
  pushes meet queues no worker reads, and its backpressure gauges, which keep
  their last value in an application that embeds Spate.
- Bad, because the thread keeps its shard-queue clone, which holds the queue
  depth claims. A second `run` in the same process with the same pipeline name
  fails with `StartError::Metrics` until the thread ends, as INV-10 requires.
- Bad, because a thread that ends on its own more than 500 ms past its deadline
  now fails the run. By reading, a flush blocked to the deadline is followed
  only by lane and chain drops, and no shipped lane has a `Drop` impl of its
  own.
- Bad, because with no feed the stuck case uses 0.5 s join + 1 s backstop + 3 s
  close grace = 4.5 s of the 5 s gap at the defaults, which leaves 0.5 s for the
  final commit.
- Bad, because a process that exits after `run` ends the thread wherever it is.
- Neutral, because INV-1 holds: the thread's acknowledgements never resolve, so
  no watermark passes them, and its data replays after restart.

### Confirmation

The tests in `crates/spate-core/src/pipeline/tests.rs`:
`a_thread_that_never_stops_does_not_hold_run`,
`a_thread_that_stops_within_the_grace_is_joined`,
`a_controller_panic_does_not_wait_on_a_thread_that_never_stops` and
`a_driver_panic_outside_the_batch_guard_fails_the_run`, and
`the_driver_joins_end_500_ms_past_the_drivers_deadline` in
`crates/spate-core/src/pipeline/runtime.rs`. The wait is
`ThreadHandle::join_by`, tested in `crates/spate-core/src/task.rs`.

## More information

- Landed in the pull request that bounded the driver joins for #2399.
- Related: [ADR-0106](0106-run-gives-up-on-a-close-past-its-grace.md) — the
  bound on the teardown after the final commit, whose set-aside scope this
  covers.
- The behaviour is described in
  [Graceful shutdown](../user-guide/03-guides/graceful-shutdown.mdx#a-pipeline-thread-that-does-not-stop).
