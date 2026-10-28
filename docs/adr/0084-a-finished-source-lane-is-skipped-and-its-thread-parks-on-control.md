---
description: "A source lane reports when it has read its input to the end, and the pipeline thread stops polling it and parks on its control channel once every lane it holds has finished."
---

# ADR-0084 — A finished source lane is skipped and its thread parks on control

- **Status:** accepted
- **Date:** 2026-10-28
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A pipeline thread polls each lane it holds in turn, and a lane stays on the
thread until the control plane retires it, which waits on the sink's
acknowledgement of the lane's last records. The S3 lane decides end of input
on the poll that finds its channel closed with nothing framed or held, and
returns `Ok(None)` at once on that poll and on every later one. The driver
parked only when it held no lanes or when every lane was at its pending-batch
ceiling. A thread whose S3 lanes had all decided end of input therefore polled
them in a loop at a full core until the sink acknowledged them, which for small
objects is up to one `batch.linger`.

The condition is every lane on a thread having decided end of input while its
units wait on the sink. It occurs at any `units_held`: at the end of a run, and
whenever a job has fewer units than lanes held. Few units held per thread make
it the steady state of a small-object run. At the 96-unit fallback, 32
one-object units over 4 pipeline threads kept 4.05 cores busy across a 1 s
linger.

## Considered options

- A provided `SourceLane::finished`, `false` by default, that the driver reads
  before each poll
- A required `finished` method with no default, as
  [ADR-0037](0037-control-plane-wait-ownership.md) chose for its waker setter
- A driver park for the rest of `poll_timeout` after any pass of empty polls
- A wait in the S3 lane's end-of-input return

## Decision outcome

Chosen option: "A provided `SourceLane::finished`", because it moves no lane
that does not opt in, and lets the driver tell a lane that will never produce
again from one that is merely idle. A lane may report `finished` only after the
poll that decided its end of input, and once `true` it stays `true`. The driver
skips a finished lane, counts it toward the same streak as a gated lane, and
parks on its control channel for up to `poll_timeout` when a whole pass is
skipped. Every control message ends the park. The S3 lane reports `finished`
from its terminal watermark, the record of the decision its unit reader already
reads.

A driver park after any empty pass would throttle the load generator
([ADR-0042](0042-first-party-load-generator-source.md)), whose rate gate sleeps
only until its next tick and returns `Ok(None)` early by design. A required
method would break every implementor of a released trait. A wait in the S3
return would keep the thread polling a lane that can produce nothing.

### Consequences

- Good, because a thread whose S3 lanes have all finished uses no CPU while it
  waits for their acknowledgements.
- Good, because a skipped lane costs one `finished()` read and one turn of the
  driver loop, with no allocation and no poll.
- Bad, because a parked thread still wakes about 100 times a second at the
  10 ms `poll_timeout` default to heartbeat and check its lull.
- Bad, because a thread with one reading lane beside k finished lanes makes
  k + 1 zero-timeout polls per lull before it blocks, as it did before.
- Neutral, because the load generator's lane and the example lane keep the
  default and are polled as before.

### Confirmation

The driver tests `a_thread_whose_lanes_are_all_finished_parks`,
`a_finished_lane_is_skipped_while_its_sibling_flows`,
`a_finished_lane_beside_a_gated_one_parks`,
`a_retire_between_skips_does_not_park_a_flowing_thread` and
`a_lane_added_after_a_wake_is_read` in
`crates/spate-core/src/pipeline/tests.rs`, and the lane tests
`finished_waits_for_the_batch_that_empties_the_framer` and
`terminal_watermark_is_recorded_on_the_poll_after_the_final_batch` in
`crates/spate-s3/src/lane.rs`. The trait shape it extends is
[ADR-0003](0003-poll-based-source-api.md); the end-of-input wake it keeps is
[ADR-0080](0080-a-work-feed-replaces-leased-fenced-split-coordination.md)'s.

## Evidence

On one macOS arm64 laptop (18 cores), at the `core:` change's head against the
S3 opt-in:

- 32 one-object units over 4 pipeline threads at the default 1 s linger and the
  96-unit fallback, with process CPU read by `getrusage` around one run: 4.05
  cores busy before, 0.14 after.
- `feed_small_objects_wall`, `small_objects/feed/get_0ms`, one iteration, 4,608
  objects at a 5 ms linger, CPU and allocations per iteration: at 96 units held,
  2.08 s and 11.6 million before, 2.10 s and 9.7 million after; at 8 units held,
  19.7 s and 108 million before, 2.9 s and 9.8 million after.

Wall time is not a claim: the runs were not on a quiet host, and the 8-unit
legs are bound by the linger either way.

## More information

The `core:` change that adds `SourceLane::finished` and the driver park is
#1479; the S3 lane opts in in the pull request that adds this record. The defect
is #1478.
