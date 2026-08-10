**`checkpoint.max_pending_batches` is a hard bound** (`spate-core`) — the
per-partition pending-batch limit is enforced at the poll boundary: a
partition at the ceiling has its lanes skipped until acknowledgments retire
batches, so `spate_checkpoint_pending_batches` can no longer overshoot the
configured value. Enforcement previously ran only on the commit tick, letting
pending exceed the limit by fetch-rate × `checkpoint.interval` per partition —
orders of magnitude under a post-rebalance replay. The ceiling is now
per-partition in effect as well as in name: one partition at its limit no
longer pauses its siblings, and a partition stalled behind a failed batch
accumulates at most the configured replay before `stalled_fail_after` fails
the pipeline. ([#200])
