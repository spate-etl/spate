**Breaking:** **Pending batches per partition** (`spate-core`)

With `metrics.per_partition_detail: true`,
`spate_checkpoint_pending_batches` now publishes a pending-batch count for
each tracked partition. Previously, the setting produced no partition-labeled
series for this metric. Counts update during regular commit cycles, and a
partition's series is set to zero after it leaves the assignment. After a
coordinated source finishes reading a split, extra commit passes update only
the aggregate. Partition counts can then trail it by up to one commit interval.

The series without a `partition` label still reports the maximum across
partitions. Select it with `{partition=""}` when you need that aggregate:
summing the bare metric name now includes both the maximum and the individual
counts. Review dashboards and recording rules that use this metric. With the
setting disabled, only the aggregate is published.
