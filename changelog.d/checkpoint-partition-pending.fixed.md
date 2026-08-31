**Breaking:** **`metrics.per_partition_detail` reaches the checkpointer**
(`spate-core`) — `spate_checkpoint_pending_batches` carried the flag's
`partition` label in the documentation and in the handle, but nothing wrote it,
so the labeled series never appeared in a scrape whatever the flag was set to.
The controller publishes one labeled series per partition it tracks, on every
commit cycle, and zeroes a partition's series when that partition leaves the
assignment.

A deployment already running with `per_partition_detail: true` gains series it
never had. The unlabeled series still carries the max across partitions, so
select it with `{partition=""}`; a `sum` over the bare name now adds that max to
the per-partition counts, and a dashboard panel or recording rule built on the
bare family needs rechecking. The default (off) publishes the aggregate alone,
as before.
