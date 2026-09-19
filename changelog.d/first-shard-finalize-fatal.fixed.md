**First shard finalization error reported** (`spate-core`)

When several shards fail to finalize their chunks during one flush, the
pipeline now reports the first error encountered in shard processing order.
Previously, each failure replaced the recorded error, so the run result and
exit report contained the last shard's error. Shards that finalize successfully
still send their chunks even if another shard fails. Multiple shard failures
still contribute at most one fatal error to the stage's
`spate_operator_errors_total` counter.
