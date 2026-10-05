**Breaking:** **A stop ends coordination waits on an unresponsive store**
(`spate-core`, `spate-coordination`, `spate-s3`)

When a stop begins, the S3 source stops waiting on the coordination store and
the drain starts. A commit tick or revocation commit that is waiting gives up,
and the final commit sends its positions under its one
`coordination.op_timeout` budget. A failure report, a revocation hand-back or a
revocation decline that is waiting gives up the same way. In previous versions
the stop waited up to three times `op_timeout` for each held split before the
drain started, so a store that stopped answering shortly before SIGTERM could
hold the stop past the grace period. A custom coordinated source gets the same
behavior when its `open` passes the new `SourceCtx::stop` to the new
`CoordinationDriver::set_stop`.

A failure report that the stop cuts short before it is sent uses no delivery
attempt, and the departure hands the split back. A commit or report that was
already sent may still land after the stop.

`CoordinationDriver::commit` now returns a retryable error when the store
defers the commit of any split. The runtime keeps every position of that commit
pending and sends it again on the next tick or in the final commit. Each such
tick counts as `error` in `spate_checkpoint_commits_total`, and
`ExitReport.final_watermarks` lists a deferred position only after a later
commit stores it, apart from a partition the source no longer holds. In
previous versions the commit returned `Ok`, the tick counted as `ok`, and the
exit report could list a position whose commit the store had deferred. An
alert on the `error` outcome of that counter sees a store outage. A test that
scripts a `Retryable` commit answer through `spate-test`'s scripted coordinator
now sees that error from the driver's `commit`.
