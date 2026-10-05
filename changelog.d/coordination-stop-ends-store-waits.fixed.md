**A deferred coordinated commit keeps its positions pending** (`spate-core`)

`CoordinationDriver::commit` now returns a retryable error when the store
defers the commit of any split. The runtime keeps every position of that commit
pending and sends it again on the next tick or in the final commit. Each such
tick counts as `error` in `spate_checkpoint_commits_total`, and
`ExitReport.final_watermarks` lists a deferred position only after a later
commit stores it. In previous versions the commit returned `Ok`, the tick
counted as `ok`, and the exit report could list a position whose commit the
store had deferred. A test that scripts a `Retryable` commit answer through
`spate-test`'s scripted coordinator now sees that error from the driver's
`commit`.
