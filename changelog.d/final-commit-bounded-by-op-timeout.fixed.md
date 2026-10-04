**The S3 source's final commit at shutdown ends within `op_timeout`**
(`spate-core`, `spate-coordination`, `spate-s3`, `spate-test`)

The S3 source now runs its final commit at shutdown under one
`coordination.op_timeout` budget for all its splits together, and splits it
does not reach in that time may replay under their next owner. In previous
versions that commit waited up to three times `op_timeout` for each split in
turn, so a store that stopped answering at shutdown could hold the stop past
the grace period. A custom coordinated source gets the same bound when it
forwards the new `Source::commit_final` to `CoordinationDriver::commit_final`,
and `CoordinatorScript::final_commits` in `spate-test` shows whether it does.

A final commit that leaves splits uncommitted counts as `error` in
`spate_checkpoint_commits_total`, and `ExitReport.final_watermarks` does not
list their new positions. In previous versions both reported that commit as
successful.
