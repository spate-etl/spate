**A failed flush names the first shard that could not finalize** (`spate-core`)
— when an encoder failed to finalize more than one shard's block in the same
flush, the `FatalError` the pipeline stopped on carried the last shard's reason.
It carries the first, which is the shard the seal loop reached first, so the
reason in the run's failure and in the exit report names the failure that
happened first. A shard whose block finalizes cleanly still ships its chunk when
another shard has already failed, and the fatal count on
`spate_operator_errors_total` is one either way.
