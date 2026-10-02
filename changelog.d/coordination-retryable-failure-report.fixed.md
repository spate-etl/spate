**A failure report the coordinator answers as retryable is offered again** (`spate-core`)

When a source hands a split back as failed and the coordinator answers with a
retryable error, such as a store write that timed out, the report is now kept
and offered on each poll until the coordinator accepts it, and the split's lane
is retired then. In previous versions the report was dropped along with any
later reports in the same batch, and the split stayed held and renewed with no
failure ever recorded. The S3 source hit this for an object that cannot be
read. `spate-test`'s `ScriptedCoordinator` gains `fail_next_report` to script
the answer to a failure report.
