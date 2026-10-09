**A commit made under an ended tenancy no longer lands in the next one**
(`spate-coordination`)

A `StoreCoordinator` commit now belongs to the tenancy of the latest `Gained`
event that `poll` returned for the split. When that tenancy has ended, the
commit returns `Fenced` and writes nothing, even if this worker has claimed the
split again since. In previous versions, a commit still queued when a worker
paused for longer than its lease could be written into the worker's next
tenancy of the same split. That tenancy started from the progress it read at
the claim, so its next commit had a lower watermark and failed with a Fatal
error that named the source. Sources that use `CoordinationDriver` need no
change.
