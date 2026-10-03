**A commit after a commit whose reply was lost keeps the split** (`spate-coordination`)

When a commit or a failure report loses its compare-and-swap, the worker now
reads the split record back. If the record is this worker's own, from an
earlier commit whose reply was lost, the worker writes again on top of it. For
a commit, a read older than the write that won, or one that fails with a
retryable error, returns a retryable error and the worker keeps the split. For
a failure report, the same read is reported as fenced and the split as lost, as
in previous versions, unless the record is this worker's own completed one. The
lease is then released, no lost split is reported, and the report is fenced
without naming a peer. A commit after this worker's own completing commit
releases the lease, reports no lost split, and returns success if it repeats
that completing commit and fenced otherwise. In previous versions, when the next
commit or failure report ran before the watch delivered the earlier commit, it
was reported as fenced, and the split was claimed again at a new epoch, which
used up a delivery attempt. On a store whose watch is polled, split records do
not arrive on the watch, and these cases are covered the same way.
