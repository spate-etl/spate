**NATS listings and watch snapshots end when their keys expire** (`spate-coordination`)

The NATS coordination store fails a key listing or a watch snapshot as
retryable when no message arrives for a quarter of `lease_duration`. The
reconcile pass and the watch then start again on their own. In previous
versions, when the last keys a listing had yet to receive expired first, the
listing waited forever and the worker's coordination loop stopped with it.
Lease keys expire all the time, so a job with many leases in flight was
exposed.
