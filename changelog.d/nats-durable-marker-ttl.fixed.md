**The NATS state bucket no longer grows with every restart** (`spate-coordination`)

A key deleted from the NATS coordination store's state bucket leaves a marker
that the server removes one `lease_duration` later. In previous versions every
marker stayed, and each coordinator start added at least one, so reconcile
listings slowed as the bucket grew. On a bucket an earlier version created, a
worker enables per-message TTLs at startup. This needs the workers'
credentials to allow `$JS.API.STREAM.UPDATE` on the bucket's stream; without
it the worker logs a warning and keeps the old behavior. Markers written
before the upgrade, or by workers not yet upgraded, stay until removed, for
example with `nats kv compact` while the job is stopped.
