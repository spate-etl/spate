**`MemoryStore` keeps reporting lease expiry after the runtime that first used it stops** (`spate-coordination`)

Expired ephemeral keys reach watchers for the life of the store. In previous
versions the expiry sweeper ran on the runtime of the first call that touched
the ephemeral keyspace and stopped with it. Coordinators on other runtimes
sharing the store then learned about expired leases only from their reconcile
listings, so a takeover waited up to `reconcile_interval` longer.
