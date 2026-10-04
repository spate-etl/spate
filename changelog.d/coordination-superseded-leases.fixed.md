**A split lease that a later claim or release replaced keeps no lane**
(`spate-coordination`)

The leader counts a split lease toward its owner's lanes only while the split's
progress record names that owner, or while the lease is a claim ahead of the
record. A lease from another owner that the record has moved past, or from a
released split, no longer holds a lane. In previous versions the leader could
still see such a lease after the record showed the next claim, because leases
and records reach the leader separately. Its old owner kept a lane for it, so
the leader could move the split back to that owner and revoke it from the
worker that had just claimed it. The old owner could also look full, and the
leader then revoked one of the old owner's other splits and moved it to another
worker. No records were lost. A revoked split that could not drain cleanly
replayed its uncommitted records under the next worker.
