**A failed election write and a lost leader-key delete are read back** (`spate-coordination`)

When the write that creates the leader key returns an error or reports a lost
race, the worker reads the key back and leads if the key holds the value it just
wrote. When a worker gives up leadership and the delete of the leader key loses
its compare-and-swap, the worker reads the key back, up to three times, and
deletes it while it still names this worker. The first heartbeat is now
scheduled before the worker's first election, so a slow first election no longer
delays the first renewal of the presence key and the leader key. In previous
versions the leader key stayed after an election write or a leader renewal whose
reply was lost, and the fleet had no leader until it expired, up to one
`lease_duration`. A first election that took most of a lease could also let the
presence key expire before its first renewal.
