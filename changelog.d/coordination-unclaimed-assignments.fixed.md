**Assigned splits stay with their worker until the claim is seen** (`spate-coordination`)

The leader keeps a split with the worker it last assigned it to while that
worker's claim is on its way to the leader, as long as the worker is still a
member with a free lane. In previous versions the leader treated such a split as
unowned. When another split completed, it could assign the split to a different
worker, and a worker that had already claimed it was asked to give it back. On
a large fleet, or on a store with slow or polled change notification, these
revocations fed each other, lowered throughput and increased store writes. No
records were lost. A revoked split that could not drain cleanly replayed its
uncommitted records under the next worker.
