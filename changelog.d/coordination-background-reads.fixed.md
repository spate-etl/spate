**Slow store reads no longer stop lease renewals** (`spate-coordination`)

Reconcile listings, the verdict listing and a leader's seeding run beside the
coordinator's loop, so heartbeats, renewals and commits continue while they
read. In previous versions they ran inline. A listing or seeding run longer than
`lease_duration` stopped every renewal, the worker's leases expired and peers
took its splits, and a leader seeding a large plan lost its leadership partway.
Reconcile ticks are now jittered, and the first lands anywhere in the first
`reconcile_interval`, so a fleet started together no longer lists together.
