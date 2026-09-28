**A departed worker's splits are reassigned when `rebalance_delay` ends** (`spate-coordination`)

The leader reassigns a departed worker's splits at its first step after
`rebalance_delay` elapses. In previous versions it also waited for the next
reconcile tick, adding up to `reconcile_interval` (default 30s) to every
reassignment after a departure. A failed assignment write is retried on the
next step, and a deleted assignment record is republished at once; in previous
versions both could wait for a reconcile.
