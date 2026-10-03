**A split lease whose renewal reply was lost is renewed again in the same
heartbeat** (`spate-coordination`)

When a split's lease renewal loses its compare-and-swap to this worker's own
earlier renewal, the worker adopts that renewal and renews the lease again in
the same heartbeat. When the other store calls in that heartbeat and the one
before it answer promptly, the lease is renewed before it expires. The
self-fence counts the adopted renewal from the first renewal that failed since
the last successful one. In previous versions the next renewal waited for the
following heartbeat. When the earlier reply was lost to `op_timeout`, that
renewal could come after the lease expired. The split was then claimed again at
a new epoch and used up a delivery attempt. The self-fence counted from the
adoption, so a worker whose watch had not yet reported the expiry kept the split
for longer after its lease expired.
