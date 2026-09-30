**A worker that releases every split it holds stays out of the fleet** (`spate-coordination`)

A `StoreCoordinator` that hands back its last split through `release` leaves
the fleet, and its presence key stays deleted while the process keeps running.
In previous versions its next heartbeat recreated the key. Peers logged
`peer joined` for it, and the leader kept assigning it splits that it never
claimed. Those splits waited until the process exited and the key expired.
