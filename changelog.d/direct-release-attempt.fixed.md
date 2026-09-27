**A split handed back at teardown keeps its delivery attempts** (`spate-coordination`)

When a `StoreCoordinator` is dropped while it holds splits, or finds its
background task gone on `release`, it releases those splits directly in the
store. A worker that claims one of them during that release consumes no
delivery attempt. In previous versions the claim could land after the lease was
deleted but before the owner was cleared. It then counted as a takeover from a
dead owner and consumed an attempt, and a split one attempt short of
`max_attempts` was quarantined. Repeated teardowns could quarantine a split that
never failed.
