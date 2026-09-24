**The coordination store probe checks guarded delete** (`spate-coordination`)

At startup, a `StoreCoordinator` checks that a delete carrying an expected
revision loses at a stale revision and wins at the current one, in both
keyspaces. A store that fails either check stops the start with a fatal error
naming guarded delete. In previous versions the probe discarded the result of
its delete, so a custom `CoordinationStore` whose `delete` ignored the expected
revision started normally. Such a store could remove an assignment, lease or
leadership record another instance had just rewritten. The NATS and in-memory
stores pass the check and are unaffected.
