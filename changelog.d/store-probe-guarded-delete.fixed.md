**The coordination store probe checks guarded delete** (`spate-coordination`)

At startup, a `StoreCoordinator` checks that a delete carrying an expected
revision loses at a stale revision and wins at the current one, in both
keyspaces. A store that fails either check stops the start with a fatal error
naming guarded delete. In previous versions the probe discarded the result of
its delete, so a custom `CoordinationStore` whose `delete` ignored the expected
revision started normally. Such a store could remove an assignment, lease or
leadership record another instance had just rewritten. The NATS and in-memory
stores pass the check and are unaffected.

Each start probes under its own key, `_probe.{instance_id}.{nonce}`. Two live
processes sharing an `instance_id` stop with the duplicate `instance_id` error.
In previous versions they shared one probe key, and a start could instead fail
with an error saying the store cannot host coordination. A start that dies
mid-probe leaves its durable probe key behind, and the coordinator ignores it.
