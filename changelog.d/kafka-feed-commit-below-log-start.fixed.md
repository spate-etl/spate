**Kafka feed: a held unit whose message retention deletes** (`spate-kafka`)

A bounded job on the Kafka feed stays open while a worker holds a unit whose message retention has deleted, and the worker reads the unit to its end. The feed's depth counts a partition's lag from the group's commit when the commit is below the log start. In previous versions it counted from the log start, so every peer read the partition as drained and ended. The rebalance a peer's exit caused then took the unit from its holder, and the job could end `Completed` with most of the unit's rows unwritten.

A rebalance while the unit is held still loses it. The worker that takes the partition next logs a `warn` line naming the offsets it could not read, and counts them in the new `spate_kafka_feed_offsets_skipped_total` counter. A bounded job's workers also commit past offsets below the log start, so a partition whose last messages retention deleted still lets the job end.
