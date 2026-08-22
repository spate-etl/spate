**A deadline for a member that never gets its partitions back**
(`spate-kafka`) — the Kafka source's `assignment_timeout` (default `5m`, `0s`
disables it) reports a fatal error once the member has held no partitions for
that long, so a pipeline whose group stops re-forming exits and is restarted
with a fresh consumer instead of idling at zero ingest while lag climbs. The
deadline counts from the moment ownership is released, and any accepted
assignment clears it, an empty one included, so a member in a group with more
members than partitions keeps running. `startup_timeout` still covers the
window before the first assignment.
