**The Kafka source scopes its per-partition series to the member's own
assignment** (`spate-kafka`) — `spate_kafka_source_partition_fetch_queue_messages`
and `spate_kafka_source_partition_lag_stored_records` render only for
partitions this member holds, matching `spate_kafka_source_partition_not_fetching`
and `spate_source_lag_records`. In a consumer group of several members, a
`sum()` across the group no longer over-counts by a multiple of the true
value, and a `count()` of either family now equals the member's assignment
size.
