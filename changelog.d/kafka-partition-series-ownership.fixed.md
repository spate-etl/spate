**The Kafka source scopes its per-partition series to the member's own
assignment** (`spate-kafka`) — `spate_kafka_source_partition_fetch_queue_messages`
and `spate_kafka_source_partition_lag_stored_records` publish nothing for a
partition this member has never held, and read 0 once a rebalance hands the
partition to another member, matching
`spate_kafka_source_partition_not_fetching`. Previously each member published
both series for every partition in the topic's metadata, and a partition
handed to another member kept reading its last value forever. Either failure
made a `sum()` across a consumer group of several members over-count.
