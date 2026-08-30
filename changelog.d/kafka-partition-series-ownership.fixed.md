**Breaking:** **The Kafka source scopes its per-partition series to the
member's own assignment** (`spate-kafka`) —
`spate_kafka_source_partition_fetch_queue_messages` and
`spate_kafka_source_partition_lag_stored_records` publish nothing for a
partition this member has never held, and read 0 once a rebalance hands the
partition to another member, matching
`spate_kafka_source_partition_not_fetching`. Previously
`spate_kafka_source_partition_fetch_queue_messages` published a series for
every partition in the topic's metadata regardless of who held it, so a
member's own scrape carried series for partitions it never touched, and a
`count()` over the family did not equal the member's assignment size.
`spate_kafka_source_partition_lag_stored_records` kept reading its last
known value forever once a partition moved to another member, since
librdkafka reports an unknown lag as `-1` and the write was skipped rather
than zeroed, so a `sum()` across a consumer group of several members
double-counted that partition. A recording rule or a dashboard panel built
on either failure now reads a different, correct number.
