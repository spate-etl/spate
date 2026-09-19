**Breaking:** **Kafka metrics follow partition ownership** (`spate-kafka`)

`spate_kafka_source_partition_fetch_queue_messages` and
`spate_kafka_source_partition_lag_stored_records` now update only for
partitions assigned to the current consumer. Their existing series are set
to zero when a rebalance moves a partition to another consumer.
Previously, the fetch-queue metric included partitions the consumer had never
owned, and stored lag could retain an old value after ownership changed.
Summing stored lag across consumers could therefore count the same partition
more than once.

Review dashboards and recording rules that use these metrics. Series for
previously owned partitions remain present with a value of zero, so counting
series does not measure the current assignment size. No series is created for
a partition the consumer has never owned.
