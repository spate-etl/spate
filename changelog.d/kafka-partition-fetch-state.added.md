**Kafka partition fetch state** (`spate-kafka`)

With `metrics.per_partition_detail: true`, the Kafka source now publishes
`spate_kafka_source_partition_not_fetching`. It reads `1` when an assigned
partition's librdkafka fetch state is not `active` and `0` when it is active.
Previously, the available metrics could not distinguish a partition stopped
locally from one waiting for a leader or offset lookup. If a partition remains
inactive across statistics windows, the source also logs its fetch state and
offsets, even when partition metrics are disabled. Use the gauge to identify
partitions that are not fetching and the log to investigate the reason.
