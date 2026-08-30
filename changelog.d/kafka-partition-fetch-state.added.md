**The Kafka source publishes librdkafka's per-partition fetch state**
(`spate-kafka`) — `spate_kafka_source_partition_not_fetching` reads 1 while a
partition this member holds is in a fetch state other than fetching, beside the
two per-partition series `metrics.per_partition_detail` already gates. A
partition that stays unfetched across statistics windows is logged as well,
naming the librdkafka state and the partition's offsets, whether or not
`per_partition_detail` is on. The state separates a partition parked on the
framework's side from a leader or an offset lookup that has not answered. Every
other series the source publishes renders those two causes the same way, since
both leave the consumer lag unknown, the assignment full and the group healthy.
