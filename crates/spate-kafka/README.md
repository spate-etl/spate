# spate-kafka

Kafka source for the [Spate](https://github.com/spate-etl/spate)
framework, built on `rdkafka`: **one consumer per process**, partitions
split into per-partition queues and fanned across pipeline threads as
zero-copy lanes. Offsets are stored when checkpoint watermarks advance and
committed on an interval; rebalances and shutdown share the framework's
drain choreography (drain → commit → release).

Key types: `KafkaSource`, `KafkaSourceConfig` (the `source: { kafka: ... }`
section). No `rdkafka` types appear in public signatures.

## Sharp edges worth knowing

- **`auto.offset.reset` defaults to `latest`** (librdkafka's default,
  passed through as user policy): a consumer group starting *after* data
  was produced reads nothing until new data arrives. Set it to `earliest`
  in the `rdkafka:` passthrough when backfilling or testing against
  pre-produced topics.
- The `rdkafka:` passthrough validates a denylist: properties the
  framework owns for correctness — `enable.auto.offset.store`,
  `enable.auto.commit`, `enable.partition.eof`, and the typed fields'
  duplicates — are rejected with explanations rather than silently
  breaking at-least-once.
- Consumption parallelism is bounded by partition count; scale
  horizontally (one group member per pod) and provision partitions.
