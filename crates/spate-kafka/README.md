# spate-kafka

Kafka source, producer sink and work feed for the
[Spate](https://github.com/spate-etl/spate) framework, built on `rdkafka`.
The source runs **one consumer per process**, with partitions split into
per-partition queues and fanned across pipeline threads as zero-copy lanes.
Offsets are stored when checkpoint watermarks advance and committed on an
interval; rebalances and shutdown share the framework's
drain choreography (drain → commit → release).

Key types: `KafkaSource`, `KafkaSourceConfig` (the `source: { kafka: ... }`
section). No `rdkafka` types appear in public signatures.

The crate also carries the producer sink (`sink::KafkaSink`, the
`sink: { kafka: ... }` section) and a work feed (`feed::KafkaFeed`, built from
a `feed: { kafka: ... }` section and passed to `Pipeline::feed`). The feed
reads units from a work topic as one consumer group and records each unit's
attempt on a compacted attempt topic before the unit is read. It commits a
unit's offset once every record in it is acknowledged and its attempt record
is tombstoned, and moves a failed unit to a retry topic, or at the attempt cap
to a dead-letter topic, before it commits. A unit whose recorded attempts
have reached the cap moves to the dead-letter topic without being read.

## Sharp edges worth knowing

- **`auto.offset.reset` defaults to `latest`** (librdkafka's default; the
  source applies it to a partition with no committed offset): a consumer
  group starting *after* data was produced reads nothing until new data
  arrives. Set it to `earliest` in the `rdkafka:` passthrough when
  backfilling or testing against pre-produced topics.
- The `rdkafka:` passthrough validates a denylist: properties the
  framework owns for correctness — `enable.auto.offset.store`,
  `enable.auto.commit`, `enable.partition.eof`, and the typed fields'
  duplicates — are rejected with explanations rather than silently
  breaking at-least-once.
- Consumption parallelism is bounded by partition count; scale
  horizontally (one group member per pod) and provision partitions.
