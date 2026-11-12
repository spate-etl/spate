# spate-kafka

The Kafka source, sink and feed (`spate-kafka`, feature `kafka` on the `spate`
facade) read and write Kafka topics through `rdkafka`. The source consumes one
topic as a member of a consumer group. The sink produces to one topic and
acknowledges a batch once every message in it has a confirmed delivery report.
The feed carries work units to a source over four topics: work, retry, dead
letter and attempts.

## Enable

Turn on feature `kafka` on the [`spate`](https://crates.io/crates/spate) facade.
Feature `kafka-tls` adds TLS, mTLS and SASL without GSSAPI or OIDC, and implies
`kafka`. Feature `kafka-json-simd` adds `KafkaJsonEncoder::with_simd_json`, and
implies `kafka`. The facade README has the Cargo snippet.

## Source

### Configuration

The source reads the pipeline's `source: { kafka: ... }` section into
`KafkaSourceConfig`. An unknown key is rejected.

```yaml
source:
  kafka:
    brokers: ${KAFKA_BROKERS:-localhost:9092}  # required
    topic: orders             # required; one topic per pipeline
    group_id: orders-etl      # required
    consumers: 1              # group members this process runs
    commit_interval: 5s       # how often stored offsets are committed
    startup_timeout: 30s      # wait for the first assignment; 0s waits forever
    assignment_timeout: 5m    # time with no partitions before failing; 0s off
    statistics_interval: 5s   # librdkafka statistics; 0s turns them off
    rdkafka: {}               # raw librdkafka properties, checked at load
```

Each value after `group_id` is the default.

Loading refuses a `topic` that starts with `^`, since librdkafka reads it as a
subscription pattern. It also refuses `consumers: 0`, a `commit_interval`
outside 1ms to 24h, and a non-zero `statistics_interval` outside the same
range. A static member id (`group.instance.id`) needs `consumers: 1`. A
passthrough that sets both `auto.offset.reset` and `topic.auto.offset.reset` is
refused too. The `rdkafka:` map refuses each property the source owns, with the
reason, and any cooperative assignment strategy.

The
[Kafka source page](https://spate.kainth.dev/docs/user-guide/connectors/sources/kafka#configuration)
has the full key table, and its
[passthrough section](https://spate.kainth.dev/docs/user-guide/connectors/sources/kafka#the-rdkafka-passthrough-and-the-denylist)
lists every refused property.

### Delivery

- The source stores a partition's offset once every batch below it is
  acknowledged. librdkafka commits the stored offsets every `commit_interval`.
- A rebalance or a graceful shutdown drains the lanes first. The source then
  stores and commits synchronously while the member still owns the partitions,
  so the next owner never starts past a record the sink has not written.
- A crash replays every record after the last commit, so the sink can receive
  a record twice.
- A rebalance error releases the partitions at once. The lanes still drain, but
  their offsets are not stored, and that work replays after the member rejoins.
- A partition with no committed offset starts under `auto.offset.reset`,
  `latest` by default. Under `latest`, every later start resumes from a
  committed offset.

[How offsets are committed](https://spate.kainth.dev/docs/user-guide/connectors/sources/kafka#how-offsets-are-committed)
covers each case, and
[Delivery guarantees](https://spate.kainth.dev/docs/user-guide/concepts/delivery-guarantees)
covers the framework's side.

### Parallelism

Each partition the group assigns to the process becomes one lane, and the
framework spreads the lanes across pipeline threads. A lane is polled only by
the thread that owns it, so decode parallelism is
`min(assigned partitions, pipeline.threads)`. The
[architecture page](https://spate.kainth.dev/docs/user-guide/concepts/architecture#lane-count-is-read-concurrency-not-decode-parallelism)
explains why lanes beyond the thread count add no throughput.

A process runs one consumer by default. `consumers` runs more members of the
group in the same process, each with its own share of the partitions. A
consumer beyond the topic's partition count holds nothing, so the partition
count bounds the consumers that do work across every process in the group. To
scale out, run more processes in the group and give the topic more partitions.
[More consumers in one process](https://spate.kainth.dev/docs/user-guide/connectors/sources/kafka/performance-tuning#more-consumers-in-one-process)
says when a second consumer helps.

## Sink

### Configuration

The sink reads the pipeline's `sink: { kafka: ... }` section into
`KafkaSinkConfig`. An unknown key is rejected.

```yaml
sink:
  kafka:
    brokers: ${KAFKA_BROKERS:-localhost:9092}  # required
    topic: orders-enriched    # required; one topic per sink
    shards: 1                 # workers, all sharing one producer
    delivery_timeout: 30s     # how long a batch waits for its delivery reports
    max_message_bytes: 1MB    # key + payload + headers, per message
    statistics_interval: 5s   # librdkafka statistics; 0s turns them off
    compression: lz4          # none | gzip | snappy | lz4 | zstd; unset by default
    rdkafka: {}               # raw librdkafka producer properties
```

Each value after `topic` except `compression` is the default. The `batch`,
`inflight`, `retry` and `breaker` sections tune the framework's sink pool, and
are checked at load against its rules.

Loading refuses a `topic` over 512 bytes, `shards: 0`, a `delivery_timeout`
under 1ms or above about 24.8 days, a `max_message_bytes` outside 1000 to
1,000,000,000 bytes, and a `compression` set beside a passthrough
`compression.codec`. Every shard writes through the one producer, so raising
`shards` adds writers that contend on that client. Batch size is the throughput
lever. The `rdkafka:` map refuses each property the sink owns, with the reason,
and the properties the framework owns for every client.

The
[Kafka sink page](https://spate.kainth.dev/docs/user-guide/connectors/sinks/kafka#configuration)
has the full key table, and the
[sink-pool reference](https://spate.kainth.dev/docs/user-guide/reference/configuration#sink-pool)
has the pool sections' keys.

### Writes and retries

- `acks=all` and `enable.idempotence=true` are forced, so a confirmed delivery
  report is a durable write. The sink acknowledges a batch only once every
  message in it has one.
- A retryable failure, such as a timed-out report or a broker transport error,
  makes the framework retry the whole sealed batch. Messages the failed attempt
  delivered are produced again, so the topic can hold duplicates.
- A fatal failure, such as an unknown topic, a refused authorization or a
  message over the broker's size limit, abandons the batch. The partition's
  watermark stops advancing.
- Idempotence removes duplicates only from librdkafka's own retries within one
  session. A crash replays records in new batches, and the sink produces them
  again.
- The sink ignores the framework's batch dedup token. Kafka has no server-side
  window to hand it to.

Consumers of the output topic have to tolerate duplicates, for example by
keying idempotent handling on a payload identity, or by compacting the topic on
the message key.
[How a batch becomes durable](https://spate.kainth.dev/docs/user-guide/connectors/sinks/kafka#how-a-batch-becomes-durable)
and
[Delivery semantics](https://spate.kainth.dev/docs/user-guide/connectors/sinks/kafka#delivery-semantics)
on the sink page cover each case.

### Keys and message size

- Each record becomes one message of key, headers and payload. `encoder_bytes`
  passes the payload through, `encoder_json` serializes the record with
  `serde_json`, and `encoder_with` wraps any `MessageEncoder`. A custom encoder
  can set a key, headers or a tombstone.
- `KafkaJsonEncoder::with_simd_json` serializes with `simd-json`, which spends
  fewer instructions on records that carry long text. The
  [sink page](https://spate.kainth.dev/docs/user-guide/connectors/sinks/kafka#json-encoding-with-simd-json)
  lists where its output differs from `serde_json`'s.
- A record carries only a hash of its source message key. A Kafka-to-Kafka
  pipeline that keeps keys derives them again with `with_key_fn` on
  `KafkaBytesEncoder` or `KafkaJsonEncoder`, or in its own `MessageEncoder`.
- The sink names no partition, so librdkafka's partitioner places each message
  by its key.
- The encoder checks `max_message_bytes` for each record. An oversized record
  fails as a record-level error under the sink stage's error policy. A message
  under that limit and over the broker's limit fails its whole batch as fatal.

## Feed

### Configuration

The feed reads the `kafka:` body of the pipeline's `feed:` section. `main()`
builds it with `KafkaFeed::connect` and passes it to `Pipeline::feed`. An
unknown key is rejected.

```yaml
feed:
  kafka:
    brokers: ${KAFKA_BROKERS:-localhost:9092}  # required
    group_id: orders-feed           # required; every worker joins it
    topic: orders-work              # required; publishes write units here
    retry_topic: orders-retry       # required; failed units below the cap
    dead_letter_topic: orders-dead  # required; units at the cap
    attempt_topic: orders-attempts  # required; compacted attempt records
    delivery_timeout: 30s           # bound on each write's delivery report
    statistics_interval: 5s         # librdkafka statistics; 0s turns them off
    rdkafka: {}                     # raw properties for every feed client
  attempt_cap: 10                   # a key of feed: itself, beside kafka:
```

Each value after `attempt_topic` is the default. `units_held`, beside `kafka:`,
sets how many units a worker holds at once. An `attempt_cap` inside the
`kafka:` body is refused.

Loading refuses an empty topic, group or broker list. The retry topic has to
differ from the work topic, the dead-letter topic from both, and the attempt
topic from the other three. `delivery_timeout` runs from 1ms to about 24.8
days. One `rdkafka:` map configures all seven of the feed's clients, and it
refuses each property any of them owns.

The
[Kafka feed page](https://spate.kainth.dev/docs/user-guide/connectors/feeds/kafka#configuration)
has the full key table, and its
[Topics section](https://spate.kainth.dev/docs/user-guide/connectors/feeds/kafka#topics)
says how to create each topic.

### Unit lifecycle

- **Lease.** A unit has no lease. It stays with the worker whose consumer owns
  its partition, for as long as that member stays in the group. A worker that
  dies loses its partitions at the group's `session.timeout.ms`, and the next
  owner reads their units again.
- **Attempt.** Before a unit is read, the feed records its attempt on the
  attempt topic. The unit reaches the reader once that write is confirmed, so
  every read counts, including one whose worker dies.
- **Acknowledgement.** Once every record in a unit is acknowledged, the feed
  tombstones the unit's attempt record. The unit's offset finishes once the
  tombstone is confirmed. Each partition commits up to its first unfinished
  offset every second, so an unfinished unit holds back the commits behind it.
- **Retry.** A unit its reader fails below `attempt_cap` is written to the retry
  topic. The group reads that topic beside the work topic. The unit's offset
  finishes once that write is confirmed.
- **Dead letter.** A unit failed at or past `attempt_cap` is written to the
  dead-letter topic, and its offset finishes once the tombstone after that
  write is confirmed. A unit whose recorded attempts have already reached the
  cap goes there without being read. So does a message whose key is not UTF-8,
  since it names no unit.

A write that fails transiently is sent again after a backoff from 200 ms up to
5 s while the worker still owns its partition, and only its own partition waits
for it.
[How a unit finishes](https://spate.kainth.dev/docs/user-guide/connectors/feeds/kafka#how-a-unit-finishes)
and
[Counting deliveries](https://spate.kainth.dev/docs/user-guide/connectors/feeds/kafka#counting-deliveries)
cover each case.

### Pairs with

- The S3 source reads its work units from a feed, and fails at start when the
  pipeline has no `feed:` section.
- A process that only publishes, such as a lister run, builds a
  `KafkaFeedPublisher` from the same section. It joins no group and records no
  attempts, so it takes no partition from the workers.
- A job is bounded when its `feed:` section names a `location`. Its workers read
  the group's lag on the work and retry topics as the feed's depth, and the job
  can end only once that depth reads zero over the settling window. If the
  dead-letter topic then holds any offset, the job ends as stalled. A job with
  no `location` keeps running.
- A dead-letter topic an earlier job wrote fails a later job as stalled, so
  give each bounded job fresh work, retry, dead-letter and attempt topics.

[Ending a bounded job](https://spate.kainth.dev/docs/user-guide/connectors/feeds/kafka#ending-a-bounded-job)
covers the depth reads, and the
[S3 source page](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#how-a-bounded-job-ends)
states when a worker ends the job.

## Sharp edges

- Under the default `auto.offset.reset` of `latest`, a partition with no
  committed offset starts at its end, and the source skips the records already
  on it. Set `auto.offset.reset: earliest` in the source's `rdkafka:` map to
  read them. See
  [How offsets are committed](https://spate.kainth.dev/docs/user-guide/connectors/sources/kafka#how-offsets-are-committed).
- On the source, `statistics_interval: 0s` also removes consumer lag. The lag
  comes from librdkafka's statistics and has no other input. See the
  [source's configuration](https://spate.kainth.dev/docs/user-guide/connectors/sources/kafka#configuration).
- Without `kafka-tls`, each role refuses a TLS or SASL property in its
  `rdkafka:` map at load. See
  [Security](https://spate.kainth.dev/docs/user-guide/connectors/sources/kafka#security-tls-mtls-sasl).
- The feed never creates a topic. Create all four before the first worker
  starts: the attempt topic compacted and nothing else, the other three not
  compacted, and the work topic with `message.timestamp.type=LogAppendTime`.
  See
  [Startup checks](https://spate.kainth.dev/docs/user-guide/connectors/feeds/kafka#startup-checks).
- Each partition of the feed's work and retry topics is read by one worker at a
  time, so the larger of the two partition counts bounds how many workers read
  at once. See
  [Topics](https://spate.kainth.dev/docs/user-guide/connectors/feeds/kafka#topics).

## Metrics

Each role registers families of its own, prefixed `spate_kafka_source_`,
`spate_kafka_sink_` and `spate_kafka_feed_`. The source and sink families, and
the feed's client statistics, translate librdkafka's statistics and are
registered only when `statistics_interval` is above zero. The feed's write
families do not depend on it. The Metrics section of the
[source](https://spate.kainth.dev/docs/user-guide/connectors/sources/kafka#metrics),
[sink](https://spate.kainth.dev/docs/user-guide/connectors/sinks/kafka#metrics)
and
[feed](https://spate.kainth.dev/docs/user-guide/connectors/feeds/kafka#metrics)
pages lists each family with its labels.

## Dependencies

`rdkafka` compiles librdkafka from vendored sources, so the build host needs a
working C toolchain, and the first build takes a few minutes. `kafka-tls` also
compiles OpenSSL from vendored sources and needs Perl 5. Windows is not
supported. The
[toolchain requirements](https://spate.kainth.dev/docs/user-guide/getting-started/installation#toolchain-requirements)
give the detail.

No `rdkafka` type appears in this crate's public API, so an `rdkafka` major
release is not a breaking change here.

## Related

- [`spate`](https://crates.io/crates/spate) — the facade an application depends
  on, with the Cargo snippet.
- [Kafka source page](https://spate.kainth.dev/docs/user-guide/connectors/sources/kafka)
  — every source key, the passthrough denylist, offset commits and the metric
  families.
- [Kafka source performance tuning](https://spate.kainth.dev/docs/user-guide/connectors/sources/kafka/performance-tuning)
  — prefetch depth, thread sizing and scaling past one consumer.
- [Kafka sink page](https://spate.kainth.dev/docs/user-guide/connectors/sinks/kafka)
  — every sink key, error classes, keys and tombstones, and the metric families.
- [Kafka feed page](https://spate.kainth.dev/docs/user-guide/connectors/feeds/kafka)
  — topics, startup checks, attempt counting, stopping and the metric families.
- [API documentation on docs.rs](https://docs.rs/spate-kafka) — the source, the
  sink and the feed, and the source and sink configuration types.
- [`kafka_avro_to_clickhouse.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/kafka_avro_to_clickhouse.rs)
  — an Avro order stream from Kafka into ClickHouse. Needs Kafka, a schema
  registry and ClickHouse.
- [`kafka_to_kafka_split.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/kafka_to_kafka_split.rs)
  — one stream fanned out to a topic per region, with a Kafka sink for each.
  Needs Kafka.
- [`s3_kafka_backfill.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/s3_kafka_backfill.rs)
  — an object-storage backfill whose work arrives through the Kafka feed. Needs
  Kafka.
- [Examples index](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/README.md)
  — every example, with the features and infrastructure each needs.
- [`spate-test`](https://crates.io/crates/spate-test) — in-memory sources and
  sinks, for a test that drives a pipeline with no broker.
