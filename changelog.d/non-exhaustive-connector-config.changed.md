**Breaking:** **Connector, sink-pool and coordination configuration structs are
`#[non_exhaustive]`** (`spate-core`, `spate-kafka`, `spate-clickhouse`,
`spate-s3`, `spate-coordination`) — from outside the defining crate a struct
literal, a functional update (`..Default::default()`) and an exhaustive pattern
all stop compiling. Programmatic assembly starts from `new` where a field has no
default (`KafkaSourceConfig`, `KafkaSinkConfig`, `ClickHouseSinkConfig`,
`ShardConfig`, `DistributedCheckSection`, `S3SourceConfig`, and `SinkPoolConfig`
over its four sections) and from `default()` where every field has one
(`BatchConfig`, `InflightConfig`, `RetryConfig`, `BreakerConfig`,
`TimeoutSection`, `CoordinationConfig`), then assigns the fields it wants, which
stay public. Both sink `Compression` enums, and ClickHouse's `Format` and
`SchemaValidation`, are sealed too, so a `match` over one needs a wildcard arm.
Loading a pipeline from YAML is unaffected, and a knob or a codec added after
this ships in an additive release.
