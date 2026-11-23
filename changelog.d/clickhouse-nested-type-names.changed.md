**Breaking:** **The ClickHouse nested config types carry the connector's name** (`spate-clickhouse`)

`Format` is renamed `ClickHouseFormat`, `Compression` is renamed `ClickHouseCompression`, `config::ShardConfig` is renamed `ClickHouseShardSection`, and `DistributedCheckSection` is renamed `ClickHouseDistributedCheckSection`. Variants, fields, constructors and YAML keys are unchanged, and `ClickHouseShardSection` stays under `spate_clickhouse::config`. A refusal or `Debug` output that names a shard or `distributed_check` block names the new type.
