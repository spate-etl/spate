**Breaking:** **The ClickHouse encoders and router carry the connector's name** (`spate-clickhouse`)

`ClickHouseEncoder` is renamed `ClickHouseRowBinaryEncoder`, `NativeEncoder` is renamed `ClickHouseNativeEncoder`, `PreEncodedRows` is renamed `ClickHouseBytesEncoder`, and `DistributedRouter` is renamed `ClickHouseDistributedRouter`. `ClickHouseEncoder::with_schema(schema)` is renamed `ClickHouseRowBinaryEncoder::new(schema)`, the same constructor name the Native encoder has. What each type does is unchanged. The router's configuration errors and its topology panic name the new type.
