**Breaking:** **The `full` feature** (`spate`)

The `spate` crate no longer declares a `full` feature, so each build names the
connectors it uses. In previous versions, `full` enabled `avro`, `json`,
`kafka`, `clickhouse`, `coordination-nats` and `s3`. A dependency that still
names `full` fails to resolve.

To keep the same set, replace `features = ["full"]` with
`features = ["avro", "json", "kafka", "clickhouse", "coordination-nats", "s3"]`.
Most pipelines need fewer. A Kafka, Avro and ClickHouse pipeline needs
`features = ["kafka", "clickhouse", "avro"]`, and does not compile the NATS
and object storage dependencies.
