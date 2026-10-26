**ClickHouse query responses arrive uncompressed** (`spate-clickhouse`)

The ClickHouse sink asks for its query responses uncompressed. These are the schema check, the `Distributed` guard and the breaker's `SELECT 1` probe. Previously the sink asked for them in the codec `compression` names, and the server compressed them when its `enable_http_compression` setting was on. Insert bodies are unchanged: `compression` still applies to them. The responses are a few rows, so throughput does not change.
