**Breaking:** **The ClickHouse sink's `connect` returns `ConnectError` and runs the `distributed_check`** (`spate-clickhouse`)

`ClickHouseSinkBuilder::connect` returns `spate_core::error::ConnectError`, where it returned `SchemaError`. Its `source()` is the `SchemaError` or `DistributedCheckError` that failed, which `spate_core::error::find_source` reaches, and its reason is that error's text. The error reads "the ClickHouse sink cannot start: …" for a refusal and "the ClickHouse sink is unavailable: …" for a failure that may pass.

A mismatch is `Refused`. A failed request classes as an insert does: a rejected TLS certificate or a fatal server code is `Refused`, and a connection that fails, a timeout, the `startup_timeout` bound or any other server answer is `Unavailable`. A reply that does not decode is `Refused` at startup, where an insert retries it.

`connect` now runs the `distributed_check` after the settings check, and `ClickHouseSink::validate_distributed` is removed. An application that has the block in its config and never called `validate_distributed` now runs the check, and the sink's account needs the grants it reads with: `SELECT ON system.clusters` and a grant on the `Distributed` table.

The server codes 452 (`SETTING_CONSTRAINT_VIOLATION`) and 467 (`CANNOT_PARSE_BOOL`) are fatal, at startup and at insert. An insert that hit either was retried before.
