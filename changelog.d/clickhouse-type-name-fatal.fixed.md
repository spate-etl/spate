**The ClickHouse sink fails an insert the server answers with `SYNTAX_ERROR` or `UNKNOWN_TYPE`** (`spate-clickhouse`)

An insert whose body carries a type name the server cannot parse, or one that names no type, is now a fatal error, handled as any fatal sink write is. In previous versions the batch retried until the drain deadline. The sink's own encoders do not write such a body; a caller's own `RowEncoder`, or frames passed to `write_batch` directly, can.
