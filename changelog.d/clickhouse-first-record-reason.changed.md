**A Native first-record mismatch reads as RowBinary's** (`spate-clickhouse`)

When the first record's row struct does not match the declared columns, the sink's fatal error reason under `format: native` now starts with `row struct does not match declared columns for` and the table name, as under `format: rowbinary`. In previous versions the Native reason began with `native encoding failed: `. The diff that follows and the error's class are unchanged.
