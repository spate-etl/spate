**Breaking:** **ClickHouse insert columns from the row type**
(`spate-clickhouse`)

The ClickHouse sink now takes its insert column list from the row type, and
`#[derive(ClickHouseRow)]` generates that list in field declaration order.
Previously, the column list was supplied separately in the sink configuration.
The derive reports duplicate or malformed names at compile time; those errors
were previously detected when the sink was built. Use `#[serde(rename)]` for
column names such as `tags.key` that cannot be Rust field names.

Remove `columns` from `sink: { clickhouse: ... }` and from calls to
`ClickHouseSinkConfig::new`. The `from_component_config` and `build` functions
now return a `ClickHouseSinkBuilder`; await its
`.with_row::<Owned<YourRow>>()` method to obtain the runnable sink.
