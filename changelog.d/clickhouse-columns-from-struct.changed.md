**Breaking: ClickHouse insert columns come from the row struct**
(`spate-clickhouse`) — `sink: { clickhouse: ... }` no longer takes a
`columns` list; `#[derive(ClickHouseRow)]` generates it from the row
struct's field declaration order, honoring `#[serde(rename)]` for a name no
Rust identifier can spell, such as a `Nested` table's dotted `tags.key`. A
duplicate or malformed name is now a compile error for a derived row,
instead of a sink-startup error. `ClickHouseSinkConfig::new` drops its
`columns` parameter, and `from_component_config`/`build` now return a
`ClickHouseSinkBuilder`; call `.with_row::<Owned<YourRow>>()` on it to get
the runnable sink.
