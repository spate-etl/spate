**`#[derive(ClickHouseRow)]`** (`spate-clickhouse-derive`) — a new proc-macro
crate implements `spate_clickhouse::ClickHouseRow` for a row struct, generating
the insert column list from the field declaration order. It honors
`#[serde(rename = "...")]` for a column name no Rust identifier can spell, such
as a flattened `Nested` table's dotted `outer.inner`, and rejects a duplicate
name, a malformed name, `#[serde(flatten)]`, a container-level
`#[serde(rename_all = "...")]`, and `#[serde(skip_serializing_if = "...")]` at
compile time. Nothing in `spate-clickhouse` consumes it yet.
