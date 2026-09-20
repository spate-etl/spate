**ClickHouse row derive** (`spate-clickhouse-derive`)

`#[derive(ClickHouseRow)]` now implements `spate_clickhouse::ClickHouseRow` for
a row struct and generates its insert column list in field declaration order.
It supports `#[serde(rename = "...")]` for names such as `outer.inner` in a
flattened `Nested` table. Duplicate or malformed column names are compile
errors. The derive also rejects `#[serde(flatten)]`, container-level
`#[serde(rename_all = "...")]`, and `#[serde(skip_serializing_if = "...")]`.
