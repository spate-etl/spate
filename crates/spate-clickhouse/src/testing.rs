//! Fixtures for testing a pipeline's ClickHouse sink against the `clickhouse`
//! crate's mock server.
//!
//! Building a sink reads `system.columns`, so a mock has to answer that query
//! before it sees an insert. [`system_columns`] builds the rows to answer it
//! with, for `clickhouse::test::handlers::provide`.
//!
//! Behind the off-by-default `testing` feature: the rows implement a
//! `clickhouse` 0.x trait, which this crate's public API otherwise avoids
//! entirely, and no stability promise attaches to them.

pub use crate::schema::ColumnRow;

/// Rows answering the sink's `system.columns` query, from
/// `(name, type, default_kind)` triples in table order. `default_kind` is
/// `""` for an ordinary column, or `DEFAULT`, `MATERIALIZED` or `ALIAS`.
///
/// ```ignore
/// mock.add(handlers::provide(system_columns(&[
///     ("id", "UInt64", ""),
///     ("name", "String", ""),
/// ])));
/// ```
#[must_use]
pub fn system_columns(specs: &[(&str, &str, &str)]) -> Vec<ColumnRow> {
    specs
        .iter()
        .map(|(name, type_, default_kind)| ColumnRow {
            name: (*name).to_string(),
            type_: (*type_).to_string(),
            default_kind: (*default_kind).to_string(),
        })
        .collect()
}
