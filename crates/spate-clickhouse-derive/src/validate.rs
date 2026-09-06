//! The column-name shape this derive enforces at compile time.
//!
//! Mirrors `spate-clickhouse::config`'s runtime `is_identifier`/
//! `is_column_name`, which stays in `spate-clickhouse` as the backstop for a
//! hand-written `ClickHouseRowFamily` impl that never goes through this
//! derive. The two copies are deliberate: this crate cannot depend on
//! `spate-clickhouse` to share one, since `spate-clickhouse` depends on this
//! crate. Keep both in sync by hand if the shape ever changes.

/// Strict identifier: `[A-Za-z_][A-Za-z0-9_]*`. Validated before being
/// backtick-quoted into SQL, so no escaping is ever needed.
pub(crate) fn is_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A column name: one or more `is_identifier` segments joined by `.`.
/// ClickHouse names a flattened `Nested` column `outer.inner`, and the whole
/// name is backtick-quoted as a single identifier, so no escaping is needed.
pub(crate) fn is_column_name(s: &str) -> bool {
    s.split('.').all(is_identifier)
}
