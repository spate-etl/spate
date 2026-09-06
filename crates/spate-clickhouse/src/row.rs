//! The ClickHouse insert column list, carried by the row type itself.
//!
//! [`ClickHouseRow`] is normally implemented via
//! `#[derive(spate_clickhouse::ClickHouseRow)]` (the trait and the derive
//! macro share this module's re-exported name — the same pattern
//! `serde::Serialize` uses, since a trait and a derive macro occupy separate
//! namespaces; this is deliberate, not a collision to "fix"). A borrowed
//! record family implements [`ClickHouseRowFamily`] itself instead, since the
//! blanket impl below only covers [`Owned`].

use spate_core::deser::{Owned, RecFamily};

/// A row's ClickHouse insert columns, in wire order.
pub trait ClickHouseRow {
    /// The insert column list, in the struct's field declaration order.
    const COLUMNS: &'static [&'static str];
}

/// Carries [`ClickHouseRow::COLUMNS`] at the record-family level.
///
/// `RecFamily::Rec<'buf>` is a generic associated type, and a const cannot be
/// reached through a higher-ranked bound, so the const lives on the family
/// itself rather than on `F::Rec<'_>`.
pub trait ClickHouseRowFamily: RecFamily {
    /// The insert column list, in wire order.
    const COLUMNS: &'static [&'static str];
}

impl<T: ClickHouseRow + Send + 'static> ClickHouseRowFamily for Owned<T> {
    const COLUMNS: &'static [&'static str] = T::COLUMNS;
}

#[cfg(test)]
mod tests {
    // `#[derive(ClickHouseRow)]` resolves `::spate_clickhouse` via
    // `extern crate self as spate_clickhouse;` in `lib.rs` here: unlike an
    // integration test under `tests/`, code inside `src/` is not
    // automatically given the crate itself as an extern dependency.
    #[derive(crate::ClickHouseRow)]
    struct InCrate {
        id: u64,
    }

    #[test]
    fn a_row_derived_inside_the_crate_resolves() {
        use crate::ClickHouseRow;
        assert_eq!(InCrate::COLUMNS, &["id"]);
        assert_eq!(InCrate { id: 1 }.id, 1);
    }
}
