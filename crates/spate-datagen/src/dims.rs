//! The storefront's dimension tables: regions, catalog, customer space.
//!
//! Small, fixed and `&'static` on purpose. A generated event references these
//! by borrowing, so producing one allocates nothing, and a downstream join or
//! `GROUP BY` in a demo has a bounded number of groups — which is what makes a
//! generated stream legible in a dashboard rather than a wall of unique keys.

/// The regions an order can be placed from.
pub const REGIONS: &[&str] = &["eu-west", "eu-north", "us-east", "us-west", "apac"];

/// The catalog: 32 stock-keeping units across a handful of product families.
pub const SKUS: &[&str] = &[
    "KBD-01", "KBD-02", "KBD-03", "KBD-04", "MSE-01", "MSE-02", "MSE-03", "MSE-04", "MON-01",
    "MON-02", "MON-03", "MON-04", "HDS-01", "HDS-02", "HDS-03", "HDS-04", "CAM-01", "CAM-02",
    "CAM-03", "CAM-04", "DCK-01", "DCK-02", "DCK-03", "DCK-04", "CBL-01", "CBL-02", "CBL-03",
    "CBL-04", "STD-01", "STD-02", "STD-03", "STD-04",
];

/// How many distinct customers place orders. Ids run `0..CUSTOMERS`.
pub const CUSTOMERS: u32 = 1024;

/// List price per catalog entry, in cents, positionally aligned with
/// [`SKUS`]. A stable price per unit is what lets a payment's
/// `amount_cents` be recomputed from its order's lines, so the two can be
/// checked against each other downstream instead of merely counted.
pub(crate) const UNIT_CENTS: &[u32] = &[
    7_900, 12_900, 18_900, 24_900, 3_500, 4_900, 7_900, 11_900, 129_000, 189_000, 249_000, 399_000,
    5_900, 9_900, 14_900, 22_900, 6_900, 10_900, 15_900, 21_900, 8_900, 13_900, 19_900, 27_900,
    900, 1_400, 1_900, 2_400, 34_900, 44_900, 59_900, 79_900,
];

/// Why a refund was issued.
pub(crate) const REFUND_REASONS: &[&str] = &[
    "damaged",
    "not_as_described",
    "late_delivery",
    "changed_mind",
    "duplicate_order",
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn the_catalog_is_the_documented_size_and_has_a_price_for_every_entry() {
        assert_eq!(SKUS.len(), 32, "the crate docs promise 32 SKUs");
        assert_eq!(
            UNIT_CENTS.len(),
            SKUS.len(),
            "prices are indexed by SKU position; a short table would panic"
        );
        assert!(UNIT_CENTS.iter().all(|&c| c > 0), "no free units");
    }

    #[test]
    fn every_dimension_value_is_distinct() {
        for (what, values) in [
            ("regions", REGIONS),
            ("skus", SKUS),
            ("refund reasons", REFUND_REASONS),
        ] {
            let unique: BTreeSet<_> = values.iter().collect();
            assert_eq!(unique.len(), values.len(), "duplicate {what}");
        }
        assert_eq!(REGIONS.len(), 5);
    }
}
