//! `#[derive(ClickHouseRow)]` against the real `ClickHouseRow` trait: does the
//! generated `COLUMNS` match what the field list, renames and skips say it
//! should.

use serde::Serialize;
use spate_clickhouse::ClickHouseRow;

#[derive(Serialize, ClickHouseRow)]
struct Order {
    order_id: u64,
    customer_id: u32,
    region: String,
}

#[test]
fn plain_fields_use_declaration_order() {
    assert_eq!(Order::COLUMNS, &["order_id", "customer_id", "region"]);
}

#[derive(Serialize, ClickHouseRow)]
struct NestedTags {
    id: u64,
    #[serde(rename = "tags.key")]
    tags_key: Vec<String>,
    #[serde(rename = "tags.value")]
    tags_value: Vec<String>,
}

#[test]
fn dotted_rename_names_a_flattened_nested_column() {
    assert_eq!(NestedTags::COLUMNS, &["id", "tags.key", "tags.value"]);
}

#[derive(Serialize, ClickHouseRow)]
struct WithInternalField {
    id: u64,
    #[serde(skip)]
    internal_only: u64,
    name: String,
}

#[test]
fn skip_excludes_the_field_from_columns() {
    assert_eq!(WithInternalField::COLUMNS, &["id", "name"]);
    let row = WithInternalField {
        id: 1,
        internal_only: 2,
        name: "a".to_string(),
    };
    assert_eq!(row.internal_only, 2);
}

#[derive(Serialize, ClickHouseRow)]
struct WithSkipSerializing {
    id: u64,
    #[serde(skip_serializing)]
    audit_only: u64,
}

#[test]
fn skip_serializing_excludes_the_field_from_columns() {
    assert_eq!(WithSkipSerializing::COLUMNS, &["id"]);
    let row = WithSkipSerializing {
        id: 1,
        audit_only: 2,
    };
    assert_eq!(row.audit_only, 2);
}

#[derive(Serialize, ClickHouseRow)]
struct Borrowed<'a> {
    id: u64,
    name: &'a str,
}

#[test]
fn a_lifetime_parameter_carries_onto_the_impl() {
    assert_eq!(Borrowed::COLUMNS, &["id", "name"]);
}

#[derive(Serialize, ClickHouseRow)]
struct WithRawIdentifier {
    id: u64,
    r#type: String,
}

#[test]
fn a_raw_identifier_field_name_is_unraw_d() {
    assert_eq!(WithRawIdentifier::COLUMNS, &["id", "type"]);
}
