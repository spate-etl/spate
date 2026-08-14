//! Opt-in startup schema validation: fetch the target table's columns
//! from every replica, fail fast (with a readable diff) when the
//! configured columns cannot work, and hand the encoder a parsed,
//! config-ordered schema for its first-record struct check.
//!
//! Two moments, two checks:
//!
//! - **Startup** (`validate`): the config's `columns` against
//!   `system.columns` on every replica of every shard, catching missing
//!   columns, non-insertable (MATERIALIZED/ALIAS) columns, and inter-replica
//!   drift.
//!   Config order differing from *table* order is not an error: the
//!   `INSERT` column list maps by name, so only the struct must agree with
//!   the config.
//! - **First record** (`check_first_record`, driven by both encoders,
//!   RowBinary's [`crate::ClickHouseEncoder::with_schema`] and the Native
//!   encoder whenever its schema was fetched): the row struct's probed
//!   field names, order, and, in `full` mode, type classes against the
//!   configured columns, including wire-wrapper scale vs the column's
//!   declared precision (`DateTime64Millis` into `DateTime64(6)` fails
//!   here). This is where the positional wire contract is enforced.

pub(crate) mod probe;
pub(crate) mod typeparse;

use crate::config::SchemaValidation;
use crate::writer::ClickHouseEndpoint;
use probe::{FieldShape, Shape, compatible};
use serde::Deserialize;
use std::fmt::Write as _;
use std::sync::Arc;
use typeparse::ChType;

/// Schema validation failed at sink startup.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SchemaError {
    /// A replica's schema could not be fetched. The user opted into
    /// fail-fast validation, so connectivity problems fail startup too —
    /// the message keeps "could not fetch" and "mismatch" distinguishable
    /// at a glance.
    #[error("sink.clickhouse: could not fetch schema for {table} from {url}: {reason}")]
    Fetch {
        /// The (possibly database-qualified) table.
        table: String,
        /// The replica URL.
        url: String,
        /// Human-readable cause.
        reason: String,
    },
    /// The configuration does not match the live table (pre-formatted
    /// multi-line diff).
    #[error("{0}")]
    Mismatch(String),
}

/// The validated, config-ordered expected schema. Produced by
/// [`crate::ClickHouseSink::validate_schema`]; consumed by
/// [`crate::ClickHouseEncoder::with_schema`] for the first-record check.
/// Opaque: holds no `clickhouse` crate types.
#[derive(Debug)]
pub struct RowSchema {
    pub(crate) mode: SchemaValidation,
    pub(crate) table: String,
    /// `(column name, parsed type, raw type string)` in **config** order.
    pub(crate) columns: Vec<(String, ChType, String)>,
}

/// What `build()` captures for a later `validate_schema()` call.
#[derive(Clone, Debug)]
pub(crate) struct SchemaCheck {
    pub(crate) mode: SchemaValidation,
    pub(crate) database: Option<String>,
    pub(crate) table: String,
    pub(crate) columns: Vec<String>,
}

/// One row of `system.columns`, crate-private.
#[derive(Clone, Debug, PartialEq, Eq, clickhouse::Row, Deserialize)]
pub(crate) struct ColumnRow {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) type_: String,
    pub(crate) default_kind: String,
}

impl SchemaCheck {
    /// `(database, bare table)`. A `db.table` qualification wins over the
    /// config's `database` field, mirroring how the INSERT resolves.
    fn target(&self) -> (Option<&str>, &str) {
        match self.table.split_once('.') {
            Some((db, tbl)) => (Some(db), tbl),
            None => (self.database.as_deref(), &self.table),
        }
    }

    fn display_table(&self) -> String {
        match self.target() {
            (Some(db), tbl) => format!("`{db}`.`{tbl}`"),
            (None, tbl) => format!("`{tbl}`"),
        }
    }
}

/// Fetch one replica's view of the table.
async fn fetch_columns(
    endpoint: &ClickHouseEndpoint,
    check: &SchemaCheck,
) -> Result<Vec<ColumnRow>, SchemaError> {
    let (db, table) = check.target();
    let query = match db {
        Some(db) => endpoint
            .client()
            .query(
                "SELECT name, type, default_kind FROM system.columns \
                 WHERE database = ? AND table = ? ORDER BY position",
            )
            .bind(db)
            .bind(table),
        None => endpoint
            .client()
            .query(
                "SELECT name, type, default_kind FROM system.columns \
                 WHERE database = currentDatabase() AND table = ? ORDER BY position",
            )
            .bind(table),
    };
    query
        .fetch_all::<ColumnRow>()
        .await
        .map_err(|e| SchemaError::Fetch {
            table: check.display_table(),
            url: endpoint.url().to_string(),
            reason: e.to_string(),
        })
}

fn table_column_list(cols: &[ColumnRow]) -> String {
    cols.iter()
        .map(|c| {
            if c.default_kind.is_empty() {
                format!("{} {}", c.name, c.type_)
            } else {
                format!("{} {} {}", c.name, c.type_, c.default_kind)
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// If a configured column's declared type is an `AggregateFunction(...)`,
/// return an actionable error explaining the correct ingestion path.
///
/// The sink cannot write aggregate *states* directly: their wire form is the
/// opaque, unframed, version-dependent internal serialization, and
/// reproducing it client-side would couple us to one ClickHouse serialization
/// version. A sink pointed straight at such a column is therefore a
/// misconfiguration. The supported pattern is to INSERT raw rows into an
/// `ENGINE = Null` landing table and let a `MATERIALIZED VIEW` build the
/// states into the target.
///
/// Matched on the exact constructor name so `SimpleAggregateFunction(...)`,
/// which stores the raw value and *is* directly insertable, is not flagged.
fn aggregate_function_remedy(col: &str, type_: &str) -> Option<String> {
    let ctor = type_.split_once('(').map(|(name, _)| name.trim())?;
    if ctor != "AggregateFunction" {
        return None;
    }
    Some(format!(
        "configured column `{col}` has type `{type_}`: the sink cannot write \
         aggregate states directly (their wire format is opaque and \
         version-dependent). Insert raw rows into an `ENGINE = Null` landing \
         table and let a `MATERIALIZED VIEW` compute the states \
         (minState/maxState/sumMapState/...) into this table. Client-side \
         aggregate-state serialization is not supported."
    ))
}

/// Startup validation against every replica of every shard. `Ok(None)`
/// when the mode is `Off`; `Ok(Some(schema))` for the encoder otherwise.
pub(crate) async fn validate(
    check: &SchemaCheck,
    endpoints: &[Vec<ClickHouseEndpoint>],
) -> Result<Option<Arc<RowSchema>>, SchemaError> {
    if check.mode == SchemaValidation::Off {
        return Ok(None);
    }

    // Every replica must agree: shard-local tables drift independently,
    // and a broken replica should surface now, not at its first rotated
    // write.
    let mut reference: Option<(String, Vec<ColumnRow>)> = None;
    for shard in endpoints {
        for endpoint in shard {
            let cols = fetch_columns(endpoint, check).await?;
            if cols.is_empty() {
                return Err(SchemaError::Mismatch(format!(
                    "sink.clickhouse: table {} not found (or not visible to this user) \
                     on replica {}",
                    check.display_table(),
                    endpoint.url()
                )));
            }
            match &reference {
                None => reference = Some((endpoint.url().to_string(), cols)),
                Some((ref_url, ref_cols)) => {
                    if *ref_cols != cols {
                        return Err(SchemaError::Mismatch(format!(
                            "sink.clickhouse: replicas disagree on the schema of {}:\n  \
                             {ref_url}: {}\n  {}: {}",
                            check.display_table(),
                            table_column_list(ref_cols),
                            endpoint.url(),
                            table_column_list(&cols),
                        )));
                    }
                }
            }
        }
    }
    let (ref_url, table_cols) =
        reference.expect("config validation guarantees at least one replica");

    let mut findings = Vec::new();
    for col in &check.columns {
        match table_cols.iter().find(|c| c.name == *col) {
            None => findings.push(format!(
                "configured column `{col}` does not exist in the table"
            )),
            Some(c) if c.default_kind == "MATERIALIZED" || c.default_kind == "ALIAS" => {
                findings.push(format!(
                    "configured column `{col}` is {} and cannot be inserted into",
                    c.default_kind
                ));
            }
            Some(c) => {
                if let Some(msg) = aggregate_function_remedy(col, &c.type_) {
                    findings.push(msg);
                }
            }
        }
    }
    for c in &table_cols {
        if !check.columns.contains(&c.name) && c.default_kind.is_empty() {
            tracing::warn!(
                table = %check.display_table(),
                column = %c.name,
                r#type = %c.type_,
                "table column is not in the configured insert columns and has no DEFAULT; \
                 the server will fill type-default values"
            );
        }
    }

    if !findings.is_empty() {
        let mut msg = format!(
            "sink.clickhouse: schema validation failed for {} (replica {ref_url}):\n",
            check.display_table()
        );
        for f in &findings {
            let _ = writeln!(msg, "  - {f}");
        }
        let _ = writeln!(
            msg,
            "  table columns:      {}",
            table_column_list(&table_cols)
        );
        let _ = write!(msg, "  configured columns: {}", check.columns.join(", "));
        return Err(SchemaError::Mismatch(msg));
    }

    let columns = check
        .columns
        .iter()
        .map(|name| {
            let c = table_cols
                .iter()
                .find(|c| c.name == *name)
                .expect("checked above");
            (name.clone(), typeparse::parse(&c.type_), c.type_.clone())
        })
        .collect();
    Ok(Some(Arc::new(RowSchema {
        mode: check.mode,
        table: check.display_table(),
        columns,
    })))
}

/// The first-record struct check: field names and order against the
/// configured columns (both modes), plus class-based type compatibility
/// per position (`full` mode). Returns the pre-formatted diff on failure.
pub(crate) fn check_first_record(schema: &RowSchema, fields: &[FieldShape]) -> Result<(), String> {
    let mut findings = Vec::new();
    if fields.len() != schema.columns.len() {
        findings.push(format!(
            "row struct serialized {} field(s) but {} column(s) are configured \
             (a #[serde(skip)] attribute shortens rows silently)",
            fields.len(),
            schema.columns.len()
        ));
    }
    for (i, (field, (col, ty, ty_str))) in fields.iter().zip(&schema.columns).enumerate() {
        if field.name != col {
            findings.push(format!(
                "position {i}: struct field `{}` vs configured column `{col}`",
                field.name
            ));
        } else if schema.mode == SchemaValidation::Full && !compatible(&field.shape, ty) {
            findings.push(format!(
                "position {i}: struct field `{}` ({}) is not compatible with `{col}` {ty_str}",
                field.name,
                shape_desc(&field.shape),
            ));
        }
    }
    if findings.is_empty() {
        return Ok(());
    }
    let mut msg = format!(
        "row struct does not match configured columns for {}:\n",
        schema.table
    );
    for f in &findings {
        let _ = writeln!(msg, "  - {f}");
    }
    let _ = writeln!(
        msg,
        "  struct fields (declaration order): {}",
        fields.iter().map(|f| f.name).collect::<Vec<_>>().join(", ")
    );
    let _ = write!(
        msg,
        "  configured columns:                {}",
        schema
            .columns
            .iter()
            .map(|(name, ..)| name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    Err(msg)
}

fn shape_desc(shape: &Shape) -> String {
    match shape {
        Shape::Bool => "bool".into(),
        Shape::I8 => "i8".into(),
        Shape::I16 => "i16".into(),
        Shape::I32 => "i32".into(),
        Shape::I64 => "i64".into(),
        Shape::I128 => "i128".into(),
        Shape::U8 => "u8".into(),
        Shape::U16 => "u16".into(),
        Shape::U32 => "u32".into(),
        Shape::U64 => "u64".into(),
        Shape::U128 => "u128".into(),
        Shape::F32 => "f32".into(),
        Shape::F64 => "f64".into(),
        Shape::Str => "String".into(),
        Shape::Bytes => "bytes".into(),
        Shape::Option(inner) => format!("Option<{}>", shape_desc(inner)),
        Shape::Seq(inner) => format!("Vec<{}>", shape_desc(inner)),
        Shape::Map(k, v) => format!("Map<{}, {}>", shape_desc(k), shape_desc(v)),
        Shape::Tuple(elems) => format!(
            "({})",
            elems.iter().map(shape_desc).collect::<Vec<_>>().join(", ")
        ),
        Shape::Struct(fields) => format!(
            "struct {{ {} }}",
            fields
                .iter()
                .map(|f| f.name.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Shape::Newtype(name, _) => (*name).to_string(),
        Shape::Unknown => "_".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use probe::probe_row;
    use serde::Serialize;

    fn schema(mode: SchemaValidation, cols: &[(&str, &str)]) -> RowSchema {
        RowSchema {
            mode,
            table: "`orders`".into(),
            columns: cols
                .iter()
                .map(|(n, t)| ((*n).to_string(), typeparse::parse(t), (*t).to_string()))
                .collect(),
        }
    }

    #[derive(Serialize)]
    struct Row {
        id: u64,
        amount: i64,
        name: String,
    }

    fn fields() -> Vec<FieldShape> {
        probe_row(&Row {
            id: 1,
            amount: 2,
            name: "x".into(),
        })
        .unwrap()
    }

    #[test]
    fn matching_struct_passes_both_modes() {
        for mode in [SchemaValidation::Names, SchemaValidation::Full] {
            let s = schema(
                mode,
                &[("id", "UInt64"), ("amount", "Int64"), ("name", "String")],
            );
            assert_eq!(check_first_record(&s, &fields()), Ok(()));
        }
    }

    #[test]
    fn field_order_mismatch_is_reported_per_position() {
        let s = schema(
            SchemaValidation::Names,
            &[("id", "UInt64"), ("name", "String"), ("amount", "Int64")],
        );
        let err = check_first_record(&s, &fields()).unwrap_err();
        assert!(err.contains("position 1: struct field `amount` vs configured column `name`"));
        assert!(err.contains("position 2: struct field `name` vs configured column `amount`"));
        assert!(err.contains("struct fields (declaration order): id, amount, name"));
        assert!(err.contains("configured columns:                id, name, amount"));
    }

    #[test]
    fn type_mismatches_only_fail_full_mode() {
        let cols = [
            ("id", "UInt64"),
            ("amount", "DateTime"), // i64 field is not a u32 DateTime
            ("name", "String"),
        ];
        let names = schema(SchemaValidation::Names, &cols);
        assert_eq!(check_first_record(&names, &fields()), Ok(()));

        let full = schema(SchemaValidation::Full, &cols);
        let err = check_first_record(&full, &fields()).unwrap_err();
        assert!(
            err.contains("struct field `amount` (i64) is not compatible with `amount` DateTime"),
            "{err}"
        );
    }

    #[test]
    fn field_count_mismatch_names_the_skip_footgun() {
        let s = schema(SchemaValidation::Names, &[("id", "UInt64")]);
        let err = check_first_record(&s, &fields()).unwrap_err();
        assert!(err.contains("serialized 3 field(s) but 1 column(s)"));
        assert!(err.contains("#[serde(skip)]"));
    }

    #[test]
    fn aggregate_function_columns_are_flagged_with_the_null_mv_remedy() {
        for ty in [
            "AggregateFunction(min, DateTime)",
            "AggregateFunction(max, DateTime)",
            "AggregateFunction(sumMap, Map(String, UInt64))",
        ] {
            let msg = aggregate_function_remedy("agg", ty)
                .unwrap_or_else(|| panic!("`{ty}` should be flagged"));
            assert!(msg.contains(ty), "{msg}");
            assert!(msg.contains("Null"), "{msg}");
            assert!(msg.contains("MATERIALIZED VIEW"), "{msg}");
        }
    }

    #[test]
    fn directly_insertable_types_are_not_flagged() {
        for ty in [
            // SimpleAggregateFunction stores the raw value, so it is
            // insertable.
            "SimpleAggregateFunction(sum, UInt64)",
            "SimpleAggregateFunction(sumMap, Map(String, UInt64))",
            "UInt64",
            "Map(String, UInt64)",
            "DateTime",
        ] {
            assert_eq!(aggregate_function_remedy("c", ty), None, "{ty}");
        }
    }
}
