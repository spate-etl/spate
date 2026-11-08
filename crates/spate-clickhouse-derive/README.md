# spate-clickhouse-derive

The derive macro behind `#[derive(ClickHouseRow)]` for the Spate ClickHouse
sink. It implements `ClickHouseRow` for a row struct and generates the insert
column list from the order in which the struct declares its fields. Depend on
the [`spate`](https://crates.io/crates/spate) facade with feature `clickhouse`,
or on `spate-clickhouse`. Both re-export the derive as `ClickHouseRow`, at
`spate::clickhouse::ClickHouseRow` and `spate_clickhouse::ClickHouseRow`.

## Usage

Derive `ClickHouseRow` on a struct with named fields. One import,
`use spate::clickhouse::ClickHouseRow`, brings in the trait and the derive.
[`kafka_avro_to_clickhouse.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/kafka_avro_to_clickhouse.rs)
derives it on `OrderRow` beside `Serialize` and hands the row type to the sink
with `with_row::<Owned<OrderRow>>()`. The example builds under the `kafka`,
`clickhouse` and `avro` features.

## Columns

The generated impl sets one constant, `COLUMNS`, with one name per field in
declaration order.

- A field's name is its column name. A raw identifier such as `r#type` names
  the column `type`.
- `#[serde(rename = "...")]` names a column no Rust identifier can spell, such
  as a flattened `Nested` column's `outer.inner`. In the list form
  `#[serde(rename(serialize = "...", deserialize = "..."))]`, the `serialize`
  name is the column.
- `#[serde(skip)]` and `#[serde(skip_serializing)]` leave the field out of the
  list.
- Other `serde` keys, such as `with` or `default`, do not change the list.

The `INSERT` lists these columns by name, so the table may declare its columns
in any order. The
[column contract](https://github.com/spate-etl/spate/blob/main/crates/spate-clickhouse/README.md#column-contract)
in the sink's README describes the checks that hold the struct to the table.

## Compile errors

The derive refuses these at compile time:

- A type other than a struct with named fields, and a struct left with no
  columns.
- A column name outside the shape `[A-Za-z_][A-Za-z0-9_]*`, or several such
  segments joined by `.`.
- Two fields with the same column name after renaming.
- `#[serde(flatten)]`. The derive cannot see the flattened type's fields.
- A struct-level `#[serde(rename_all = "...")]`. Rename each field with
  `#[serde(rename = "...")]` instead.
- `#[serde(skip_serializing_if = "...")]`. For a value that may be absent, use
  an `Option<T>` field against a `Nullable` column. That field stays in the list
  on every row.

## Crate path

Without the attribute below, the generated code names `ClickHouseRow` by an
absolute path. The derive looks for `spate-clickhouse` among the calling crate's
dependencies first, then for `spate`. A crate that reaches the sink through a
facade of its own sets the path on the struct with
`#[clickhouse(crate = "my_facade::clickhouse")]`, naming the module that holds
`ClickHouseRow`. With neither dependency and no attribute, the build fails with
an error that names the attribute.

## Related

- [`spate`](https://crates.io/crates/spate) — the facade an application depends
  on, with the Cargo snippet.
- [API documentation on docs.rs](https://docs.rs/spate-clickhouse) — the
  `ClickHouseRow` trait and, in the `rowbinary` module, the Rust field each
  column type takes.
- [ClickHouse sink page](https://spate.kainth.dev/docs/user-guide/connectors/sinks/clickhouse)
  — configuration and `with_row`.
- [ClickHouse column types](https://spate.kainth.dev/docs/user-guide/connectors/sinks/clickhouse/column-types)
  — the column types each format writes, including `Nested` columns named with
  `#[serde(rename)]`.
- [Schema validation](https://spate.kainth.dev/docs/user-guide/connectors/sinks/clickhouse/schema-validation)
  — the three checks that hold the row struct to the table.
