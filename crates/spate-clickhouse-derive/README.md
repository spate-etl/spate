# spate-clickhouse-derive

The proc-macro behind `#[derive(ClickHouseRow)]` for the
[Spate](https://github.com/spate-etl/spate) ClickHouse sink. Depend on
`spate` or `spate-clickhouse`, which re-export the derive, and not on this
crate directly.

```rust,ignore
#[derive(Serialize, ClickHouseRow)]
struct OrderRow { id: u64, name: String, amount: f64 }
```

The derive implements `ClickHouseRow` and generates the insert column list
from the struct's field declaration order. `#[serde(rename = "...")]` names
a column no Rust identifier can spell, such as a flattened `Nested` table's
`outer.inner`, and `#[serde(skip)]` leaves a field out.

A duplicate or malformed column name is a compile error, as are
`#[serde(flatten)]`, a struct-level `#[serde(rename_all = "...")]` and
`#[serde(skip_serializing_if = "...")]`.

`#[clickhouse(crate = "path::to::spate_clickhouse")]` points the generated
code at `spate-clickhouse` when a crate reaches it through a facade of its
own.

See the [`spate-clickhouse` docs](https://docs.rs/spate-clickhouse) for the
row type mapping and why column order is the wire contract.
