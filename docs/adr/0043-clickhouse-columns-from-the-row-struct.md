# ADR-0043 — The row struct is the ClickHouse insert column list, replacing configured `columns`

- **Status:** accepted
- **Date:** 2026-08-21
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A ClickHouse pipeline carries three descriptions of one row: the `columns` list
in the `sink: { clickhouse: ... }` section, the field declaration order of the
Rust row struct, and the live table. RowBinary is positional and carries no
column names, so a disagreement between the first two is silent data corruption
rather than a protocol error.

`crates/spate-clickhouse/src/schema/` exists to reconcile them, and it cannot
do so at one moment. It checks `columns` against the live table at startup, and
the row struct against `columns` at the first record each pipeline thread
encodes, because the struct is reachable only as serde behavior and
`schema::probe` needs a real value to drive through `Serialize`.

The question this record settles is whether the configuration should carry the
column list at all.

## Considered options

- Generate the insert column list from the row struct with a
  `#[derive(ClickHouseRow)]` in a new `spate-clickhouse-derive` crate, and
  remove `columns` from the sink configuration.
- Keep `columns`, and fill it from the live table's `system.columns` order when
  it is absent.
- Keep `columns` as the source of truth, and rely on `validate_schema` to catch
  a disagreement.
- Generate the list from the `clickhouse` crate's existing
  `Row::COLUMN_NAMES` const rather than shipping a first-party derive.

## Decision outcome

Chosen option: "generate the insert column list from the row struct", because
the struct is the only one of the three descriptions that the wire format
obeys. The bytes on the wire are the struct's fields in declaration order.
Everything else is a copy, and the schema module is machinery for detecting
that a copy drifted.

`#[derive(ClickHouseRow)]` emits `const COLUMNS: &'static [&'static str]` from
the field list, honoring `#[serde(rename)]` so that a column no Rust identifier
can spell, such as the dotted `tags.key` name a `Nested` table takes, has one
spelling rather than two. `build()` stops emitting the `INSERT` statement, and
a typed step produces it:

```rust
let sink = from_component_config(section)?.with_row::<Owned<OrderRow>>()?;
```

Naming the family at a sink method follows `ClickHouseSink::router`, which
already takes its family by turbofish because it is not inferable. `with_row`
stays separate from `validate_schema`, because `validate_schema: off` still
needs the column list and still must issue no queries.

`RecFamily::Rec<'buf>` is a generic associated type, and a const cannot be
reached through a higher-ranked bound, so the const is carried by a
family-level trait with a blanket impl for the owned case:

```rust
pub trait ClickHouseRowFamily: RecFamily {
    const COLUMNS: &'static [&'static str];
}

impl<T: ClickHouseRow + Send + 'static> ClickHouseRowFamily for Owned<T> {
    const COLUMNS: &'static [&'static str] = T::COLUMNS;
}
```

The derive covers `Owned<T>` through that impl. A borrowed family implements
the family trait itself, naming its own row's const.

**Filling `columns` from the live table was rejected** because it makes the
table the authority on order. `system.columns` is read `ORDER BY position`,
which is physical table order, while `columns` is documented as the insert
columns in *field* order and startup maps the two by name. Inferring from the
table means an `ALTER TABLE … ADD COLUMN x AFTER y` changes a pipeline's wire
contract without anybody touching the pipeline. It also cannot know which
columns were left out on purpose, and omitting a column so the server applies
its `DEFAULT` is a supported pattern that `schema::validate` warns about rather
than rejects.

**Keeping `columns` was rejected** because the checks it needs are the cost of
the copy existing. Two of the three descriptions are written by the same
person in the same change, and the module that reconciles them is load-bearing
only because they can disagree.

**The `clickhouse` crate's `Row::COLUMN_NAMES` was rejected** under
[ADR-0011](0011-msrv-and-dependency-policy.md) and INV-6: no 0.x dependency
type may appear in a public trait bound. Requiring `#[derive(clickhouse::Row)]`
on a user's row struct puts a 0.x trait in the encoder's bound, so every
`clickhouse` minor release would become a breaking release for anybody
depending on `spate`.

### Consequences

- Good, because the struct check moves from the first record to startup. The
  names and their order are a const, so `columns` against the live table
  becomes *struct* against the live table, before any thread spawns. Only the
  type-class check still needs a first record, because shapes need a value to
  record.
- Good, because a `columns` and struct disagreement stops being expressible.
  There is no second list, so the failure the schema module was built to catch
  cannot occur.
- Good, because duplicate and malformed column names become compile errors.
  Today they are load errors in `config::validate`, which rejects a duplicate
  because `INSERT INTO t (id, id)` returns `DUPLICATE_COLUMN`, a code the
  writer classifies retryable and would otherwise loop on forever.
- Bad, because an operator reading the YAML no longer sees which columns the
  pipeline writes. The generated list is logged once at startup, which puts the
  answer in the logs of the running process rather than in a file that goes
  through review.
- Bad, because `spate-clickhouse-derive` is a new published crate, so the
  derive's attribute surface is versioned and a proc-macro crate joins the
  release set. It cannot be published against `spate` 0.1.0, so its first
  release is manual, as `spate-datagen`'s was.
- Bad, because every existing pipeline breaks: each YAML loses `columns`, each
  row struct gains a derive, and `ClickHouseSinkConfig::new` loses a parameter.
- Neutral, because the break is loud. `ClickHouseSinkConfig` carries
  `#[serde(deny_unknown_fields)]`, so a YAML still holding `columns:` fails to
  parse with an error naming the key rather than ignoring it.

### Confirmation

INV-6, and structurally in three places. `ClickHouseRowFamily` is a first-party
trait, so no 0.x type enters the encoder's bound. `build()` no longer accepts a
column list, so a configuration that disagrees with the struct cannot be
written. And `#[serde(deny_unknown_fields)]` on `ClickHouseSinkConfig` makes a
stale `columns:` key a load error.

The name checks that `config::validate` holds today move into the derive, where
they are compile errors: a duplicate name after `#[serde(rename)]`, and a name
outside the identifier or dotted `outer.inner` shape.

## More information

- Landed in the pull request removing `columns` from the ClickHouse sink
  configuration.
- [ADR-0011](0011-msrv-and-dependency-policy.md) — the no-0.x-types-in-bounds
  rule that decides the derive question.
- [ADR-0009](0009-yaml-configuration-with-opaque-passthrough.md) — the
  configuration surface this record takes a field out of.
- [ADR-0007](0007-clickhouse-insert-path.md) — the pre-encoded RowBinary
  frames whose field order is the wire contract.
- [Schema validation](../user-guide/04-connectors/sinks/clickhouse/schema-validation.mdx)
  — the two-moment check this record moves half of to startup.
