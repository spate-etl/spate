**Breaking: the ClickHouse sink always fetches the schema, and RowBinary
carries it on the wire** (`spate-clickhouse`) — `sink: { clickhouse: ... }` no
longer takes `validate_schema`; the table's columns are read from every replica
whenever a sink is built, and `format: rowbinary` sends
`INSERT … FORMAT RowBinaryWithNamesAndTypes` with a header naming each column
and its type. The server checks that header on every insert, so a table
`ALTER`ed under a running pipeline is rejected rather than silently storing
rescaled values — the case a same-width change (a `Decimal` scale, a
`DateTime64` precision) used to slip through. `ClickHouseSinkBuilder::with_row`
is now async and returns the runnable sink, replacing
`ClickHouseSink::validate_schema`; pass `sink.schema()` to
`ClickHouseEncoder::with_schema`, which is now its only constructor, and call
`sink.native_schema()` synchronously. `input_format_with_names_use_header` and
`input_format_with_types_use_header` join the settings the sink manages, so a
`settings:` map naming either is rejected at load. `NativeSchema::from_columns`
now checks a row against the types it declares, not only their names.

The type check running on every sink is what a pipeline previously on
`validate_schema: off` or `names` will notice: a field whose width matches its
column by coincidence is now rejected at the first record. A raw `u64` epoch
against a `DateTime64` column is the likely one — that column is `Int64`-backed,
so it takes an `i64`, or the `DateTime64Millis` wrapper, which pins the scale
as well.
