**Breaking:** **ClickHouse schema checks on every sink** (`spate-clickhouse`)

The ClickHouse sink now fetches the table schema from every replica when it is
built, and `format: rowbinary` sends column names and types using
`RowBinaryWithNamesAndTypes`. Previously, `validate_schema` could disable type
checks, and RowBinary inserts carried no type information. The server now
checks the header on every insert, so changing a column's decimal scale or
`DateTime64` precision while a pipeline is running causes an error instead of
silently interpreting values with the new scale.

Remove `validate_schema` from the sink configuration. Await
`ClickHouseSinkBuilder::with_row` to fetch the schema and obtain the sink;
this replaces `ClickHouseSink::validate_schema`. Construct the encoder with
`ClickHouseEncoder::with_schema` and pass `sink.schema()` to it. Call
`sink.native_schema()` without awaiting it. Remove
`input_format_with_names_use_header` and `input_format_with_types_use_header`
from custom `settings`; the sink manages them and rejects overrides.

Row type checks now also apply to pipelines previously configured with
`validate_schema: off` or `names`, and `NativeSchema::from_columns` validates
declared types as well as names. A field with the same byte width as its
column can therefore fail on the first record if its type is incompatible.
For example, use an `i64` for a raw `DateTime64` value or a matching wrapper
such as `DateTime64Millis`; a `u64` is rejected.
