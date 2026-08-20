**A flattened `Nested` column can be written** (`spate-clickhouse`) — under
ClickHouse's default `flatten_nested = 1` a `Nested` column is stored as
parallel `outer.inner` columns, and naming one in `columns:` failed the sink's
identifier check, which rejected any name containing a dot. There was no way
around it, so a table with a `Nested` column could not be written in either
format. A column name may now be a dotted path of identifiers. The check is
otherwise unchanged: a backtick, a space and everything else are still
rejected, so the name cannot escape its backtick quoting.
