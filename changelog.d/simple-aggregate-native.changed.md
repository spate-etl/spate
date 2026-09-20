**Breaking:** **SimpleAggregateFunction column support** (`spate-clickhouse`)

With `format: native`, the sink now supports `SimpleAggregateFunction(f, T)`
columns whenever the Native encoder supports the stored type `T`. Previously,
these columns were rejected as an unknown type when the encoder was built.
You can now insert their values using the same row field types as for `T`.

With `format: rowbinary`, the first-record check now validates the row field
against `T`; previously, that type check was skipped. Incompatible fields
therefore fail on the first record. A column storing `Nullable(U)` now accepts
a compatible `Option` field, which the previous check rejected.
