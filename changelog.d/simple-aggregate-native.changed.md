**Breaking: `SimpleAggregateFunction` columns build under `format: native`**
(`spate-clickhouse`) — the type parser resolves `SimpleAggregateFunction(f,
T)` to `T`, the type it stores, instead of treating it as unrecognized. Native
now writes these columns for any `T` it already writes on its own. Under
`format: rowbinary`, the first-record check now validates the row field
against `T` instead of skipping it, so a field that disagrees with `T` is
rejected on the first record; a `T` of `Nullable(U)` against an `Option`
field, previously rejected unconditionally under this check, is now accepted.
