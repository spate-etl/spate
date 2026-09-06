**An `Int256` field feeds a `Decimal256` column** (`spate-clickhouse`) — the
sink's type check accepts an `Int256` field against a `Decimal(P, S)` column
with `P` above 38, which is what the type documents: the row carries the value
already scaled by `10^S`. The field is checked against the column's precision
band, and the scaling stays the row's responsibility, since an `Int256` carries
no scale to check.
