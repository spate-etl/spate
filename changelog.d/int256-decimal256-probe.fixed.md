**Int256 values for Decimal256 columns** (`spate-clickhouse`)

The sink's type check now accepts an `Int256` field for a `Decimal(P, S)`
column with precision from 39 to 76, including `Decimal256(S)`. Previously,
the check rejected this documented pairing. This allows rows containing wide
decimal values to pass schema validation. The row must still supply the value
scaled by `10^S`; an `Int256` carries no scale information for the checker to
validate.
