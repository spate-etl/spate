**Config errors no longer print the rejected value** (`spate-core`, `spate-clickhouse`)

A config error names the field and the type it expected, and does not print the
value it rejected. In previous versions a value of the wrong type appeared in
the error message, for example ``invalid type: integer `918273645`, expected a
string``. Because `${VAR}` interpolation runs before parsing, that value could
be a secret. This applies to pipeline parse errors, connector config errors and
the ClickHouse `compression` setting. An unknown enum value is also no longer
repeated, so a typo reads as `unknown variant, expected …` and the field path
shows where it is.
