**Breaking:** **The value that turns pinning or ClickHouse compression off is `none`** (`spate-core`, `spate-clickhouse`)

`pipeline.pinning` takes `none` or `compact`, and `none` is the default. In previous versions the value that turned pinning off was `off`, which is now refused. `PinningMode::Off` is renamed `PinningMode::None`.

The ClickHouse sink's `compression` key no longer takes `off`. `none` sends insert bodies uncompressed, as before, and the same applies to parsing a `ClickHouseCompression` from a string.

To migrate, replace `pinning: off` and `compression: off` with `none`. No other configuration value changes spelling.
