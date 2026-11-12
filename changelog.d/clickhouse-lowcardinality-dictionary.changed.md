**The ClickHouse sink's Native encoder no longer allocates for each new `LowCardinality` value** (`spate-clickhouse`)

A `LowCardinality(String)` or `LowCardinality(Nullable(String))` column looks up each value against the block's dictionary bytes. Previously each value new to a block was also copied into a heap allocation of its own, once per block it appeared in. The bytes sent to ClickHouse are unchanged.
