**LowCardinality columns hash their values with a seeded quality hasher** (`spate-clickhouse`)

The native encoder's per-block LowCardinality dictionary hashes each value with the quality variant of its seeded hasher, which spreads every input bit into the bits the table indexes on. On the encode benchmarks this costs about 1% more instructions on x86_64 and 2% on aarch64 for a schema built around LowCardinality columns, and under 0.2% on the other encode cases. The encoded bytes are unchanged.
