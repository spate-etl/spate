**The ClickHouse sink's lz4 codec reuses its hash tables** (`spate-clickhouse`)

An lz4 frame allocates no hash table once the codec has coded a chunk of its size: the codec keeps one table for chunks under 64 KiB and one for larger chunks. Compressed bytes are unchanged. Previously every frame allocated a new 8 or 16 KiB table.
