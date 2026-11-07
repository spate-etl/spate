**The ClickHouse sink's lz4 codec reuses its hash table** (`spate-clickhouse`)

An lz4 frame allocated a new 8 or 16 KiB hash table per frame. The codec keeps one table of each size, so a warm frame allocates nothing and its bytes are unchanged.
