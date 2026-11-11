**The ClickHouse sink writes strings with fewer instructions** (`spate-clickhouse`)

Both the RowBinary and the Native encoder write a length below 128 as one inline byte, and copy a string of up to 32 bytes without a call into libc. String-heavy rows encode faster under either format. The bytes sent to ClickHouse are unchanged.
