**The ClickHouse sink writes strings with fewer instructions** (`spate-clickhouse`)

The RowBinary and Native encoders reserve space once for a string shorter than 128 bytes and its one-byte length, and copy a string of up to 32 bytes without calling libc's `memcpy`. Previously each string made an out-of-line call to write its length and a `memcpy` call to copy its bytes. Rows with many short strings take fewer instructions to encode under either format. The bytes sent to ClickHouse are unchanged.
