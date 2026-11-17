**Spate requires `bytes` 1.12.2** (`spate-core`, `spate-clickhouse`)

Spate now requires `bytes` 1.12.2 or later, which marks `BytesMut::put_slice` as `#[inline]`. The ClickHouse encoders write most values through it, directly or through methods such as `put_u32_le`, so a build without link-time optimization (LTO) now inlines those writes. Previously a build could resolve `bytes` 1.11.1 or 1.12.1, where each of those writes made an out-of-line call unless the binary enabled LTO across crates. Code that uses `bytes` through the `spate_core::bytes` re-export now gets 1.12.2 as well.
