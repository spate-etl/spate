**Breaking:** **The ClickHouse sink compresses insert bodies on pipeline threads** (`spate-clickhouse`)

With `compression: lz4` or `zstd`, the sink compresses each chunk on the
pipeline thread when the chunk seals. It reuses one zstd context per thread.
Previously the I/O runtime compressed every frame inside the send, and a retry
compressed it again. A retry now resends the stored bytes. `compression: off`
sends the same request as before. `settings: { decompress: … }` is now
rejected, because the sink sets it. Server errors 40, 89 and 271 mean the
server could not decode a block. They now stop the sink instead of being
retried. On a host where the pipeline threads are already the limit,
compression now uses their CPU.

A manually assembled pipeline must attach the sink's codec. Take
`parts.frame_codec` from `into_parts()` and pass it to
`ShardQueues::with_frame_codec`, or call `ClickHouseSink::frame_codec()` before
`into_parts`. A caller that uses `ClickHouseWriter::write_batch` directly must
code its frames with that codec. Otherwise the sink records a fatal error and
sends no request.
