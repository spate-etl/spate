**Default compression works on ClickHouse 26.9** (`spate-clickhouse`)

Under `compression: lz4`, the default, the ClickHouse sink sends
`network_compression_method=LZ4` on every request, so the server compresses its
responses with LZ4. Previously a sink on `lz4` failed to start against ClickHouse
26.9 and later. Those servers compress responses with ZSTD by default, and the
sink's schema fetch failed with `decompression error: incorrect magic number`.
The setting changes nothing on servers before 26.9, and `compression: zstd` and
`compression: off` send nothing new.
