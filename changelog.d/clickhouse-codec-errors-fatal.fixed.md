**Codec errors stop the pipeline** (`spate-clickhouse`)

The ClickHouse sink classifies the client's `Compression` and `Decompression`
errors as `ErrorClass::Fatal`. The batch is abandoned on the first failure, the
partition stalls, and the pipeline exits `Failed` once
`checkpoint.stalled_fail_after` elapses (120 seconds by default). Previously
both were retryable, so under the default `retry.max_attempts: 0` the shard
reattempted the same request for the life of the process while holding its
in-flight slot, and reported no failure.

Neither error is reachable from a write on the default `compression: lz4` with
`clickhouse` 0.15.2, so pipelines on that setting behave the same. The client
compresses the request body under `lz4`, and the LZ4 encoder has no failure it
can report. The zstd encoder can report one, so `compression: zstd` is where
this change takes effect. Decoding failures stay off the write path, because
the insert response is read uncompressed.
