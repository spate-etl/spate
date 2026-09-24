**ClickHouse certificate verification failures stop the pipeline** (`spate-clickhouse`)

A replica certificate that fails TLS verification is `ErrorClass::Fatal`, and
the error message carries the rustls reason, such as `UnknownIssuer`. At
startup, the schema check and `distributed_check` fail with that reason. In
previous versions every such failure read `network error: client error
(Connect)` and was retryable. A shard kept writing through its other replicas
while the circuit breaker held back the failing one. Under the default
`retry.max_attempts: 0`, a shard with no replica left retried for the life of
the process. A single replica whose certificate stops verifying mid-run now
abandons the batch, and the pipeline fails once `checkpoint.stalled_fail_after`
elapses.
