**Schema registry task panics are logged** (`spate-avro`)

When the Avro deserializer's schema registry fetcher or its startup pre-warm panics, the panic is logged at `ERROR` with the message `background task panicked` and the task name `avro-registry-fetcher` or `avro-registry-prewarm`. In previous versions such a panic was only printed to standard error by the default panic hook, so it did not appear in the pipeline's `tracing` output. The tasks run and end as before.
