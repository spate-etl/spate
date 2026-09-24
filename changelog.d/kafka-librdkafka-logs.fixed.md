**Kafka client logs** (`spate-kafka`)

Kafka sources and sinks forward librdkafka warnings, info and debug lines to
`tracing` when the subscriber enables them. Set a librdkafka `debug` category
and `RUST_LOG=info,librdkafka=debug` for detailed diagnostics.
