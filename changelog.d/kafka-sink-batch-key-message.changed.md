**The Kafka sink names the batch key it refuses** (`spate-kafka`)

A `sink.kafka` section with `batch.max_rows: 0` fails at load with `sink.kafka: batch.max_rows must be at least 1`, and one with `batch.max_bytes: 0` fails with `sink.kafka: batch.max_bytes must be at least 1`. When both are `0`, the error names `batch.max_rows`. In previous versions both read `sink.kafka: batch thresholds must be non-zero`. The values the sink refuses are unchanged, and so are its messages for the `inflight`, `retry` and `breaker` sections.
