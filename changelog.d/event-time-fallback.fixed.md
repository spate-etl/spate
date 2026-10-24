**Event-time latency falls back when source timestamps are missing** (`spate-core`)

With `metrics.e2e_basis: event`, records without a source timestamp use their ingest age. Previously, these records looked as if they had waited since the Unix epoch. This keeps `spate_e2e_latency_seconds` meaningful when Kafka or S3 does not supply an event time.
