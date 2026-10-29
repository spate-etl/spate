**Kafka event-time latency falls back when source timestamps are missing** (`spate-kafka`)

With `metrics.e2e_basis: event`, Kafka records without a source timestamp use their ingest age instead of appearing to wait since the Unix epoch. This keeps `spate_e2e_latency_seconds` meaningful when Kafka omits a timestamp.
