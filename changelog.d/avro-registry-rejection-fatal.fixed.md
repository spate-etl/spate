**A schema registry that rejects the client stops the pipeline** (`spate-core`, `spate-avro`)

The Avro deserializer stops the pipeline when the registry answers `401` or
`403`, or when its certificate fails verification. The error names the registry
URL and the status or certificate error. In previous versions both were retried
indefinitely: the batch was held, the source paused, and a warning was the only
signal. The pipeline stops at the first payload whose schema is not cached,
whatever the deserializer's `ErrorPolicy`.

A deserializer reports such a failure with the new `DeserError::Fatal`. The
chain fails the batch under either policy and counts the payload in
`spate_deser_records_total{outcome="error"}`, never as a skip-policy drop.
