**A registry schema that is not Avro no longer holds the batch** (`spate-avro`)

When the schema registry returns a schema whose type is not `AVRO`, such as
`PROTOBUF` or `JSON`, the Avro deserializer reports the payload as
`SchemaUnavailable` and applies its `ErrorPolicy`: `Skip` drops and counts the
record, and `Fail` stops the pipeline. The id is cached as unusable for
`negative_cache_ttl`. A subject in `prewarm_subjects` with such a schema is
skipped. In previous versions such a schema was retried without limit while the
batch was held. A pre-warmed subject with such a schema was cached, and its
records decoded with no error when the schema text was also a valid Avro schema.
