**A registry schema that is not Avro no longer holds the batch** (`spate-avro`)

When the schema registry returns a schema whose type is not `AVRO`, such as
`PROTOBUF` or `JSON`, the Avro deserializer reports the payload as
`SchemaUnavailable` and applies its `ErrorPolicy`: `Skip` drops and counts the
record, and `Fail` stops the pipeline. The id is cached as unusable for
`negative_cache_ttl`. In previous versions such a schema was retried without
limit while the batch was held.
