**Breaking:** **Truncated datum rejection** (`spate-avro`)

The value and two-pass serde deserializers reject truncated string bodies,
boolean fields, and union indexes as malformed. These payloads previously
could emit records containing null values. They follow the configured Skip
or Fail record error policy. Pipelines using Fail can stop on payloads
previously accepted.
