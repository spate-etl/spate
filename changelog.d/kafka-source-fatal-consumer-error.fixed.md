**A consumer librdkafka has failed stops the Kafka source** (`spate-kafka`)

The Kafka source stops the pipeline when librdkafka reports a fatal consumer
error, such as a static member fenced by another member with the same
`group.instance.id`. The error carries librdkafka's text and, for a fenced
member, the configured `group.instance.id`. In previous versions the source
classified the error as retryable and kept polling a consumer that could no
longer deliver, with no deadline to end it.
