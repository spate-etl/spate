**Kafka source: an assignment on another topic** (`spate-kafka`)

The Kafka source now fails the run when its consumer group assigns it a partition of a topic other than `source.kafka.topic`, under every `auto.offset.reset` and before the consumer is assigned. In previous versions the source fetched the other topic with the pipeline's credentials and dropped its records, and a topic name longer than 512 bytes could crash the process inside librdkafka. A group whose members all use a stock assignor never sends such an assignment.
