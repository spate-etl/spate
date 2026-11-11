**Kafka sink: four more producer errors are fatal** (`spate-kafka`)

A Kafka sink delivery report that carries `UnknownProducerId`, `Fenced`, `Inconsistent` or `GaplessGuarantee` now fails the run as fatal, as the Kafka feed's producer already treats them. In previous versions the sink classed each as retryable and sent the batch again, until librdkafka's own fatal error for the idempotent producer ended the run on a later produce. The Kafka feed's writes take the same class.
