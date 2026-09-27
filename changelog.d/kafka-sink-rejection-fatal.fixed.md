**A rejected SASL login or TLS handshake fails the Kafka sink's batch** (`spate-kafka`)

The Kafka sink fails a batch as fatal when its deliveries time out after a
broker rejected the connection: a SASL authentication failure, a broker
certificate the client cannot verify, or a TLS alert such as
`handshake failure`. The rejection counts for `delivery_timeout` + 35s +
`reconnect.backoff.max.ms` after it arrives. The `sink write failed` and
`sink probe failed` warnings end with how long ago the rejection arrived and
librdkafka's text. The pipeline fails once `checkpoint.stalled_fail_after`
elapses. In previous versions these deliveries timed out as retryable. Under
the default retry settings the pipeline kept running with nothing written, and
the reason was only in the `librdkafka` log lines.
