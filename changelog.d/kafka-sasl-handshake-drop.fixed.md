**SASL handshake dropped mid-connect** (`spate-kafka`)

The Kafka source and the Kafka feed now treat a connection that drops or times out during the SASL mechanism handshake as retryable, so the client reconnects and the run continues. In previous versions the source ended the run and the feed stopped with a fatal error, because librdkafka reports this under the same `Authentication` code as a refused login. A broker restart could cause it for any client reconnecting during the restart. A login the broker refuses is still fatal.
