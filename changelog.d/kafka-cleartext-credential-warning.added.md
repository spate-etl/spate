**Cleartext SASL password warning** (`spate-kafka`)

A Kafka source, sink, feed or feed publisher whose `rdkafka` passthrough sends a non-empty `sasl.password` with `security.protocol` set to `sasl_plaintext`, in any case, now logs one warning naming `rdkafka."sasl.password"` when its client is built. The pipeline keeps running.
