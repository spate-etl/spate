**Cleartext SASL warning** (`spate-kafka`)

A Kafka source, sink, feed or feed publisher whose `rdkafka` passthrough sets `security.protocol` to `sasl_plaintext`, in any case, now logs one warning naming `rdkafka."security.protocol"` when its client is built. The pipeline keeps running.
