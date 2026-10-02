**Kafka client creation errors no longer print rejected values** (`spate-kafka`)

A Kafka source or sink startup error names the `rdkafka:` property librdkafka rejected and its result code, and does not include the value. In previous versions a rejected value, including one interpolated from `${VAR}`, appeared in the error twice: once from rdkafka and once quoted in librdkafka's reason. The reason text is no longer part of the message.
