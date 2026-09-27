**The Kafka sink logs each librdkafka error once** (`spate-kafka`)

The Kafka sink logs one `librdkafka` warning for each error librdkafka
reports. In previous versions it logged each one twice, so a count of these
warnings read double. librdkafka's own `FAIL` log line for a broker failure
still appears beside the warning. The Kafka source is unchanged.
