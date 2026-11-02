**Breaking:** **The Kafka sink refuses a `delivery_timeout` under 1ms** (`spate-kafka`)

A Kafka sink `delivery_timeout` below `1ms`, such as `500us`, fails at load with an error that names the key. The sink passes the value to librdkafka as `message.timeout.ms` in whole milliseconds. In previous versions such a value loaded and reached librdkafka as `0`, which librdkafka reads as no timeout. A write still failed at its own deadline, but librdkafka kept that write's messages queued with no expiry while the retry produced them again. Set `delivery_timeout` to `1ms` or more.
