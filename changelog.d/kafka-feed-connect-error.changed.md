**Breaking:** **The Kafka feed's `connect` returns `spate_core::feed::ConnectError`** (`spate-kafka`)

`KafkaFeed::connect` and `KafkaFeedPublisher::connect` return `spate_core::feed::ConnectError`, and `spate_kafka::feed::KafkaFeedError` is removed. The three variants keep their meaning and their text. A match on `KafkaFeedError::Refused(reason)` becomes `ConnectError::Refused { reason, .. }`, and the same for `Unavailable`.
