**Breaking:** **The Kafka feed builds from a public `KafkaFeedConfig`** (`spate-kafka`)

`KafkaFeed::connect` and `KafkaFeedPublisher::connect` take a `KafkaFeedConfig` in place of the `feed:` section. Code that passed the section passes `KafkaFeedConfig::from_section(&section)?` instead. Code that builds its pipeline without a file starts from `KafkaFeedConfig::new(brokers, group_id, topic, retry_topic, dead_letter_topic, attempt_topic)` and sets the public fields it needs, `attempt_cap` among them. Both `connect`s run `KafkaFeedConfig::validate` before they create a client, so a value in the `kafka:` body that `from_section` refuses is refused there too, as a configuration error.

`KafkaFeedConfig` is exported at the crate root and is `#[non_exhaustive]`, so code outside the crate builds it through `new` or `from_section`. The YAML keys and their defaults are unchanged.
