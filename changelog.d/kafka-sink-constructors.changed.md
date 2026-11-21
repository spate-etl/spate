**Breaking:** **The Kafka sink builds through `KafkaSink::new` and `KafkaSink::from_component_config`** (`spate-kafka`)

`KafkaSink::from_component_config(section)` and `KafkaSink::new(config)` replace the free functions `spate_kafka::sink::from_component_config` and `spate_kafka::sink::build`, which are removed. The sources build the same way, and both are reachable from the crate root through `spate_kafka::KafkaSink`. Their errors and the producers they create are unchanged.

`KafkaSinkConfig::from_component_config(section)` is new. It deserializes and validates the section without creating a producer, and its errors start at the section's path, such as `sinks.eu.kafka.topic: must not be empty`.
