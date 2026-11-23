**Breaking:** **`KafkaSource::new` validates its config** (`spate-kafka`)

`KafkaSource::new(config)` returns `Result<KafkaSource, ConfigError>` and refuses what `KafkaSourceConfig::validate` refuses, including a TLS or SASL passthrough on a build without the `tls` feature. In previous versions it returned the source, and an invalid config was refused only when the pipeline opened the source, as a fatal error whose reason held the config error's text. `KafkaSource::from_component_config` returns the same errors as before.
