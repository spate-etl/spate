**Breaking:** **The JSON deserializer config follows the source and sink config shape** (`spate-json`)

The `deserializer: { json: ... }` section deserializes into `JsonDeserializerConfig`, which `JsonDeserializerConfig::from_component_config` builds and `validate` checks. In previous versions the type was `JsonSettings`. `JsonDeserializerBuilder::from_component_config` and `JsonDeserializerBuilder::new` replace `from_component` and `from_settings`. `new` returns a `Result` and runs `validate`, where `from_settings` returned the builder. The `on_error` and `reject_duplicate_keys` keys and their defaults are unchanged.

`JsonDeserializerConfig`, `JsonFraming` and `OnError` are `#[non_exhaustive]`. Code outside the crate can no longer build the config with a struct literal. Start from `JsonDeserializerConfig::default()` and set the fields instead. A `match` on `JsonFraming` or `OnError` needs a wildcard arm.

`JsonConfigError` is removed, and every refusal is a `spate_core::config::ConfigError`.
