**Breaking:** **The Avro deserializer config follows the source and sink config shape** (`spate-avro`)

The `deserializer: { avro: ... }` section deserializes into `AvroDeserializerConfig`, which `AvroDeserializerConfig::from_component_config` builds and `validate` checks. In previous versions the type was `AvroSettings`. `AvroDeserializerBuilder::from_component_config(section, runtime)` and `AvroDeserializerBuilder::new(config, runtime)` replace `from_component` and `from_settings`, and `new` takes the config by value. The YAML keys and their defaults are unchanged.

`AvroDeserializerConfig`, `AvroMode` and `AvroSchemaSection` are `#[non_exhaustive]`. Code outside the crate can no longer build the config with a struct literal. Start from `AvroDeserializerConfig::confluent(registry)`, `raw(schema)` or `single_object(schema)` and set the optional fields instead. A `match` on `AvroMode` needs a wildcard arm.

`validate` runs every check that reads no file, parses no schema and makes no network call, and `AvroDeserializerBuilder::new` runs it before it loads and parses a schema or builds the registry client. Three refusals now come earlier than they did:

- In every mode, the mode checks run before `reader_schema` is read.
- In `raw` and `single_object`, a `prewarm_subjects` list and a `single_object` `reader_schema` are refused before `schema` is read.
- A `schema` or `reader_schema` with both `inline` and `path`, or neither, is refused before either is read, and the refusal names the key.

The same configurations are refused.

`AvroConfigError` is removed, and every refusal is a `spate_core::config::ConfigError` that leads with the key's path. A `negative_cache_ttl` of `0s` reads `deserializer.avro.negative_cache_ttl: must be at least 100ms; found 0s: a shorter entry can expire before the blocked record is replayed, and each replay then fetches the schema id again`. In previous versions it read ``avro configuration: `negative_cache_ttl` must be at least 100ms; found 0s: …``. The `avro schema:` and `avro schema registry client:` prefixes go the same way, so a trust store with no certificates reads `deserializer.avro.registry.tls.root_ca: the system trust store has no certificates; …`.

`build_value` and `build_serde` return the deserializer directly. Every schema they could refuse is already refused by `new`, so callers drop the `?` or `unwrap`. `build_datum` and `build_serde_datum` still return a `Result`, now with `ConfigError`.
