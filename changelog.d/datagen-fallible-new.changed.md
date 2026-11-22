**Breaking:** **`DatagenSource::new` validates its config** (`spate-datagen`)

`DatagenSource::new(config)` returns `Result<DatagenSource, ConfigError>` and refuses what `DatagenSourceConfig::validate` refuses. In previous versions it returned the source, and an invalid config was refused only when the pipeline opened the source, as a fatal error whose reason held the config error's text. `DatagenSource::from_component_config` returns the same errors as before.
