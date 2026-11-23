**Breaking:** **`S3Source::new` takes the framer and a borrowed handle, and validates its config** (`spate-s3`)

`S3Source::new(config, &io, framer)` and `S3Source::from_component_config(section, &io, framer)` borrow the I/O runtime handle, take the framer factory as an argument, and return `Result<S3Source, ConfigError>`. `new` refuses what `S3SourceConfig::validate` refuses. `S3Source::with_framer` is removed: pass its factory to the constructor instead.

In previous versions `new` took the handle by value and returned the source, and the framer came from `with_framer`. A source built in code with an invalid config or no framer was refused only when the pipeline opened it or `list_once` ran.
