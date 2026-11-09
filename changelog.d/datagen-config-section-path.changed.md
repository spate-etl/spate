**Datagen source configuration errors start at the path of the key they refuse** (`spate-datagen`)

An error from the datagen source's configuration starts with the path of the key it refuses, then a colon, such as `source.datagen.partitions: must be at least 1` or `source.datagen.count: (4) is below source.datagen.partitions (8): …`. These errors, from `DatagenSourceConfig::from_component_config` and `DatagenSourceConfig::validate`, are now `ConfigError::Component`. The same text reaches the fatal error `open` returns for a config built in code.

In previous versions they were `ConfigError::Validation` and read `invalid configuration: source.datagen.partitions must be at least 1`. Code that matches these errors by variant or by text needs updating.
