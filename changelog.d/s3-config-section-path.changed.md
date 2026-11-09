**S3 source configuration errors start at the path of the key they refuse** (`spate-s3`)

An error from the S3 source's configuration starts with the path of the key it refuses, then a colon, such as `source.s3.url: must not be empty`, `source.s3.prefetch_bytes: (1.0 MiB) must be at least chunk_bytes (2.0 MiB)` or `source.s3.store."endpont": is not an object_store S3 option`. A refused bucket key, whose text already names it, starts at the section, such as `source.s3: store."Bucket" cannot be overridden: …`. These errors, from `S3SourceConfig::from_component_config` and `S3SourceConfig::validate`, are now `ConfigError::Component`.

In previous versions they were `ConfigError::Validation` and read `invalid configuration: source.s3.url must not be empty` or `invalid configuration: source.s3.store."Bucket" cannot be overridden: …`. Code that matches these errors by variant or by text needs updating.
