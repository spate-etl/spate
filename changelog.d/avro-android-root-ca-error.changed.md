**On Android, `registry.tls.root_ca` is refused as a registry client error** (`spate-avro`)

On Android, a deserializer with `registry.tls.root_ca` set fails to build with `AvroConfigError::Registry` and the text `avro schema registry client: registry.tls.root_ca: is not supported on Android`. In previous versions it failed with `AvroConfigError::Invalid` and the text `avro configuration: registry.tls.root_ca is not supported on Android`. The setting is still refused, so only code that matches on the variant or the text needs updating. Other platforms are not affected.
