**Cleartext registry password warning** (`spate-avro`)

A non-empty `registry.password` with an `http://` registry URL now logs one warning when the deserializer is built, naming `deserializer.avro.registry.password`. The pipeline keeps running.
