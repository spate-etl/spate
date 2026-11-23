**Breaking:** **The Avro nested config types carry the connector's name** (`spate-avro`)

`SchemaSource` is renamed `AvroSchemaSection`, and `RegistrySection` is renamed `AvroRegistrySection`. Their fields, constructors and YAML keys are unchanged. A refusal or `Debug` output that names either type names the new one.
