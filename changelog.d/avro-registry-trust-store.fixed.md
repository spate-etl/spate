**The Avro registry client starts with an empty system trust store** (`spate-avro`)

The Avro deserializer builds its schema registry client when a host has no CA
certificates. On Linux and other Unix systems, an `https://` registry is then
verified against the Mozilla root bundle compiled into the binary, and a warning
is logged. In previous versions the deserializer panicked at startup on such a
host, as on a container image without a CA package, even with an `http://`
registry. A client that cannot be built returns `AvroConfigError::Registry`.
macOS and Windows verify `https://` registries with the operating system as
before.
