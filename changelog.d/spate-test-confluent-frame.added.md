**A Confluent-framed payload for deserializer tests** (`spate-test`)

`spate_test::confluent_frame` writes a payload in the Confluent wire format: the magic byte `0x00`, the schema id big-endian, then the payload. It builds the input a schema-registry deserializer reads, such as one in the `avro` feature's `confluent` mode.
