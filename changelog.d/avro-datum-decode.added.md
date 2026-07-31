**Typed Avro datum decoding** (`spate-avro`) — `AvroDatumDeserializer`, built
with `AvroConfig::build_datum` or `build_serde_datum`, decodes a datum straight
into a typed record in a single pass rather than materialising a dynamic `Value`
first. Reach for it when the record shape is known at compile time; the
`Value`-based path is unchanged and remains the one to use when it is not.
([#31])
