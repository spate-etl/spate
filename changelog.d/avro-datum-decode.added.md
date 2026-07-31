**Typed Avro datum decoding** (`spate-avro`) — `AvroDeserializerBuilder::build_datum`
and `build_serde_datum` return an `AvroDatumDeserializer` that decodes a datum
straight into a typed record in a single pass, rather than materialising a
dynamic `Value` first. Reach for it when the record shape is known at compile
time; the `Value`-based path is unchanged and remains the one to use when it is
not. ([#31])
