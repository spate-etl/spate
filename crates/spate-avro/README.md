# spate-avro

Avro deserialization for the
[Spate](https://github.com/spate-etl/spate) framework: bare-datum
decoding for Confluent wire format (magic byte + schema id), Avro
single-object encoding, and raw datums with a fixed schema — into
dynamically-typed `AvroValue` records or your own serde types.

Registry schemas are fetched by an async task on the I/O runtime and
cached per process. A cache miss never blocks a pipeline thread and never
drops the record: the batch reports "not ready" and replays once the
schema lands. Unresolvable ids are negatively cached with a TTL and then
follow the deserializer's error policy like any poison payload.

Schema evolution follows Avro resolution rules via an optional
`reader_schema` (field reordering, defaults, promotions).

Key types: `AvroDeserializerBuilder` (from the
`deserializer: { avro: ... }` section), `AvroDatumDeserializer<F>`
(single-pass typed decode, optionally zero-copy), `AvroSerdeDeserializer<T>`
(typed decode with Avro schema resolution), and `AvroValueDeserializer`
(dynamically-typed records).
