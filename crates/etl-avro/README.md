# etl-avro

Avro deserialization for the
[etl-rs](https://github.com/marcuskainth/etl-rs) framework: bare-datum
decoding for Confluent wire format (magic byte + schema id), Avro
single-object encoding, and raw datums with a fixed schema — into
dynamically-typed `AvroValue` records or your own serde types.

Registry schemas are fetched by an async task on the I/O runtime and
cached per process. A cache miss never blocks a pipeline thread and never
drops the record: the batch reports "not ready" and replays once the
schema lands. Unresolvable ids are negatively cached with a TTL and then
follow the deserializer's error policy like any poison payload.

Schema evolution follows Avro resolution rules via an optional
`reader_schema` (field reordering, defaults, promotions, aliases).

Key types: `AvroDeserializerBuilder` (from the
`deserializer: { avro: ... }` section), `AvroSerdeDeserializer<T>`,
`AvroValueDeserializer`.

A zero-copy backend (`serde_avro_fast`, 10–20× faster datum decoding) was
evaluated and is not shipped: its crates.io release metadata still declares
`LGPL-3.0-only` while the repository shows MPL-2.0. Revisit when a release
ships with corrected metadata.
