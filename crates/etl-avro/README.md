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
`AvroValueDeserializer`, and — behind the `fast` feature —
`AvroFastDeserializer<F>`.

The opt-in `fast` feature adds a `serde_avro_fast` backend: single-pass
datum→`T` decoding several times faster than the apache-avro paths, and the
only backend able to emit borrowed (zero-copy) records. Its evolution model
is serde attributes against each writer schema (no reader-schema
resolution); backends are chosen per pipeline and coexist in one build.
License note: `serde_avro_fast`'s crates.io metadata declares
`LGPL-3.0-only` while its repository is MPL-2.0 (fix merged upstream,
unreleased as of 2026-07), and its dependency
`serde_serializer_quick_unsupported` (a ~300-line macro-only helper) is
genuinely LGPL-3.0-only. The feature is off by default, the default build
contains no trace of either crate, and enabling it is your project's own
compliance call.
