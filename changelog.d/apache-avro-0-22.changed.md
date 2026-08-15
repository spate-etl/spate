**Breaking:** **`apache-avro` moves to 0.22** (`spate-avro`, `spate-datagen`).
The schema parser is stricter — duplicate union members, duplicate field names,
duplicate enum symbols, a `record`/`enum`/`fixed` used as a type name, and an
out-of-range decimal precision are all rejected where 0.21 accepted them — so a
schema running today can stop compiling on upgrade, surfacing as
`SchemaUnavailable` per record for a registry id and at build time for a fixed
schema.

A `uuid` logical type now carries its backing. A 16-byte `fixed` backing
decoded to nothing usable and decodes correctly now; a `bytes` backing was not
recognized as a `uuid` at all, so such a field moves from `AvroValue::Bytes` to
`AvroValue::Uuid` and a borrowed `&[u8]` target for it stops decoding.
`AvroValue` is a re-export of `apache_avro::types::Value`, so the
dynamically-typed path takes that crate's own breaking changes with this bump;
the typed paths do not.
