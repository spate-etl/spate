# spate-avro

The Avro format (`spate-avro`, feature `avro` on the `spate` facade) decodes
bare Avro datums carried by a source into records, either your own serde types
or dynamically-typed `AvroValue` values. It reads three framings: Confluent wire
format with a schema registry, Avro single-object encoding, and raw datums with
a fixed schema. Registry schemas are fetched off the pipeline threads, and a
payload whose schema is not cached yet waits for it.

## Enable

Turn on feature `avro` on the [`spate`](https://crates.io/crates/spate) facade,
which re-exports this crate as `spate::avro`. The facade README has the Cargo
snippet.

## Configuration

The deserializer reads the pipeline's `deserializer: { avro: ... }` section into
`AvroDeserializerConfig`. An unknown key is rejected.

```yaml
deserializer:
  avro:
    mode: confluent                # confluent | raw | single_object
    registry:                      # confluent only, and required there
      url: https://registry.internal:8081
      username: ${SR_USER}         # optional basic auth
      password: ${SR_PASSWORD}     # needs username
      tls:
        root_ca: /etc/spate/registry-ca.pem  # extra trusted CAs; https only
    prewarm_subjects: [orders-value]  # confluent only; fetched at startup
    negative_cache_ttl: 30s        # at least 100ms
    reader_schema:                 # optional; confluent and raw only
      path: /etc/spate/orders.avsc # or inline: '{"type": ...}'
    # schema: { path | inline }    # raw and single_object; required there
```

`mode` and `negative_cache_ttl` show their defaults. `prewarm_subjects` is empty
by default.

Loading fails when `confluent` has no `registry` or sets `schema`, when `raw` or
`single_object` has no `schema` or sets `registry` or `prewarm_subjects`, and
when `single_object` sets `reader_schema`. It also fails on a `registry.url`
that is not `http://` or `https://` or that carries credentials, a `password`
with no `username`, a `negative_cache_ttl` under `100ms`, and a `tls.root_ca`
beside a URL that is not `https://`, and on a `schema` or `reader_schema` that
sets both `inline` and `path`, or neither. `AvroDeserializerConfig::validate`
runs all of these checks without reading a file or the network.

Building the deserializer runs `validate` first, then reads the schema files
and builds the registry client. On Linux and other Unix systems other than
macOS it fails on an `https://` registry when neither the system trust store
nor `tls.root_ca` holds a usable certificate. A fixed `schema` is parsed there,
and one that does not parse fails the build.

A non-empty `password` with an `http://` registry URL loads, and logs one
warning that the credential is sent without TLS.

`AvroDeserializerBuilder::from_component_config` takes the section and the
pipeline's I/O runtime handle, where the registry fetcher runs. In code, build
the config with `AvroDeserializerConfig::confluent`, `raw` or `single_object`,
set any optional fields, and pass it to `AvroDeserializerBuilder::new` with the
same handle.
[`kafka_avro_to_clickhouse.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/kafka_avro_to_clickhouse.rs)
wires it into a whole pipeline.

The
[Configuration section of the Avro format page](https://spate.kainth.dev/docs/user-guide/connectors/formats/avro#configuration)
has the full key table, and
[Security](https://spate.kainth.dev/docs/user-guide/connectors/formats/avro#security-tls-and-registry-credentials)
covers the trust store and the registry credentials.

## Framings

`mode` sets how each payload frames its datum:

| Mode | Payload | Writer schema |
| --- | --- | --- |
| `confluent` | `0x00`, a 4-byte big-endian schema id, then the datum | Fetched from the registry by that id |
| `single_object` | `0xC3 0x01`, the 8-byte little-endian Rabin fingerprint of the writer schema, then the datum | `schema`; a payload whose fingerprint differs is an error |
| `raw` | The datum alone | `schema` |

An empty payload decodes to no record in every mode. A header that is too short
or carries the wrong magic bytes is a malformed payload, and [Errors](#errors)
says what happens to it.

## Schema registry

In `confluent` mode the writer schema comes from the registry, by the id in each
payload.

- A task on the pipeline's I/O runtime fetches schemas, so a pipeline thread
  never makes a request. Each builder keeps one in-memory cache, shared by every
  deserializer it builds and so by every pipeline thread.
- A payload whose schema id is not cached reports not ready and requests the
  fetch. The chain holds the batch at that payload, and the pipeline thread
  replays the batch from there until the schema is cached. The held payload is
  not dropped and does not count as an error.
- `prewarm_subjects` fetches the latest schema of each listed subject at
  startup. Payloads that arrive after the fetch finds a schema need no fetch of
  their own.
- A `404`, a schema that is not Avro or uses schema references, and a schema the
  parser rejects are permanent answers. The id is negatively cached for
  `negative_cache_ttl`, and its payloads take the error policy. The negative
  cache holds at most 1,024 ids and drops the oldest for a new one. A dropped id
  is fetched again on its next payload.
- The cache holds at most 1,024 compiled schemas and an estimated 64 MiB of
  them. At either bound it evicts the oldest schema that no payload has used
  since the cache last considered it, and it evicts the 256 newest last. An
  evicted id is fetched again on its next payload, and that fetch can fail as
  a first fetch can. A pre-warm above a bound keeps the subjects listed last.
  A pipeline thread keeps the schemas it last saw until its next payload, so
  each thread that receives no payloads can hold up to the bounds again.
- A schema estimated to hold more than 64 MiB compiled is refused. Its id is
  negatively cached and its payloads take the error policy. The estimate is
  20 bytes for each byte of the schema's JSON, and 20 more for each byte of a
  record, enum or fixed, once per name it has and again inside each record
  that holds it.
- Any other failure, such as another `5xx`, a `429`, a timeout or a refused
  connection, is transient. The id stays uncached and its batch stays held. Each
  later fetch of the id waits a backoff that starts at 200 ms and doubles up to
  5 s. Each wait is drawn at random between 80% and 100% of its step, so ids
  and replicas that fail together spread their fetches.
- A `401` or `403` on a schema fetch, a `401` on a pre-warm, a certificate that
  fails verification, or a TLS handshake the registry rejects marks the client
  as rejected. The next payload whose schema is not cached then stops the
  pipeline, whatever the error policy. Schemas already cached keep decoding
  until then.
- A fetch by id asks for soft-deleted schemas too, so records written under a
  schema that was later soft-deleted still decode.

[Schema fetching never blocks a pipeline thread](https://spate.kainth.dev/docs/user-guide/connectors/formats/avro#schema-fetching-never-blocks-a-pipeline-thread)
covers the mechanism.

## Records and schema evolution

The builder hands out three deserializers:

- `build_serde_datum::<T>()`, or `build_datum::<F>()` for any record family,
  gives an `AvroDatumDeserializer`. It decodes each datum straight into the
  target type in one pass. With a borrowed record family, string and bytes
  fields point into the payload buffer. Its build refuses a `reader_schema`. A
  schema with a `duration` or `big-decimal` logical type fails the build in
  `raw` and `single_object` modes, and fails each payload that names it in
  `confluent` mode.
- `build_serde::<T>()` gives an `AvroSerdeDeserializer`. It decodes each datum
  into an `AvroValue` and then into `T`, and applies a `reader_schema`.
- `build_value()` gives an `AvroValueDeserializer`. It emits `AvroValue`
  records, and also applies a `reader_schema`.

A `reader_schema` pins the shape records resolve into, under Avro's resolution
rules. Fields are matched by name and come out in the reader's order, a reader
field the writer lacks takes its default, and a writer `int` can be read as a
`long`. Without a `reader_schema`, records decode in the writer schema's shape,
and `#[serde(default)]` and `#[serde(alias)]` on the record type cover added and
renamed fields.

A `reader_schema` that recurses through a union, such as a linked list whose
`next` field is `["null", "Node"]`, limits how deep a datum may recurse. Each
pass through the same union doubles what resolving the values below it costs,
so a datum is refused as a malformed record when its doubling adds more than
1,024 steps, and more than three times what it costs with no doubling. Where
the datum's encoded length in bytes is less than that undoubled cost, the
length takes its place. A linked list
resolves up to 9 nodes when its nodes have one other field, and up to 5 when
they have 40. A reader schema that does not recurse refuses nothing.

[Schema evolution](https://spate.kainth.dev/docs/user-guide/connectors/formats/avro#schema-evolution)
lists what resolves and what does not, and
[`avro_schema_evolution.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/avro_schema_evolution.rs)
asserts the rules in one program.

## Errors

A payload that does not decode is a record-level error. That covers a short or
mis-tagged header, a single-object fingerprint that does not match `schema`, a
truncated datum, a datum that does not fit the target type, a datum nested
deeper than the deserializer's depth limit or past its item or decoded-size
budget, and a schema id the registry cannot serve. One payload
is at most one record, so a payload fails whole.

Such a payload takes the deserializer stage's error policy, which
`deser_error_policy` on the chain sets:

- **Skip** is the default for the deserializer stage. The payload is dropped, a
  rate-limited `WARN` is logged with an error text that quotes no string,
  bytes or field value from the record, and the chain moves on. A refusal by
  the decode bounds names the count, length or index it read from the payload. The source commits past
  it, and it does not replay.
- **Fail** stops the pipeline at that payload. The batch's acknowledgement
  fails, so the source does not commit past it, and the payload replays after a
  restart.

A rejected registry stops the pipeline under either policy, as
[Schema registry](#schema-registry) describes. A payload waiting on a fetch
takes no policy.

On a chain built with `with_metrics`, the framework's deserializer metrics count
a skipped payload. This crate keeps no counter of its own, so on a chain built
without `with_metrics` nothing counts a skipped payload, and the skip shows in
the rate-limited `WARN` log.

[Error handling](https://spate.kainth.dev/docs/user-guide/connectors/formats/avro#error-handling)
on the Avro format page covers each case.

## Sharp edges

- **Reader field aliases.** A reader field that renames a writer's field and
  lists the old name in `aliases` fails every payload, so the default Skip
  policy drops the whole stream. Rename with `#[serde(alias)]` on the record
  type. See
  [Schema evolution](https://spate.kainth.dev/docs/user-guide/connectors/formats/avro#schema-evolution).
- **Registry outages.** While the registry cannot be reached, every batch that
  carries an uncached schema id stays held, for as long as the outage lasts. See
  [Security](https://spate.kainth.dev/docs/user-guide/connectors/formats/avro#security-tls-and-registry-credentials)
  for which failures are transient.
- **`AvroValue` stability.** `AvroValue` is `apache-avro`'s `Value` type,
  re-exported, so a pipeline on the dynamic path follows that 0.x crate's
  releases. The two typed paths do not expose it. See
  [the installation page](https://spate.kainth.dev/docs/user-guide/getting-started/installation).

## Metrics

This crate registers no metric families of its own. The framework's deserializer
families count its outcomes on a chain built with `with_metrics`. The
[Metrics section of the Avro format page](https://spate.kainth.dev/docs/user-guide/connectors/formats/avro#metrics)
names them.

## Related

- [`spate`](https://crates.io/crates/spate) — the facade an application depends
  on, with the Cargo snippet.
- [Avro format page](https://spate.kainth.dev/docs/user-guide/connectors/formats/avro)
  — every key, registry TLS and credentials, the three deserializers and the
  resolution rules.
- [API documentation on docs.rs](https://docs.rs/spate-avro) — the builder, the
  settings types and the three deserializers.
- [Error handling](https://spate.kainth.dev/docs/user-guide/concepts/error-handling)
  — Skip and Fail for every stage.
- [`avro_schema_evolution.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/avro_schema_evolution.rs)
  — a reader schema resolving an older producer's records, with nothing
  installed
  (`cargo run -p spate --features avro --example avro_schema_evolution`).
- [`kafka_avro_to_clickhouse.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/kafka_avro_to_clickhouse.rs)
  — a Confluent-framed order stream from Kafka into ClickHouse. Needs Kafka, a
  schema registry and ClickHouse.
- [`kafka_avro_flatmap_clickhouse.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/kafka_avro_flatmap_clickhouse.rs)
  — raw datums fanned out into a row per order line. Needs Kafka and ClickHouse.
- [Examples index](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/README.md)
  — every example, with the features and infrastructure each needs.
- [`spate-test`](https://crates.io/crates/spate-test) — in-memory sources and
  sinks, for a test that drives Avro payloads through a pipeline with no
  registry.
