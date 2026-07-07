# Avro format

The Avro deserializer (`etl-avro`, feature `avro` on the `etl` facade)
decodes bare Avro datums as carried by Kafka messages into records — either
your own serde types or dynamically-typed values. Three framings are
supported; the framework handles the framing bytes so your schemas and
structs only ever describe the datum itself.

Construct it from the pipeline's opaque section, handing it the builder's
I/O runtime handle (the registry fetcher lives there):

```rust
let deserializer =
    AvroDeserializerBuilder::from_component(deser_section, &pipeline.io_handle())?
        .build_serde::<Order>();
```

## Configuration

The `deserializer: { avro: ... }` section deserializes into `AvroSettings`
(`crates/etl-avro/src/config.rs`); unknown fields are rejected.

```yaml
deserializer:
  avro:
    mode: confluent                # confluent | raw | single_object
    registry:                      # required for confluent
      url: ${SCHEMA_REGISTRY_URL}
      username: ${SR_USER}         # optional basic auth
      password: ${SR_PASSWORD}
    prewarm_subjects: [orders-value]
    negative_cache_ttl: 30s
    reader_schema:                 # optional: pin the resolved shape
      path: /etc/etl/orders.avsc   # (or `inline: '{"type": ...}'`)
    # schema: { inline | path }    # required for raw / single_object
```

| Key | Type | Default | Meaning |
|---|---|---|---|
| `mode` | string | `confluent` | Payload framing: `confluent`, `raw`, or `single_object`. |
| `registry.url` | string | — | Base URL of a Confluent-compatible schema registry. Required in `confluent` mode; rejected in the others. |
| `registry.username` / `registry.password` | string | none | Optional basic auth (interpolate secrets via `${VAR}`). |
| `prewarm_subjects` | string list | `[]` | Subjects whose latest schemas are fetched at startup (`confluent` mode only). |
| `negative_cache_ttl` | duration | `30s` | How long a schema id the registry could not serve stays negatively cached before a refetch is allowed. |
| `reader_schema` | `{ inline }` or `{ path }` | none | Optional reader schema pinning the resolved record shape (`confluent` and `raw` modes; not `single_object`). |
| `schema` | `{ inline }` or `{ path }` | — | The writer schema. Required in `raw` and `single_object` modes; rejected in `confluent` mode (writer schemas come from the registry there). |

Mode requirements are validated at construction with explicit messages —
`confluent` without a `registry`, `raw` without a `schema`, `prewarm_subjects`
outside `confluent`, and so on, all fail fast at startup.

## The three modes

- **`confluent`** — Confluent wire format: each payload starts with a
  5-byte header (magic byte `0` plus a 4-byte schema id) followed by the
  datum. The framework parses the header, resolves the id against the
  registry, and decodes; producers using Confluent-ecosystem serializers
  just work.
- **`raw`** — bare datums with a fixed schema you configure (`inline` or a
  mounted `.avsc` file).
- **`single_object`** — Avro single-object encoding (marker plus Rabin
  schema fingerprint), verified against the configured schema's
  fingerprint.

## Typed or dynamic records

The builder hands out two deserializer flavors:

- `build_serde::<T>()` — decodes each datum into your own
  `T: serde::de::DeserializeOwned` struct (field names match the writer
  schema). This is the flagship path — see
  `crates/etl/examples/kafka_avro_to_clickhouse.rs`, where the same struct
  derives `Deserialize` for Avro in and `Serialize` for RowBinary out.
- `build_value()` — decodes into dynamically-typed `AvroValue` records, for
  pipelines that inspect or route on structure not known at compile time.

## Schema fetching never blocks a pipeline thread

In `confluent` mode, registry schemas are fetched by an **asynchronous task
on the I/O runtime** and cached per process. When a payload references a
schema that is not cached yet, the deserializer does not wait: it reports
"not ready", the operator chain holds the batch (backpressure pauses the
source if needed), and **the batch retries once the fetch lands**. Records
are never dropped or duplicated by this mechanism, and the CPU-pinned
pipeline threads never perform I/O. Two knobs shape the edge cases:

- `prewarm_subjects` fetches your subjects' latest schemas at startup, so
  the first batch does not pay the miss.
- Ids the registry *cannot* serve are negatively cached for
  `negative_cache_ttl` and then handled by the deserializer's
  `ErrorPolicy` (Skip or Fail) like any other poison payload — a permanent
  bad id cannot wedge the pipeline into an infinite retry.

## Schema evolution

Writer schemas come from the payload (registry id or fingerprint) or
configuration; an optional `reader_schema` pins the shape records resolve
into, using Avro's standard schema-resolution rules — field reordering,
defaults for missing fields, type promotions, aliases. Pin a reader schema
when your struct must stay stable while producers evolve. Registry schemas
using schema references are not supported yet and surface as unavailable.

## Backend note

Decoding uses the `apache-avro` crate. A `serde_avro_fast` zero-copy
backend (10–20x faster datum decoding) was evaluated and is deliberately
**not shipped**: the upstream repository is MPL-2.0, but every published
crates.io release still declares `LGPL-3.0-only` in its metadata — the
artifact a compliance review sees — so it stays out of the dependency tree
(enforced by `deny.toml`) until a release ships with corrected metadata.
Details in the `etl-avro` crate docs and
[docs/DESIGN.md](../../DESIGN.md) § Dependency policy.

## Related

- [Kafka source](kafka.md) — the payloads this format decodes.
- [Error handling](../02-concepts/04-error-handling.md) — Skip vs Fail for
  undecodable payloads.
- [Backpressure](../02-concepts/03-backpressure.md) — how a held batch
  interacts with flow control.
