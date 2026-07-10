# Avro fast backend

The Avro deserializer defaults to the `apache-avro` crate. An opt-in
**fast backend** built on `serde_avro_fast` decodes each datum **directly
into your type in a single pass** — no intermediate `AvroValue` — and is
the only backend that can emit **borrowed (zero-copy) records** whose
string and byte fields point straight into the payload buffer:

```rust
let deserializer =
    AvroDeserializerBuilder::from_component(deser_section, &pipeline.io_handle())?
        .build_serde_fast::<Order>()?;   // owned records, single-pass
```

Everything else is unchanged: the three framings (`confluent`, `raw`,
`single_object`), the YAML section, the non-blocking registry fetch with
"not ready" replay, tombstones decoding to zero records, Skip/Fail error
policies, and the `etl_deser_*` metrics taxonomy — the backends are
indistinguishable there.

## When to use it

When decode throughput matters. On this workspace's own bench:

| Path | 15-field record | batch of 50 events (per event) |
|---|---|---|
| apache `build_serde::<T>()` | ~1.03 µs | ~164 ns |
| fast `build_serde_fast::<T>()` | ~200 ns | ~43 ns |
| fast `build_fast::<F>()` (borrowed) | ~129 ns | ~23 ns |

> Measured 2026-07-10 on a quiet Apple M5 Max with
> `cargo bench -p etl-avro --features fast --bench decode` (rustc 1.96.1,
> release); these are criterion outputs, hand-recorded here because the bench
> emits no machine-readable `Report` record (unlike the `benchmarks/` rigs).

The apache `build_serde` path decodes each datum **twice** — once into an
`AvroValue`, then again into your type — so its ~1.03 µs is against a value-only
pass of ~708 ns; the fast backend does the single typed pass directly. That makes
the borrowed fast path about **8.0×** and the owned fast path about **5.2×**
faster on the single 15-field record, and **7.1×** / **3.8×** faster per event on
the 50-event batch.

Stay on the apache backend when you need real Avro **reader-schema
resolution** (see the semantics below), the dynamically-typed
`AvroValue` path, or when you cannot take the license consideration.

## Enabling it

The feature is deliberately **off by default** and excluded from `full`:

```toml
etl = { version = "...", features = ["kafka", "avro-fast", "clickhouse"] }
# or on the crate directly: etl-avro = { ..., features = ["fast"] }
```

> **License note.** The feature pulls in two crates worth knowing about:
> `serde_avro_fast` itself, whose crates.io release metadata declares
> `LGPL-3.0-only` while the upstream repository is MPL-2.0 (the fix is
> merged upstream but unreleased as of 2026-07); and its dependency
> `serde_serializer_quick_unsupported`, a ~300-line `no_std` macro-only
> helper that is **genuinely** LGPL-3.0-only. The default build contains no
> trace of either; enabling the feature is your project's own compliance
> call. The workspace pins the exact evaluated versions.

Enabling the feature only makes the backend *available*. Selection is **per
pipeline**, by which builder method you call — `build_serde` /
`build_value` (apache) and `build_serde_fast` / `build_fast` (fast) coexist
in one binary, so one pipeline can keep reader-schema resolution while
another decodes single-pass.

## Schema evolution semantics (the one real difference)

The fast backend has **no reader-schema resolution by design**: your serde
target type *is* the reader shape, and each datum is decoded against its
writer schema alone. Evolution is expressed with serde attributes instead
of Avro resolution rules:

- a field newer writers added → give your struct field `#[serde(default)]`;
- a renamed field → `#[serde(alias = "old_name")]`;
- Avro type *promotions* (int→long, float→double) and reader-side field
  reordering are **not** applied.

In `confluent` mode the consequence is concrete: every writer schema alive
in the registry decodes straight into your type, so **the type must
tolerate every live writer version** of the subject. Configuring a
`reader_schema` together with a fast builder is rejected at build time with
a clear error; use `build_serde` when you need reader pinning.

Where the schema is fixed at build time — the `raw` and `single_object`
framings — a fast-incompatible schema now fails the **builder** immediately,
not at runtime. Only `confluent` registry schemas, discovered per writer id as
messages arrive, can surface a fast-compile failure **per record**.

A schema the fast backend cannot compile (while apache accepts it) never
poisons the apache path: fast pipelines surface it per record as a
schema-unavailable error, subject to the usual Skip/Fail policy. That
guarantee is **one-directional**, though: the apache parse is a hard
prerequisite for *both* backends, so a schema `apache-avro` itself rejects (or
panics on) is negative-cached *before* any fast compile is attempted. A
fast-rejection never breaks the apache path; an apache-rejection breaks both.

## Borrowed (zero-copy) records

Records flow through the chain **borrowing the payload buffer** — string
contents are never copied. A borrowed family is two lines:

```rust
#[derive(serde::Deserialize)]
struct Order<'a> {
    sku: &'a str,        // points into the payload buffer
    quantity: i32,
}

struct OrderFam;
impl RecFamily for OrderFam {
    type Rec<'buf> = Order<'buf>;
}

let deserializer = builder.build_fast::<OrderFam>()?;
let chain = chain(deserializer)         // chain(), not chain_owned()
    .map_rec::<OrderFam, _>(clean)      // fn items for borrowing families
    .sink(encoder, router, cfg, queues, budget);
```

The flagship shape is a **batch payload exploded with `flat_map`**: one
datum carries an array of events, the fast backend decodes the batch
borrowed, and `flat_map` moves each `Event<'buf>` out —
the borrows keep pointing at the payload, which outlives the whole
synchronous fan-out. The ClickHouse encoders accept borrowed families
directly (`RowEncoder` is family-generic), so zero-copy reaches the sink's
encode step. `crates/etl-avro/tests/zero_copy_flat_map.rs` is a runnable
template that *proves* the zero-copy property by pointer provenance — copy
it for your own pipelines.

One honest caveat: zero-copy covers string/bytes *contents*. A batch's
`Vec` containers still allocate once per payload, amortized over its
events.

## Not supported (yet)

- `reader_schema` with a fast builder (by design — see above).
- The dynamically-typed `AvroValue` path has no fast variant.
- Registry schemas using schema references (same as the apache backend).
- Union coverage beyond `Option<T>`-style null unions and logical types
  are not part of the verified surface; test your schema shape before
  relying on them.

## Related

- [Avro format](README.md) — configuration, framings, registry behavior.
- [Error handling](../../02-concepts/04-error-handling.md) — Skip vs Fail
  for undecodable payloads.
