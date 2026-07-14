# etl-json

JSON deserialization for the [`etl-rs`](https://github.com/marcuskainth/etl-rs)
framework. Decodes JSON payloads from a source into your own serde types or
dynamically-typed values.

Applications should depend on the **`etl`** facade crate with the **`json`**
feature rather than on this crate directly.

## Framings

One payload maps to 0..N records, chosen by `framing`:

- `single` — the whole payload is one JSON document → one record (the
  Kafka-message default). Empty/whitespace-only payloads are tombstones.
- `ndjson` — newline-delimited JSON (JSON Lines): one value per `\n`-separated
  line → one record per line, with **per-line** error isolation.
- `array` — a top-level JSON array → one record per element (decoded in one
  pass; a malformed array is handled atomically).

## Configuration

```yaml
deserializer:
  json:
    framing: single              # single | ndjson | array
    on_error: skip               # skip | fail
    reject_duplicate_keys: false
```

```rust
let deserializer = JsonDeserializerBuilder::from_component(deser_section)?
    .with_metrics(ctx.pipeline, "main")
    .build_serde::<Order>();
```

## Errors and metrics

A document that does not parse (or match the target type) is a record-level
error. `on_error: skip` drops it, counts it in
`etl_json_deser_records_dropped_total{reason}`, and continues; `on_error: fail`
stops the batch, which replays under at-least-once. The framework's generic
`etl_deser_*` stage metrics wrap every decoder in addition.

## Fidelity features

`reject_duplicate_keys` (config) turns silent last-value-wins on duplicate
object keys into an error. Opt-in Cargo features pass straight through to
`serde_json`: `float-roundtrip`, `arbitrary-precision` (crate-wide; interacts
with flatten/untagged/RawValue), and `raw-value`.

## Backends

Decoding uses `serde_json` (stable 1.x), behind an internal seam so a SIMD
backend can be added later without an API change. `from_reader` is never used —
decoding always operates on the in-memory payload slice.
