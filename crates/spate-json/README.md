# spate-json

The JSON format (`spate-json`, feature `json` on the `spate` facade) decodes
JSON payloads from a source into records, either your own serde types or
dynamically-typed `serde_json::Value`s. One payload yields zero or more records,
depending on its framing. Parsing uses `serde_json` by default, or `simd-json`
behind an opt-in feature.

## Enable

Turn on feature `json` on the [`spate`](https://crates.io/crates/spate) facade.
The facade README has the Cargo snippet.

Four more facade features each turn on `json`, and all are off by default.
`json-simd` swaps the parser to `simd-json`. `json-float-roundtrip`,
`json-arbitrary-precision` and `json-raw-value` turn on the `serde_json`
features `float_roundtrip`, `arbitrary_precision` and `raw_value`.

`BACKEND_ID` names the compiled parser, `serde_json` or `simd-json`. Both
parsers decode from the payload already in memory, and the crate never calls
`from_reader`.

## Configuration

The deserializer reads the pipeline's `deserializer: { json: ... }` section into
`JsonSettings`. Every key has a default, so `json: {}` is a complete section. An
unknown key is rejected.

```yaml
deserializer:
  json:
    framing: single               # single | ndjson | array
    on_error: skip                # skip | fail
    reject_duplicate_keys: false  # true: a repeated object key is an error
```

Each value above is the default.

With `reject_duplicate_keys: false`, `build_value()` keeps the last value of a
repeated key. A `#[derive(Deserialize)]` struct rejects a repeated field on its
own, so under `build_serde` that document is a `malformed` record error even
with the check off. With `true`, a repeated key at any depth makes the document
an error for `on_error` to handle. The check parses each document a second time.

`JsonDeserializerBuilder::from_component` reads the section.
`build_serde::<T>()` decodes each document into your own `T: DeserializeOwned`,
and `build_value()` into `serde_json::Value`.
[`json_skip_bad_records.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/json_skip_bad_records.rs)
builds one from settings in code, and
[`s3_backfill.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/s3_backfill.rs)
builds one from the section.

The
[Configuration section of the JSON format page](https://spate.kainth.dev/docs/user-guide/connectors/formats/json#configuration)
has the full key table.

## Framings

`framing` decides how many records one payload yields:

- `single` — the whole payload is one document and yields one record.
- `ndjson` — newline-delimited JSON. Each `\n`-separated line is one document
  and yields one record. Blank lines are skipped.
- `array` — the payload is one top-level JSON array, decoded in one pass. Each
  element yields one record.

Under every framing, an empty or whitespace-only payload is a tombstone: it
yields no records and no error. Every record decoded from one payload carries
that payload's offset.

A source that frames its own byte stream, such as the S3 source, takes this
crate's `NdjsonFramer`. It is a bounded, chunk-fed framer: the source feeds it
an object in chunks, and it cuts a record at each `\n`, strips one trailing `\r`
and skips blank lines. A line longer than the bound it is built with is an
error. `S3Source::with_framer` takes a factory that builds one per object, and
`s3_backfill.rs` wires one in.

Exactly one side frames. A source with a framer hands over one record per
payload, and `for_source_framing` then keeps `single` and returns an error for
`ndjson` or `array`. A source that hands over whole payloads, such as the Kafka
source, leaves the configured `framing` in force.
[Framing vs deserialization](https://spate.kainth.dev/docs/user-guide/connectors/formats#framing-vs-deserialization)
explains the split.

## Errors

A document that does not parse, does not match the target type, or repeats a key
under `reject_duplicate_keys` is a record error. `on_error` decides what the
decoder does with it:

- `skip` (the default) drops the record and decodes the rest of the payload.
  Under `ndjson` the good lines around a bad one still produce records. Under
  `array` one bad element drops the whole array.
- `fail` returns a decode error at the first bad record, and the decoder emits
  no record from that payload under any framing.

A decode error then meets the chain's deserializer error policy, set with
`deser_error_policy` and `Skip` by default:

- `Skip` drops the whole payload, good records included, and the pipeline
  continues.
- `Fail` stops the pipeline. The batch resolves as failed, the watermark stops
  there, and the payload replays after a restart.

A record that `on_error: skip` drops never reaches the sink. The source commits
past it once the rest of its batch is written.

Drops under `on_error: skip` log a rate-limited `WARN`. The crate also counts
each drop, when the builder is given `JsonDeserializerBuilder::with_metrics`,
and the chain's `with_metrics` does not affect that count. A dropped array
counts once. The framework's deserializer metrics count each payload that the
chain's `Skip` policy drops, on a chain built with its own `with_metrics`, and
never see these drops, because the decode call succeeds.

The
[Error handling section of the JSON format page](https://spate.kainth.dev/docs/user-guide/connectors/formats/json#error-handling)
and the
[record-level policy](https://spate.kainth.dev/docs/user-guide/concepts/error-handling#record-level-policy-skip-or-fail--nothing-else)
cover both policies.

## Sharp edges

- **`on_error: fail` drops good records by default.** Under the chain's default
  `Skip` policy, one bad record drops its whole payload. Set
  `deser_error_policy(ErrorPolicy::Fail)` for a bad record to stop the pipeline.
  See
  [Error handling](https://spate.kainth.dev/docs/user-guide/connectors/formats/json#error-handling).
- **`json-arbitrary-precision` is crate-wide.** Cargo unifies the `serde_json`
  feature across the dependency graph, and it interacts with
  `#[serde(flatten)]`, `#[serde(untagged)]` and `RawValue`. See
  [Fidelity & features](https://spate.kainth.dev/docs/user-guide/connectors/formats/json#fidelity--features).
- **`json-simd` is a different parser.** The three `serde_json` features do not
  apply under it, and the two parsers differ on some numbers and on nesting
  limits. The duplicate-key check always runs on `serde_json`. See
  [Backends](https://spate.kainth.dev/docs/user-guide/connectors/formats/json#backends).

## Metrics

The
[Metrics section of the JSON format page](https://spate.kainth.dev/docs/user-guide/connectors/formats/json#metrics)
lists the crate's drop counter and the framework's deserializer families, with
their labels.

## Related

- [`spate`](https://crates.io/crates/spate) — the facade an application depends
  on, with the Cargo snippet.
- [JSON format page](https://spate.kainth.dev/docs/user-guide/connectors/formats/json)
  — every key, the framings, both backends and the metric family.
- [API documentation on docs.rs](https://docs.rs/spate-json) — the builder, both
  deserializers, the settings types and `NdjsonFramer`.
- [`json_skip_bad_records.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/json_skip_bad_records.rs)
  — one NDJSON payload with a malformed line skipped and counted, with nothing
  installed
  (`cargo run -p spate --features json --example json_skip_bad_records`).
- [`s3_backfill.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/s3_backfill.rs)
  — `NdjsonFramer` wired into the S3 source over a staged local directory
  (`cargo run -p spate --features s3,json --example s3_backfill`).
- [Decoding records](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#decoding-records)
  — how the S3 source runs a format's framer.
- [Examples index](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/README.md)
  — every example, with the features and infrastructure each needs.
- [`spate-test`](https://crates.io/crates/spate-test) — in-memory sources and
  sinks, for a test that drives a JSON pipeline with nothing running.
