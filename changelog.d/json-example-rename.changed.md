**Breaking:** **The JSON example is `json_skip_bad_records`** (`spate`) —
`cargo run --example json_ndjson_memory` no longer resolves; the target is
`cargo run -p spate --features json --example json_skip_bad_records`. The name
says what the example teaches — a malformed record is skipped and counted in
`spate_json_deser_records_dropped_total` rather than stopping the pipeline —
instead of the NDJSON-in-memory plumbing it happens to use to show it.
