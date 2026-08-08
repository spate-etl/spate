**Breaking:** **The JSON example is `json_skip_bad_records`** (`spate`) — the
`json_ndjson_memory` example target is gone; run it as
`cargo run -p spate --features json --example json_skip_bad_records`. The name
says what the example teaches — a malformed record is skipped and counted in
`spate_json_deser_records_dropped_total` rather than stopping the pipeline —
instead of the NDJSON-in-memory plumbing it happens to use to show it.
