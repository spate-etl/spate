**`cargo test` compiles without `--all-features`** (`spate`) — the
`e2e_drain_outage` target was undeclared, so it carried no `required-features`
and failed on `spate::avro`, `spate::clickhouse` and `spate::kafka` under any
smaller feature set. Its stanza declares `full`.
