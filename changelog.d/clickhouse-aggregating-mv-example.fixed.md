**The `clickhouse_aggregating_mv` example builds on default features**
(`spate`) — it needs the `clickhouse` feature but declared no
`required-features`, so `cargo build --examples` against a default-feature
checkout failed to resolve `spate::clickhouse`. The example now carries a
`[[example]]` entry gating it, and is built when the feature is enabled rather
than always attempted.
