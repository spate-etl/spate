**Breaking:** **The coordination-seam example is `custom_coordinated_source`**
(`spate`) — the `coordinated_pipeline` example target is gone; run it as
`cargo run -p spate --features coordination --example custom_coordinated_source`.
The name says what the example is for: writing a coordination-aware source from
scratch, as distinct from `s3_coordinated_backfill`, which consumes coordination
through a connector that already implements it.
