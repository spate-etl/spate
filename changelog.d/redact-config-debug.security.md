**Config `Debug` output redacts credentials** (`spate-core`, `spate-avro`, `spate-clickhouse`, `spate-kafka`, `spate-coordination`, `spate-s3`)

Formatting a config with `{:?}` renders every credential as `<redacted>`. In
previous versions it printed the Avro registry password, the ClickHouse
password and `settings` values, every `rdkafka:` value, and every value in a
`PipelineConfig` connector section. URLs in the registry, ClickHouse replica
and NATS server settings show `<redacted>` in place of userinfo and of any
query or fragment. This applies to `Debug` output, ClickHouse startup error
messages and the ClickHouse `replica` metric label. A dashboard that filters
on a replica URL carrying userinfo or a query needs the redacted form.

The public `spate_core::config::redact` module provides the same helpers for
the `Debug` impl of a custom connector's config.
