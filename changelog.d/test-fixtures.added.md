**Fixtures for stage and config tests** (`spate-test`)

`spate-test` now provides `record`, `raw_payload` and `test_ack` to build the
input of a deserializer or encoder under test. The records and payloads carry
zeroed metadata, and their acknowledgement handle discards its resolution.
`component_config` builds a `ComponentConfig` from a YAML body, and
`unique_name` returns a pipeline or component name no other call in the process
returns, so tests sharing one binary do not claim the same gauge series.
`spate-test` now depends on `serde_yaml` (the `yaml_serde` package), which
`spate-core` already builds.
