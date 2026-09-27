**A `coordination:` section configures the coordination store** (`spate-core`, `spate-coordination`, `spate-s3`)

The pipeline file takes an optional top-level `coordination:` section. It names
the store under `store:` (`nats`, with the fields of `NatsConfig`) and sets the
coordinator's tuning, such as `instance_id` and `lease_duration`. The S3 source
builds its coordinator from the section when the pipeline starts, so a
deployment changes its NATS servers, credentials or identity without a rebuild.
In previous versions these settings were Rust code passed to
`S3Source::with_coordinator`, which still works for a coordinator built in
code. Setting both the section and `with_coordinator` fails startup, and so
does a `coordination:` section on a source that does not coordinate. NATS
`credentials` also deserialize from a single-key map naming the mechanism
(`user_password`, `token` or `creds_file`); the tagged form (`!token …`) still
parses. Custom sources accept the section by overriding the new
`Source::configure_coordination` method.
