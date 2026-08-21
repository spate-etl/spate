**Breaking:** **The framework configuration sections are `#[non_exhaustive]`**
(`spate-core`) — `PipelineConfig`, `PipelineSection`, `CheckpointSection`,
`BackpressureSection`, `AdminSection` and `MetricsSection` join the connector
configs sealed before them, so from outside the crate a struct literal, a
functional update (`..Default::default()`) and an exhaustive pattern all stop
compiling. A config built in code starts from `PipelineConfig::new(pipeline,
source, sink)`, or `PipelineConfig::new_multi_sink(pipeline, source, sinks)`
for a `sinks:` map, with the `pipeline` section from
`PipelineSection::new(name)`, the four optional sections from `default()` and a
deserializer from `PipelineConfig::with_deserializer`, then assigns the fields
it wants, which stay public. Each of those entry points tags the component it
takes with its section, so a bad connector body reports the same
`source.<type>.<key>` error path it reports when the config was loaded from
YAML. Loading a pipeline from YAML is unaffected, and a section or a key added
after this ships arrives in an additive release.
