**Breaking:** **The JSON deserializer takes its metrics scope from the chain** (`spate-json`)

The JSON deserializers count skipped records on a chain built with `ChainBuilder::with_metrics`, through the `Meter` the chain hands them. `spate_json_deser_records_dropped_total` now carries `component="<component>.deserializer"` and `component_type="json"`, the same values as the chain's `spate_deser_*` series, so the two join. The chain's `spate_deser_*` series for a JSON deserializer also carry `component_type="json"`, where they carried `"deserializer"`. In previous versions the family was scoped by `JsonDeserializerBuilder::with_metrics` and carried `component="<component>"` and `component_type="deserializer"`, so it did not join the stage series.

`JsonDeserializerBuilder::with_metrics` is removed. Drop the call: a chain built with `with_metrics` now counts the drops. Dashboards and alerts that select these series by `component` or `component_type` need the new values.
