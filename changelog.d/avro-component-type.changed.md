**Breaking:** **Avro chains label their deserializer stage `avro`** (`spate-avro`)

The Avro deserializers report `component_type` `"avro"`, so a chain built with `ChainBuilder::with_metrics` labels its `spate_deser_*` series `component_type="avro"`. In previous versions those series carried `component_type="deserializer"`. Dashboards and alerts that select an Avro chain's deserializer series by `component_type` need the new value.
