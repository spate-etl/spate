**Breaking:** **Every component reports its type through a required `component_type`** (`spate-core`, `spate-test`)

The key a component registers under has one name on every role, `component_type`. `ComponentConfig::type_tag` is now `ComponentConfig::component_type`, `Feed::backend` is now `Feed::component_type`, and `spate_core::feed::memory::BACKEND` is now `memory::COMPONENT_TYPE`. `FeedSection::backend` and `SinkSection::backend` keep their names and still return the section. The YAML is unchanged.

`Source::component_type` has no default. In previous versions a source that did not implement it reported `"source"`. Implement it and return the key the source is configured under. A source that returns `"source"` keeps its previous label and still gets no `Meter`.

`SinkParts::new` takes the sink's `component_type` as its first argument, and `SinkParts::with_component_type` is removed. In previous versions a sink that never called `with_component_type` reported `"custom"`. Pass the type to `new` instead. A sink that passes `"custom"` keeps its previous label and still gets no `Meter`.

A chain over `BytesPassthrough` labels its `spate_deser_*` series `component_type="passthrough"`, and a chain over `spate_test::TestDeserializer` labels them `component_type="test"`. In previous versions both carried `component_type="deserializer"`. Dashboards and alerts that select these series by `component_type` need the new value.
