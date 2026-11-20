**Breaking:** **A deserializer receives a `Meter` from its chain** (`spate-core`)

`Deserializer` has two new methods with defaults. `component_type` names the deserializer, and `ChainBuilder::with_metrics` uses it as the `component_type` label on the chain's `spate_deser_*` series. `attach_metrics` receives a `DeserMetricsCtx`, whose `meter` is a `Meter` for the deserializer's own families under `spate_<component_type>_deser_*`, labeled `component="<component>.deserializer"` like the stage, so the two join. A deserializer that keeps the default `component_type`, `"deserializer"`, gets no `Meter`, and its stage labels are the same as in previous versions. A deserializer that wraps another should forward both methods to the inner one.

`deser` joins `ROLE_SEGMENTS`, so a `Meter` local name that starts with `deser_` panics, as one that starts with `source_`, `sink_` or `feed_` does. The rule holds in every namespace, including the `spate_custom_*` names `ChainCtx::meter` registers. v0.5.0 accepted such names, so a connector or pipeline author who registered one must rename it.

Two cases may need a code change. A type that implements both `Deserializer` and `Source` and calls `component_type()` with method syntax must name the trait, as in `Source::component_type(&x)`. Generic code that calls `ChainBuilder::with_metrics` without a `D: Deserializer<DF>` bound must add one.
