**Breaking:** **In-flight byte budget usage** (`spate-core`)

`spate_backpressure_inflight_bytes` now reports the in-flight byte budget's
current usage, sampled once per controller pass. Previously, it always
reported zero, even when the pipeline held data in flight. Review alerts and
dashboards that use this metric because they now receive actual usage values.

The metric is published once per pipeline with `component="runtime"` and
`component_type="pipeline"`. Queries grouping the `spate_backpressure_*`
metrics by component therefore see this additional label set.
`BackpressureMetrics::set_inflight_bytes` has been removed; remove calls to it
because the runtime publishes the value automatically.
