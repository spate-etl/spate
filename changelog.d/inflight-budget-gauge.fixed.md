**Breaking:** **The in-flight byte budget reports its usage** (`spate-core`) —
`spate_backpressure_inflight_bytes` was registered and never written, so it
rendered `0` for the life of every process, which is the same reading a healthy
idle pipeline gives. It carries the budget's current usage, sampled once per
controller pass. Alerts and dashboards keyed on it were reading a constant and
need rechecking against real values.

The series is published once per pipeline, under `component="runtime"` and
`component_type="pipeline"`, so a query that aggregated the
`spate_backpressure_*` family by `component` sees one more label set than the
three that stay per pipeline thread. `BackpressureMetrics::set_inflight_bytes`
is removed, and nothing replaces the call, because the runtime publishes the
series itself.
