**Metric assertions for tests** (`spate-test`)

`spate-test` now provides `render_metrics`, which runs a closure under a
recorder local to the calling thread and returns the Prometheus text
exposition. Histograms render with the same buckets as the installed exporter.
`metric_value`, `metric_sum` and `metric_series` read series back out of that
text by sample name and a subset of labels in any order. `metric_value` returns
`None` when no series matches and panics when more than one does, so a test can
tell an absent series from one that reads zero.
