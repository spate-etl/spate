**Breaking:** **Fatal operator errors and failed flushes** (`spate-core`)

`spate_operator_errors_total{error_type="fatal"}` now counts fatal errors
under the `component` label of the stage that stops the pipeline. Previously,
the `fatal` and `retryable` series always reported zero; only `record_level`
was updated. The fatal count covers `try_map` failures under
`ErrorPolicy::Fail`, unmatched split records under `Fail`, and encoder errors
under `Fail` or classified as `ErrorClass::Fatal`. Each stage instance counts
at most one fatal error, so a stage's shared series counts how many pipeline
threads encountered one.

The metric no longer registers `error_type="retryable"`; queries selecting
it now return no series. Sums over `spate_operator_errors_total` now include
fatal errors, so review affected dashboards and recording rules.
`spate_sink_errors_total` still reports all three error classes, and skipped
encoder errors still count as `record_level` on the operator metric. Replace
calls to `OperatorMetrics::errors` with `record_errors` or `fatal_error`, as
appropriate.

A failure in `RowEncoder::finish_chunk` now causes the run to fail, including
during shutdown. Previously, the pipeline could report `Completed` and exit
with code `0` despite leaving a batch unsent. Unsent records remain eligible
for replay; the reported run status now reflects the failure.
