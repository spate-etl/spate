**Breaking:** **An operator stage that stops the pipeline names itself**
(`spate-core`) — `spate_operator_errors_total` registered an `error_type` of
`retryable`, `record_level` and `fatal`, and only `record_level` had a writer,
so the other two rendered `0` for the life of the process whatever happened. A
`try_map` under `ErrorPolicy::Fail`, a split whose `unmatched` policy is `Fail`,
and a sink encoder that fails under `Fail` or with an error classed
`ErrorClass::Fatal` each count one `error_type="fatal"` under their own
`component` label, so the exposition names the stage the pipeline stopped in.
An encoder error the Skip policy drops still counts `record_level`, as before.

A stage counts one fatal however often it is re-entered afterwards. Every
pipeline thread's instance of a stage shares one series, so the value is the
number of threads that tripped.

`error_type="retryable"` no longer registers on this family. An operator stage
carries no error class to retry on, and `spate_sink_errors_total` still carries
all three classes. A query selecting
`spate_operator_errors_total{error_type="retryable"}` now matches no series, and
a `sum` over the bare family name grows by the fatal counts.
`OperatorMetrics::errors` is replaced by `record_errors` and `fatal_error`.

Fixing the same family exposed a second defect and closes it. An encoder that
failed to finalize a block latched a fatal that the flush latching it never
returned, and the controller stopped reading driver events before the drain
began, so the pipeline reported `Completed` and exited `0` while a batch had
been abandoned. The data always replayed, since the unsent rows'
acknowledgments fail on teardown, but the exit code said the run had succeeded.
The flush returns the fatal and the controller takes what the drain reported,
so the run fails.
