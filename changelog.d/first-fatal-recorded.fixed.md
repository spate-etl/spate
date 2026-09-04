**A stopped pipeline names the failure that happened first** (`spate-core`) — when
two stages of one chain failed within the same payload, the `FatalError` the pipeline
stopped on carried whichever stage sat nearest the source. A record that latched a
sink encoder's fatal and a later record in the same payload that failed a `try_map`
under `ErrorPolicy::Fail` reported the `try_map`'s reason. The reason now names the
stage that recorded first, wherever it sits, so the run's failure and the exit report
point at the failure rather than at its position; the same holds across the branches
of a split sink, and between a deserializer under `ErrorPolicy::Fail` and a stage that
already latched on a record the same payload emitted. Reporting a fatal drains every
stage's slot, so the batch after it is no longer failed a second time on the reason
that was passed over. Counts on `spate_operator_errors_total{error_type="fatal"}` are
unchanged.
