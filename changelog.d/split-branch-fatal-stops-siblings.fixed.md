**A split branch's fatal stops records reaching the other branches**
(`spate-core`) — a fatal latched by one branch of a split sink left the rest of
that payload routing normally, so a sibling branch could encode, seal and ship a
chunk after the pipeline had already decided to stop. Once any branch holds a
fatal, no further record of that payload reaches any branch, which is what a
single `.sink()` already did. The batch fails and replays either way, so the
records were never at risk; the change is that no destination is written past
the point the run failed. Records dropped this way no longer move the split
stage's `spate_operator_records_out_total`, a branch's
`spate_operator_records_in_total`, or a sibling's
`spate_operator_errors_total{error_type="fatal"}` where such a record would have
failed in turn; a post-latch record matching no branch no longer counts the
split stage's own `spate_operator_errors_total{error_type="fatal"}` or
`spate_operator_records_dropped_total{reason="unrouted"}`. The split stage's
`spate_operator_records_in_total` and the reported `component` are unchanged.
