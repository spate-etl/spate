**Breaking:** **A leader keeps its election when its plan-record write's
reply is lost** (`spate-coordination`)

A newly elected leader whose write to the plan record applies, but whose
reply is lost, now reads the record back, finds its own write and keeps the
leadership. The plan record names the worker that won the election, by its
`instance_id` and a value unique to the process. In previous versions the
leader gave the leadership back, and the next election advanced the plan
generation a second time. No split progress was lost either way.

A worker on 0.2 stops with a fatal error when it reads a plan record this
version wrote. The error starts with `plan record: not a schema-3 record` and
names the unknown field `elector`. This version reads plan records that 0.2
wrote.

To upgrade, stop every 0.2 worker on a job before starting this version on
it. Otherwise the remaining 0.2 workers stop once an upgraded worker leads,
and their splits move to upgraded workers. Records may replay, as after any
takeover. To return to 0.2, run it under a new job name. A bounded job then
runs again from the beginning.
