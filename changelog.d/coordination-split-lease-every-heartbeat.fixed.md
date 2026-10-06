**A split lease survives one failed renewal** (`spate-coordination`)

A coordinated worker renews every split lease it holds at every heartbeat, and
arms the next heartbeat 0.8 to 1.2 renewal intervals after the last one
finishes. A lease survives one failed renewal when the worker's store calls from
its last successful renewal to the retry, the failed call included, take under a
fifth of `lease_duration` in total. A renewal that times out spends
`op_timeout`, a third of the lease at the defaults, so it can still cost the
lease. In previous versions a heartbeat that came less than one renewal interval
after the last renewal skipped the split. If the next renewal then failed, the
lease could expire before the following heartbeat, in about two in seven such
failures at any `lease_duration`. The worker lost the split as `starved` and
claimed it again at a new epoch, which used a delivery attempt and replayed the
uncommitted tail. Split-lease renewal writes rise to one per held split per
heartbeat.
