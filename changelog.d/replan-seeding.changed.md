**Replan seeding** (`spate-coordination`)

A plan run writes only the splits the leader has not already seen in the store,
and keeps up to 64 of those writes in flight. Previously every run sent two
create requests for every split the planner returned, one at a time, and the
store refused all but the new ones. With `refresh_listing: true` on a large
prefix, a replan now costs store writes in proportion to the new splits. A
custom `CoordinationStore` receives these creates concurrently on one handle.
`spate_coordination_splits_planned_total` now also counts the splits a run
created before its seeding failed; previously those splits were never counted.
A split record deleted from the store by hand is no longer re-created while the
leader still holds it in its view. The listing that recounts the plan after
each run is unchanged.
