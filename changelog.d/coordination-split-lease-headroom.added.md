**Split lease headroom histogram** (`spate-coordination`, `spate-core`)

A coordinated worker now records `spate_coordination_split_lease_headroom_seconds`
each time a split-lease renewal is confirmed. The value is the time left before
the worker would self-fence the split, on the worker's clock, and 0 when none
was left. Alert on its low buckets to see a lease running short before
`split_losses_total{reason="starved"}` counts. `CoordinationMetrics::split_lease_headroom`
records it.
