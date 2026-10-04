**A worker's leftover lease no longer quarantines a released split**
(`spate-coordination`)

A worker that finds its own live lease on a split whose record names no owner
now claims the split without using a delivery attempt. Such a lease is left
behind when a claim's lease write applies but its reply is lost, or when the
lease delete fails after a release or a failure report cleared the record. In
previous versions the worker counted that claim as a fast reclaim after a
restart and used an attempt. A split handed back one attempt below
`max_attempts` was then quarantined with no new failure, and with
`max_attempts: 1` a split that had never run could be quarantined. These claims
now count under the `create` or `reassigned` reason of
`spate_coordination_acquisitions_total`.
