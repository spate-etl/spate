**A release after this worker's own failure report counts as forced**
(`spate-coordination`)

When a worker releases a split after its own failure report, and the report did
not quarantine the split, the release now settles a revocation in progress as
`forced` and does not count `spate_coordination_releases_total`, whether or not
the worker had seen the report. A departure counts such a split the same way.
In previous versions, a release or `release_drained` after a failure report
whose reply was lost, once the worker had seen the report, counted a release
and settled the revocation as `drained`.
