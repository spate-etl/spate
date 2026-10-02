**A release after a write whose reply was lost hands the split back** (`spate-coordination`)

When a release's compare-and-swap loses, the release reads the split record
back. If the record shows this worker's own earlier commit, the release clears
the owner on top of it and keeps the committed watermark. If it shows this
worker's own failure report that did not quarantine the split, the release
deletes the lease. A split's lease is deleted the same way after a lease renewal
whose reply was lost, on release, completion and failure; if the lease read
answers from before that renewal, the lease stays until it expires. In previous
versions such a release was counted as fenced. After a commit, the owner and the
lease stayed, so another worker waited out the lease and took the split over as
expired, which used up a delivery attempt; after a failure report, the lease
stayed until it expired. A release after a failure report that quarantined the
split is still counted as fenced and leaves the lease until it expires. A forced
revocation that ends this way counts
`spate_coordination_split_losses_total{reason="revoked"}` where it counted
`reason="fenced"`. A read-back that fails or returns an older record still
leaves the owner set and deletes the lease; a read-back that finds no record is
still treated as fenced.
