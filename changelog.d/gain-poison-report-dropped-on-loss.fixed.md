**A refused poison report no longer outlives its split** (`spate-core`)

When a source rejects a split's carried progress on resume and the coordinator
refuses the poison report with a retryable error, the report is re-offered on
each poll. The queued report is now dropped when the split is lost or
quarantined. In previous versions it was still offered after the split was
regained at a new epoch, and the coordinator charged it to the new tenancy: it
consumed a delivery attempt and released the lease while the lane kept reading
the split.
