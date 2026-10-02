**A retryable store error during leader election leaves the coordinator running** (`spate-coordination`)

A newly elected leader whose plan-record read fails with a retryable store
error, such as a timeout, now gives the leadership back and keeps running, and
the next election tries again. The read follows a plan-record write that lost
its compare-and-swap, to another worker's write or to the leader's own earlier
write whose reply was lost. In previous versions the coordinator stopped on
that error: every later coordinator call failed, and other workers took its
splits over once their leases expired.
