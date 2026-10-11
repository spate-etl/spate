**A live claim on a split's last attempt is no longer quarantined** (`spate-coordination`)

A worker that sees a split's record name a new owner before it sees that
owner's lease now reads the lease from the store before it quarantines the
split, and leaves the split runnable while the lease is live. In previous
versions it quarantined the split when the claim started the split's last
delivery attempt, the new owner dropped it, and a bounded job could end
`Stalled` on a healthy split. Under `max_attempts: 1` this could happen on any
first claim. A split whose owner died is still quarantined once its lease
expires.
