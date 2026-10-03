**Failure report after a completed split** (`spate-coordination`)

A failure report for a split whose completing commit this worker already landed
hands the lease back and returns `Fenced` without naming a peer. No `Lost`
event is queued. A completing commit can apply while its reply is lost, so the
worker still holds the previous revision when the report arrives. The report
returned `Fenced` for a peer, queued `Lost` and left the lease key in place
until its TTL expired.
