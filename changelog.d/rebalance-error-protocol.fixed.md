**Rebalance errors no longer wedge the consumer** (`spate-kafka`) — a rebalance
event that is neither an assignment nor a revocation is answered with
`unassign`, as the client library's callback contract requires, instead of only
being recorded. A member that received one previously stayed mid-rebalance for
the life of the process — no rejoin, no fresh assignment, commits fenced —
until a restart. The error is now classified like every other consumer error,
so an authorization failure fails the pipeline fast while transient rebalance
codes retry, and the affected lanes are drained through the ordinary
revocation choreography first; their uncommitted work replays. A warning is
also logged when rebalance events queue faster than they complete.
