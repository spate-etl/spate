**Coordination stores can declare a polled watch** (`spate-coordination`)

`CoordinationStore::watch_mode` returns `WatchMode::Push` by default. A store
whose watch lists its prefix at an interval returns `WatchMode::Polled`, and the
coordinator then reads what such a watch can miss: the records of a split a
worker was assigned but never saw, and, on the leader, the record of each
assigned split that shows no lease. A new leader lists every split and spec
record before it plans or publishes. The first worker to report the job
terminal writes a durable `verdict` key, which tells the others to list the
split records and report too. A polled interval of zero, or not below
`lease_duration`, is rejected at startup.
