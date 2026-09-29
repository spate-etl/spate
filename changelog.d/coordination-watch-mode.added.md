**Coordination stores can declare a polled watch** (`spate-coordination`)

`CoordinationStore::watch_mode` returns `WatchMode::Push` by default. A store
whose watch lists its prefix at an interval returns `WatchMode::Polled`, and
the coordinator then watches only the assignment records, the plan record and a
verdict marker in the durable keyspace. It reads what that watch does not
carry: a worker reads the records of a split it was assigned but never saw, and
the leader re-reads each assigned split that shows no lease every interval and
reads any spec it lacks. Only the leader reconciles, over the split records. A
new leader lists every split and spec record before it plans or publishes. The
first worker to report the job terminal writes a durable `verdict` key, which
tells the others to list the split records and report too. A polled interval of
zero, or not below `lease_duration`, is rejected at startup.
