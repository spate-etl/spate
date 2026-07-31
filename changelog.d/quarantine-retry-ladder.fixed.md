**A sink shard can no longer be left unwritable by a writer that panicked
mid-probe** (`spate-core`). A replica's half-open probe budget was returned only
by leaving `HalfOpen`, so a panic during a probe consumed the slot permanently
and pinned the replica half-open for the life of the process — every later batch
for that shard had nowhere to go. Picks now report whether they spent a slot, and
the write task holds a guard that returns it if no outcome is ever reported. A
related case let a slot released against an already-ended half-open run credit
the current one, admitting `half_open_probes + 1` concurrent writes to the
endpoint the breaker exists to shield; a release naming a run that has ended is
now discarded. Both are reachable on the default `inflight.max_per_shard: 2`.
([#34])
