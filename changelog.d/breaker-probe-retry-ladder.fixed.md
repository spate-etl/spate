**A sink shard waiting for a circuit-breaker probe no longer consumes its retry
ladder** (`spate-core`). With every replica half-open and its probe budget spent,
the write loop fell back to the retry backoff — advancing the ladder while
publishing no backoff gauge and incrementing no `spate_sink_retries_total`, so
two batches contending for one probe ended up on different steps for reasons no
metric explained. The wait now selects on a breaker wake alongside any real probe
deadline. Reachable on the defaults, where `half_open_probes: 1` and
`inflight.max_per_shard: 2` put two batches in exactly that contention. ([#34])
