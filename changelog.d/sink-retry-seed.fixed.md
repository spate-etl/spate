**Sink replicas no longer retry a batch at the same instant** (`spate-core`)

Each sink pool now seeds its retry jitter from a value that differs between processes, combined with the shard and the batch's sequence number. Replicas whose writes fail together spread their retries across the `retry.jitter` window. In previous versions the seed was the sequence number alone, so batch _n_ of every replica, and of every shard in one pool, waited the same delay. The `retry` settings and the range of each delay are unchanged.

For connector authors, `spate_core::rng::fresh_seed` returns such a seed, `Backoff::with_fresh_seed` makes each copy of a `const` schedule seed itself on its first delay, and `spate_core::backoff::DEFAULT_JITTER` (0.2) is the jitter fraction `retry.jitter` defaults to.
