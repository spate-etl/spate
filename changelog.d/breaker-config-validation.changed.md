**Breaking:** **`breaker.open_for` is validated at load** (`spate-core`,
`spate-kafka`, `spate-clickhouse`). `breaker.open_for: 0s`, and any value above a
year, now fail startup. Both previously loaded and ran degenerately — a zero
quarantine re-probed on the very next check — because the connectors validated
only `failure_threshold` and `half_open_probes`. A pipeline relying on either has
to set a real duration.

`half_open_probes: 0` is unaffected in practice: both connectors already rejected
it, and what changes is the framework's own normalisation of it. The rules now
live on `BreakerConfig::validate` beside `RetryConfig::validate`, with
`BreakerConfig::MAX_OPEN_FOR` as the bound and `BreakerConfigError` naming the
failure — `open_for` is stamped into a deadline, and `Instant + Duration` panics
rather than saturating, so it needed a load-time limit it had nowhere. ([#35])
