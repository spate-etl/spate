**Complete counter name list** (`spate-core`)

`metrics::names::COUNTERS` now includes `spate_sink_drain_overrun_total`.
Previously, the counter was recorded and documented but missing from this
list. Code that enumerates counter names through `COUNTERS` can now discover
it.
