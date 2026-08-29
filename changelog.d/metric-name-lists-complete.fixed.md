**`spate_sink_drain_overrun_total` is listed in the name taxonomy**
(`spate-core`) — the constant was declared, documented and incremented in
production, but missing from `metrics::names::COUNTERS`. Anything enumerating
the taxonomy through those lists, rather than through the constants, could not
see it.
