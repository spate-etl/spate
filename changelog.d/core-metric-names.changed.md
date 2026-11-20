**Breaking:** **Three engine metrics are renamed to follow the naming rules** (`spate-core`)

The engine now exports `spate_sink_drain_overruns_total`, `spate_deser_not_ready_replays_total` and `spate_source_active_lanes`. In previous versions they were `spate_sink_drain_overrun_total`, `spate_deser_not_ready_total` and `spate_source_lanes_active`. Their labels and values are unchanged. Dashboards, alerts and recording rules that select these series need the new names.

In `spate_core::metrics::names`, `SINK_DRAIN_OVERRUN_TOTAL`, `DESER_NOT_READY_TOTAL` and `SOURCE_LANES_ACTIVE` are now `SINK_DRAIN_OVERRUNS_TOTAL`, `DESER_NOT_READY_REPLAYS_TOTAL` and `SOURCE_ACTIVE_LANES`. `SourceMetrics::set_lanes_active` is now `set_active_lanes`.
