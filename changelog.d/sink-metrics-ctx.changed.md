**Breaking:** **A sink writer receives its `Meter` through a `SinkMetricsCtx`** (`spate-core`)

`ShardWriter::attach_metrics` takes a `SinkMetricsCtx`, whose `meter` is the `Meter` for the writer's own `spate_<component_type>_sink_*` families. The builder calls it once before the writer is shared, as before. In previous versions it received the `Option<Meter>` directly.

An override becomes `fn attach_metrics(&mut self, cx: SinkMetricsCtx)` and reads `cx.meter`. A direct call wraps its argument in `SinkMetricsCtx::new`. A writer that takes the default needs no change.
