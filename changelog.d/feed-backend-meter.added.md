**Breaking:** **A feed backend receives a `Meter` of its own** (`spate-core`)

`Feed::attach_metrics` hands a feed backend a `FeedMetricsCtx`. Its `meter` is a `Meter` for the backend's own metric families, under `spate_<backend>_feed_*` with `component="feed"` and the backend's key as `component_type`. The runtime calls it before the source is handed the feed. Calls made before then, and a feed the runtime is not given, get no `Meter`. The context's `per_partition_detail` is the pipeline's `metrics.per_partition_detail`, which gates the backend's own per-partition families.

`feed` joins `ROLE_SEGMENTS`, so a `Meter` local name that starts with `feed_` panics, as one that starts with `source_` or `sink_` does. The rule holds in every namespace, including the `spate_custom_*` names `ChainCtx::meter` registers. v0.3.0 accepted such names, so a connector or pipeline author who registered one must rename it.
