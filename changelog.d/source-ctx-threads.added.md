**A source learns the pipeline thread count at `open`** (`spate-core`)

`SourceCtx::threads` carries the number of pipeline threads the runtime resolved, the value `spate_pipeline_threads` reports, whether `pipeline.threads` sets it or the host derives it. A custom source can size its read parallelism from it. It is `None` when a source is opened by hand, as in a test, and `SourceCtx::with_threads` sets it there.
