**Zero `max_records` is refused** (`spate-core`)

`PipelineRuntime::run` now returns `StartError::Config` when `RuntimeOptions::max_records` is `0`. In previous versions the run started, polled no records and exited `Completed` on stop, so a record waiting at the source was never written. No YAML key sets this value, and the default stays `512`, so only code that builds `RuntimeOptions` with a zero is affected.
