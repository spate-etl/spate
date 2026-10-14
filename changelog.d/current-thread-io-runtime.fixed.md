**Breaking:** **A current-thread I/O runtime fails startup** (`spate-core`)

`PipelineRuntime::run` returns `StartError::Config` when the runtime passed to `PipelineRuntime::with_io_runtime` is a current-thread runtime. In previous versions, `run` accepted it. Tasks on that runtime, such as sink workers, the admin server and the upkeep task, ran only during brief startup and drain steps, and not while records flowed. Build the I/O runtime with `tokio::runtime::Builder::new_multi_thread`. Pipelines built with `Pipeline::from_config` are not affected.
