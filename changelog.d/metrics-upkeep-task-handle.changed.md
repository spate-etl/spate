**Breaking:** **`MetricsHandle::spawn_upkeep` returns a `TaskHandle`** (`spate-core`)

`MetricsHandle::spawn_upkeep` returns a `spate_core::task::TaskHandle<()>`, and dropping that handle stops the upkeep task. In previous versions it returned a `tokio::task::JoinHandle<()>`, and the task kept running after the handle was dropped. Code that discards the returned handle stops the upkeep at once. Keep the handle for as long as histograms and process metrics should stay current. The pipeline runtime holds its own upkeep handle until the run ends.
