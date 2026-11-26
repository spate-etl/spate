**Breaking:** **Startup errors keep the configuration or metrics error they report, and core's error types share one shape** (`spate-core`)

`StartError::Config` and `BuildError::Config` hold the `ConfigError`, and `StartError::Metrics` and `BuildError::Metrics` hold the `MetricsError`. In previous versions `StartError::Config` and both `Metrics` variants held its text as a `String`. `BuildError::DuplicateSeries` is removed: a metric series another live pipeline owns is `BuildError::Metrics(MetricsError::DuplicateSeries(_))`, the same variant `PipelineRuntime::run` returns it under.

Each of these variants prints the wrapped error's text with no prefix of its own. A configuration `run` refuses reads `invalid configuration: pipeline.io_threads: must be at least 1`, where it read `invalid runtime configuration: pipeline.io_threads: must be at least 1`. A refusal that already carried `invalid configuration:` loses the doubled prefix, and one that names its section, such as `deserializer.json: …`, now starts with the section. A metrics failure no longer starts with `metrics: `. `BuildError::Io` reads `io: …`, as `StartError::Io` does, where it read `io runtime: …`.

`FatalErrorReport` is removed. `ExitState::Failed` holds the `FatalError` that stopped the run, and `ExitReport::ok` returns it, with the same `component` and `reason` fields. It prints `fatal error in <component>: <reason>`, where it printed `pipeline failed in <component>: <reason>`.

`FatalError`, `ChunkSendError`, `ReservedKeyError`, `UrlRuleError` and `DurationBoundsError` are `#[non_exhaustive]`. Code outside `spate-core` builds a `FatalError` with the new `FatalError::new`, and matches a rejected chunk as `ChunkSendError(chunk, ..)`. `ChunkSendError` implements `std::error::Error`. The `Debug` of an `EncodedChunk`, and of anything holding one, shows the frame's length and not its bytes.

`RetrySectionError::Multiplier` and `RetrySectionError::Jitter` are renamed `MultiplierOutOfRange` and `JitterOutOfRange`. Their messages are unchanged.
