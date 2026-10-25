**Breaking:** **`spate-coordination` and the coordination traits are removed** (`spate`, `spate-core`, `spate-coordination`, `spate-test`)

The `spate-coordination` crate is removed with its memory and NATS stores. A work feed replaces it: the S3 source reads its input only through a feed, which a `feed:` section names. The `spate` features `coordination` and `coordination-nats` and the `spate::coordination` path are removed, so a `Cargo.toml` that names either feature no longer resolves.

`spate_core::coordination` is removed with every item in it, among them `SplitPlanner`, `SplitCoordinator`, `SplitSource`, `CoordinationDriver`, `SplitId`, `SplitSpec`, `SplitPlan`, `LeaseEpoch` and `CoordinationError`. `spate-test` no longer exports `scripted_coordinator`, `ScriptedCoordinator` or `CoordinatorScript`. `Source::configure_coordination` and `Source::commit_final` are removed, so a source that overrides either stops compiling; the final commit of a stop is a call to `Source::commit`. `PipelineConfig::coordination` and `CoordinationSection` are removed, along with `CoordinationMetrics`, `AcquireReason`, `ReplanOutcome`, `RevocationOutcome`, `SplitLossReason`, `StoreOp` and the `L_OP` label constant.

A pipeline file that keeps a `coordination:` section, in any spelling, fails to load. The section and the two `Source` methods first shipped in v0.3.0. The 23 `spate_coordination_*` metric families are removed, so update any dashboard or alert that reads them; the `coordination` prefix stays reserved for custom metric namespaces.

A coordinated job started on v0.3.0 cannot resume on this release. It has to finish on v0.3.0, or run again through a feed, which lists the prefix again, so a sink without idempotence sees those records twice.
