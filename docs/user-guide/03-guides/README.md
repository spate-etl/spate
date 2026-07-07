# Guides

Task-oriented how-tos. Each guide gets you to one concrete outcome; none of
them re-explains the architecture — for that, start with
[Concepts](../02-concepts/README.md) or [docs/DESIGN.md](../../DESIGN.md).

| Guide | Outcome |
|---|---|
| [Assembling a pipeline](assembling-a-pipeline.md) | Build and run a pipeline with the `Pipeline` builder — the primary assembly path. Read this one first. |
| [Configuring pipelines](configuring-pipelines.md) | Write a pipeline YAML: typed framework sections, environment interpolation, opaque connector sections. |
| [Testing pipelines](testing-pipelines.md) | Test a whole assembly deterministically with `etl-test`'s in-memory source and capturing sink. |
| [Schema validation](schema-validation.md) | Fail fast when your row struct, `columns` list, and the live ClickHouse table disagree. |
| [Graceful shutdown](graceful-shutdown.md) | Drain cleanly on SIGTERM, size `drain_timeout` against Kubernetes, and interpret exit codes. |
| [Manual assembly](manual-assembly.md) | Desugar every builder step into the public primitives it composes — the escape hatch. |

Where to go next:

- Connector-specific configuration and guarantees:
  [Connectors](../04-connectors/README.md).
- Running in production: [Deployment](../05-deployment/README.md).
- Writing your own source, sink, or operator:
  [Extending](../06-extending/README.md).
- Every configuration key in one table:
  [Configuration reference](../07-reference/configuration.md).
