# Deployment

Everything after `cargo build`. Your pipeline compiles to a single binary
that reads one YAML file, serves its own probes and metrics, and drains
cleanly on SIGTERM — the deployment story is packaging that binary, watching
it, and sizing it.

## [Docker](docker.md)

Packaging a pipeline binary into a minimal, non-root image, using the
flagship image (`examples/docker/Dockerfile`) as the template: the
`ETL_CONFIG` mount pattern, the `/readyz`, `/healthz`, and `/metrics`
endpoints on the admin server, and aligning the SIGTERM grace period with
the drain timeout.

## [Monitoring](monitoring.md)

The metrics model: the `metrics` facade as the one instrumentation API, the
Prometheus exporter on the admin server, label conventions, the hot-path
counting discipline, and a small set of starting alerts. The full metric
taxonomy lives in [docs/METRICS.md](../../METRICS.md).

## [Tuning](tuning.md)

Sizing a pipeline: thread counts, batch thresholds, in-flight limits, the
backpressure budget and its sizing rule, queue capacities, checkpoint
timing, and core pinning — with the exact defaults and why each knob moves
what it moves.

## Related reading

- [Graceful shutdown](../03-guides/graceful-shutdown.md) — the drain
  choreography these pages configure.
- [Backpressure](../02-concepts/03-backpressure.md) — the mental model
  behind the budget knobs in [Tuning](tuning.md).
- [Configuration reference](../07-reference/configuration.md) — every YAML
  key, type, and default in one place.
- [docs/DESIGN.md](../../DESIGN.md) — the rationale behind the operational
  model (one process, one pipeline; scrape-based metrics; drain-on-SIGTERM).
