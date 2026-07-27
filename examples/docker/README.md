# Running Spate pipelines on Kubernetes

The `Dockerfile` here builds the flagship example
(`kafka_avro_to_clickhouse`) into a distroless, non-root image — the
template for any pipeline binary you build on the framework.

```sh
docker build -f examples/docker/Dockerfile -t spate-pipeline .
docker run -e KAFKA_BROKERS=broker:9092 -e CLICKHOUSE_URL=http://ch:8123 \
           -p 9090:9090 spate-pipeline
```

## Configuration

The pipeline reads one YAML file (`SPATE_CONFIG`, default
`/etc/spate/pipeline.yaml`). Every `${VAR:-default}` in it interpolates from
the environment at startup, so the Kubernetes pattern is:

- **ConfigMap** mounted over `/etc/spate/pipeline.yaml` for structure;
- **env / Secret-backed env** for credentials and endpoints
  (`KAFKA_BROKERS`, `CLICKHOUSE_PASSWORD`, ...).

There is no hot reload: roll the Deployment to reconfigure (annotate the
pod template with a config checksum so edits trigger a rollout).

## Graceful shutdown

On SIGTERM the pipeline stops consuming, flushes its chains, gives sink
batches `checkpoint.drain_timeout` (default 25s) to complete, commits
offsets, and exits. **Set `terminationGracePeriodSeconds` above the drain
timeout** (e.g. 30s for the default) — if Kubernetes kills the pod
mid-drain, nothing is lost (at-least-once: uncommitted offsets replay),
but the drain does its job when given the time.

## Probes

The admin server (`metrics.listen`, default `:9090`) serves:

- `GET /readyz` — 200 once the source has an assignment **and** the sink's
  replicas answer probes. Wire this to `readinessProbe`.
- `GET /healthz` — 200 while every pipeline thread's poll loop beats and
  the checkpoint watermark isn't stuck while data flows. Wire this to
  `livenessProbe`; a wedged pipeline restarts instead of idling.
- `GET /metrics` — Prometheus exposition (see `docs/METRICS.md` for the
  taxonomy and alerting starting points). Scrape via PodMonitor or
  annotations.

```yaml
readinessProbe: { httpGet: { path: /readyz, port: 9090 } }
livenessProbe:  { httpGet: { path: /healthz, port: 9090 }, periodSeconds: 10 }
```

## Resources and threads

- `pipeline.threads` defaults to `available_parallelism` minus the I/O
  reserve. `available_parallelism` honours cgroup CPU limits, so **set a
  CPU limit** (or `pipeline.threads` explicitly) — a limitless pod sees
  the node's cores.
- Kafka consumption parallelism is bounded by partition count; scale
  horizontally by replicas (one consumer-group member per pod) and give
  the topic enough partitions.
- `pipeline.pinning: compact` pins pipeline threads to cores. That only
  yields exclusive cores under the kubelet **static CPU manager** with
  Guaranteed QoS and integer CPU requests; otherwise it just sets affinity
  inside the shared cpuset. Leave it `off` unless you run that setup.
- Memory: the dominant knobs are `backpressure.max_inflight_bytes` (in
  the framework) plus librdkafka's prefetch, which is charged **per assigned
  partition** and sits outside the in-flight budget — at the default
  `queued.max.messages.kbytes` (65.5 MB) a 100-partition assignment can hold
  ~6.6 GB. Size `resources.limits` above the sum with headroom for batches held
  during retries, and cap prefetch explicitly if the assignment is wide.

## Backpressure and batch sizing

The in-flight byte budget (`backpressure.max_inflight_bytes`) must
comfortably exceed what the sink legitimately keeps in flight, or a
saturated pipeline duty-cycles against the pause controller (a ~24x collapse
— see the benchmarks section, `docs/benchmarks/framework-overhead.mdx`):

```text
max_inflight_bytes x low_ratio >= 2 x ( shards x inflight.max_per_shard x batch.max_bytes
                                      + shards x queue_capacity x chunk.target_bytes )
```

Raise the budget or cap `batch.max_bytes` accordingly; the full rule and a
worked example live in `docs/DESIGN.md` § Backpressure.

## Scaling and rebalances

One pod = one consumer-group member. Scaling a Deployment up or down
triggers a rebalance: revoked partitions drain and commit before handover
(the framework's drain choreography), so scale events cost redelivery of
only the uncommitted tail.
