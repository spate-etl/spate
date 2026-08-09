**Breaking:** **The admin server has its own `admin:` section** (`spate-core`)
— `metrics.listen` moves to `admin.listen`, and `MetricsSettings::listen` is
gone. One server carries `/metrics`, `/healthz` and `/readyz`, so its address
belongs beside neither the exporter that supplies one of them nor the probes
that need no exporter at all. `admin: { listen: none }` is new and runs no
server, which is what lets two pipelines share a host without one of them
naming a port it never wanted. A file still carrying `metrics.listen` is
rejected at load, naming the key.

Three further breaks for anyone below the YAML layer: `PipelineConfig` gains a
public `admin` field, so an exhaustive struct literal needs `..Default::default()`
or the new field; `AdminServer::bind` takes `Option<RenderFn>`, where `None`
serves the probes without `/metrics`; and `MetricsHandle::exports()` is new,
reporting whether a handle has an exposition to render.
