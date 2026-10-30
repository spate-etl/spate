**A panicking sink probe turns readiness off** (`spate-core`)

When the sink connectivity probe panics, the pipeline marks the sinks not connected, so `/readyz` returns 503 and stays there. The panic is logged at `ERROR` with the task name `sink-probe`. In previous versions the probe stopped running after a panic and `/readyz` kept the answer of the last successful probe, so a pod could stay ready with no check behind it.
