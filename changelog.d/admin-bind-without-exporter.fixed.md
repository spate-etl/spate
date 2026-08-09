**`metrics.exporter: none` no longer takes a port it cannot use**
(`spate-core`) — the admin server was bound unconditionally, so a pipeline
configured for no metrics still occupied `0.0.0.0:9090`, and a second one on
the same host failed to start with an I/O error rather than a message about
the address. The bind follows `admin.listen`, and a failure to take an address
reports that address as `StartError::AdminBind`.

`/metrics` answers 404 where the pipeline has no exposition of its own to
render, rather than 200 with an empty body — which reads to a scraper as a
healthy target delivering nothing. That covers `exporter: none` and equally a
recorder another library installed first, where the pipeline records into that
recorder and cannot render it.
