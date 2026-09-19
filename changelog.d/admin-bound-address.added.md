**Admin server address** (`spate-core`)

The pipeline now logs the admin server's bound address at `INFO` with the
message `admin server listening`. Previously, startup did not report this
address, so configuring `admin.listen` with port `0` left the automatically
assigned port out of the logs. You can use the logged address to reach
`/metrics`, `/healthz`, and `/readyz`.
