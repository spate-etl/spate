**Cleartext password warning** (`spate-clickhouse`)

A non-empty `password` with an `http://` replica or `distributed_check.endpoint` now logs one warning when the sink is built, and so does a non-empty `password` under `settings` with an `http://` replica. Each warning names its key. The pipeline keeps running.
