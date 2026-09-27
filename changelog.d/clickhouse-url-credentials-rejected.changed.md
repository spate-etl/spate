**Breaking:** **A ClickHouse URL with credentials is rejected at load** (`spate-clickhouse`)

A replica URL in `shards` or a `distributed_check.endpoint` URL that carries a
user or password, such as `http://svc:secret@ch-0:8123`, fails at load. In
previous versions the sink accepted such a URL but never sent those
credentials. Requests authenticated as the configured `user`, or as the
server's `default` user when `user` was unset. Move the credentials into `user`
and `password`.
