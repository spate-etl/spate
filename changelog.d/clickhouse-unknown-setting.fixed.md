**The ClickHouse sink refuses a setting the server does not accept at startup** (`spate-clickhouse`)

Building the sink sends the `settings` passthrough to every replica, and a setting a replica refuses fails the build with a new `SchemaError::Settings` that carries the replica's URL and the server's message. This covers an unknown name and a value the server cannot parse. An insert answered with `UNKNOWN_SETTING` mid-run is now fatal. In previous versions a misspelled key loaded, and the first batch retried until the drain deadline while the pipeline made no progress.
