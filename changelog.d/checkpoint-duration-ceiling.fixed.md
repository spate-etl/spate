**Breaking:** **`checkpoint.interval` and `checkpoint.drain_timeout` have a one-day ceiling** (`spate-core`)

A pipeline config whose `checkpoint.interval` or `checkpoint.drain_timeout` is longer than `1day` fails pipeline load with an error that names the key. In previous versions such a value loaded, and one past about 292 billion years panicked the pipeline when the runtime added it to a clock reading. A config built in code is not checked.
