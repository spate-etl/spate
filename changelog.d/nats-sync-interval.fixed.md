**The NATS store needs `sync_interval: always` in production** (`spate-coordination`)

The NATS store page lists `sync_interval: always` in the `jetstream` block of
every server as a production requirement. The documentation in previous versions
did not mention it. With the server default, a write is acknowledged before it
reaches disk, so an OS crash or power loss can lose coordination writes the
server acknowledged, and a lost write can let a fenced worker commit. Add the
setting to each server and restart it, because a configuration reload does not
apply it.
