**Breaking:** **The ClickHouse sink's timeouts are flat keys, and its login key is `username`** (`spate-clickhouse`)

The `timeouts:` block is replaced by three keys at the level of `clickhouse:`, and `user` is renamed `username`, the key every connector uses for a login. A section that still sets `timeouts:` or `user:` fails at load as an unknown field:

```yaml
sink:
  clickhouse:
    username: default            # was user
    send_timeout: 30s            # was timeouts.send
    end_timeout: 180s            # was timeouts.end
    startup_timeout: 30s         # was timeouts.startup
```

The defaults are unchanged. `0s` turns a timeout off, where in previous versions `null` did. With `end_timeout: 0s` an insert is bounded only by `checkpoint.drain_timeout` at shutdown.

`TimeoutSection` is removed. `ClickHouseSinkConfig` has `send_timeout`, `end_timeout` and `startup_timeout` fields of type `Duration`, and its `user` field is renamed `username`.

The sink's refusals now name other keys in backticks, and an empty `shards` list reads `shards: must not be empty`, where it read `at least one shard is required`.
