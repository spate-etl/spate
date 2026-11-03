**Breaking:** **The Avro deserializer refuses a `negative_cache_ttl` under 100ms** (`spate-avro`)

Building an Avro deserializer with a `negative_cache_ttl` below `100ms`, such as `0s`, fails with an error that names the key, in every mode, `raw` and `single_object` included. In previous versions a `0s` TTL loaded, and a record whose schema id the registry answered with `404` never reached the deserializer's `ErrorPolicy`. Its batch stayed held, and each replay asked the registry again. Set `negative_cache_ttl` to `100ms` or more, or leave the `30s` default.
