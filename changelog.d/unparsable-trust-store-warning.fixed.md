**The ClickHouse sink and the Avro registry client no longer call an unparsable system trust store empty** (`spate-clickhouse`, `spate-avro`)

When the store yields certificates and none of them parse, the warning says how many failed. "Has no certificates" stays for a store that yielded none. Both still fall back to the Mozilla root bundle.
