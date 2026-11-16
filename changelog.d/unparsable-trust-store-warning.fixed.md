**The Avro registry client no longer calls an unparsable system trust store empty** (`spate-avro`)

When the store yields certificates and none of them parse, the warning says how many failed. "Has no certificates" stays for a store that yielded none. It still falls back to the Mozilla root bundle.
