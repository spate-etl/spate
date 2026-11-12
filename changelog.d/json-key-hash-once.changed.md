**The JSON deserializer hashes a payload's key once** (`spate-json`)

A payload that decodes to several records under `ndjson` or `array` framing now computes its record metadata, including the hash of the source key, once and copies it into each record. Previously it hashed the key again for every record. The records are unchanged. Keyed payloads that carry many records take fewer instructions per record.
