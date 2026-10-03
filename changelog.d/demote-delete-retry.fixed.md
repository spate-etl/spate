**Leader cleanup retries** (`spate-coordination`)

A worker giving up leadership retries transient leader-key delete failures
within a bounded cleanup period. Previously, one transient failure could
leave the key blocking another election until its TTL expired. Persistent
failures still leave the key to expire. No configuration changes are needed.
