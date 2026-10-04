**Breaking:** **The NATS store rejects an existing bucket with a different
replica count** (`spate-coordination`)

The NATS coordination store fails startup with a fatal error when the job's
existing state or lease bucket has a replica count other than `replicas`. The
message names the bucket and both counts. In previous versions the store used
the existing bucket and ignored `replicas`, so changing `replicas` for a job
whose buckets existed had no effect. To change the replica count, run
`nats kv edit <bucket> --replicas <n>` on both buckets and set `replicas` to
the same value. To keep the current count, set `replicas` to the count the
error names.
