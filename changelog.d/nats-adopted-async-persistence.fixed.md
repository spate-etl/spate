**Adopted async state bucket warning** (`spate-coordination`)

The worker warns when a pre-created state bucket uses `persist_mode: async`.
Acknowledged writes can be lost despite `sync_interval: always`; operators
should provision production state buckets with default stream persistence.
Previously, adoption did not report this condition. The worker preserves the
existing stream settings and continues serving operations.
