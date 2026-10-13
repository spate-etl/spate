**Chunks reach each sink shard in the order they were sealed** (`spate-core`)

The pipeline sends each sink shard's worker its chunks in the order it sealed them. In previous versions, a chunk sealed while an earlier chunk for the same shard waited for queue space could be sent first, when the shard's queue freed a slot before the waiting chunk was retried. Writes for one shard can still complete out of order while several batches are in flight, up to `inflight.max_per_shard`. No record was lost or duplicated, because each chunk carries its own acknowledgments.
