**Chunks reach each sink shard in the order they were sealed** (`spate-core`)

The pipeline sends each sink shard its chunks in the order it sealed them. In previous versions, a chunk sealed while an earlier chunk for the same shard waited for queue space could be sent first, when the shard's queue freed a slot before the waiting chunk was retried. The sink then wrote the newer rows before the older ones. No record was lost or duplicated, because each chunk carries its own acknowledgments.
