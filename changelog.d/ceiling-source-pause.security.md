**A partition held at the pending-batch ceiling is paused at the source** (`spate-core`, `spate-kafka`)

A partition that stays at `checkpoint.max_pending_batches` for `backpressure.min_pause` has its lanes paused at the source, and they resume when acknowledgments retire batches. `spate_checkpoint_ceiling_pauses_total` counts these pauses. In previous versions the pipeline stopped polling such a partition and left the source fetching for it. The Kafka client kept every fetched response in memory until the pipeline polled it, and counted only the records it would deliver against `queued.max.messages.kbytes`. Anyone who could write to the topic could therefore exhaust the pipeline's memory while a sink was stalled, by writing transactions and aborting them.

A pause purges the Kafka prefetch for that partition, and the records are fetched again on resume. `backpressure.min_pause` set to `0s` pauses a partition as soon as it reaches the ceiling, and a longer value leaves the source fetching for that long.

`SourceLane` gains `release_batch`, which the pipeline thread calls once a batch is finished. The default does nothing. The Kafka lane frees the batch's messages there, where it previously kept them until its next poll. A custom source's `pause` and `resume` are also called for a partition at the ceiling, and a call that fails is made again on a later commit tick.
