**`Source::stalled` tells a source which partitions are stalled behind a failed batch** (`spate-core`)

The runtime calls `Source::stalled` on each commit tick while a partition's watermark is stalled behind a failed batch, such as one the sink abandoned, and names every stalled partition. A source can revoke the partition's lanes to end the stall and give its input back for another read. A source that does nothing keeps the existing behaviour: `checkpoint.stalled_fail_after` fails the run, and the input replays after a restart. The default implementation does nothing, so existing sources need no change.

So that a source always has a chance to revoke, a stall now fails the pipeline no earlier than the second commit tick that finds it. This changes the timing only when `checkpoint.stalled_fail_after` is shorter than `checkpoint.interval`: such a stall could previously fail the pipeline on the first tick that found it, and now fails on the next tick.
