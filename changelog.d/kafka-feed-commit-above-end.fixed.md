**A bounded job on the Kafka feed waits while the group's commit is above a partition's end** (`spate-kafka`)

A work or retry partition whose group commit is above its end offset counts from its log start in the feed's depth, so a bounded job waits until a worker reads that partition. An unclean leader election leaves such a commit when it truncates a partition. In previous versions the depth read such a partition as drained, and a worker that could not join the group within its settling window ended the job `Completed`, exit 0, over units written after the truncation.

The feed puts no bound on the wait. A worker that held the partition through the truncation, with nothing written to it since, commits nothing on it, and the job waits until the group rebalances. After 20 depth reads with the commits unchanged, the worker logs an `info` line naming the partition. Starting or stopping any worker in the group ends the wait.
