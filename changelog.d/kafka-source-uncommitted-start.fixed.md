**Breaking:** **A Kafka partition with no committed offset keeps its first position under `latest`** (`spate-kafka`)

Under `auto.offset.reset: latest`, the default, the source commits a partition's end offset before it first reads the partition, and every later start resumes from a committed offset for as long as the group holds it. In previous versions the partition restarted at its end and the next commit moved past records no sink wrote: on a restart of a quiet partition, a restart with the sink down, a process joining the group, and a member the group removed.

Records already skipped are not recovered. Moving the group's committed offset back is the only route to them. On the first start after upgrading, a partition with no commit starts at its end once more.

A pipeline can stop where it ran before. A record the chain fails on, or a corrupt message, among a partition's first records stops the source again after a restart, until the committed offset is moved past it; in previous versions the partition started past it under `latest`. A committed-offset read or a start commit that keeps failing holds the assignment until `startup_timeout` or `assignment_timeout` ends the run, and a permanent refusal such as a failed group authorization stops the run at once.

A partition added to the topic still starts at its end under `latest`, and a group left empty past its offset retention starts at the end again. A partition with a commit, and `earliest` and `error`, start as before.
