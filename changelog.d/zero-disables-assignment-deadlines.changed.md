**Breaking:** **`startup_timeout: 0s` disables the startup deadline**
(`spate-kafka`) — a zero there leaves the Kafka source waiting for its first
partition assignment for as long as the group takes, which is what
`assignment_timeout: 0s` already means for a member that loses one. It
previously failed the pipeline on the first poll, before any broker could
answer. The two deadlines are one mechanism with two windows: the wait runs
from `open` under `startup_timeout`, and from every later loss of the last
partitions under `assignment_timeout`.
