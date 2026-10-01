**A throttled seed write no longer holds back the rest of the plan** (`spate-coordination`)

When the store rejects a split write with a retryable error, such as
throttling, the leader pauses briefly and continues the same plan run. Splits
it has already written become assignable straight away. In previous versions
the run stopped at the first such error, and the rest of the plan waited for
the next `replan_interval`, 60 seconds by default, while workers sat idle. A
run that seeds no split for a whole `replan_interval` while its writes fail
stops, and the next run retries.
