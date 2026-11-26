**The `checkpoint.drain_timeout` stop reserve refusal states its range as other durations do** (`spate-core`)

With a feed that hands its units back, a `checkpoint.drain_timeout` of 5s or less now reads ``checkpoint.drain_timeout: must be more than 5s and at most 1day; found 5s: the `sqs` feed stops reading 5s before the drain deadline to hand its units back``. In previous versions it read ``checkpoint.drain_timeout must be more than 5s, the stop reserve of the `sqs` feed; found 5s``. The same values are refused as before.
