**The SQS feed's `settling_window` refusal states its bound as other durations do** (`spate-sqs`)

A `settling_window` under one minute on an SQS feed now reads ``feed.sqs: `settling_window` must be at least 1m; found 59s: `sqs` message counts take at least a minute to settle``. In previous versions it read ``… must be at least 1m on `sqs`, whose message counts take at least a minute to settle; got 59s``. The same values are refused as before.
