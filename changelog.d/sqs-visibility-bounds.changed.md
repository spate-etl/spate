**The SQS feed's `visibility` refusal states its bound as other durations do** (`spate-sqs`)

A `visibility` outside 3s to 12h, or with a part below one second, now reads `feed.sqs.visibility: must be from 3s to 12h in whole seconds; found 2s 500ms`. In previous versions it read `feed.sqs.visibility: must be whole seconds from 3s to 12h; got 2.5s`. The same values are refused as before.
