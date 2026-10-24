# spate-sqs

Amazon SQS work-feed backend for the [Spate](https://github.com/spate-etl/spate)
framework. A source that reads work units, such as the S3 source, reads them
from one SQS standard queue that every worker of a pipeline shares.

Applications should depend on the **`spate`** facade crate with the **`sqs`**
feature rather than on this crate directly.

```yaml
feed:
  sqs:
    queue_url: "https://sqs.eu-west-2.amazonaws.com/111122223333/jobs"
    visibility: 60s
```

`main()` builds the feed from that section with `SqsFeed::connect` and passes
it to `Pipeline::feed`.

- A received unit stays hidden from other workers while its reader renews it
  or its worker is stopping, and returns to the queue at most one visibility
  period after both end.
- A unit is deleted only once every record in it is acknowledged.
- A stopping worker hands back the units it has not finished, each visible to
  another worker at once, and sends its last deletes before it exits.
- A failed unit is made visible at once, and the queue's redrive policy moves a
  unit that keeps failing to its dead-letter queue.
- A bounded job reads the queue's and the dead-letter queue's approximate
  message counts to decide when it has ended.

The [SQS feed page](https://spate.kainth.dev/docs/user-guide/connectors/feeds/sqs)
covers configuration, permissions, the 12-hour hold and cost per request.
