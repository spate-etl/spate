# spate-sqs

The Amazon SQS feed (`spate-sqs`, feature `sqs` on the `spate` facade) carries
work units in one SQS standard queue that every worker of a pipeline reads. A
received unit stays hidden from the other workers while its reader renews it or
its worker is stopping, and is deleted once every record in it is acknowledged.
A failed unit returns to the queue at once, and the queue's redrive policy moves
a unit that keeps failing to its dead-letter queue.

## Enable

Turn on feature `sqs` on the [`spate`](https://crates.io/crates/spate) facade.
The facade README has the Cargo snippet.

The feed serves a source that reads work units, such as the S3 source behind
feature `s3`.

## Configuration

The feed reads the `sqs:` body of the pipeline's `feed:` section. `main()`
builds it with `SqsFeed::connect` and passes it to `Pipeline::feed`. Only
`queue_url` is required, and an unknown key is rejected.

```yaml
feed:
  sqs:
    queue_url: "https://sqs.eu-west-2.amazonaws.com/111122223333/jobs"  # required
    visibility: 60s      # hidden this long without a renewal
    # region: eu-west-2  # unset: from the queue URL's host, then the AWS chain
    # endpoint: "http://127.0.0.1:9324"  # unset: AWS_ENDPOINT_URL if set, else the region's endpoint
  units_held: 8          # a key of feed: itself, beside sqs:
  location: "s3://my-bucket/jobs/orders-backfill/"  # makes the job bounded
  settling_window: 1m    # at least 1m on sqs; 5m when unset
```

`visibility` shows its default.

- `visibility` takes whole seconds from `3s` to `12h`. The feed extends units
  every third of it, as Unit lifecycle describes.
- `queue_url` takes an `http` or `https` URL. A URL whose path ends in `.fifo`
  is refused, because the feed reads standard queues only.
- Unset, `region` comes from the queue URL's host when the AWS SDK builds that
  host for the region. Otherwise it comes from `AWS_REGION`, the shared config
  file or instance metadata, and connecting fails when none of them names one.
- `endpoint` replaces the region's endpoint, for a local emulator or a VPC
  endpoint.
- `attempt_cap` is refused on `sqs`, because the queue's `maxReceiveCount`
  counts deliveries. A `settling_window` below `1m` is refused too, because
  AWS documents SQS's message counts as taking at least a minute to settle.

`units_held`, `location` and `settling_window` belong to the `feed:` section and
are read by the source. Unset, `units_held` leaves the source's own default.

`SqsFeed::connect` reads the queue's attributes and its dead-letter queue's
once, and takes at most 30 seconds. It refuses a FIFO queue, a queue with no
redrive policy, a dead-letter queue it cannot read or that keeps messages no
longer than the queue, and a queue of the two that does not report its message
counts. Credentials come from the AWS SDK's default provider chain.

The
[Configuration section of the SQS feed page](https://spate.kainth.dev/docs/user-guide/connectors/feeds/sqs#configuration)
has the full key table.
[Startup checks](https://spate.kainth.dev/docs/user-guide/connectors/feeds/sqs#startup-checks)
lists the queue checks connect runs, and
[Security](https://spate.kainth.dev/docs/user-guide/connectors/feeds/sqs#security-credentials-permissions-and-tls)
lists the IAM actions a worker needs.

## Delivery

Delivery is at-least-once. A unit carries no progress, so a unit delivered again
is read from its start, and the records read before reach the sink a second
time.

- A worker that crashes or stalls stops renewing its units. They return to the
  queue at most one visibility period after their last extension, and another
  worker reads them.
- On a graceful stop, the feed receives nothing more. It sets each unit it
  received and had not handed out visible at once, and each unit the source
  hands back too, so another worker can read them.
- After the final commit, the feed sends its queued deletes and sets each unit
  it still holds visible, within the time left before the drain deadline. A
  delete not sent by then leaves its unit to return at its deadline, at most one
  visibility period after its last extension, and be read again.

[Stop](https://spate.kainth.dev/docs/user-guide/connectors/feeds/sqs#stop) on
the SQS feed page covers each case. The
[Work feed](https://spate.kainth.dev/docs/user-guide/concepts/work-feed) and
[Delivery guarantees](https://spate.kainth.dev/docs/user-guide/concepts/delivery-guarantees)
pages cover the framework's side.

## Unit lifecycle

- **Visibility.** Each receive hides its units from other workers for
  `visibility`. Every third of that period, the feed extends each unit handed
  out or renewed since the last extension back to the full period. A stopping
  worker extends every unit it holds. A unit
  received and not yet handed out is extended while the reader keeps calling the
  feed. SQS keeps a message hidden for at most 12 hours from its receive, and
  the feed clamps every extension to 30 seconds short of that.
- **Acknowledgement.** The source acknowledges a unit once it has read the unit
  to its end and the sink has acknowledged every record in it. The feed then
  deletes the message, in batches of up to ten.
- **Retry.** A unit its reader fails is set visible at once, so the queue hands
  it out again. A fail that comes after the unit's visibility has lapsed is not
  sent, since the message is back in the queue already. A unit's delivery count
  is the message's `ApproximateReceiveCount`.
- **Dead letter.** After `maxReceiveCount` receives, the queue's redrive policy
  moves the unit to the dead-letter queue. The feed never sends to that queue
  itself. Every receive counts, including one whose unit was handed back at a
  stop or held by a worker that crashed.

A batch of deletes or fails that fails transiently is sent again, backing off
from 200 ms up to 5 s, until the earliest deadline among its units.
[Visibility and renewal](https://spate.kainth.dev/docs/user-guide/connectors/feeds/sqs#visibility-and-renewal),
[Failing a unit](https://spate.kainth.dev/docs/user-guide/connectors/feeds/sqs#failing-a-unit)
and
[Redrive](https://spate.kainth.dev/docs/user-guide/connectors/feeds/sqs#redrive)
cover each step.

## Pairs with

- The S3 source reads its work units from a feed. If the pipeline has no `feed:`
  section, it fails when it opens.
- A lister run, `S3Source::list_once`, publishes a bounded job's units to the
  queue from the same section. Publishes go out in batches of up to ten units
  and 1 MiB.
- A bucket's storage notifications, sent to the queue, feed a continuous job. A
  notification carries no `spate.unit` message attribute, so its unit's id is
  the message's `MessageId`.
- A job is bounded when its `feed:` section names a `location`. Its workers read
  the queue's approximate counts of messages ready, delayed and in flight, plus
  the deletes and fails the feed has accepted and not yet sent. The job can end
  only once those read zero over the settling window. If the dead-letter queue
  then holds any message, the job fails with an error that includes
  `bounded feed job stalled:`. A job with no `location` keeps running.
- A dead-letter queue that holds an earlier job's messages fails a later job as
  stalled, and storage notifications on the queue keep the window from closing.
  Give each bounded job a queue and a dead-letter queue of its own.

[Ending a bounded job](https://spate.kainth.dev/docs/user-guide/connectors/feeds/sqs#ending-a-bounded-job)
covers the depth reads, and the
[S3 source page](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#how-a-bounded-job-ends)
states when a worker ends the job.

## Sharp edges

- **The startup checks run only at connect.** A redrive policy removed, or a
  dead-letter queue's retention shortened, while the pipeline runs goes
  unnoticed. See
  [Startup checks](https://spate.kainth.dev/docs/user-guide/connectors/feeds/sqs#startup-checks).
- **The end check reads the dead-letter queue only after the window.** SQS
  counts a message's retention from its first send. A unit moved to the
  dead-letter queue earlier in the job and deleted there at the end of that
  retention leaves nothing for the end check to find, and the job ends
  completed. While the unit is there, `spate_feed_dead_letter_units` counts it.
  See
  [Ending a bounded job](https://spate.kainth.dev/docs/user-guide/connectors/feeds/sqs#ending-a-bounded-job).
- **`checkpoint.drain_timeout` must be more than `5s`.** Startup refuses `5s` or
  less with this feed, because the source stops reading 5 seconds before
  the drain deadline to hand its units back. See
  [Stop](https://spate.kainth.dev/docs/user-guide/connectors/feeds/sqs#stop).
- **A unit is held for at most 12 hours less 30 seconds from its receive.** Its
  visibility then lapses, and SQS hands it to a reader again. A unit less than
  one renewal period from its lapse when the reader asks for it is dropped
  unread. See
  [The 12-hour hold](https://spate.kainth.dev/docs/user-guide/connectors/feeds/sqs#the-12-hour-hold).

## Metrics

The feed's own families, prefixed `spate_sqs_feed_`, are listed with their
labels in the
[Metrics section of the SQS feed page](https://spate.kainth.dev/docs/user-guide/connectors/feeds/sqs#metrics).
They are registered when the runtime starts the pipeline, so a feed handed to a
source directly counts nothing. The runtime also records the calls a source
makes on the feed under the framework's `spate_feed_*` families.

## Related

- [`spate`](https://crates.io/crates/spate) — the facade an application depends
  on, with the Cargo snippet.
- [SQS feed page](https://spate.kainth.dev/docs/user-guide/connectors/feeds/sqs)
  — every key, the IAM actions, the startup checks, redriving dead-lettered
  units, cost per request and the metric families.
- [API documentation on docs.rs](https://docs.rs/spate-sqs) — `SqsFeed`,
  `SqsFeed::connect` and `SqsError`.
- [`s3_sqs_backfill.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/s3_sqs_backfill.rs)
  — a bounded backfill whose work arrives through an SQS queue, with a job
  location. Needs an SQS queue, a dead-letter queue with a longer retention, and
  AWS credentials
  (`cargo run -p spate --features s3,json,sqs --example s3_sqs_backfill`).
- [`s3_sqs_notifications.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/s3_sqs_notifications.rs)
  — a continuous job on a bucket's storage notifications. Needs an S3 bucket, an
  SQS queue with a dead-letter queue, and AWS credentials.
- [Examples index](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/README.md)
  — every example, with the features and infrastructure each needs.
- [`spate-s3`](https://crates.io/crates/spate-s3) — the S3 source that reads its
  work units from this feed.
- [`spate-test`](https://crates.io/crates/spate-test) — `scripted_feed`, for a
  test that drives a feed-reading source with no queue.
