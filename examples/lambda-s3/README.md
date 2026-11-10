# Reading S3 notifications on AWS Lambda

This crate is an AWS Lambda function that reads one SQS batch of S3 storage
notifications per invocation. It reads the NDJSON records in each announced
object and writes them to ClickHouse. The
[S3 source page](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#running-on-aws-lambda)
explains the model and what it costs.

`src/lib.rs` holds the handler: the pipeline file, the batch item failures and
the stop before the deadline. `src/main.rs` holds the ClickHouse row and the
wiring to the Lambda runtime. The tests in `tests/batch.rs` run the handler over
a `file://` store with no AWS account.

## Build

The function runs on the `provided.al2023` OS-only runtime.
[Cargo Lambda](https://www.cargo-lambda.info/) builds it from the repository
root:

```sh
cargo lambda build --release --arm64 --package spate-example-lambda-s3 --output-format zip
```

The zip lands under `target/lambda/spate-example-lambda-s3/`.

## Configure

The function reads these environment variables:

| Variable | Value |
| --- | --- |
| `SPATE_S3_URL` | The bucket and prefix the notifications name, such as `s3://events/page-views/` |
| `CLICKHOUSE_URL` | The ClickHouse HTTP endpoint |
| `CLICKHOUSE_TABLE` | The table to write, `page_views` when unset |
| `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` | The ClickHouse credentials, `default` and empty when unset |

The table holds one row per record:

```sql
CREATE TABLE page_views
(
    user_id UInt64,
    path String,
    at DateTime64(3)
)
ENGINE = MergeTree
ORDER BY (user_id, at);
```

The function's role needs `s3:GetObject` on the prefix and `s3:ListBucket` on
the bucket, and `s3:GetObjectVersion` on a versioned bucket, as
[the S3 source's permissions](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#security-credentials-and-region)
explain. Without `s3:ListBucket`, AWS S3 answers `403` for an object deleted
before its read, and that fails the run and every message in the batch. The role
also needs the permissions the event source mapping uses on the queue.

## Connect the queue

- Send the bucket's `s3:ObjectCreated:*` notifications straight to a standard
  SQS queue. The source does not read an EventBridge or SNS envelope.
- Give the queue a redrive policy with a dead-letter queue and `maxReceiveCount`
  of at least 5.
- Set the queue's visibility timeout to at least six times the function timeout,
  plus the batching window.
- Create the event source mapping with `FunctionResponseTypes` set to
  `ReportBatchItemFailures`. Without it, Lambda deletes the whole batch when the
  function returns, including the messages the function reported as failed.
- Set `BatchSize` above 10 and `MaximumBatchingWindowInSeconds` to 1 or more, so
  that one invocation carries many small objects.
- Set the function timeout above `checkpoint.drain_timeout` plus 3 seconds. The
  function stops the run that long before its deadline, and an invocation with
  less time left fails without reading.
