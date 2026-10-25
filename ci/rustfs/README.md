# RustFS

[`../README.md`](../README.md) has the convention this lane follows. This file
covers what is specific to RustFS.

| Lane | Line |
| --- | --- |
| `stable` | The newest RustFS release. **Primary**, and the only lane. |

The image is `rustfs/rustfs`, an S3-compatible object store. The spate-s3
container suites start it through
`spate_test_support::Rustfs`, which sets a fixed access key pair through
`RUSTFS_ACCESS_KEY` and `RUSTFS_SECRET_KEY` and reaches the S3 API on port
9000. The server refuses unsigned requests, so every client signs.
spate-kafka's notification suite starts it with `Rustfs::start_notifying`
inside the broker container's network namespace, where RustFS publishes each
created object's notification to a Kafka topic.

## Why one lane

RustFS is a test fixture with no support window and backs no support claim, so
there is no `DOCS` file. The one lane follows the newest release.

## What a bump runs

`cargo xtask ci-changes` maps this directory to the `spate-kafka`, `spate-s3`
and `spate-test-support` container suites.
