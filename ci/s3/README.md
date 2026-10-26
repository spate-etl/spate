# S3 stores

[`../README.md`](../README.md) has the convention these lanes follow. This file
covers what is specific to the S3 stores.

Each lane is one store at its newest release.

| Lane | Store | Capabilities |
| --- | --- | --- |
| `rustfs` | The newest RustFS release. **Primary**: the lane the `containers` job runs, and the default. | `notifications` |

`SPATE_S3_LANE` selects one:

```sh
SPATE_S3_LANE=rustfs cargo xtask integration-test
```

The spate-s3 container suites, spate-kafka's notification suite and
spate-test-support's smoke test start the selected store through
`spate_test_support::S3Store`. Every store accepts one fixed access key pair and
region, and refuses unsigned requests, so every client signs. What differs per
store, such as its credential settings, its S3 port and its readiness check, is
that store's arm in `spate_test_support::Store`. A lane with no arm there fails
`every_s3_lane_names_a_known_store` without Docker.

## Capabilities

A lane's `CAPABILITIES` file names one capability per line. An absent file
declares none.

- `notifications`: the Kafka feed reads this store's storage notifications.
  spate-kafka's notification suite runs on the lane, and the store's arm
  configures its Kafka target and a bucket's notification configuration.
  `S3Store::start_notifying` starts the store inside the broker container's
  network namespace, because the broker advertises `localhost:9093` to other
  containers.

## RustFS

The image is `rustfs/rustfs`. Its arm sets the key pair through
`RUSTFS_ACCESS_KEY` and `RUSTFS_SECRET_KEY`, reaches the S3 API on port 9000, and
waits on `GET /health/ready`. Its notification target comes from the
`RUSTFS_NOTIFY_*` variables, and a `PUT ?notification` points a bucket at it.

## What a bump runs

`cargo xtask ci-changes` maps this directory to the `spate-kafka`, `spate-s3`
and `spate-test-support` container suites.

## Scope of the claim

The lanes say which stores CI proves the source against. The S3 source page
states which stores are supported.
