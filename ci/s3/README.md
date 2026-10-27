# S3 stores

[`../README.md`](../README.md) has the convention these lanes follow. This file
covers what is specific to the S3 stores.

Each lane is one store at its newest release.

| Lane | Store | Capabilities |
| --- | --- | --- |
| `rustfs` | The newest RustFS release. **Primary**: the lane the `containers` job runs, and the default. | `notifications` |
| `rgw` | The newest Ceph release, running Ceph RGW. | `notifications` |
| `garage` | The newest Garage release. | None |

`SPATE_S3_LANE` selects one:

```sh
SPATE_S3_LANE=rustfs cargo xtask integration-test
```

`integration-test` leaves out the suites the selected lane's `CAPABILITIES` does
not declare.

The spate-s3 container suites, spate-kafka's notification suite on a lane
declaring `notifications`, and spate-test-support's smoke test start the
selected store through
`spate_test_support::S3Store`. Every store accepts one fixed access key pair and
region, and refuses unsigned writes, so every client signs. What differs per
store, such as its credential settings, its S3 port and its readiness check, is
that store's arm in `spate_test_support::Store`. A lane with no arm there fails
`every_s3_lane_names_a_known_store` without Docker.

## Capabilities

A lane's `CAPABILITIES` file names one capability per line. An absent file
declares none.

- `notifications`: the Kafka feed reads this store's storage notifications.
  spate-kafka's notification suite runs on the lane, and the store's arm
  configures its Kafka target, at start or through the store's topic API, and
  a bucket's notification configuration.
  `S3Store::start_notifying` starts the store inside the broker container's
  network namespace, because the broker advertises `localhost:9093` to other
  containers.

## RustFS

The image is `rustfs/rustfs`. Its arm sets the key pair through
`RUSTFS_ACCESS_KEY` and `RUSTFS_SECRET_KEY`, reaches the S3 API on port 9000, and
waits on `GET /health/ready`. Its notification target comes from the
`RUSTFS_NOTIFY_*` variables, and a `PUT ?notification` points a bucket at it.

## Ceph RGW

The image is `quay.io/ceph/ceph`, which runs no cluster by itself. The arm
copies in a bootstrap, `test-support/src/s3/rgw.sh`, that starts one monitor and
one memstore OSD on loopback, creates the fixed key pair's user, then runs
`radosgw` on port 7480. It waits on `GET /`, which RGW answers without
credentials once it listens.

Once RGW is ready, `S3Store::start_notifying` creates a persistent topic named
for the work topic, with the push endpoint `kafka://localhost:9093`, and a
`PUT ?notification` points a bucket at the topic's ARN. RGW finds a new
persistent topic's queue on its next 30-second scan, so the first notification
of a topic can take up to 30 seconds to reach the broker. Later ones arrive in
well under a second.

The image index carries `linux/amd64` and `linux/arm64`, so the lane runs
locally on either. CI runs extra lanes on x86_64.

Ceph numbers a major's development builds `x.0.z` and its release candidates
`x.1.z`, and its first stable release is `x.2.0`. Dependabot reads neither as a
prerelease, so the lane's entry in `.github/dependabot.yml` ignores the next
major's `x.0` and `x.1` tags, one range per major, added before that major's
candidates appear.

## Garage

The image is `dxflrs/garage`, one static binary with no shell. The arm copies a
config file to `/etc/garage.toml` and runs
`/garage server --single-node --default-access-key`, which imports the fixed key
pair from `GARAGE_DEFAULT_ACCESS_KEY` and `GARAGE_DEFAULT_SECRET_KEY`. The
config sets `s3_region` to the suites' region, `replication_factor = 1` and
`consistency_mode = "consistent"`. One node cannot show what `consistency_mode`
changes. The arm reaches the S3 API on port 3900 and waits on `GET /health` on
the admin API, port 3903, because the S3 API refuses unsigned requests.

Garage implements no bucket notification configuration, so the lane declares no
capability. `S3Store::start_notifying` and `S3Store::put_bucket_notification`
refuse a Garage store.

The image index carries `linux/amd64` and `linux/arm64`, so the lane runs
locally on either.

## What a bump runs

`cargo xtask ci-changes` maps this directory to the `spate-kafka`, `spate-s3`
and `spate-test-support` container suites.

## Scope of the claim

The lanes say which stores CI proves the source against. The S3 source page
states which stores are supported.
