# Kafka brokers

[`../README.md`](../README.md) has the convention these lanes follow. This file
covers what is specific to the Kafka brokers.

Each lane is one broker at its newest release.

| Lane | Broker |
| --- | --- |
| `apache` | The newest Apache Kafka release. **Primary**: the lane the `containers` job runs, and the default. |

`SPATE_KAFKA_LANE` selects one:

```sh
SPATE_KAFKA_LANE=apache cargo xtask integration-test
```

The spate-kafka container suites, spate's end-to-end suites and
spate-test-support's smoke test start the selected broker through
`spate_test_support::KafkaBroker`. A client on the host reaches it on the port
mapped to 9092. A container in the broker's network namespace, such as a
notifying S3 store, reaches it on `localhost:9093`. What differs per broker,
such as its configuration, its start command and the log line it is ready on,
is that broker's arm in `spate_test_support::Broker`. A lane with no arm there
fails `every_kafka_lane_names_a_known_broker` without Docker.

The broker advertises a port that is only known once its container runs, so
the container waits for a start script that `KafkaBroker` writes once the port
is mapped.

## What a bump runs

`cargo xtask ci-changes` maps this directory to the `spate-kafka`, `spate` and
`spate-test-support` container suites.

## Apache Kafka

The image is `apache/kafka-native`, the GraalVM build of the broker. Its arm
runs one KRaft node as broker and controller, configured through the image's
`KAFKA_*` variables, and starts the image's own `/etc/kafka/docker/run`. It is
ready on the "Kafka Server started" log line, so a release that changes the
start script or that line fails the suites.

Apache Kafka has no LTS line. The lane follows the newest release, so a broker
change reaches CI first.

Dependabot proposes a release once Apache points `latest` at it. A tag above
`latest` counts as a pre-release, so the `-rcN` tags Apache pushes to the same
repository are never proposed.

The lane may pin a release before Apache points `latest` at it, when that
release carries a fix the suites need. While the pin sits above `latest`,
Dependabot counts the pin itself as a pre-release, so it proposes the next
release as soon as Apache publishes it. The `-rcN` tags still stay out, because
a tag with a suffix is never compared against a plain release tag.

The three-node cluster in spate's `e2e_leader_move` starts the `apache` lane's
image as a `GenericImage` with its own `KAFKA_*` variables, whichever lane is
selected.
