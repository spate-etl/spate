# Kafka brokers

[`../README.md`](../README.md) has the convention these lanes follow. This file
covers what is specific to the Kafka brokers.

Each lane is one broker at its newest release.

| Lane | Broker |
| --- | --- |
| `apache` | The newest Apache Kafka release. **Primary**: the lane the `containers` job runs, and the default. |
| `redpanda` | The newest Redpanda release, which the `kafka-brokers` job runs. |

`SPATE_KAFKA_LANE` selects one:

```sh
SPATE_KAFKA_LANE=redpanda cargo xtask integration-test
```

The spate-kafka container suites, spate's end-to-end suites and
spate-test-support's smoke test start the selected broker through
`spate_test_support::KafkaBroker`. A client on the host reaches it on the port
mapped to 9092. A container in the broker's network namespace, such as a
notifying S3 store, reaches it on `localhost:9093`. What differs per broker,
such as its configuration, its start command and the log line it is ready on,
is that broker's arm in `spate_test_support::Broker`. A setting a suite needs
from every broker is a `KafkaBroker` method, such as `transactional`. A lane with no arm there
fails `every_kafka_lane_names_a_known_broker` without Docker.

The broker advertises a port that is only known once its container runs, so
the container waits for a start script that `KafkaBroker` writes once the port
is mapped.

## What a bump runs

`cargo xtask ci-changes` maps this directory to the `spate-kafka`, `spate` and
`spate-test-support` container suites. A change that selects `spate-kafka` or
`spate` also runs a `kafka-brokers` leg for each lane beyond the primary one.

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

## Redpanda

The image is `redpandadata/redpanda`. Its arm runs one node with
`rpk redpanda start --mode dev-container`, which keeps topics on one replica and
starts a new group with no rebalance delay. Redpanda reads no `KAFKA_*`
variable, so `transactional` and `without_rebalance_delay` set nothing on it.

The node is ready on the "Successfully started Redpanda!" line, which Redpanda
logs to stderr. Dev-container mode raises a topic's `segment.ms` to ten minutes
and compacts every ten seconds, and the compaction suites roll a segment and
wait for the cleaner within the test. Once the node is ready, the arm runs
`rpk cluster config set` to lower `log_segment_ms_min` to 100 ms and
`log_compaction_interval_ms` to 500 ms. A bootstrap file would replace every
cluster setting dev-container mode applies, topic auto-creation among them.

Redpanda takes one offset ahead of each transaction's records on a partition,
so the transaction suites read positions from the broker once each ended
transaction's marker is in the log.

The `kafka-brokers` leg leaves two suites to the primary lane.
`BROKER_LANE_SUITES` in `xtask/src/ci/outputs.rs` lists them:

- `feed_notifications`: the primary S3 store, RustFS, does not deliver its
  notifications to Redpanda.
- `tls_system_ca`: it configures Apache Kafka's SSL listener through `KAFKA_*`
  variables.

`cargo xtask integration-test` leaves the same two out when
`SPATE_KAFKA_LANE` names a broker other than the primary one.

The image index carries `linux/amd64` and `linux/arm64`. CI runs the lane on
x86_64.

Redpanda also publishes each release under `-amd64`, `-arm64` and `-fips`
tags. A tag with a suffix is never compared against a plain release tag, so
Dependabot proposes plain releases only. A patch to an older feature line, such
as v26.1.20 after v26.2.6, sorts below the pin and is not proposed.

## The three-node cluster

`spate_test_support::KafkaCluster` starts three nodes of the selected broker on
a Docker network the caller names, with node ids 1 to 3. Spate's
`e2e_leader_move` runs on it. Each node is a `KafkaBroker` with a cluster arm
per broker:

- Apache Kafka runs three KRaft nodes, each a broker and a controller. Its
  internal topics go on all three nodes, and an `acks=all` write needs two in
  sync.
- Redpanda sets each node's id before it starts, because it numbers nodes from
  0 when none is set. Every node seeds all three.

A node advertises a host port chosen before it starts, and a restart keeps the
port, the node id and the data. A node cannot finish starting before the others
are up, so the cluster waits for no ready line, and the caller waits until its
client sees three brokers. A node runs no command once ready: a restart would
run it again before the node had rejoined.

A node has no DNS server outside Docker. It looks up each peer by container
name, before the peer starts and again while one is stopped. Docker answers a
name it does not know with a server failure, which Redpanda does not cache, so
the node finds the peer once it runs. Given a DNS server, Docker would pass the
lookup to the host's resolver, and some answer with a not-found that Redpanda
keeps for up to an hour.

Redpanda places leaders unevenly, and a restarted node leads nothing until its
balancer moves leaders back. `KafkaCluster::transfer_leadership` moves one
through `rpk`, and a failed move names rpk's exit code and output. Apache Kafka
has no transfer a test can ask for, so it returns an error there.

When a test panics while a cluster runs, the cluster writes each node's state
and the tail of its output to stderr as it drops, with `rpk cluster health` from
each running Redpanda node.
