# Kafka

[`../README.md`](../README.md) has the convention this lane follows. This file
covers what is specific to Kafka.

| Lane | Line |
| --- | --- |
| `stable` | The newest Apache Kafka release. **Primary**, and the only lane. |

The image is `apache/kafka-native`, the GraalVM build of the broker. The suites
boot it through the `testcontainers-modules` Kafka module, which runs the
image's own start script and waits for its "Kafka Server started" log line, so
a release that changes either fails the suites.

## Why one lane

Apache Kafka has no LTS line. The one lane follows the newest release, so a
broker change reaches CI first.

Dependabot proposes a release once Apache points `latest` at it. A tag above
`latest` counts as a pre-release, so the `-rcN` tags Apache pushes to the same
repository are never proposed.

The lane may pin a release before Apache points `latest` at it, when that
release carries a fix the suites need. While the pin sits above `latest`,
Dependabot counts the pin itself as a pre-release, so it proposes the next
release as soon as Apache publishes it. The `-rcN` tags still stay out, because
a tag with a suffix is never compared against a plain release tag.
