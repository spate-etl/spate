---
description: "Each connector builds its vendor client in one constructor; owned settings go after the passthrough and are refused in it under every spelling."
---

# ADR-0088 — A connector builds its vendor client in one place, and refuses the settings it owns in the passthrough

- **Status:** accepted
- **Date:** 2026-10-29
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Five connectors build a vendor client: the Kafka source, sink and feed
(`spate-kafka`, rdkafka), the S3 source (`spate-s3`, `object_store`), the SQS
feed (`spate-sqs`, the AWS SDK), the ClickHouse sink (`spate-clickhouse`,
`clickhouse` over hyper) and the Avro schema registry client (`spate-avro`,
reqwest). Two questions were open for each of them. Where is the client built,
and how do the connector's own choices combine with the operator's passthrough
([ADR-0009](0009-yaml-configuration-with-opaque-passthrough.md))?

The first question had cost a defect. `spate-kafka` built its clients from
three hand-written base builders, and the settings every client needs drifted
between them: the sink's producer and readiness probe kept librdkafka's
`allow.auto.create.topics=true`, so a sink pointed at a missing topic created
it (#1513). #1490 replaced the three builders with one factory,
`client::Base`, and banned rdkafka's constructors in `clippy.toml`.

On the second, the connectors that take a passthrough mostly agreed without a
record saying so. In Kafka and S3 the passthrough replaces any default and a key
the connector owns is refused at load. S3 applies its defaults and then the
passthrough; Kafka applies the passthrough and then each default the
passthrough left unset. Kafka
applies its owned keys after the passthrough, and refuses each under every
librdkafka alias, because rdkafka's `ClientConfig` applies keys in hash order.
The ClickHouse sink sets its owned insert settings before the user's
`settings:` map and refuses them by exact key only. ADR-0009 names a denylist
as the mechanism and says nothing about order.
[ADR-0018](0018-kafka-sink-forced-durability.md) chose force-and-refuse for
the Kafka sink's delivery keys.

## Considered options

- Build the client wherever it is needed, and keep the copies in step by
  review
- Build each client in one private constructor that `clippy.toml` makes the
  only caller of the library's constructors, layering connector defaults, then
  the passthrough, then owned settings applied last and refused in the
  passthrough under every spelling
- The same constructor, with the connector's defaults applied after the
  passthrough so that none can be replaced
- The same constructor, with the passthrough applied last so that the operator
  can replace an owned setting

## Decision outcome

Chosen option: "Build each client in one private constructor … owned settings
applied last and refused in the passthrough under every spelling", because a
setting every client needs is then written once, and a setting a guarantee
depends on cannot be replaced under any spelling.

A connector builds each vendor client that sends requests in one private
constructor. A test, an example, or a build that sends nothing (the S3 source
builds an `object_store` store to parse a bucket out of its URL) calls the
library's constructor under an `expect` that names why.

Settings come in three layers, each taking precedence over the one before:

1. **Defaults.** Values the connector picks because the library's are worse for
   Spate, such as a timeout, a CA location or a client identity. The
   passthrough may replace any of them. Where the library also reads the
   environment, as `AmazonS3Builder::from_env` does, the environment sits below
   the defaults.
2. **The passthrough.** The operator's map. A connector applies it over the
   defaults, or applies a default only where the passthrough leaves its key
   unset, as the Kafka factory does.
3. **Owned settings.** Values a delivery or correctness guarantee depends on,
   such as the Kafka sink's `acks=all`. They are applied after the passthrough
   wherever the library allows, and a passthrough key naming one is refused at
   load under every spelling the library accepts for it. The refusal is what
   holds the guarantee. A library may apply two spellings of one setting in an
   unspecified order, so applying an owned setting last wins only over the same
   spelling.

The rule binds a client passthrough (`rdkafka:`, `store:`) and a per-request
settings map (the ClickHouse sink's `settings:`) alike. A connector with
neither is not required to add one.

Applying defaults after the passthrough was rejected because it removes the
operator's only way to tune a timeout or name a CA, and because the Kafka
`client.id` default and the S3 source's timeouts are documented as replaceable.
Applying the passthrough last was rejected for the reason ADR-0018 gives: an
override of an owned setting does not fail, it produces a pipeline that appears
to work.

### Consequences

- Good, because a reviewer reads one function per client to see every setting
  it sends, and a setting each client needs is set once.
- Good, because an owned setting in the passthrough fails at load with the key
  named, under whatever spelling the operator wrote.
- Bad, because four connectors do not conform yet. The ClickHouse sink sets its
  owned settings first and has no connect timeout (#1548). The SQS feed (#1549),
  the S3 source (#1550) and the Avro registry client (#1551) have no
  construction ban, and the S3 source has no trust-root fallback.
- Bad, because refusing every spelling needs the library's alias table, which a
  library bump can change. Kafka pins each of its alias pairs against
  librdkafka in a test; the other libraries offer no such oracle.
- Bad, because every test that builds a raw client carries an `expect`, and
  each new library needs its own `clippy.toml` entries.

### Confirmation

Kafka's construction point is enforced by the `disallowed-methods` entries in
`clippy.toml`, and its derived denylist by
`every_key_a_role_sets_is_refused_in_the_passthrough` in
`crates/spate-kafka/src/client.rs`. The S3 source's bucket keys and the
ClickHouse sink's reserved settings are refused by load-time checks with tests
of their own. The other connectors' construction points rest on nothing yet;
#1548 to #1551 add their bans.

## More information

- Landed in the pull request that closed #1494.
- [ADR-0009](0009-yaml-configuration-with-opaque-passthrough.md) — the
  passthrough and denylist this orders.
- [ADR-0018](0018-kafka-sink-forced-durability.md) — the first owned settings,
  and the reason an owned setting is refused rather than defaulted.
- [ADR-0017](0017-kafka-sink-one-producer-per-sink.md) — one constructor may
  build more than one client.
- [ADR-0081](0081-a-rejected-credential-or-tls-handshake-is-fatal-unless-throttled.md)
  — how a client's errors are classified once it is built.
- [ADR-0011](0011-msrv-and-dependency-policy.md) and
  [INV-6](../INVARIANTS.md) — where a vendor type may appear in a public
  signature, unchanged by this record.
- #1537 — one `spate-core` check for refusing reserved passthrough keys.
- #1542 — where the shared TLS trust-store code lives, decided in its own
  record.
- [Contributing](https://github.com/spate-etl/spate/blob/main/CONTRIBUTING.md#building-the-vendor-client)
  — the conventions for building a vendor client that follow from this and
  earlier records.
