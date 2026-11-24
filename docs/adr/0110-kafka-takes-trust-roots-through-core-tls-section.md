---
description: "The tls section type sits outside spate-core's tls-roots gate, so Kafka takes tls.root_ca and compiles the loader only under kafka-tls."
---

# ADR-0110 — The `tls:` section type sits outside the `tls-roots` gate, and Kafka compiles the loader only under `kafka-tls`

- **Status:** accepted
- **Date:** 2026-11-24
- **Supersedes:** [ADR-0105](0105-connectors-share-one-trust-root-loader-in-core.md)
  (in part: the whole `spate_core::tls` module behind `tls-roots`, and a
  Kafka-only build compiling none of it)
- **Superseded by:** —

## Context and problem statement

[ADR-0105](0105-connectors-share-one-trust-root-loader-in-core.md) put
`spate_core::tls` behind the `tls-roots` feature, and a Kafka-only build
compiled none of it. The Kafka source, sink and feed now take core's
`tls: { root_ca }` section, as the other TLS connectors do. Their `rdkafka`
passthrough refuses TLS settings in a build without `kafka-tls`, so a section
that names `root_ca` there has to parse and be refused with the same rebuild
instruction. Where do the section type and the loader sit, so that a Kafka
build without TLS can read the key and still compile no loader?

## Considered options

- The section type outside the gate; `spate-kafka`'s `tls` feature turns on
  `tls-roots`
- `spate-kafka` turns on `tls-roots` in every build
- A Kafka section type of its own with the same key

## Decision outcome

Chosen option: "the section type outside the gate", because a Kafka build
without `kafka-tls` then reads `tls.root_ca` and refuses it with the rebuild
instruction, and compiles none of the loader's dependencies:
`rustls-native-certs`, `rustls-webpki`, `rustls-pki-types`, `base64`,
`untrusted`, `zeroize`, and `openssl-probe` on Linux or `schannel` on
Windows. A section type of Kafka's own would be a second definition of one key
for its rustdoc and its parse rules to drift from.

`TlsSection` is in `spate_core::tls` in every build. The loader,
`SystemRoots`, `TrustRoots` and the rest, sits in the same module behind
`tls-roots`. `spate-kafka`'s `tls` feature, which the facade's `kafka-tls`
turns on, turns on `spate-core/tls-roots`.

### Consequences

- Good, because a user sets a private CA for Kafka through the same key as for
  every other connector, and a Kafka build without TLS still compiles no
  loader.
- Bad, because `spate_core::tls` holds different items with and without
  `tls-roots`, so a reader checks the feature to know whether the loader is
  there.
- Neutral, because a build that enables `kafka-tls` through the facade also
  gets the loader under `spate::tls`, as a `tls-roots` build does.

### Confirmation

`cargo xtask hack` checks each package with each feature alone and with none,
so `spate-kafka` without `tls` builds against a `spate-core` without
`tls-roots`. `crates/spate-core/tests/tls_section.rs` pins how the section
parses. The loader is pinned by the unit tests in
`crates/spate-core/src/tls/roots.rs`. In a build without the `tls` feature,
`spate-kafka`'s `client::tests::root_ca_needs_the_tls_feature` refuses
`tls.root_ca` in each section.

## More information

- Landed in the pull request that gave the Kafka connectors `tls.root_ca`,
  closing #2322, after #2386 moved `TlsSection` out of the gate.
- [ADR-0105](0105-connectors-share-one-trust-root-loader-in-core.md) — the
  shared loader, with no fallback bundle; its decision on an empty system store
  stands.
- [Securing Kafka](../user-guide/04-connectors/sources/kafka/README.mdx#security-tls-mtls-sasl)
  — how the Kafka connectors take `tls.root_ca`.
