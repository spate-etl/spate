---
description: "Connectors load TLS trust roots through spate_core::tls, with no fallback bundle; an https endpoint with no usable root fails at load."
---

# ADR-0105 — Connectors load TLS trust roots through one module in spate-core, with no fallback bundle

- **Status:** accepted
- **Date:** 2026-11-15
- **Supersedes:** —
- **Superseded by:** [ADR-0110](0110-kafka-takes-trust-roots-through-core-tls-section.md),
  for the whole module behind `tls-roots` and a Kafka-only build compiling
  none of it

## Context and problem statement

Three connectors fill a rustls root store: the ClickHouse sink
(`spate-clickhouse`), the Avro schema registry client (`spate-avro`) and the SQS
feed (`spate-sqs`). Each carries its own copy of one loader. It reads the system
trust store, falls back to a Mozilla root bundle when nothing usable loads, and
in ClickHouse and Avro adds the certificates in a `root_ca` file. The copies
had drifted: #1444 and #1445 were one bug, filed once per copy. The S3 source
(#1550) and a `root_ca` key for SQS (#1549) need the same loader.

Two things had to be decided together. Where can one loader live, when
[INV-6](../INVARIANTS.md#inv-6) keeps 0.x dependency types out of
`spate-core`'s public API and rustls is 0.x? And what does it do when the
system store yields no usable certificate?

## Considered options

Placement:

- A new crate, `spate-tls`, that each connector depends on
- A `spate_core::tls` module behind a feature, whose public signatures carry
  only `rustls-pki-types` 1.x values and spate types
- A family of macros that expand against each caller's rustls, as
  `tls_rejection!` does
- Keep a copy per connector

An empty system store:

- Fall back to the Mozilla root bundle, as the copies do
- Fail at load when a configured endpoint is `https://`, and keep the empty set
  otherwise
- Fail at load always

## Decision outcome

Chosen option: "a `spate_core::tls` module behind a feature" and "fail at load
when a configured endpoint is `https://`", because a certificate is a
`CertificateDer` from `rustls-pki-types` 1.x, which every rustls-based client
takes and INV-6 allows, and because a fallback bundle trusts roots the operator
did not choose and hides a missing CA bundle until a server presents a
certificate the bundle does not cover (CWE-455).

The module is `spate_core::tls`, behind the `tls-roots` feature of `spate-core`
and of the `spate` facade. A Kafka-only build compiles none of it.
`rustls-native-certs` 0.8 and `rustls-webpki` 0.103 stay private
dependencies. `tls_reason!`, the shared reason text for a TLS error, is a macro
beside `tls_rejection!` for the same reason as that one: it reads a
`rustls::Error`.

The loader keeps each system certificate rustls accepts as a trust anchor,
using the same `rustls-webpki` check rustls runs, and drops the rest. It then
adds every certificate in `root_ca`, and fails at load when that file cannot be
read, holds no certificate, or holds one that is not a trust anchor. A
connector with an `https://` endpoint fails at load when the set is empty. The
error names the count the store yielded and its read errors, and the fixes:
install a CA bundle, point `SSL_CERT_FILE` at one, or set `root_ca`. A
connector with no `https://` endpoint keeps the set, empty or not, so a
plaintext pipeline on an image with no CA bundle runs.

`root_ca` adds to the system roots. Replacing them was rejected because both AWS
clients share their trust store with the credential chain, which reaches AWS
endpoints whose certificates chain to roots in the public trust stores.

On Linux the reqwest-based clients take this loader's roots: the platform
verifier they use otherwise fails on an empty store for any endpoint scheme. On
Apple and Windows they keep the operating system's verifier and add `root_ca`.

A new crate was rejected because it adds a crate to publish and version for one
module whose public types are already stable. Macros were rejected because the
loader has branches that cargo-mutants cannot reach inside a `macro_rules!`
body. Failing always was rejected because it stops a pipeline that makes no
TLS connection.

### Consequences

- Good, because one loader replaces three copies, and S3 and SQS take
  `root_ca` from it.
- Good, because an `https://` pipeline on an image with no CA bundle fails at
  load with the fixes named, where the copies started it on a bundle the
  operator never chose.
- Bad, because the change is breaking for each connector that falls back
  today. Until it moves to this module, the ClickHouse sink, the Avro registry
  client and the SQS feed still fall back to the Mozilla bundle.
- Bad, because an `http://` pipeline with no CA bundle that follows an https
  redirect or uses an https proxy fails at the handshake, where the fallback
  let it through. The error carries the `tls_reason!` hint.
- Bad, because the check that decides a usable root runs in `spate-core`'s copy
  of `rustls-webpki`, which has to stay the version rustls uses.
- Neutral, because this replaces the expectation in
  [ADR-0088](0088-a-connector-builds-its-vendor-client-in-one-place.md) that the
  S3 source gains a trust-root fallback.

### Confirmation

INV-6, by the public signatures of `spate_core::tls`. `deny.toml` bans a second
version of `rustls-webpki`, so the loader's check and each client's verifier
stay one version. The behaviour is pinned by the unit tests in
`crates/spate-core/src/tls.rs`.

## More information

- Landed in the pull request that added `spate_core::tls`, part of #1542.
- [ADR-0088](0088-a-connector-builds-its-vendor-client-in-one-place.md) — how
  a connector builds its vendor client; it deferred this decision.
- [ADR-0081](0081-a-rejected-credential-or-tls-handshake-is-fatal-unless-throttled.md)
  — a rejected TLS handshake is fatal, and carries the `tls_reason!` text.
- [ADR-0011](0011-msrv-and-dependency-policy.md) and
  [INV-6](../INVARIANTS.md#inv-6) — where a dependency type may appear in a
  public signature.
- [Building a vendor client](../user-guide/06-extending/vendor-clients.mdx#trust-roots)
  — the trust-root rule for a connector author.
