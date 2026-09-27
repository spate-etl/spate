---
description: "Every connector fails on a credential or TLS handshake that is identifiably rejected, and retries a failure to obtain credentials or reach the server."
---

# ADR-0047 — A rejected credential or TLS handshake is fatal in every connector

- **Status:** accepted
- **Date:** 2026-09-26
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Each connector classifies its own authentication and TLS failures, and they
disagree. The ClickHouse sink fails on a certificate that does not verify and
on a rejected user, and retries a TLS alert. The S3 source fails on a rejected
object read and retries a rejected listing. The Kafka sink retries every
authentication and TLS failure until the process is stopped.
[ADR-0021](0021-kafka-sink-retry-duplicates-stance.md) and
[ADR-0035](0035-s3-poison-policy.md) each carve a security case out as fatal
for one connector. Nothing states the rule for a connector that does not exist
yet, and the securing-connections page has to describe each connector
separately.

A security failure is either a rejection, where the server or the client's
own TLS stack refused the credential, the certificate or the handshake, or an
outage, where the client could not obtain credentials or reach the server. A
rejection does not resolve without a configuration change. An outage often
does.

## Considered options

- Leave the classification to each connector
- Treat every authentication or TLS error as fatal
- Fatal for an identifiable rejection, including a TLS handshake either side
  refuses; retryable for an outage or an ambiguous signal
- Fatal for a certificate that fails verification only
- Check credentials once at startup and retry every failure after that

## Decision outcome

Chosen option: "Fatal for an identifiable rejection", because a rejection is
provably permanent, the class of failure
[ADR-0021](0021-kafka-sink-retry-duplicates-stance.md) carves out of its retry
stance. Retrying it holds the pipeline in a state where nothing moves and no
error names the cause.

A rejection is identifiable when the client meets one of:

- a certificate its own TLS stack fails to verify;
- a fatal TLS alert from the server that names the certificate, the
  credential or the negotiation, such as `bad_certificate`, `unknown_ca`,
  `certificate_required` or `handshake_failure`;
- no protocol version or cipher suite in common with the server;
- an authentication or authorization error code from the server;
- an HTTP 401 or 403 from the service the connector talks to.

Everything else stays retryable: a failure to fetch or refresh credentials, a
connection reset, closed or timed out during the handshake, a malformed
TLS message, and an alert such as `internal_error` that names none of those.

Leaving the classification to each connector was rejected because the
connectors already disagree, and a new connector has nothing to follow.
Treating every authentication or TLS error as fatal was rejected because a
credential provider that is briefly unreachable, or a load balancer that
drops connections while it restarts, would restart the fleet. Limiting the
rule to certificate verification was rejected because a server that refuses
the client's certificate says so with an alert, and that refusal would retry.
A startup check alone was rejected because it lets a credential revoked
mid-run hold the pipeline indefinitely.

### Consequences

- Good, because one rule describes every connector, including a custom one.
- Good, because a restart re-reads the configuration and its secrets, which
  is how a corrected credential takes effect.
- Bad, because a server that switches credentials before its clients restarts
  them until the new secret arrives.
- Bad, because a refusal the server signals by closing the connection still
  retries. Some servers refuse a client certificate that way.
- Bad, because a client library that reports every TLS failure under one code
  hides the alert behind it. The connector then has to read the error text,
  or leave the case retryable.
- Bad, because some client libraries drop the rejection before the connector
  sees it, with no code or text to read. Those cases stay retryable until the
  client path changes. The schema registry client drops a TLS 1.3 server's
  refusal of the client certificate, and the NATS client keeps only an error
  kind when a reconnect fails.
- Bad, because a fatal sink write fails the pipeline only through
  `checkpoint.stalled_fail_after`. Until that failure carries the write's
  reason, the rule stops the pipeline without naming the cause.

### Confirmation

Nothing yet across connectors. Each connector's classification tests pin its
own cases, and nothing checks a new connector against the rule.

## More information

- Landed in #719. The implementation is tracked by #718.
- [ADR-0021](0021-kafka-sink-retry-duplicates-stance.md) — the retry stance
  whose permanent set this extends to authentication and TLS failures.
- [ADR-0035](0035-s3-poison-policy.md) — the S3 source's scope rule, which
  this generalizes.
- [ADR-0046](0046-fatal-deserializer-error-outside-the-record-policy.md) — how
  a deserializer reports a fatal failure.
- [Securing connections](../user-guide/03-guides/securing-connections.mdx) —
  what each class of failure does at runtime.
