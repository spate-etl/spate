---
description: "Every connector fails on an identifiably rejected credential or TLS handshake, and retries a 401 or 403 whose error code names throttling."
---

# ADR-0081 — A rejected credential or TLS handshake is fatal, and a throttled 401 or 403 is retried

- **Status:** accepted
- **Date:** 2026-10-26
- **Supersedes:**
  [ADR-0047](0047-a-rejected-credential-or-tls-handshake-is-fatal.md)
- **Superseded by:** —

## Context and problem statement

[ADR-0047](0047-a-rejected-credential-or-tls-handshake-is-fatal.md) makes an
HTTP 401 or 403 from the service fatal in every connector. SQS documents its
throttling and in-flight message limit codes with status 400 in its API
reference and with 403 in its service model and common errors page. Under
ADR-0047 a throttled SQS call answered with 403 stops the pipeline, though
the condition clears without an operator. The rest of ADR-0047 holds: a
rejection does not resolve without a configuration change, and an outage
often does.

## Considered options

- Keep every 401 and 403 fatal
- Retry a 401 or 403 whose error code names throttling or a limit that
  clears by itself
- Decide on the status alone, with a 403 retryable

## Decision outcome

Chosen option: "Retry a 401 or 403 whose error code names throttling or a
limit that clears by itself", because the code is the service's own statement
of the cause, and a throttle is not a rejection.

A rejection is identifiable when the client meets one of:

- a certificate its own TLS stack fails to verify;
- a fatal TLS alert from the server that names the certificate, the
  credential or the negotiation, such as `bad_certificate`, `unknown_ca`,
  `certificate_required` or `handshake_failure`;
- no protocol version, cipher suite or other handshake parameter in common
  with the server;
- an authentication or authorization error code from the server;
- an HTTP 401 or 403 from the service the connector talks to, unless the
  service's error code names throttling or a limit that clears by itself.

Everything else stays retryable: a failure to fetch or refresh credentials, a
connection reset, closed or timed out during the handshake, a malformed
TLS message, and an alert such as `internal_error` that names none of those.

Keeping every 401 and 403 fatal was rejected because it stops a pipeline on a
condition the service clears itself, and the operator has nothing to change.
Deciding on the status alone was rejected because a 403 is how a service
refuses a credential or a permission, and that refusal would retry with no
error naming the cause. ADR-0047's reasons for rejecting its other options
stand.

### Consequences

- Good, because one rule describes every connector, including a custom one.
- Good, because a service that reports throttling with a 403 slows the
  pipeline and does not stop it.
- Bad, because a connector has to read the error code of a 401 or 403 and
  keep a list of throttling codes, and a code missing from that list stops
  the pipeline.
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

Each connector's classification tests pin its own cases.
`throttling_and_the_in_flight_limit_are_retryable_at_400_and_403` in
`crates/spate-sqs/src/error.rs` pins the throttling exception for SQS. Nothing
checks a new connector against the rule.

## More information

- Landed in the pull request that closed #1188.
- [ADR-0047](0047-a-rejected-credential-or-tls-handshake-is-fatal.md) — the
  rule this restates with the throttling exception.
- [ADR-0021](0021-kafka-sink-retry-duplicates-stance.md) — the retry stance
  whose permanent set this extends to authentication and TLS failures.
- [ADR-0035](0035-s3-poison-policy.md) — the S3 source's scope rule, which
  this generalizes.
- [ADR-0046](0046-fatal-deserializer-error-outside-the-record-policy.md) — how
  a deserializer reports a fatal failure.
- [Securing connections](../user-guide/03-guides/securing-connections.mdx) —
  what each class of failure does at runtime.
