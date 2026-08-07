# ADR-0020 — Oversized records are caught at encode time, not at write time

- **Status:** accepted
- **Date:** 2026-07-13 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A single record can be too large for the broker to accept. That is a
**record-level** problem — one bad record among many good ones — and
[ADR-0010](0010-skip-or-fail-record-error-policies.md) says record-level problems
get a Skip or Fail policy.

But the write path has no per-record policy available to it. A sealed batch is
all-or-nothing: the writer either sends it or does not, and by the time the
broker rejects one oversized message the batch has already been assembled and
partly produced. Applying a Skip policy there would mean unpicking a batch
mid-flight.

## Considered options

- Let the broker reject it, and treat the resulting error as record-level
- Guard the size at encode time, where the record still exists individually, and
  mirror the same limit as the client-side `message.max.bytes`
- Let the broker reject it, and fail the pipeline

## Decision outcome

Chosen option: "Guard the size at encode time, and mirror the limit
client-side", because encoding is the last point at which a record is still an
individual thing that a policy can be applied to. The encoder checks
`max_message_bytes` per record and applies the configured Skip or Fail, exactly
like any other record-level error.

The same value is then applied client-side as librdkafka's `message.max.bytes`.
That gives a useful property: because every record was already checked against
that limit at encode time, **a writer-side size rejection can only mean the
broker's limit is lower than ours** — a misconfiguration, not a data problem. So
it is fatal rather than retried, and the error says so.

Letting the broker reject was rejected because it converts a record-level
problem into a batch-level one, and the batch has no policy to apply.

### Consequences

- Good, because an oversized record follows the same Skip or Fail semantics as a
  malformed one, with the same metrics.
- Good, because a size error from the writer is unambiguous — it means the
  broker limit and the configured limit disagree — so it fails fast instead of
  retrying forever.
- Bad, because the limit is configured in two places from one value, and an
  operator reading the librdkafka properties will see `message.max.bytes` set by
  the framework rather than by them.
- Bad, because the check runs per record on the hot path. It is a length
  comparison, but it is not free.

### Confirmation

INV-7 — the policy is Skip or Fail and both surface through metrics. The
client-side mirroring is what makes the writer-side classification sound.

## More information

- Landed in `6af6861` (#24).
- [ADR-0010](0010-skip-or-fail-record-error-policies.md) — the policy this
  applies.
- [ADR-0021](0021-kafka-sink-retry-duplicates-stance.md) — how the writer
  classifies the errors that are not this one.
