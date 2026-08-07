# ADR-0021 — Retry unless provably permanent, accepting known duplicates

- **Status:** accepted
- **Date:** 2026-07-13 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A delivery report can fail for many reasons, and the writer has to decide for
each whether to retry the batch. Two things make this harder than it looks.

A retry re-produces the **whole** sealed batch, including any prefix that was
already delivered — the framing carries no per-message progress, and the
deduplication token that makes a ClickHouse retry idempotent has no Kafka
equivalent. So every retry is a known duplicate window.

And some failures are not transient at all. An idempotent producer that has been
fenced, or lost its sequence state, will fail identically forever; retrying it
spins until the stalled-watermark timeout fires, turning a clear error into a
two-minute silence.

## Considered options

- Retry everything until the stalled-watermark timeout gives up
- Classify as retryable unless provably permanent, and fail fast on the
  permanent set
- Never retry; fail the batch and let restart replay it

## Decision outcome

Chosen option: "Classify as retryable unless provably permanent", because
at-least-once means preferring replay to loss, and the default has to lean that
way — but the permanent cases have to be carved out or the pipeline hides a
clear failure behind a timeout.

Authorization failures, unknown topic, and fenced idempotent-producer states are
fatal: they will not resolve by trying again, and failing fast reports the actual
problem. Everything else retries.

The stance is stated explicitly for one case rather than left implicit:
`NotEnoughReplicasAfterAppend` means the append happened but the replication
requirement was not met. Retrying it **knowingly duplicates** — the data is
already there. That is the correct trade under at-least-once, and it is recorded
here so the duplicate is understood as a decision rather than found later as a
surprise.

Never retrying was rejected because it converts every transient broker hiccup
into a pipeline restart.

### Consequences

- Good, because a transient failure costs a retry rather than a restart.
- Good, because a fenced producer fails immediately with the real error instead
  of spinning to a timeout.
- Bad, because whole-batch retry re-produces any delivered prefix. That is a
  documented duplicate window alongside crash replay, and it is wider than the
  ClickHouse sink's because no token suppresses it.
- Bad, because the classification is a list, and a broker error code not on it
  is treated as retryable — the safe direction, but it means a new permanent
  failure mode presents as a spin until somebody adds it.

### Confirmation

Nothing automated; the classification is a match arm reviewed against the
librdkafka error set. The stalled-watermark timeout is the backstop that keeps a
misclassification bounded rather than infinite.

## More information

- Landed in `6af6861` (#24).
- [ADR-0002](0002-at-least-once-delivery.md) — the guarantee that makes replay
  the preferred direction.
- [Kafka sink](../user-guide/04-connectors/sinks/kafka/README.mdx) — the
  duplicate window as an operator needs to understand it.
